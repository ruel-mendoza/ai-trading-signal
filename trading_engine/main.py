import os
import time
import logging
import threading
import traceback
from datetime import datetime
from functools import wraps
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from contextlib import asynccontextmanager
import pytz
import pandas as pd

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED

ET_ZONE = pytz.timezone("America/New_York")

MISFIRE_GRACE_SECONDS = 120
MAX_RETRIES = 2
RETRY_DELAY_SECONDS = 5
WATCHDOG_INTERVAL_SECONDS = 300

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.INFO)
logger = logging.getLogger("trading_engine")

from trading_engine.database import (
    init_db, get_candles, get_candle_count, get_all_signals, get_active_signals,
    create_job_log, finish_job_log, get_scheduler_health_summary,
    compute_signal_metrics,
    upsert_strategy_execution_log, get_last_successful_execution,
)
from trading_engine.models import VALID_TIMEFRAMES
from trading_engine.fcsapi_client import FCSAPIClient
from trading_engine.cache_layer import CacheLayer
from trading_engine.indicators import IndicatorEngine
from trading_engine.strategy_engine import StrategyEngine
from trading_engine.admin import router as admin_router
from trading_engine.api_v1 import router as api_v1_router
from trading_engine.api.v1.public_signals import router as public_signals_router
from trading_engine.api.v1.auth import router as auth_router
from trading_engine.strategies.trend_forex import TARGET_SYMBOLS as TREND_FOREX_SYMBOLS
from trading_engine.strategies.trend_non_forex import TARGET_SYMBOLS as TREND_NON_FOREX_SYMBOLS
from trading_engine.strategies.multi_timeframe import ALL_ASSETS as MTF_EMA_ASSETS
from trading_engine.notifications import (
    notify_strategy_failure, notify_scheduler_down, configure_webhook,
    set_notifications_enabled, set_category_enabled,
)

scheduler = BackgroundScheduler()

_watchdog_stop = threading.Event()
_scheduler_heartbeat = {"last_tick": None}

init_db()

api_client = FCSAPIClient()
cache = CacheLayer(api_client)
strategy_engine = StrategyEngine(cache)


def _get_et_context() -> dict:
    now_et = datetime.now(pytz.utc).astimezone(ET_ZONE)
    is_dst = bool(now_et.dst() and now_et.dst().total_seconds() > 0)
    return {
        "now": now_et,
        "label": "EDT" if is_dst else "EST",
        "dst": is_dst,
        "time_str": now_et.strftime("%Y-%m-%d %H:%M:%S"),
    }


def _retry_asset_eval(func, asset, max_retries=MAX_RETRIES, delay=RETRY_DELAY_SECONDS):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            return func(asset), None
        except Exception as e:
            last_err = e
            if attempt < max_retries:
                logger.warning(
                    f"[SCHEDULER] Retry {attempt}/{max_retries} for {asset} "
                    f"after error: {e} — waiting {delay}s"
                )
                time.sleep(delay)
            else:
                logger.error(
                    f"[SCHEDULER] All {max_retries} attempts failed for {asset}: {e}"
                )
    return None, last_err


def _scheduler_event_listener(event):
    if event.exception:
        logger.error(
            f"[SCHEDULER-WATCHDOG] Job {event.job_id} raised exception: {event.exception}",
            exc_info=event.exception,
        )


def _scheduler_missed_listener(event):
    logger.warning(
        f"[SCHEDULER-WATCHDOG] Job {event.job_id} MISSED its scheduled run "
        f"(misfire_grace_time={MISFIRE_GRACE_SECONDS}s)"
    )


def _watchdog_thread():
    logger.info("[SCHEDULER-WATCHDOG] Watchdog thread started")
    while not _watchdog_stop.is_set():
        _watchdog_stop.wait(WATCHDOG_INTERVAL_SECONDS)
        if _watchdog_stop.is_set():
            break
        try:
            _scheduler_heartbeat["last_tick"] = datetime.utcnow().isoformat()
            try:
                running = scheduler.running
                jobs = scheduler.get_jobs()
            except Exception:
                running = False
                jobs = []
            if not running:
                logger.critical(
                    "[SCHEDULER-WATCHDOG] Scheduler is NOT running — attempting restart"
                )
                try:
                    if hasattr(scheduler, '_event') and scheduler._event is not None:
                        scheduler.start()
                        logger.info("[SCHEDULER-WATCHDOG] Scheduler restarted successfully")
                        jobs = scheduler.get_jobs()
                        if not jobs:
                            logger.warning(
                                "[SCHEDULER-WATCHDOG] Scheduler restarted but has NO jobs — "
                                "manual intervention may be needed"
                            )
                        notify_scheduler_down(restart_attempted=True, restart_success=True)
                    else:
                        logger.critical(
                            "[SCHEDULER-WATCHDOG] Scheduler in unrecoverable state — "
                            "cannot restart (already shut down)"
                        )
                        notify_scheduler_down(restart_attempted=True, restart_success=False)
                except Exception as restart_err:
                    logger.critical(
                        f"[SCHEDULER-WATCHDOG] Scheduler restart FAILED: {restart_err}"
                    )
                    notify_scheduler_down(restart_attempted=True, restart_success=False)
            else:
                logger.debug(
                    f"[SCHEDULER-WATCHDOG] Heartbeat OK — {len(jobs)} jobs registered, "
                    f"scheduler running"
                )
        except Exception as e:
            logger.error(f"[SCHEDULER-WATCHDOG] Watchdog check error: {e}")
    logger.info("[SCHEDULER-WATCHDOG] Watchdog thread stopped")


def _scheduled_trend_forex_evaluate():
    from trading_engine.database import get_open_position
    from trading_engine.strategies.trend_forex import TIMEFRAME as TF_TIMEFRAME

    et = _get_et_context()
    log_id = create_job_log("trend_forex_daily", "trend_forex")
    logger.info(f"[SCHEDULER] ====== Triggered trend_forex daily evaluation at 4:58 PM ET | {et['time_str']} {et['label']} ======")

    v3_prices = {}
    try:
        v3_prices = cache.api_client.get_v3_latest_prices(list(TREND_FOREX_SYMBOLS))
        logger.info(f"[SCHEDULER] trend_forex | v3 batch fetch: {len(v3_prices)}/{len(TREND_FOREX_SYMBOLS)} prices (1 credit)")
    except Exception as e:
        logger.warning(f"[SCHEDULER] trend_forex | v3 batch fetch failed: {e} — will fall back to v4 per-asset")

    assets_eval = 0
    signals_gen = 0
    error_count = 0
    error_details = []

    for asset in TREND_FOREX_SYMBOLS:
        assets_eval += 1
        def _eval(a):
            candles = cache.get_candles(a, TF_TIMEFRAME, 300)
            if not candles:
                logger.warning(f"[SCHEDULER] trend_forex | {a} | No candles available")
                return None
            df = pd.DataFrame(candles)
            open_pos = get_open_position("trend_forex", a)
            batch_price = v3_prices.get(a)
            return strategy_engine.trend_forex_strategy.evaluate(a, TF_TIMEFRAME, df, open_pos, batch_price=batch_price)

        result, err = _retry_asset_eval(_eval, asset)
        if err:
            error_count += 1
            error_details.append(f"{asset}: {err}")
        elif result and result.is_entry:
            signals_gen += 1
            signal = result.metadata.get("signal", {})
            logger.info(f"[SCHEDULER] trend_forex | {asset} | NEW SIGNAL generated: {signal.get('direction', '')} id={signal.get('id')}")
        else:
            logger.info(f"[SCHEDULER] trend_forex | {asset} | No signal triggered")

    try:
        exits = strategy_engine.trend_forex_strategy.check_exits()
        if exits:
            logger.info(f"[SCHEDULER] trend_forex | {len(exits)} exit(s) triggered")
        else:
            logger.info("[SCHEDULER] trend_forex | No exits triggered")
    except Exception as e:
        error_count += 1
        error_details.append(f"exit_check: {e}")
        logger.error(f"[SCHEDULER] trend_forex | Exit check exception: {e}")

    asset_errors = sum(1 for d in error_details if not d.startswith("exit_check"))
    status = "FAILED" if asset_errors == assets_eval else ("PARTIAL" if error_count > 0 else "SUCCESS")
    finish_job_log(log_id, status, assets_eval, signals_gen, error_count,
                   "; ".join(error_details) if error_details else None)
    upsert_strategy_execution_log("trend_forex", status)
    if status in ("FAILED", "PARTIAL"):
        notify_strategy_failure("trend_forex", error_count, assets_eval, "; ".join(error_details))
    logger.info(f"[SCHEDULER] ====== trend_forex complete | {status} | {assets_eval} assets, {signals_gen} signals, {error_count} errors ======")


def _scheduled_trend_non_forex_evaluate():
    from trading_engine.database import get_open_position
    from trading_engine.strategies.trend_non_forex import TIMEFRAME as TNF_TIMEFRAME

    et = _get_et_context()
    log_id = create_job_log("trend_non_forex_daily", "trend_non_forex")
    logger.info(
        f"[SCHEDULER] ====== Triggered trend_non_forex daily evaluation at 4:59 PM ET | "
        f"{et['time_str']} {et['label']} | DST={'active' if et['dst'] else 'inactive'} ======"
    )

    assets_eval = 0
    signals_gen = 0
    error_count = 0
    error_details = []

    try:
        strategy_engine.trend_non_forex_strategy.prefetch_prices()
    except Exception as e:
        logger.error(f"[SCHEDULER] trend_non_forex | Batch price prefetch failed: {e}")

    for asset in TREND_NON_FOREX_SYMBOLS:
        assets_eval += 1
        def _eval(a):
            candles = cache.get_candles(a, TNF_TIMEFRAME, 300)
            if not candles:
                logger.warning(f"[SCHEDULER] trend_non_forex | {a} | No candles available")
                return None
            df = pd.DataFrame(candles)
            open_pos = get_open_position("trend_non_forex", a)
            return strategy_engine.trend_non_forex_strategy.evaluate(a, TNF_TIMEFRAME, df, open_pos)

        result, err = _retry_asset_eval(_eval, asset)
        if err:
            error_count += 1
            error_details.append(f"{asset}: {err}")
        elif result and result.is_entry:
            signals_gen += 1
            signal = result.metadata.get("signal", {})
            logger.info(f"[SCHEDULER] trend_non_forex | {asset} | NEW SIGNAL generated: {signal.get('direction', '')} id={signal.get('id')}")
        else:
            logger.info(f"[SCHEDULER] trend_non_forex | {asset} | No signal triggered")

    try:
        exits = strategy_engine.trend_non_forex_strategy.check_exits()
        if exits:
            logger.info(f"[SCHEDULER] trend_non_forex | {len(exits)} exit(s) triggered")
        else:
            logger.info("[SCHEDULER] trend_non_forex | No exits triggered")
    except Exception as e:
        error_count += 1
        error_details.append(f"exit_check: {e}")
        logger.error(f"[SCHEDULER] trend_non_forex | Exit check exception: {e}")

    asset_errors = sum(1 for d in error_details if not d.startswith("exit_check"))
    status = "FAILED" if asset_errors == assets_eval else ("PARTIAL" if error_count > 0 else "SUCCESS")
    finish_job_log(log_id, status, assets_eval, signals_gen, error_count,
                   "; ".join(error_details) if error_details else None)
    upsert_strategy_execution_log("trend_non_forex", status)
    if status in ("FAILED", "PARTIAL"):
        notify_strategy_failure("trend_non_forex", error_count, assets_eval, "; ".join(error_details))
    logger.info(f"[SCHEDULER] ====== trend_non_forex complete | {status} | {assets_eval} assets, {signals_gen} signals, {error_count} errors ======")


def _scheduled_highest_lowest_fx():
    from trading_engine.database import get_open_position as db_get_open_pos
    from trading_engine.utils.holiday_manager import is_trading_holiday

    et = _get_et_context()
    logger.info(
        f"[SCHEDULER] ====== highest_lowest_fx hourly tick | {et['time_str']} {et['label']} ======"
    )

    if is_trading_holiday(et["now"]):
        logger.info("[SCHEDULER] highest_lowest_fx | Trading holiday — skipping")
        return

    if et["now"].hour not in (9, 10):
        logger.info(
            f"[SCHEDULER] highest_lowest_fx | ET hour {et['now'].hour}:00 not in (9, 10) — skipping"
        )
        return

    log_id = create_job_log("highest_lowest_fx_hourly", "highest_lowest_fx")
    asset = "EUR/USD"
    signals_gen = 0
    error_count = 0
    error_details = []

    def _eval(a):
        candles = get_candles(a, "1H", 300)
        hlc_df = pd.DataFrame(candles) if candles else pd.DataFrame()
        open_pos = db_get_open_pos("highest_lowest_fx", a)
        return strategy_engine.highest_lowest_strategy.evaluate(a, "1H", hlc_df, open_pos)

    result, err = _retry_asset_eval(_eval, asset)
    if err:
        error_count += 1
        error_details.append(f"{asset}: {err}")
    elif result and result.is_entry:
        signals_gen += 1
        signal = result.metadata.get("signal", {})
        logger.info(
            f"[SCHEDULER] highest_lowest_fx | {asset} | NEW SIGNAL: "
            f"{signal.get('direction', '')} @ {signal.get('entry_price', 0):.5f}"
        )
    else:
        logger.info(f"[SCHEDULER] highest_lowest_fx | {asset} | No signal triggered")

    try:
        exits = strategy_engine.highest_lowest_strategy.check_exits()
        if exits:
            logger.info(f"[SCHEDULER] highest_lowest_fx | {len(exits)} exit(s) triggered")
        else:
            logger.info("[SCHEDULER] highest_lowest_fx | No exits triggered")
    except Exception as e:
        error_count += 1
        error_details.append(f"exit_check: {e}")
        logger.error(f"[SCHEDULER] highest_lowest_fx | Exit check exception: {e}")

    status = "FAILED" if error_count > 0 and signals_gen == 0 else ("PARTIAL" if error_count > 0 else "SUCCESS")
    finish_job_log(log_id, status, 1, signals_gen, error_count,
                   "; ".join(error_details) if error_details else None)
    if status in ("FAILED", "PARTIAL"):
        notify_strategy_failure("highest_lowest_fx", error_count, 1, "; ".join(error_details))
    logger.info(f"[SCHEDULER] ====== highest_lowest_fx complete | {status} ======")


def _scheduled_sp500_momentum_30m():
    et = _get_et_context()
    et_minutes = et["now"].hour * 60 + et["now"].minute
    arca_start = 9 * 60 + 30
    arca_end = 15 * 60 + 30

    logger.info(
        f"[SCHEDULER] ====== SP500 Momentum 30m tick | "
        f"{et['time_str']} {et['label']} | DST={'active' if et['dst'] else 'inactive'} ======"
    )

    if et_minutes < arca_start or et_minutes > arca_end:
        logger.info(
            f"[SCHEDULER] sp500_momentum | Outside ARCA session "
            f"({et['now'].strftime('%H:%M')} {et['label']} not in 09:30-15:30 ET) — skipping"
        )
        return

    log_id = create_job_log("sp500_momentum_30m", "sp500_momentum")
    signals_gen = 0
    error_count = 0
    error_details = []

    logger.info(
        f"[SCHEDULER] sp500_momentum | Inside ARCA session "
        f"({et['now'].strftime('%H:%M')} {et['label']}) — running intraday cycle"
    )

    try:
        result = strategy_engine.run_sp500_intraday_cycle("SPX")
        entry = result.get("entry")
        exits = result.get("exits", [])
        state_updated = result.get("state_updated", False)

        if entry:
            signals_gen += 1
            logger.info(
                f"[SCHEDULER] sp500_momentum | NEW SIGNAL: BUY @ {entry.get('entry_price', 0):.2f} | "
                f"atr_at_entry={entry.get('atr_at_entry', 0):.6f} | "
                f"stop={entry.get('stop_loss', 0):.2f} | id={entry.get('id')}"
            )
        if exits:
            for ex in exits:
                logger.info(
                    f"[SCHEDULER] sp500_momentum | EXIT: {ex.get('exit_reason')} @ {ex.get('exit_price', 0):.2f}"
                )
        if state_updated:
            logger.info("[SCHEDULER] sp500_momentum | Position state updated (peak tracking)")
        if not entry and not exits and not state_updated:
            logger.info("[SCHEDULER] sp500_momentum | No action taken this tick")
    except Exception as e:
        error_count += 1
        error_details.append(f"SPX: {e}")
        logger.error(f"[SCHEDULER] sp500_momentum | Exception: {e}", exc_info=True)

    status = "FAILED" if error_count > 0 else "SUCCESS"
    finish_job_log(log_id, status, 1, signals_gen, error_count,
                   "; ".join(error_details) if error_details else None)
    if status == "FAILED":
        notify_strategy_failure("sp500_momentum", error_count, 1, "; ".join(error_details))
    logger.info(f"[SCHEDULER] ====== SP500 Momentum 30m tick complete | {status} ======")


def _scheduled_mtf_ema_evaluate():
    from trading_engine.database import get_open_position as db_get_open_pos
    from trading_engine.strategies.multi_timeframe import PRIMARY_TIMEFRAME

    et = _get_et_context()
    log_id = create_job_log("mtf_ema_hourly", "mtf_ema")
    logger.info(
        f"[SCHEDULER] ====== MTF EMA hourly evaluation | "
        f"{et['time_str']} {et['label']} | DST={'active' if et['dst'] else 'inactive'} ======"
    )

    assets_eval = 0
    signals_gen = 0
    error_count = 0
    error_details = []

    for asset in MTF_EMA_ASSETS:
        assets_eval += 1
        def _eval(a):
            candles = cache.get_candles(a, PRIMARY_TIMEFRAME, 300)
            if not candles:
                logger.warning(f"[SCHEDULER] mtf_ema | {a} | No candles available for {PRIMARY_TIMEFRAME}")
                return None
            df = pd.DataFrame(candles)
            open_pos = db_get_open_pos("mtf_ema", a)
            return strategy_engine.mtf_ema_strategy.evaluate(a, PRIMARY_TIMEFRAME, df, open_pos)

        result, err = _retry_asset_eval(_eval, asset)
        if err:
            error_count += 1
            error_details.append(f"{asset}: {err}")
        elif result and (result.is_entry or result.is_exit):
            if result.is_entry:
                signals_gen += 1
                signal = result.metadata.get("signal", {})
                logger.info(f"[SCHEDULER] mtf_ema | {asset} | NEW SIGNAL: {signal.get('direction', '')} id={signal.get('id')}")
            if result.is_exit:
                logger.info(f"[SCHEDULER] mtf_ema | {asset} | EXIT triggered: {result.metadata.get('exit_reason', '')}")
        else:
            logger.info(f"[SCHEDULER] mtf_ema | {asset} | No action")

    status = "FAILED" if error_count == assets_eval else ("PARTIAL" if error_count > 0 else "SUCCESS")
    finish_job_log(log_id, status, assets_eval, signals_gen, error_count,
                   "; ".join(error_details) if error_details else None)
    if status in ("FAILED", "PARTIAL"):
        notify_strategy_failure("mtf_ema", error_count, assets_eval, "; ".join(error_details))
    logger.info(
        f"[SCHEDULER] ====== MTF EMA complete | {status} | "
        f"{assets_eval} assets, {signals_gen} signals, {error_count} errors ======"
    )


def _run_metrics_worker():
    import time as _time
    t0 = _time.monotonic()
    try:
        rows = compute_signal_metrics()
        elapsed = round(_time.monotonic() - t0, 2)
        logger.info(f"[METRICS] Worker completed: {rows} metric rows in {elapsed}s")
    except Exception as e:
        logger.error(f"[METRICS] Worker failed: {e}")


def _run_daily_backup():
    from trading_engine.utils.backup_manager import run_daily_backup
    run_daily_backup()


RECOVERY_MAX_HOURS = 4

RECOVERY_STRATEGIES = [
    {
        "name": "trend_forex",
        "scheduled_hour": 16,
        "scheduled_minute": 58,
        "run_func_name": "_scheduled_trend_forex_evaluate",
    },
    {
        "name": "trend_non_forex",
        "scheduled_hour": 16,
        "scheduled_minute": 59,
        "run_func_name": "_scheduled_trend_non_forex_evaluate",
    },
]


def _get_recovery_assets(strategy_name: str) -> list[str]:
    if strategy_name == "trend_forex":
        return list(TREND_FOREX_SYMBOLS)
    elif strategy_name == "trend_non_forex":
        return list(TREND_NON_FOREX_SYMBOLS)
    return []


def _check_existing_signals_for_window(strategy_name: str, window_timestamp: str) -> dict:
    from trading_engine.database import signal_exists, has_open_signal

    assets = _get_recovery_assets(strategy_name)
    results = {}
    for asset in assets:
        existing_at_window = signal_exists(strategy_name, asset, window_timestamp)
        has_open = has_open_signal(strategy_name, asset)

        if existing_at_window:
            logger.info(
                f"[RECOVERY] [SKIP] Signal for {asset} at "
                f"{window_timestamp} already exists. No data overwritten."
            )
            results[asset] = "duplicate_at_window"
        elif has_open:
            logger.info(
                f"[RECOVERY] [SKIP] {asset} has an existing OPEN signal for "
                f"strategy={strategy_name}. No duplicate entry created."
            )
            results[asset] = "open_signal_exists"
        else:
            results[asset] = "eligible"
    return results


def recovery_check():
    from trading_engine.utils.holiday_manager import is_trading_holiday

    et = _get_et_context()
    now_et = et["now"]
    today_date = now_et.date()

    logger.info(
        f"[RECOVERY] ====== Startup recovery check | "
        f"{et['time_str']} {et['label']} | "
        f"max_catchup_hours={RECOVERY_MAX_HOURS} ======"
    )

    if now_et.weekday() >= 5:
        logger.info("[RECOVERY] [SKIP] Weekend — no D1 candles expected, skipping all strategies")
        return

    if is_trading_holiday(today_date):
        logger.info("[RECOVERY] [SKIP] Trading holiday — markets closed, skipping all strategies")
        return

    run_funcs = {
        "_scheduled_trend_forex_evaluate": _scheduled_trend_forex_evaluate,
        "_scheduled_trend_non_forex_evaluate": _scheduled_trend_non_forex_evaluate,
    }

    recovered = 0
    for strat in RECOVERY_STRATEGIES:
        strategy_name = strat["name"]
        sched_hour = strat["scheduled_hour"]
        sched_minute = strat["scheduled_minute"]
        func = run_funcs[strat["run_func_name"]]

        scheduled_time_today = now_et.replace(
            hour=sched_hour, minute=sched_minute, second=0, microsecond=0
        )

        if now_et < scheduled_time_today:
            logger.info(
                f"[RECOVERY] [SKIP] {strategy_name} | "
                f"Scheduled at {sched_hour:02d}:{sched_minute:02d} ET — "
                f"current time is before window, skipping"
            )
            continue

        hours_past = (now_et - scheduled_time_today).total_seconds() / 3600.0
        if hours_past > RECOVERY_MAX_HOURS:
            logger.info(
                f"[RECOVERY] [SKIP] {strategy_name} | "
                f"{hours_past:.1f}h past scheduled window (max {RECOVERY_MAX_HOURS}h) — "
                f"D1 candle data too stale for reliable signals, skipping"
            )
            continue

        last_exec = get_last_successful_execution(strategy_name)
        if last_exec:
            try:
                last_run_dt = datetime.fromisoformat(last_exec["last_run_at"])
                last_run_date = last_run_dt.date()
            except (ValueError, TypeError):
                last_run_date = None

            if last_run_date == today_date:
                logger.info(
                    f"[RECOVERY] [SKIP] {strategy_name} | "
                    f"Already ran successfully today ({last_exec['last_run_at']}) — "
                    f"no recovery needed, skipping"
                )
                continue

            logger.info(
                f"[RECOVERY] {strategy_name} | "
                f"Last successful run: {last_exec['last_run_at']} (not today) | "
                f"{hours_past:.1f}h past window — evaluating for catch-up"
            )
        else:
            logger.info(
                f"[RECOVERY] {strategy_name} | "
                f"No previous successful run found | "
                f"{hours_past:.1f}h past window — evaluating for catch-up"
            )

        window_timestamp = scheduled_time_today.strftime("%Y-%m-%dT%H:%M:%S")
        signal_audit = _check_existing_signals_for_window(strategy_name, window_timestamp)

        eligible_count = sum(1 for s in signal_audit.values() if s == "eligible")
        skipped_count = len(signal_audit) - eligible_count

        logger.info(
            f"[RECOVERY] {strategy_name} | Pre-flight duplicate check: "
            f"{eligible_count} eligible, {skipped_count} skipped "
            f"(duplicates/open signals already exist)"
        )

        if eligible_count == 0:
            logger.info(
                f"[RECOVERY] [SKIP] {strategy_name} | "
                f"All {len(signal_audit)} assets already have signals at this window — "
                f"nothing to recover"
            )
            continue

        try:
            logger.info(
                f"[RECOVERY] {strategy_name} | "
                f"Running catch-up for {eligible_count} eligible asset(s) "
                f"(strategy has built-in idempotency guards + DB unique constraints)..."
            )
            func()

            post_audit = _check_existing_signals_for_window(strategy_name, window_timestamp)
            new_signals = sum(
                1 for a, s in post_audit.items()
                if s == "duplicate_at_window" and signal_audit.get(a) == "eligible"
            )
            affected_assets = [
                a for a, s in post_audit.items()
                if s == "duplicate_at_window" and signal_audit.get(a) == "eligible"
            ]

            recovered += 1
            logger.info(
                f"[RECOVERY] {strategy_name} | "
                f"Catch-up execution completed — "
                f"{new_signals} new signal(s) created, "
                f"{skipped_count} pre-existing signal(s) preserved"
            )

            from trading_engine.services.notification_service import log_recovery_event
            log_recovery_event(
                strategy_name=strategy_name,
                missed_window_time=window_timestamp,
                assets_affected=affected_assets if affected_assets else _get_recovery_assets(strategy_name),
                status="SUCCESS",
            )
        except Exception as e:
            logger.error(
                f"[RECOVERY] {strategy_name} | "
                f"Catch-up execution FAILED. Strategy functions use atomic "
                f"per-signal transactions — no partial signals written. Error: {e}",
                exc_info=True,
            )

            from trading_engine.services.notification_service import log_recovery_event
            log_recovery_event(
                strategy_name=strategy_name,
                missed_window_time=window_timestamp,
                assets_affected=_get_recovery_assets(strategy_name),
                status="FAILED",
            )

    logger.info(
        f"[RECOVERY] ====== Recovery check complete | "
        f"{recovered} strategy(ies) recovered ======"
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    from trading_engine.database import get_setting
    saved_webhook = get_setting("webhook_url")
    if saved_webhook:
        configure_webhook(saved_webhook)
        logger.info("[NOTIFY] Webhook URL loaded from database")

    notif_enabled = get_setting("notifications_enabled")
    if notif_enabled is not None:
        set_notifications_enabled(notif_enabled == "true")

    import json as _json
    notif_categories = get_setting("notification_categories")
    if notif_categories:
        try:
            cats = _json.loads(notif_categories)
            for cat_key, cat_val in cats.items():
                set_category_enabled(cat_key, bool(cat_val))
        except Exception:
            pass

    scheduler.add_listener(_scheduler_event_listener, EVENT_JOB_ERROR)
    scheduler.add_listener(_scheduler_missed_listener, EVENT_JOB_MISSED)

    scheduler.add_job(
        _scheduled_trend_forex_evaluate,
        trigger=CronTrigger(hour=16, minute=58, timezone=ET_ZONE),
        id="trend_forex_daily",
        name="Forex Trend Daily Evaluation (4:58 PM ET)",
        replace_existing=True,
        misfire_grace_time=MISFIRE_GRACE_SECONDS,
    )
    scheduler.add_job(
        _scheduled_trend_non_forex_evaluate,
        trigger=CronTrigger(hour=16, minute=59, timezone=ET_ZONE),
        id="trend_non_forex_daily",
        name="Non-Forex Trend Daily Evaluation (4:59 PM ET)",
        replace_existing=True,
        misfire_grace_time=MISFIRE_GRACE_SECONDS,
    )
    scheduler.add_job(
        _scheduled_sp500_momentum_30m,
        trigger=CronTrigger(minute="0,30", timezone=ET_ZONE),
        id="sp500_momentum_30m",
        name="SP500 Momentum 30m Evaluation (:00 and :30 ET)",
        replace_existing=True,
        misfire_grace_time=MISFIRE_GRACE_SECONDS,
    )
    scheduler.add_job(
        _scheduled_highest_lowest_fx,
        trigger=CronTrigger(hour="9,10", minute=0, timezone=ET_ZONE),
        id="highest_lowest_fx_hourly",
        name="Highest/Lowest FX Evaluation (9:00 & 10:00 AM ET)",
        replace_existing=True,
        misfire_grace_time=MISFIRE_GRACE_SECONDS,
    )
    scheduler.add_job(
        _scheduled_mtf_ema_evaluate,
        trigger=CronTrigger(minute=0, timezone=ET_ZONE),
        id="mtf_ema_hourly",
        name="MTF EMA Hourly Evaluation (every hour ET)",
        replace_existing=True,
        misfire_grace_time=MISFIRE_GRACE_SECONDS,
    )
    scheduler.add_job(
        _run_metrics_worker,
        trigger=CronTrigger(minute="*/5", timezone=ET_ZONE),
        id="signal_metrics_worker",
        name="Signal Metrics Worker (every 5 minutes)",
        replace_existing=True,
        misfire_grace_time=MISFIRE_GRACE_SECONDS,
    )
    scheduler.add_job(
        _run_daily_backup,
        trigger=CronTrigger(hour=1, minute=0, timezone=ET_ZONE),
        id="daily_backup",
        name="Daily Database Backup (1:00 AM ET)",
        replace_existing=True,
        misfire_grace_time=MISFIRE_GRACE_SECONDS,
    )
    scheduler.start()

    import asyncio as _asyncio
    from trading_engine.engine.watchdog import start_price_watchdog
    _asyncio.create_task(start_price_watchdog())
    from trading_engine.websocket import broadcaster as _ws_broadcaster
    _ws_broadcaster.set_loop(_asyncio.get_running_loop())
    logger.info("[WS] Signal broadcaster initialized")

    try:
        compute_signal_metrics()
        logger.info("[METRICS] Initial metrics computation completed on startup")
    except Exception as e:
        logger.warning(f"[METRICS] Initial computation failed: {e}")

    try:
        recovery_check()
    except Exception as e:
        logger.error(f"[RECOVERY] Startup recovery check failed: {e}", exc_info=True)

    watchdog = threading.Thread(target=_watchdog_thread, daemon=True, name="scheduler-watchdog")
    watchdog.start()

    et = _get_et_context()
    logger.info(
        f"[SCHEDULER] APScheduler started with {len(scheduler.get_jobs())} jobs | "
        f"mtf_ema every hour (:00), sp500_momentum every 30m (:00/:30), "
        f"highest_lowest_fx at 09:00 & 10:00, "
        f"trend_non_forex at 16:59, trend_forex at 16:58 | "
        f"metrics worker every 5m | "
        f"misfire_grace={MISFIRE_GRACE_SECONDS}s | "
        f"watchdog interval={WATCHDOG_INTERVAL_SECONDS}s | "
        f"America/New_York ({et['label']}, DST={'active' if et['dst'] else 'inactive'})"
    )
    yield
    _watchdog_stop.set()
    scheduler.shutdown(wait=False)
    logger.info("[SCHEDULER] APScheduler + watchdog shut down")


API_TAGS_METADATA = [
    {
        "name": "Signals",
        "description": "Retrieve trading signals — active, historical, and per-asset. All data is read from the local SQLite database with no external API calls.",
    },
    {
        "name": "Market Data",
        "description": "OHLC candle data and computed technical indicators (SMA, EMA, RSI, ATR) for supported assets and timeframes.",
    },
    {
        "name": "Positions",
        "description": "Open position tracking with trailing-stop metadata (ATR at entry, highest/lowest price since entry).",
    },
    {
        "name": "Metrics",
        "description": "Signal performance analytics — win rate, gain/loss averages, and per-strategy breakdowns. Computed every 5 minutes by a background worker.",
    },
    {
        "name": "Strategies",
        "description": "Strategy summary with open/closed signal counts for each registered trading strategy.",
    },
    {
        "name": "Scheduler",
        "description": "APScheduler health monitoring — 24h success/failure counts and recent job execution logs.",
    },
    {
        "name": "Health",
        "description": "API liveness and readiness checks. The public health endpoint exposes no internal metadata.",
    },
    {
        "name": "State Management",
        "description": "Cache control operations. Flush the 4-shard TTLCache to force fresh data on subsequent requests.",
    },
    {
        "name": "Public Signals",
        "description": "Hardened, read-only signal and asset endpoints with strict Pydantic schemas. No internal fields are exposed. POST/PUT/DELETE/PATCH requests are blocked.",
    },
]

app = FastAPI(
    title="DailyForex Premium API",
    version="1.0.0",
    description=(
        "**DailyForex Premium** — Read-only trading signals API for forex, crypto, commodities, and indices.\n\n"
        "All data is served from a local SQLite database with a 4-shard TTLCache (60s default). "
        "No external API calls are made on request — OHLC data and signals are pre-computed by a background scheduler.\n\n"
        "**Credit Limit:** The platform operates under a 500,000 FCSAPI credit cap. "
        "A kill switch automatically halts outbound data fetches at 495,000 credits to prevent overages.\n\n"
        "**Internal Use Only:** This API is designed for the DailyForex frontend and is not intended for public redistribution.\n\n"
        "**Strategies:** MTF EMA (multi-timeframe), Trend Following (forex & non-forex), SP500 Momentum, Highest/Lowest Close FX.\n\n"
        "**Assets:** EUR/USD, GBP/USD, USD/JPY, AUD/USD, BTC/USD, ETH/USD, XAU/USD, XAG/USD, SPX, NDX, RUT, and more."
    ),
    openapi_tags=API_TAGS_METADATA,
    lifespan=lifespan,
)

import os as _os

_allowed_origins = [
    origin.strip()
    for origin in _os.environ.get("CORS_ALLOWED_ORIGINS", "").split(",")
    if origin.strip()
]
if not _allowed_origins:
    _replit_slug = _os.environ.get("REPL_SLUG", "")
    _replit_owner = _os.environ.get("REPL_OWNER", "")
    _replit_id = _os.environ.get("REPL_ID", "")
    _allowed_origins = [
        "https://dailyforex.com",
        "https://www.dailyforex.com",
    ]
    if _replit_slug and _replit_owner:
        _allowed_origins.append(f"https://{_replit_slug}.{_replit_owner}.repl.co")
    if _replit_id:
        _allowed_origins.append(f"https://{_replit_id}.id.repl.co")
    if _replit_slug:
        _allowed_origins.append(f"https://{_replit_slug}-00-{_replit_owner}.replit.dev" if _replit_owner else "")
    _allowed_origins.append("http://localhost:5000")
    _allowed_origins.append("http://localhost:5001")
    _allowed_origins.append("http://127.0.0.1:5000")
    _allowed_origins.append("http://127.0.0.1:5001")
    _allowed_origins = [o for o in _allowed_origins if o]

if "*" in _allowed_origins:
    _allowed_origins.remove("*")
    logger.warning("[CORS] Wildcard (*) origin removed — explicit origins required")

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_origin_regex=r"^https://([a-zA-Z0-9-]+\.)*dailyforex\.com$",
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "Cookie", "X-API-KEY"],
    expose_headers=["X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"],
)

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["60/minute"],
    application_limits=["1000/hour"],
    strategy="fixed-window",
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

from trading_engine.security_middleware import SecurityMiddleware, SecurityHeadersMiddleware, PayloadLimitMiddleware
app.add_middleware(SecurityMiddleware)
app.add_middleware(PayloadLimitMiddleware)
app.add_middleware(SecurityHeadersMiddleware)

from starlette.requests import Request as StarletteRequest
from fastapi.responses import JSONResponse as FastAPIJSONResponse
import traceback as tb_module

@app.exception_handler(Exception)
async def global_exception_handler(request: StarletteRequest, exc: Exception):
    logger.error(
        f"[UNHANDLED] {request.method} {request.url.path} — "
        f"{type(exc).__name__}: {exc}\n{tb_module.format_exc()}"
    )
    return FastAPIJSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "type": type(exc).__name__,
        },
    )

app.include_router(admin_router)
app.include_router(api_v1_router)
app.include_router(public_signals_router)
app.include_router(auth_router)


@app.websocket("/ws/signals")
async def websocket_signals(ws: WebSocket):
    from trading_engine.websocket import broadcaster
    await broadcaster.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await broadcaster.disconnect(ws)


@app.get("/health", include_in_schema=False)
def health_endpoint():
    from trading_engine.database import get_scheduler_health_summary as _health_summary, _get_session
    db_ok = False
    try:
        with _get_session() as session:
            session.execute(__import__('sqlalchemy').text("SELECT 1"))
            db_ok = True
    except Exception:
        pass

    scheduler_running = False
    job_count = 0
    try:
        scheduler_running = scheduler.running
        job_count = len(scheduler.get_jobs())
    except Exception:
        pass

    health_data = _health_summary()

    status = "healthy"
    checks_failed = []
    if not scheduler_running:
        status = "degraded"
        checks_failed.append("scheduler_stopped")
    if not db_ok:
        status = "degraded"
        checks_failed.append("database_error")

    ws_clients = 0
    try:
        from trading_engine.websocket import broadcaster as _ws_b
        ws_clients = _ws_b.client_count
    except Exception:
        pass

    return {
        "status": status,
        "checks_failed": checks_failed,
        "scheduler": {
            "running": scheduler_running,
            "jobs_registered": job_count,
        },
        "database": {
            "connected": db_ok,
        },
        "last_24h": {
            "success": health_data.get("last_24h_success", 0),
            "failures": health_data.get("last_24h_failures", 0),
        },
        "watchdog": {
            "last_heartbeat": _scheduler_heartbeat.get("last_tick"),
        },
        "websocket": {
            "clients": ws_clients,
        },
        "api_key_configured": bool(api_client.api_key),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


class CandleResponse(BaseModel):
    asset: str
    timeframe: str
    candle_count: int
    candles: list[dict]


class IndicatorResponse(BaseModel):
    asset: str
    timeframe: str
    latest: dict
    series: Optional[dict] = None


class StatusResponse(BaseModel):
    status: str
    api_key_configured: bool
    database: str
    timeframes: list[str]


class RefreshResponse(BaseModel):
    asset: str
    timeframe: str
    candles_stored: int
    message: str


@app.get("/", response_model=StatusResponse, include_in_schema=False)
def health_check():
    return StatusResponse(
        status="running",
        api_key_configured=bool(api_client.api_key),
        database="SQLite",
        timeframes=VALID_TIMEFRAMES,
    )


@app.get("/api/candles", response_model=CandleResponse, include_in_schema=False)
def get_candle_data(
    symbol: str = Query(..., description="Trading pair symbol, e.g. EUR/USD"),
    timeframe: str = Query(..., description="Timeframe: 30m, 1H, 4H, or D1"),
    limit: int = Query(300, ge=1, le=1000, description="Number of candles to return"),
):
    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Must be one of: {VALID_TIMEFRAMES}",
        )

    candles = cache.get_candles(symbol, timeframe, limit)

    return CandleResponse(
        asset=symbol,
        timeframe=timeframe,
        candle_count=len(candles),
        candles=candles,
    )


@app.get("/api/indicators", response_model=IndicatorResponse, include_in_schema=False)
def get_indicators(
    symbol: str = Query(..., description="Trading pair symbol, e.g. EUR/USD"),
    timeframe: str = Query(..., description="Timeframe: 30m, 1H, 4H, or D1"),
    include_series: bool = Query(False, description="Include full indicator series"),
    limit: int = Query(300, ge=1, le=1000, description="Number of candles to calculate from"),
):
    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Must be one of: {VALID_TIMEFRAMES}",
        )

    candles = cache.get_candles(symbol, timeframe, limit)

    if not candles:
        raise HTTPException(
            status_code=404,
            detail=f"No candle data available for {symbol} on {timeframe}",
        )

    latest = IndicatorEngine.get_latest(candles)

    series = None
    if include_series:
        series = IndicatorEngine.calculate_all(candles)

    return IndicatorResponse(
        asset=symbol,
        timeframe=timeframe,
        latest=latest,
        series=series,
    )


@app.post("/api/candles/refresh", response_model=RefreshResponse, include_in_schema=False)
def refresh_candles(
    symbol: str = Query(..., description="Trading pair symbol, e.g. EUR/USD"),
    timeframe: str = Query(..., description="Timeframe: 30m, 1H, 4H, or D1"),
    limit: int = Query(300, ge=1, le=1000),
):
    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Must be one of: {VALID_TIMEFRAMES}",
        )

    cache.force_refresh(symbol, timeframe, limit)
    count = get_candle_count(symbol, timeframe)

    return RefreshResponse(
        asset=symbol,
        timeframe=timeframe,
        candles_stored=count,
        message=f"Successfully refreshed {count} candles for {symbol} on {timeframe}",
    )


@app.get("/api/symbols", include_in_schema=False)
def list_symbols():
    try:
        symbols = api_client.get_available_symbols()
        return {"symbols": symbols, "count": len(symbols)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch symbols: {str(e)}")


@app.get("/api/cache/status", include_in_schema=False)
def cache_status(
    symbol: str = Query(..., description="Trading pair symbol"),
    timeframe: str = Query(..., description="Timeframe: 30m, 1H, 4H, or D1"),
):
    from trading_engine.database import get_cache_metadata

    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Must be one of: {VALID_TIMEFRAMES}",
        )

    meta = get_cache_metadata(symbol, timeframe)
    count = get_candle_count(symbol, timeframe)

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "candles_stored": count,
        "cache_metadata": meta,
        "should_fetch": cache._should_fetch(symbol, timeframe),
    }


@app.get("/api/credit-control/status", include_in_schema=False)
def credit_control_status():
    from trading_engine.credit_control import check_credit_thresholds, is_api_blocked
    projection = check_credit_thresholds()
    projection["api_blocked"] = is_api_blocked()
    return projection


@app.post("/api/credit-control/reset-kill-switch", include_in_schema=False)
def reset_credit_kill_switch():
    from trading_engine.credit_control import reset_kill_switch, is_api_blocked
    reset_kill_switch()
    return {"success": True, "api_blocked": is_api_blocked()}


VALID_QUOTE_PERIODS = {"30m", "1h", "4h", "1d", "1H", "4H", "D1"}


@app.get("/api/quotes", include_in_schema=False)
def get_quotes(
    symbols: str = Query(..., description="Comma-separated list of symbols, e.g. EUR/USD,SPX,BTC/USD"),
    period: str = Query("1h", description="Timeframe period: 30m, 1h, 4h, 1d"),
    merge: str = Query("latest,profile", description="Data to merge: latest, profile, tech, perf"),
):
    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        raise HTTPException(status_code=400, detail="At least one symbol is required")
    if len(symbol_list) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 symbols per request")
    if period not in VALID_QUOTE_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period '{period}'. Must be one of: 30m, 1h, 4h, 1d",
        )

    try:
        quotes = api_client.get_advance_data(symbol_list, period=period, merge=merge)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch quotes: {str(e)}")

    return {
        "quotes": quotes,
        "count": len(quotes),
        "requested": symbol_list,
    }


@app.post("/api/strategies/evaluate", include_in_schema=False)
def evaluate_strategies(
    symbols: Optional[str] = Query(None, description="Comma-separated list of symbols to evaluate"),
):
    symbol_list = [s.strip() for s in symbols.split(",")] if symbols else None
    new_signals = strategy_engine.evaluate_all(symbols=symbol_list)
    return {
        "new_signals": new_signals,
        "count": len(new_signals),
        "message": f"Evaluated all strategies, generated {len(new_signals)} new signal(s)",
    }


@app.post("/api/strategies/evaluate/{strategy_name}", include_in_schema=False)
def evaluate_single_strategy(
    strategy_name: str,
    symbol: str = Query(..., description="Symbol to evaluate"),
):
    result = None
    if strategy_name == "mtf_ema":
        from trading_engine.strategies.multi_timeframe import MultiTimeframeEMAStrategy
        from trading_engine.database import get_candles as db_get_candles_mtf, get_open_position as db_get_open_pos_mtf
        mtf_strat = MultiTimeframeEMAStrategy(strategy_engine.cache)
        h1_candles_mtf = db_get_candles_mtf(symbol, "1H", 300)
        mtf_df = pd.DataFrame(h1_candles_mtf) if h1_candles_mtf else pd.DataFrame()
        for col in ("open", "high", "low", "close"):
            if col in mtf_df.columns:
                mtf_df[col] = pd.to_numeric(mtf_df[col], errors="coerce")
        mtf_pos = db_get_open_pos_mtf("mtf_ema", symbol)
        sr = mtf_strat.evaluate(symbol, "1H", mtf_df, mtf_pos)
        result = sr.to_dict() if (sr.is_entry or sr.is_exit) else None
    elif strategy_name == "trend_following":
        result = strategy_engine.evaluate_trend_following(symbol)
    elif strategy_name == "sp500_momentum":
        result = strategy_engine.evaluate_sp500_momentum(symbol)
    elif strategy_name == "highest_lowest_fx":
        from trading_engine.database import get_candles as db_get_candles, get_open_position as db_get_open_pos
        h1_candles = db_get_candles(symbol, "1H", 300)
        hlc_df = pd.DataFrame(h1_candles) if h1_candles else pd.DataFrame()
        hlc_pos = db_get_open_pos("highest_lowest_fx", symbol)
        sr = strategy_engine.highest_lowest_strategy.evaluate(symbol, "1H", hlc_df, hlc_pos)
        result = sr.metadata.get("signal") if sr.is_entry else None
    elif strategy_name == "trend_forex":
        from trading_engine.database import get_candles as db_get_candles_tf, get_open_position as db_get_open_pos_tf
        tf_candles = db_get_candles_tf(symbol, "D1", 300)
        tf_df = pd.DataFrame(tf_candles) if tf_candles else pd.DataFrame()
        tf_pos = db_get_open_pos_tf("trend_forex", symbol)
        sr = strategy_engine.trend_forex_strategy.evaluate(symbol, "D1", tf_df, tf_pos)
        result = sr.metadata.get("signal") if sr.is_entry else None
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown strategy '{strategy_name}'. Available: mtf_ema, trend_following, sp500_momentum, highest_lowest_fx, trend_forex",
        )

    return {
        "strategy": strategy_name,
        "symbol": symbol,
        "signal": result,
        "triggered": result is not None,
    }


@app.post("/api/strategies/check-exits", include_in_schema=False)
def check_exit_conditions():
    closed = strategy_engine.check_exit_conditions()
    return {
        "closed_signals": closed,
        "count": len(closed),
    }


@app.get("/api/strategy-signals", include_in_schema=False)
def list_strategy_signals(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    status: Optional[str] = Query(None, description="Filter by status: OPEN, CLOSED"),
    limit: int = Query(100, ge=1, le=500),
):
    signals = get_all_signals(strategy_name=strategy, asset=symbol, status=status, limit=limit)
    return {
        "signals": signals,
        "count": len(signals),
    }


@app.get("/api/strategy-signals/active", include_in_schema=False)
def list_active_signals(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
):
    signals = get_active_signals(strategy_name=strategy, asset=symbol)
    return {
        "signals": signals,
        "count": len(signals),
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PYTHON_ENGINE_PORT", "5001"))
    uvicorn.run(app, host="0.0.0.0", port=port)
