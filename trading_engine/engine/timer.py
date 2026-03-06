import logging
from datetime import datetime
from typing import Optional

import pytz
import pandas as pd

from trading_engine.cache_layer import CacheLayer
from trading_engine.database import (
    get_open_position,
    get_all_open_positions,
    get_active_signals,
    close_signal,
    close_position,
    update_position_tracking,
    create_job_log,
    finish_job_log,
    upsert_strategy_execution_log,
)
from trading_engine.notifications import notify_strategy_failure

logger = logging.getLogger("trading_engine.engine.timer")

ET_ZONE = pytz.timezone("America/New_York")

NON_FOREX_EVAL_HOUR = 16
NON_FOREX_EVAL_MINUTE = 1
FOREX_EVAL_HOUR = 17
FOREX_EVAL_MINUTE = 1


def _get_et_now() -> datetime:
    return datetime.now(pytz.utc).astimezone(ET_ZONE)


def _dual_endpoint_batch_fetch(cache: CacheLayer) -> dict:
    from trading_engine.strategies.trend_forex import TARGET_SYMBOLS as FOREX_SYMBOLS
    from trading_engine.strategies.trend_non_forex import TARGET_SYMBOLS as ETF_SYMBOLS

    result = {"forex_prices": {}, "etf_prices": {}}
    api = cache.api_client

    logger.info(
        f"[BATCH-FETCH] ====== Dual-Endpoint Batch Fetcher | "
        f"Forex: {len(FOREX_SYMBOLS)} symbols | ETFs: {len(ETF_SYMBOLS)} symbols ======"
    )

    try:
        result["forex_prices"] = api.get_forex_latest_prices(list(FOREX_SYMBOLS))
        logger.info(
            f"[BATCH-FETCH] forex/latest: {len(result['forex_prices'])}/{len(FOREX_SYMBOLS)} "
            f"prices (1 credit)"
        )
    except Exception as e:
        logger.error(f"[BATCH-FETCH] forex/latest failed: {e}")

    try:
        result["etf_prices"] = api.get_stock_latest_prices(list(ETF_SYMBOLS), batch_size=9)
        logger.info(
            f"[BATCH-FETCH] stock/latest: {len(result['etf_prices'])}/{len(ETF_SYMBOLS)} "
            f"prices (~2-3 credits)"
        )
    except Exception as e:
        logger.error(f"[BATCH-FETCH] stock/latest failed: {e}")

    total = len(result["forex_prices"]) + len(result["etf_prices"])
    total_expected = len(FOREX_SYMBOLS) + len(ETF_SYMBOLS)
    logger.info(f"[BATCH-FETCH] Complete: {total}/{total_expected} total prices fetched")
    return result


def pre_close_trend_evaluate(strategy_engine, cache: CacheLayer):
    et_now = _get_et_now()
    is_dst = bool(et_now.dst() and et_now.dst().total_seconds() > 0)
    tz_label = "EDT" if is_dst else "EST"
    logger.info(
        f"[PRE-CLOSE] ====== 4:01 PM ET Post-Close Evaluation | "
        f"{et_now.strftime('%Y-%m-%d %H:%M:%S')} {tz_label} ======"
    )

    batch_data = _dual_endpoint_batch_fetch(cache)

    _run_trend_non_forex(strategy_engine, cache, et_now, tz_label, batch_data["etf_prices"])
    _run_trend_forex(strategy_engine, cache, et_now, tz_label, batch_data["forex_prices"])
    _run_highest_lowest(strategy_engine, cache, et_now, tz_label)


def _retry_eval(func, asset, max_retries=2, delay=5):
    import time
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            return func(asset), None
        except Exception as e:
            last_err = e
            if attempt < max_retries:
                logger.warning(
                    f"[PRE-CLOSE] {asset} | Attempt {attempt} failed: {e} — retrying in {delay}s"
                )
                time.sleep(delay)
    return None, str(last_err)


def _run_trend_non_forex(strategy_engine, cache: CacheLayer, et_now, tz_label, etf_prices: dict = None):
    from trading_engine.strategies.trend_non_forex import (
        TARGET_SYMBOLS,
        TIMEFRAME,
    )

    log_id = create_job_log("pre_close_trend_non_forex", "trend_non_forex")
    logger.info(f"[PRE-CLOSE] --- Trend Non-Forex (LONG_ONLY ETFs) | {len(TARGET_SYMBOLS)} assets ---")

    assets_eval = 0
    signals_gen = 0
    error_count = 0
    error_details = []

    if etf_prices:
        strategy_engine.trend_non_forex_strategy._batch_prices = etf_prices
        logger.info(f"[PRE-CLOSE] trend_non_forex | Using {len(etf_prices)} pre-fetched ETF prices from batch fetcher")
    else:
        try:
            strategy_engine.trend_non_forex_strategy.prefetch_prices()
        except Exception as e:
            logger.error(f"[PRE-CLOSE] trend_non_forex | Batch price prefetch failed: {e}")

    for asset in TARGET_SYMBOLS:
        assets_eval += 1

        def _eval(a):
            candles = cache.get_candles(a, TIMEFRAME, 300)
            if not candles:
                logger.warning(f"[PRE-CLOSE] trend_non_forex | {a} | No candles available")
                return None
            df = pd.DataFrame(candles)
            open_pos = get_open_position("trend_non_forex", a)
            return strategy_engine.trend_non_forex_strategy.evaluate(a, TIMEFRAME, df, open_pos)

        result, err = _retry_eval(_eval, asset)
        if err:
            error_count += 1
            error_details.append(f"{asset}: {err}")
        elif result and result.is_entry:
            signals_gen += 1
            signal = result.metadata.get("signal", {})
            logger.info(
                f"[PRE-CLOSE] trend_non_forex | {asset} | NEW SIGNAL: "
                f"{signal.get('direction', '')} id={signal.get('id')}"
            )
        else:
            logger.info(f"[PRE-CLOSE] trend_non_forex | {asset} | No signal triggered")

    try:
        exits = strategy_engine.trend_non_forex_strategy.check_exits()
        if exits:
            logger.info(f"[PRE-CLOSE] trend_non_forex | {len(exits)} exit(s) triggered")
        else:
            logger.info("[PRE-CLOSE] trend_non_forex | No exits triggered")
    except Exception as e:
        error_count += 1
        error_details.append(f"exit_check: {e}")
        logger.error(f"[PRE-CLOSE] trend_non_forex | Exit check exception: {e}")

    asset_errors = sum(1 for d in error_details if not d.startswith("exit_check"))
    status = "FAILED" if asset_errors == assets_eval else ("PARTIAL" if error_count > 0 else "SUCCESS")
    finish_job_log(log_id, status, assets_eval, signals_gen, error_count,
                   "; ".join(error_details) if error_details else None)
    upsert_strategy_execution_log("trend_non_forex", status)
    if status in ("FAILED", "PARTIAL"):
        notify_strategy_failure("trend_non_forex", error_count, assets_eval, "; ".join(error_details))
    logger.info(
        f"[PRE-CLOSE] trend_non_forex complete | {status} | "
        f"{assets_eval} assets, {signals_gen} signals, {error_count} errors"
    )


def _run_trend_forex(strategy_engine, cache: CacheLayer, et_now, tz_label, forex_prices: dict = None):
    from trading_engine.strategies.trend_forex import (
        TARGET_SYMBOLS,
        TIMEFRAME,
    )

    log_id = create_job_log("pre_close_trend_forex", "trend_forex")
    logger.info(f"[PRE-CLOSE] --- Trend Forex (EUR/USD, USD/JPY) | {len(TARGET_SYMBOLS)} assets ---")

    if forex_prices:
        logger.info(f"[PRE-CLOSE] trend_forex | Using {len(forex_prices)} pre-fetched forex prices from batch fetcher")

    assets_eval = 0
    signals_gen = 0
    error_count = 0
    error_details = []

    for asset in TARGET_SYMBOLS:
        assets_eval += 1

        def _eval(a):
            candles = cache.get_candles(a, TIMEFRAME, 300)
            if not candles:
                logger.warning(f"[PRE-CLOSE] trend_forex | {a} | No candles available")
                return None
            df = pd.DataFrame(candles)
            open_pos = get_open_position("trend_forex", a)
            batch_price = forex_prices.get(a) if forex_prices else None
            return strategy_engine.trend_forex_strategy.evaluate(a, TIMEFRAME, df, open_pos, batch_price=batch_price)

        result, err = _retry_eval(_eval, asset)
        if err:
            error_count += 1
            error_details.append(f"{asset}: {err}")
        elif result and result.is_entry:
            signals_gen += 1
            signal = result.metadata.get("signal", {})
            logger.info(
                f"[PRE-CLOSE] trend_forex | {asset} | NEW SIGNAL: "
                f"{signal.get('direction', '')} id={signal.get('id')}"
            )
        else:
            logger.info(f"[PRE-CLOSE] trend_forex | {asset} | No signal triggered")

    try:
        exits = strategy_engine.trend_forex_strategy.check_exits()
        if exits:
            logger.info(f"[PRE-CLOSE] trend_forex | {len(exits)} exit(s) triggered")
        else:
            logger.info("[PRE-CLOSE] trend_forex | No exits triggered")
    except Exception as e:
        error_count += 1
        error_details.append(f"exit_check: {e}")
        logger.error(f"[PRE-CLOSE] trend_forex | Exit check exception: {e}")

    asset_errors = sum(1 for d in error_details if not d.startswith("exit_check"))
    status = "FAILED" if asset_errors == assets_eval else ("PARTIAL" if error_count > 0 else "SUCCESS")
    finish_job_log(log_id, status, assets_eval, signals_gen, error_count,
                   "; ".join(error_details) if error_details else None)
    upsert_strategy_execution_log("trend_forex", status)
    if status in ("FAILED", "PARTIAL"):
        notify_strategy_failure("trend_forex", error_count, assets_eval, "; ".join(error_details))
    logger.info(
        f"[PRE-CLOSE] trend_forex complete | {status} | "
        f"{assets_eval} assets, {signals_gen} signals, {error_count} errors"
    )


def _run_highest_lowest(strategy_engine, cache: CacheLayer, et_now, tz_label):
    from trading_engine.strategies.highest_lowest import STRATEGY_NAME
    from trading_engine.database import get_candles

    log_id = create_job_log("pre_close_highest_lowest", "highest_lowest_fx")
    logger.info("[PRE-CLOSE] --- Highest/Lowest Close Breakout (EUR/USD, USD/JPY) ---")

    from trading_engine.strategies.highest_lowest import TARGET_SYMBOLS as HLC_SYMBOLS

    assets_eval = 0
    signals_gen = 0
    error_count = 0
    error_details = []

    for asset in HLC_SYMBOLS:
        assets_eval += 1

        def _eval(a):
            candles = get_candles(a, "D1", 200)
            hlc_df = pd.DataFrame(candles) if candles else pd.DataFrame()
            open_pos = get_open_position(STRATEGY_NAME, a)
            return strategy_engine.highest_lowest_strategy.evaluate(a, "D1", hlc_df, open_pos)

        result, err = _retry_eval(_eval, asset)
        if err:
            error_count += 1
            error_details.append(f"{asset}: {err}")
        elif result and result.is_entry:
            signals_gen += 1
            logger.info(f"[PRE-CLOSE] highest_lowest | {asset} | NEW SIGNAL generated")
        else:
            logger.info(f"[PRE-CLOSE] highest_lowest | {asset} | No signal triggered")

    try:
        exits = strategy_engine.highest_lowest_strategy.check_exits()
        if exits:
            logger.info(f"[PRE-CLOSE] highest_lowest | {len(exits)} exit(s) triggered")
        else:
            logger.info("[PRE-CLOSE] highest_lowest | No exits triggered")
    except Exception as e:
        error_count += 1
        error_details.append(f"exit_check: {e}")

    status = "FAILED" if error_count > 0 and signals_gen == 0 else ("PARTIAL" if error_count > 0 else "SUCCESS")
    finish_job_log(log_id, status, assets_eval, signals_gen, error_count,
                   "; ".join(error_details) if error_details else None)
    if status in ("FAILED", "PARTIAL"):
        notify_strategy_failure("highest_lowest_fx", error_count, assets_eval, "; ".join(error_details))
    logger.info(f"[PRE-CLOSE] highest_lowest complete | {status}")
