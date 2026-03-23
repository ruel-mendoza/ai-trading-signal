import logging
from datetime import datetime, timedelta
from typing import Optional

import pytz
import pandas as pd

from trading_engine.strategies.base import BaseStrategy, SignalResult, Action, Direction
from trading_engine.indicators import IndicatorEngine
from trading_engine.cache_layer import CacheLayer
from trading_engine.database import (
    signal_exists,
    has_open_signal,
    has_any_open_signal_for_asset,
    close_opposite_signal_if_exists,
    insert_signal,
    close_signal,
    open_position as db_open_position,
    get_open_position,
    get_all_open_positions,
    update_position_tracking,
    close_position,
    get_active_signals,
)

logger = logging.getLogger("trading_engine.strategy.highest_lowest_fx")

STRATEGY_NAME = "highest_lowest_fx"
TARGET_SYMBOLS = ["EUR/USD"]
ATR_PERIOD = 100
STOP_LOSS_ATR_MULT = 2.0
TIME_EXIT_HOURS = 6

ET_ZONE = pytz.timezone("America/New_York")
TOKYO_ZONE = pytz.timezone("Asia/Tokyo")


class HighestLowestFXStrategy(BaseStrategy):
    def __init__(self, cache: CacheLayer):
        self.cache = cache

    @property
    def name(self) -> str:
        return STRATEGY_NAME

    def _is_eval_window(self) -> bool:
        now_utc = datetime.now(pytz.utc)
        now_et = now_utc.astimezone(ET_ZONE)
        is_dst = bool(now_et.dst() and now_et.dst().total_seconds() > 0)
        tz_abbr = "EDT" if is_dst else "EST"

        in_window = now_et.hour in (9, 10) and now_et.minute <= 5

        logger.info(
            f"[HLC-FX] Timing check | now_ET={now_et.strftime('%H:%M')} {tz_abbr} | "
            f"eval_hours=9:00,10:00 ET | in_window={in_window} | "
            f"DST={'active' if is_dst else 'inactive'}"
        )
        return in_window

    def _get_tokyo_session_levels(self, h1_candles: list[dict]) -> Optional[dict]:
        now_utc = datetime.now(pytz.utc)
        now_et = now_utc.astimezone(ET_ZONE)
        today_et = now_et.date()

        tokyo_start_local = TOKYO_ZONE.localize(datetime(today_et.year, today_et.month, today_et.day, 8, 0))
        ny_start_et = ET_ZONE.localize(datetime(today_et.year, today_et.month, today_et.day, 8, 0))

        tokyo_start_utc = tokyo_start_local.astimezone(pytz.utc)
        ny_start_utc = ny_start_et.astimezone(pytz.utc)

        logger.info(
            f"[HLC-FX] Tokyo session window | "
            f"tokyo_start={tokyo_start_utc.strftime('%Y-%m-%d %H:%M')} UTC | "
            f"ny_start={ny_start_utc.strftime('%Y-%m-%d %H:%M')} UTC"
        )

        session_candles = []
        for c in h1_candles:
            ts = c.get("timestamp", "")
            try:
                if isinstance(ts, datetime):
                    c_utc = ts if ts.tzinfo else pytz.utc.localize(ts)
                else:
                    c_utc = pytz.utc.localize(datetime.strptime(str(ts)[:19], "%Y-%m-%dT%H:%M:%S"))
            except (ValueError, TypeError):
                try:
                    c_utc = pytz.utc.localize(datetime.strptime(str(ts)[:19], "%Y-%m-%d %H:%M:%S"))
                except (ValueError, TypeError):
                    continue

            if tokyo_start_utc <= c_utc < ny_start_utc:
                session_candles.append(c)

        if not session_candles:
            logger.warning("[HLC-FX] No H1 candles found in Tokyo→NY session window")
            return None

        highest_close = max(c["close"] for c in session_candles)
        lowest_close = min(c["close"] for c in session_candles)

        logger.info(
            f"[HLC-FX] Tokyo session levels | "
            f"candles={len(session_candles)} | "
            f"highest_close={highest_close:.5f} | lowest_close={lowest_close:.5f}"
        )

        return {
            "highest_close": highest_close,
            "lowest_close": lowest_close,
            "candle_count": len(session_candles),
            "tokyo_start_utc": tokyo_start_utc,
            "ny_start_utc": ny_start_utc,
        }

    def _get_ny_session_candles(self, h1_candles: list[dict]) -> list[dict]:
        now_utc = datetime.now(pytz.utc)
        now_et = now_utc.astimezone(ET_ZONE)
        today_et = now_et.date()

        ny_start_et = ET_ZONE.localize(datetime(today_et.year, today_et.month, today_et.day, 8, 0))
        ny_start_utc = ny_start_et.astimezone(pytz.utc)

        ny_candles = []
        for c in h1_candles:
            ts = c.get("timestamp", "")
            try:
                if isinstance(ts, datetime):
                    c_utc = ts if ts.tzinfo else pytz.utc.localize(ts)
                else:
                    c_utc = pytz.utc.localize(datetime.strptime(str(ts)[:19], "%Y-%m-%dT%H:%M:%S"))
            except (ValueError, TypeError):
                try:
                    c_utc = pytz.utc.localize(datetime.strptime(str(ts)[:19], "%Y-%m-%d %H:%M:%S"))
                except (ValueError, TypeError):
                    continue

            if c_utc >= ny_start_utc:
                ny_candles.append(c)

        return ny_candles

    def _get_previous_daily(self, d1_candles: list[dict]) -> Optional[dict]:
        now_et = datetime.now(pytz.utc).astimezone(ET_ZONE)
        today = now_et.date()

        for candle in reversed(d1_candles):
            ts = candle.get("timestamp", "")
            try:
                if isinstance(ts, datetime):
                    c_date = ts.date()
                else:
                    c_date = datetime.strptime(str(ts)[:10], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                continue
            if c_date < today and c_date.weekday() < 5:
                return {"high": candle["high"], "low": candle["low"], "date": c_date}

        return None

    def _get_advance_price(self, asset: str) -> Optional[dict]:
        try:
            api_client = self.cache.api_client
            quotes = api_client.get_advance_data([asset], period="1d", merge="latest,profile")
            if quotes and len(quotes) > 0:
                quote = quotes[0]
                current = quote.get("current", {})
                close_price = current.get("close")
                if close_price is not None:
                    close_price = float(close_price)
                    logger.info(f"[HLC-FX] {asset} | v4 advance quote: close={close_price}")
                    return {
                        "close": close_price,
                        "high": float(current["high"]) if current.get("high") else None,
                        "low": float(current["low"]) if current.get("low") else None,
                        "open": float(current["open"]) if current.get("open") else None,
                        "timestamp": current.get("timestamp", ""),
                    }
        except Exception as e:
            logger.error(f"[HLC-FX] {asset} | v4 advance request failed: {e}")
        return None

    def evaluate(
        self,
        asset: str,
        timeframe: str,
        df: pd.DataFrame,
        open_position: Optional[dict],
    ) -> SignalResult:
        logger.info(f"[HLC-FX] ====== Evaluating {asset} (Tokyo Sweep & Recover) ======")

        if asset not in TARGET_SYMBOLS:
            logger.info(f"[HLC-FX] {asset} | Not a target asset — skipping")
            return SignalResult()

        now_et = datetime.now(pytz.utc).astimezone(ET_ZONE)
        is_dst = bool(now_et.dst() and now_et.dst().total_seconds() > 0)
        tz_abbr = "EDT" if is_dst else "EST"

        logger.info(
            f"[HLC-FX] {asset} | Current time: {now_et.strftime('%Y-%m-%d %H:%M')} {tz_abbr} | "
            f"DST={'active' if is_dst else 'inactive'}"
        )

        if not self._is_eval_window():
            logger.info(f"[HLC-FX] {asset} | Outside 9:00/10:00 AM ET eval window — skipping")
            return SignalResult()

        if open_position and open_position.get("direction") in ("BUY", "SELL"):
            pos_id = open_position.get("id")
            direction = open_position.get("direction")
            logger.info(f"[HLC-FX] {asset} | IDEMPOTENCY: Open {direction} position #{pos_id} — skipping entry")
            return SignalResult()

        h1_candles = self.cache.get_candles(asset, "1H", 300)
        if not h1_candles or len(h1_candles) < ATR_PERIOD:
            logger.warning(f"[HLC-FX] {asset} | Insufficient H1 candles: {len(h1_candles) if h1_candles else 0}")
            return SignalResult()

        d1_candles = self.cache.get_candles(asset, "D1", 200)
        if not d1_candles or len(d1_candles) < 5:
            logger.warning(f"[HLC-FX] {asset} | Insufficient D1 candles: {len(d1_candles) if d1_candles else 0}")
            return SignalResult()

        tokyo_levels = self._get_tokyo_session_levels(h1_candles)
        if tokyo_levels is None:
            logger.info(f"[HLC-FX] {asset} | No Tokyo session data — skipping")
            return SignalResult()

        tokyo_high = tokyo_levels["highest_close"]
        tokyo_low = tokyo_levels["lowest_close"]

        ny_candles = self._get_ny_session_candles(h1_candles)
        if not ny_candles:
            logger.warning(f"[HLC-FX] {asset} | No NY session candles yet — skipping")
            return SignalResult()

        ny_lows = [c["low"] for c in ny_candles]
        ny_highs = [c["high"] for c in ny_candles]
        swept_below_tokyo_low = min(ny_lows) < tokyo_low
        swept_above_tokyo_high = max(ny_highs) > tokyo_high

        current_candle = ny_candles[-1]
        current_close = current_candle["close"]
        current_open = current_candle["open"]
        is_bullish = current_close > current_open
        is_bearish = current_close < current_open

        h1_closes = [c["close"] for c in h1_candles]
        h1_highs = [c["high"] for c in h1_candles]
        h1_lows = [c["low"] for c in h1_candles]
        atr_values = IndicatorEngine.atr(h1_highs, h1_lows, h1_closes, ATR_PERIOD)
        atr_val = atr_values[-1] if atr_values and atr_values[-1] is not None else None

        prev_daily = self._get_previous_daily(d1_candles)
        prev_daily_high = prev_daily["high"] if prev_daily else None
        prev_daily_low = prev_daily["low"] if prev_daily else None

        logger.info(
            f"[HLC-FX] {asset} | Tokyo high={tokyo_high:.5f} low={tokyo_low:.5f} | "
            f"NY candles={len(ny_candles)} | current_close={current_close:.5f} | "
            f"candle={'BULL' if is_bullish else 'BEAR' if is_bearish else 'DOJI'} | "
            f"ATR({ATR_PERIOD})={f'{atr_val:.6f}' if atr_val else 'None'}"
        )
        logger.info(
            f"[HLC-FX] {asset} | Sweep status: below_tokyo_low={swept_below_tokyo_low} "
            f"(NY low={min(ny_lows):.5f} vs tokyo_low={tokyo_low:.5f}) | "
            f"above_tokyo_high={swept_above_tokyo_high} "
            f"(NY high={max(ny_highs):.5f} vs tokyo_high={tokyo_high:.5f})"
        )
        if prev_daily:
            logger.info(
                f"[HLC-FX] {asset} | Prev daily high={prev_daily_high:.5f} low={prev_daily_low:.5f} "
                f"(date={prev_daily['date']})"
            )

        signal_timestamp = now_et.strftime("%Y-%m-%dT%H:%M:%S")
        signal_data = None

        long_cond_1 = swept_below_tokyo_low
        long_cond_2 = current_close > tokyo_low
        long_cond_3 = is_bullish
        long_cond_4 = prev_daily_low is not None and current_close > prev_daily_low

        logger.info(
            f"[HLC-FX] {asset} | LONG check: swept_below={long_cond_1} | "
            f"close_above_tokyo_low={long_cond_2} | bullish_candle={long_cond_3} | "
            f"above_prev_daily_low={long_cond_4}"
        )

        if long_cond_1 and long_cond_2 and long_cond_3 and long_cond_4:
            signal_data = {
                "direction": "BUY",
                "reason": (
                    f"Sweep & Recover LONG: NY swept below Tokyo low ({tokyo_low:.5f}), "
                    f"H1 close recovered above it ({current_close:.5f}), bullish candle, "
                    f"above prev daily low ({prev_daily_low:.5f})"
                ),
            }
            logger.info(f"[HLC-FX] {asset} | ALL LONG CONDITIONS MET — Sweep & Recover BUY")

        if signal_data is None:
            short_cond_1 = swept_above_tokyo_high
            short_cond_2 = current_close < tokyo_high
            short_cond_3 = is_bearish
            short_cond_4 = prev_daily_high is not None and current_close < prev_daily_high

            logger.info(
                f"[HLC-FX] {asset} | SHORT check: swept_above={short_cond_1} | "
                f"close_below_tokyo_high={short_cond_2} | bearish_candle={short_cond_3} | "
                f"below_prev_daily_high={short_cond_4}"
            )

            if short_cond_1 and short_cond_2 and short_cond_3 and short_cond_4:
                signal_data = {
                    "direction": "SELL",
                    "reason": (
                        f"Sweep & Recover SHORT: NY swept above Tokyo high ({tokyo_high:.5f}), "
                        f"H1 close recovered below it ({current_close:.5f}), bearish candle, "
                        f"below prev daily high ({prev_daily_high:.5f})"
                    ),
                }
                logger.info(f"[HLC-FX] {asset} | ALL SHORT CONDITIONS MET — Sweep & Recover SELL")

        if signal_data is None:
            logger.info(f"[HLC-FX] {asset} | No sweep & recover pattern detected — no action")
            return SignalResult()

        direction = signal_data["direction"]

        if has_any_open_signal_for_asset(asset):
            logger.info(
                f"[HLC-FX] {asset} | IDEMPOTENCY BLOCK: "
                f"An OPEN signal already exists for this asset "
                f"(cross-strategy check) — entry skipped"
            )
            return SignalResult()

        if has_open_signal(STRATEGY_NAME, asset):
            logger.info(f"[HLC-FX] {asset} | IDEMPOTENCY: An OPEN signal already exists — duplicate blocked")
            return SignalResult()

        if signal_exists(STRATEGY_NAME, asset, signal_timestamp):
            logger.info(f"[HLC-FX] {asset} | Signal already exists for timestamp {signal_timestamp} — blocked")
            return SignalResult()

        if atr_val is None:
            logger.warning(f"[HLC-FX] {asset} | H1 ATR({ATR_PERIOD}) is None — cannot set stop loss")
            return SignalResult()

        stop_distance = STOP_LOSS_ATR_MULT * atr_val
        if direction == "BUY":
            stop_loss = current_close - stop_distance
        else:
            stop_loss = current_close + stop_distance

        exit_time = now_et + timedelta(hours=TIME_EXIT_HOURS)

        logger.info(
            f"[HLC-FX] {asset} | SIGNAL: {direction} @ {current_close:.5f} | "
            f"SL={stop_loss:.5f} ({STOP_LOSS_ATR_MULT}x H1 ATR) | "
            f"Time exit: {exit_time.strftime('%H:%M')} ET (+{TIME_EXIT_HOURS}h) | "
            f"reason={signal_data['reason']}"
        )

        signal_direction = Direction.LONG if direction == "BUY" else Direction.SHORT

        signal = {
            "strategy_name": STRATEGY_NAME,
            "asset": asset,
            "direction": direction,
            "action": "ENTRY",
            "entry_price": current_close,
            "stop_loss": stop_loss,
            "take_profit": None,
            "atr_at_entry": round(atr_val, 6),
            "signal_timestamp": signal_timestamp,
        }

        # Close opposite direction signal if this strategy has one open
        close_opposite_signal_if_exists(STRATEGY_NAME, asset, direction)
        signal_id = insert_signal(signal)
        if signal_id:
            db_open_position({
                "asset": asset,
                "strategy_name": STRATEGY_NAME,
                "direction": direction,
                "entry_price": current_close,
                "atr_at_entry": round(atr_val, 6),
            })
            signal["id"] = signal_id
            signal["status"] = "OPEN"
            logger.info(f"[HLC-FX] {asset} | Signal stored with id={signal_id}")

        return SignalResult(
            action=Action.ENTRY,
            direction=signal_direction,
            price=current_close,
            stop_loss=stop_loss,
            atr_at_entry=round(atr_val, 6),
            metadata={
                "reason": signal_data["reason"],
                "tokyo_high": tokyo_high,
                "tokyo_low": tokyo_low,
                "time_exit": exit_time.isoformat(),
                "signal": signal,
            },
        )

    def _close_orphaned_signals(self, now_et: datetime) -> list[dict]:
        """Close any OPEN signals that have no corresponding open_positions record.

        This handles the case where a server restart clears open_positions but
        leaves signals in OPEN status. Since we have no position tracking data,
        we close immediately — the time_exit threshold has certainly passed for
        any genuine orphan (all entries are intraday 9-10 AM only).
        """
        orphans = get_active_signals(strategy_name=STRATEGY_NAME)
        if not orphans:
            return []

        positions = get_all_open_positions(strategy_name=STRATEGY_NAME)
        position_assets = {p["asset"] for p in positions}

        closed = []
        for sig in orphans:
            asset = sig["asset"]
            if asset in position_assets:
                continue

            sig_id = sig["id"]
            entry_price = sig.get("entry_price", 0)
            direction = sig.get("direction", "")
            ts_raw = sig.get("signal_timestamp") or sig.get("created_at", "")

            hours_open = None
            try:
                if isinstance(ts_raw, str):
                    entry_time = datetime.strptime(str(ts_raw)[:19], "%Y-%m-%dT%H:%M:%S")
                    entry_time = ET_ZONE.localize(entry_time)
                    hours_open = (now_et - entry_time).total_seconds() / 3600
            except (ValueError, TypeError):
                pass

            logger.warning(
                f"[HLC-FX-EXIT] ORPHAN SIGNAL detected | id={sig_id} | {asset} {direction} | "
                f"entry={entry_price:.5f} | "
                f"hours_open={f'{hours_open:.1f}' if hours_open is not None else 'unknown'} | "
                f"No open_positions record found — closing immediately"
            )

            advance_quote = self._get_advance_price(asset)
            if advance_quote and advance_quote.get("close") is not None:
                exit_price = float(advance_quote["close"])
            else:
                candles = self.cache.get_candles(asset, "1H", 5)
                exit_price = candles[-1]["close"] if candles else entry_price

            hours_str = f"{hours_open:.1f}h" if hours_open is not None else "unknown duration"
            exit_reason = (
                f"Orphaned signal close | No open_positions record (cleared on restart) | "
                f"open for {hours_str} | time_exit threshold {TIME_EXIT_HOURS}h exceeded"
            )
            close_signal(sig_id, exit_reason, exit_price=exit_price)
            closed.append({
                "asset": asset, "direction": direction,
                "entry_price": entry_price, "exit_price": exit_price,
                "exit_reason": "orphaned_time_exit",
            })
            logger.info(
                f"[HLC-FX-EXIT] ORPHAN SIGNAL closed | id={sig_id} | {asset} {direction} | "
                f"exit_price={exit_price:.5f}"
            )

        return closed

    def check_exits(self) -> list[dict]:
        closed_signals = []
        positions = get_all_open_positions(strategy_name=STRATEGY_NAME)
        logger.info(f"[HLC-FX-EXIT] ====== Checking exits | {len(positions)} open position(s) ======")

        now_et = datetime.now(pytz.utc).astimezone(ET_ZONE)

        # Safety net: close any OPEN signals with no position record
        orphan_closes = self._close_orphaned_signals(now_et)
        closed_signals.extend(orphan_closes)
        if orphan_closes:
            logger.warning(
                f"[HLC-FX-EXIT] {len(orphan_closes)} orphaned signal(s) closed — "
                f"open_positions was out of sync with signals table"
            )

        if not positions:
            return closed_signals

        for pos in positions:
            asset = pos["asset"]
            pos_id = pos["id"]
            entry_price = pos["entry_price"]
            direction = pos["direction"]
            atr_at_entry = pos["atr_at_entry"]
            created_at = pos.get("created_at")

            logger.info(f"[HLC-FX-EXIT] Position #{pos_id} | {asset} {direction} | entry={entry_price:.5f}")

            time_exit_triggered = False
            if created_at:
                try:
                    if isinstance(created_at, str):
                        entry_time = datetime.strptime(created_at[:19], "%Y-%m-%dT%H:%M:%S")
                        entry_time = ET_ZONE.localize(entry_time)
                    elif isinstance(created_at, datetime):
                        entry_time = created_at if created_at.tzinfo else ET_ZONE.localize(created_at)
                    else:
                        entry_time = None

                    if entry_time:
                        hours_since_entry = (now_et - entry_time).total_seconds() / 3600
                        logger.info(
                            f"[HLC-FX-EXIT] Position #{pos_id} | "
                            f"entry_time={entry_time.strftime('%H:%M')} ET | "
                            f"hours_since={hours_since_entry:.1f} | "
                            f"time_exit_threshold={TIME_EXIT_HOURS}h"
                        )
                        if hours_since_entry >= TIME_EXIT_HOURS:
                            time_exit_triggered = True
                except (ValueError, TypeError) as e:
                    logger.warning(f"[HLC-FX-EXIT] Position #{pos_id} | Cannot parse created_at: {e}")

            advance_quote = self._get_advance_price(asset)
            if advance_quote and advance_quote.get("close") is not None:
                current_close = float(advance_quote["close"])
                logger.info(f"[HLC-FX-EXIT] Position #{pos_id} | Using v4 advance close: {current_close:.5f}")
            else:
                try:
                    candles = self.cache.get_candles(asset, "1H", 10)
                except Exception as e:
                    logger.error(f"[HLC-FX-EXIT] Position #{pos_id} | Exception fetching candles: {e}")
                    continue
                if not candles:
                    logger.warning(f"[HLC-FX-EXIT] Position #{pos_id} | No candles available")
                    continue
                current_close = candles[-1]["close"]
                logger.info(f"[HLC-FX-EXIT] Position #{pos_id} | Using cached H1 close: {current_close:.5f}")

            if time_exit_triggered:
                exit_reason = (
                    f"Time exit | {TIME_EXIT_HOURS}h elapsed since entry | "
                    f"close={current_close:.5f}"
                )
                logger.info(f"[HLC-FX-EXIT] Position #{pos_id} | EXIT: time_exit ({TIME_EXIT_HOURS}h)")

                active_sigs = get_active_signals(strategy_name=STRATEGY_NAME, asset=asset)
                for sig in active_sigs:
                    close_signal(sig["id"], exit_reason, exit_price=current_close)
                close_position(STRATEGY_NAME, asset)
                closed_signals.append({
                    **pos, "exit_price": current_close, "exit_reason": "time_exit",
                })
                continue

            if atr_at_entry is None:
                logger.warning(f"[HLC-FX-EXIT] Position #{pos_id} | No atr_at_entry — skipping SL check")
                continue

            stop_distance = STOP_LOSS_ATR_MULT * atr_at_entry
            if direction == "BUY":
                stop_level = entry_price - stop_distance
                stop_hit = current_close < stop_level
            else:
                stop_level = entry_price + stop_distance
                stop_hit = current_close > stop_level

            logger.info(
                f"[HLC-FX-EXIT] Position #{pos_id} | {direction} | close={current_close:.5f} | "
                f"SL_level={stop_level:.5f} ({STOP_LOSS_ATR_MULT}×ATR={atr_at_entry:.6f}) | "
                f"hit={stop_hit}"
            )

            if stop_hit:
                exit_reason = (
                    f"Stop loss hit | close={current_close:.5f}, "
                    f"stop={stop_level:.5f}, "
                    f"ATR_at_entry={atr_at_entry:.6f} (H1, fixed), "
                    f"mult={STOP_LOSS_ATR_MULT}"
                )
                logger.info(f"[HLC-FX-EXIT] Position #{pos_id} | EXIT: stop_loss")

                active_sigs = get_active_signals(strategy_name=STRATEGY_NAME, asset=asset)
                for sig in active_sigs:
                    close_signal(sig["id"], exit_reason, exit_price=current_close)
                close_position(STRATEGY_NAME, asset)
                closed_signals.append({
                    **pos, "exit_price": current_close, "exit_reason": "stop_loss",
                    "atr_at_entry": atr_at_entry,
                })
            else:
                logger.info(f"[HLC-FX-EXIT] Position #{pos_id} | No exit triggered — holding")

        return closed_signals
