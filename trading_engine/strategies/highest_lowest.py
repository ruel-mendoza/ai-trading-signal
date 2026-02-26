import logging
from datetime import datetime, timedelta
from typing import Optional

import pytz
import pandas as pd

from trading_engine.strategies.base import BaseStrategy, SignalResult, Action, Direction
from trading_engine.indicators import IndicatorEngine
from trading_engine.cache_layer import CacheLayer
from trading_engine.utils.holiday_manager import is_trading_holiday
from trading_engine.database import (
    signal_exists,
    has_open_signal,
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
SYMBOL = "EUR/USD"
TIMEFRAME_H1 = "1H"
TIMEFRAME_D1 = "D1"
LOOKBACK_DAYS = 50
ATR_PERIOD = 100
TRAILING_STOP_ATR_MULT = 0.25
TAKE_PROFIT_ATR_MULT = 6.0
MIN_H1_BARS = 100
MIN_D1_BARS = 50
ALLOWED_ET_HOURS = (9, 10)
NY_REVERSAL_THRESHOLD = 0.998

ET_ZONE = pytz.timezone("America/New_York")


def _parse_candle_date(candle: dict) -> Optional[datetime]:
    ts = candle.get("timestamp", "")
    if isinstance(ts, datetime):
        return ts
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(ts), fmt)
        except (ValueError, TypeError):
            continue
    return None


def _get_previous_weekday_candle(d_candles: list[dict], now_et: Optional[datetime] = None) -> Optional[dict]:
    if len(d_candles) < 2:
        return None

    today = now_et.date() if now_et else datetime.now(pytz.utc).astimezone(ET_ZONE).date()

    for candle in reversed(d_candles):
        candle_dt = _parse_candle_date(candle)
        if candle_dt is None:
            continue
        candle_date = candle_dt.date()
        if candle_date >= today:
            continue
        if candle_date.weekday() >= 5:
            continue
        if is_trading_holiday(candle_date):
            continue
        return candle

    if len(d_candles) >= 2:
        return d_candles[-2]
    return None


class HighestLowestFXStrategy(BaseStrategy):
    def __init__(self, cache: CacheLayer):
        self.cache = cache

    @property
    def name(self) -> str:
        return STRATEGY_NAME

    def _get_advance_price(self, asset: str) -> Optional[dict]:
        try:
            api_client = self.cache.api_client
            quotes = api_client.get_advance_data([asset], period="1h", merge="latest,profile")
            if quotes and len(quotes) > 0:
                quote = quotes[0]
                current = quote.get("current", {})
                close_price = current.get("close")
                if close_price is not None:
                    close_price = float(close_price)
                    logger.info(
                        f"[HLC-FX] {asset} | v4 advance quote: close={close_price}"
                    )
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
        logger.info(f"[HLC-FX] ====== Evaluating {asset} ======")

        now_et = datetime.now(pytz.utc).astimezone(ET_ZONE)
        is_dst = bool(now_et.dst() and now_et.dst().total_seconds() > 0)
        tz_abbr = "EDT" if is_dst else "EST"

        logger.info(
            f"[HLC-FX] {asset} | Current time: {now_et.strftime('%Y-%m-%d %H:%M')} {tz_abbr} | "
            f"DST={'active' if is_dst else 'inactive'}"
        )

        if is_trading_holiday(now_et):
            logger.info(f"[HLC-FX] {asset} | Today is a trading holiday (US/JP) — skipping")
            return SignalResult()

        current_et_hour = now_et.hour
        if current_et_hour not in ALLOWED_ET_HOURS:
            logger.info(
                f"[HLC-FX] {asset} | Current ET hour is {current_et_hour}:00 — "
                f"only runs at {ALLOWED_ET_HOURS[0]}:00 and {ALLOWED_ET_HOURS[1]}:00 ET — skipping"
            )
            return SignalResult()

        logger.info(f"[HLC-FX] {asset} | ET hour {current_et_hour}:00 is within allowed window")

        try:
            d_candles = self.cache.get_candles(asset, TIMEFRAME_D1, 200)
        except Exception as e:
            logger.error(f"[HLC-FX] {asset} | Exception fetching D1 candles: {e}")
            return SignalResult()

        logger.info(
            f"[HLC-FX] {asset} | H1 candles from df: {len(df)} (need {MIN_H1_BARS}), "
            f"D1 candles: {len(d_candles)} (need {MIN_D1_BARS})"
        )

        if len(df) < MIN_H1_BARS:
            logger.warning(f"[HLC-FX] {asset} | INSUFFICIENT H1 DATA — have {len(df)}, need {MIN_H1_BARS}")
            return SignalResult()

        if len(d_candles) < MIN_D1_BARS:
            logger.warning(f"[HLC-FX] {asset} | INSUFFICIENT D1 DATA — have {len(d_candles)}, need {MIN_D1_BARS}")
            return SignalResult()

        d_closes = [c["close"] for c in d_candles]

        highest_50d = max(d_closes[-LOOKBACK_DAYS:])
        lowest_50d = min(d_closes[-LOOKBACK_DAYS:])

        h1_closes = df["close"].tolist()
        h1_highs = df["high"].tolist()
        h1_lows = df["low"].tolist()

        h1_atr_values = IndicatorEngine.atr(h1_highs, h1_lows, h1_closes, ATR_PERIOD)
        h1_atr_val = h1_atr_values[-1] if h1_atr_values and h1_atr_values[-1] is not None else None

        advance_quote = self._get_advance_price(asset)
        if advance_quote and advance_quote.get("close") is not None:
            current_price = float(advance_quote["close"])
            logger.info(f"[HLC-FX] {asset} | Using v4 advance close: {current_price:.5f}")
        else:
            current_price = float(df["close"].iloc[-1])
            logger.info(f"[HLC-FX] {asset} | Using cached H1 candle close: {current_price:.5f}")

        signal_timestamp = now_et.strftime("%Y-%m-%dT%H:%M:%S")

        atr_str = f"{h1_atr_val:.6f}" if h1_atr_val is not None else "None"

        prev_day = _get_previous_weekday_candle(d_candles, now_et)
        prev_high = prev_day["high"] if prev_day else None
        prev_low = prev_day["low"] if prev_day else None
        prev_high_str = f"{prev_high:.5f}" if prev_high is not None else "None"
        prev_low_str = f"{prev_low:.5f}" if prev_low is not None else "None"

        logger.info(
            f"[HLC-FX] {asset} | price={current_price:.5f} | "
            f"highest_50d={highest_50d:.5f} | lowest_50d={lowest_50d:.5f} | "
            f"H1 ATR({ATR_PERIOD})={atr_str} | "
            f"prev_day_high={prev_high_str} | prev_day_low={prev_low_str}"
        )

        if open_position and open_position.get("direction") in ("BUY", "SELL"):
            pos_id = open_position.get("id")
            direction = open_position.get("direction")

            if direction == "BUY":
                stored_highest = open_position.get("highest_price_since_entry") or open_position.get("entry_price", current_price)
                new_highest = max(stored_highest, current_price)
                if new_highest > stored_highest:
                    update_position_tracking(pos_id, highest_price=new_highest)
                    logger.info(f"[HLC-FX] {asset} | ACTIVE LONG #{pos_id} | Peak updated: {stored_highest:.5f} → {new_highest:.5f}")
                else:
                    logger.info(f"[HLC-FX] {asset} | ACTIVE LONG #{pos_id} | Peak unchanged: {stored_highest:.5f}")
            elif direction == "SELL":
                stored_lowest = open_position.get("lowest_price_since_entry") or open_position.get("entry_price", current_price)
                new_lowest = min(stored_lowest, current_price)
                if new_lowest < stored_lowest:
                    update_position_tracking(pos_id, lowest_price=new_lowest)
                    logger.info(f"[HLC-FX] {asset} | ACTIVE SHORT #{pos_id} | Trough updated: {stored_lowest:.5f} → {new_lowest:.5f}")
                else:
                    logger.info(f"[HLC-FX] {asset} | ACTIVE SHORT #{pos_id} | Trough unchanged: {stored_lowest:.5f}")

            logger.info(f"[HLC-FX] {asset} | IDEMPOTENCY: Open {direction} position #{pos_id} — skipping entry")
            return SignalResult()

        session_label = f"{current_et_hour}:00 ET"
        signal_data = None

        if current_price >= highest_50d:
            signal_data = {
                "direction": "BUY",
                "reason": (
                    f"Price ({current_price:.5f}) at/above 50-day highest close "
                    f"({highest_50d:.5f}) at {session_label}"
                ),
            }
        elif current_price <= lowest_50d:
            if current_price > lowest_50d * NY_REVERSAL_THRESHOLD:
                signal_data = {
                    "direction": "BUY",
                    "reason": (
                        f"Price ({current_price:.5f}) near 50-day lowest close "
                        f"({lowest_50d:.5f}) — potential reversal at {session_label}"
                    ),
                }
            else:
                signal_data = {
                    "direction": "SELL",
                    "reason": (
                        f"Price ({current_price:.5f}) at/below 50-day lowest close "
                        f"({lowest_50d:.5f}) at {session_label}"
                    ),
                }

        if signal_data is None:
            logger.info(
                f"[HLC-FX] {asset} | No breakout/reversal condition met at {session_label} — no action"
            )
            return SignalResult()

        direction = signal_data["direction"]
        if prev_day is not None:
            if direction == "BUY" and prev_low is not None and current_price < prev_low:
                logger.info(
                    f"[HLC-FX] {asset} | PREV DAY FILTER REJECTED: LONG entry price "
                    f"{current_price:.5f} < previous day low {prev_low:.5f} — blocked"
                )
                return SignalResult()
            if direction == "SELL" and prev_high is not None and current_price > prev_high:
                logger.info(
                    f"[HLC-FX] {asset} | PREV DAY FILTER REJECTED: SHORT entry price "
                    f"{current_price:.5f} > previous day high {prev_high:.5f} — blocked"
                )
                return SignalResult()
            logger.info(
                f"[HLC-FX] {asset} | PREV DAY FILTER PASSED: {direction} entry "
                f"{current_price:.5f} vs prev_high={prev_high_str}, prev_low={prev_low_str}"
            )
        else:
            logger.warning(f"[HLC-FX] {asset} | No previous day candle — prev day filter skipped")

        if has_open_signal(STRATEGY_NAME, asset):
            logger.info(
                f"[HLC-FX] {asset} | IDEMPOTENCY: An OPEN signal already exists — duplicate blocked"
            )
            return SignalResult()

        if signal_exists(STRATEGY_NAME, asset, signal_timestamp):
            logger.info(f"[HLC-FX] {asset} | Signal already exists for timestamp {signal_timestamp} — blocked")
            return SignalResult()

        if h1_atr_val is None:
            logger.warning(f"[HLC-FX] {asset} | H1 ATR({ATR_PERIOD}) is None — cannot set trailing stop")
            return SignalResult()

        stop_distance = TRAILING_STOP_ATR_MULT * h1_atr_val
        if direction == "BUY":
            stop_loss = current_price - stop_distance
            take_profit = current_price + (TAKE_PROFIT_ATR_MULT * h1_atr_val)
        else:
            stop_loss = current_price + stop_distance
            take_profit = current_price - (TAKE_PROFIT_ATR_MULT * h1_atr_val)

        logger.info(
            f"[HLC-FX] {asset} | SIGNAL: {direction} @ {current_price:.5f} | "
            f"SL={stop_loss:.5f} (0.25x H1 ATR) | TP={take_profit:.5f} | "
            f"H1 ATR_at_entry={atr_str} | "
            f"reason={signal_data['reason']}"
        )

        signal_direction = Direction.LONG if direction == "BUY" else Direction.SHORT

        return SignalResult(
            action=Action.ENTRY,
            direction=signal_direction,
            price=current_price,
            stop_loss=stop_loss,
            atr_at_entry=round(h1_atr_val, 6),
            metadata={
                "take_profit": take_profit,
                "reason": signal_data["reason"],
                "session": session_label,
                "prev_day_high": prev_high,
                "prev_day_low": prev_low,
                "signal": {
                    "strategy_name": STRATEGY_NAME,
                    "asset": asset,
                    "direction": direction,
                    "entry_price": current_price,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    "atr_at_entry": round(h1_atr_val, 6),
                    "signal_timestamp": signal_timestamp,
                },
            },
        )

    def check_exits(self) -> list[dict]:
        closed_signals = []
        positions = get_all_open_positions(strategy_name=STRATEGY_NAME)
        logger.info(f"[HLC-FX-EXIT] ====== Checking exits | {len(positions)} open position(s) ======")

        if not positions:
            return closed_signals

        for pos in positions:
            asset = pos["asset"]
            pos_id = pos["id"]
            entry_price = pos["entry_price"]
            direction = pos["direction"]
            atr_at_entry = pos["atr_at_entry"]

            logger.info(f"[HLC-FX-EXIT] Position #{pos_id} | {asset} {direction} | entry={entry_price:.5f}")

            if atr_at_entry is None:
                logger.warning(f"[HLC-FX-EXIT] Position #{pos_id} | No atr_at_entry — skipping")
                continue

            advance_quote = self._get_advance_price(asset)

            try:
                candles = self.cache.get_candles(asset, TIMEFRAME_H1, 300)
            except Exception as e:
                logger.error(f"[HLC-FX-EXIT] Position #{pos_id} | Exception fetching candles: {e}")
                continue

            if not candles:
                logger.warning(f"[HLC-FX-EXIT] Position #{pos_id} | No candles available")
                continue

            closes = [c["close"] for c in candles]

            if advance_quote and advance_quote.get("close") is not None:
                current_close = float(advance_quote["close"])
                logger.info(f"[HLC-FX-EXIT] Position #{pos_id} | Using v4 advance close: {current_close:.5f}")
            else:
                current_close = closes[-1]
                logger.info(f"[HLC-FX-EXIT] Position #{pos_id} | Using cached H1 close: {current_close:.5f}")

            trailing_stop_hit = False
            trailing_stop_level = 0.0

            if direction == "BUY":
                stored_highest = pos.get("highest_price_since_entry") or entry_price
                highest_close = max(stored_highest, current_close)
                if highest_close > stored_highest:
                    update_position_tracking(pos_id, highest_price=highest_close)
                    logger.info(f"[HLC-FX-EXIT] Position #{pos_id} | Peak updated: {stored_highest:.5f} → {highest_close:.5f}")

                trailing_stop_level = highest_close - (atr_at_entry * TRAILING_STOP_ATR_MULT)
                trailing_stop_hit = current_close < trailing_stop_level

                logger.info(
                    f"[HLC-FX-EXIT] Position #{pos_id} | LONG | close={current_close:.5f} | "
                    f"highest={highest_close:.5f} | ATR_entry={atr_at_entry:.6f} (FIXED, H1) | "
                    f"trailing_stop={trailing_stop_level:.5f} (highest - 0.25×ATR) | hit={trailing_stop_hit}"
                )
            elif direction == "SELL":
                stored_lowest = pos.get("lowest_price_since_entry") or entry_price
                lowest_close = min(stored_lowest, current_close)
                if lowest_close < stored_lowest:
                    update_position_tracking(pos_id, lowest_price=lowest_close)
                    logger.info(f"[HLC-FX-EXIT] Position #{pos_id} | Trough updated: {stored_lowest:.5f} → {lowest_close:.5f}")

                trailing_stop_level = lowest_close + (atr_at_entry * TRAILING_STOP_ATR_MULT)
                trailing_stop_hit = current_close > trailing_stop_level

                logger.info(
                    f"[HLC-FX-EXIT] Position #{pos_id} | SHORT | close={current_close:.5f} | "
                    f"lowest={lowest_close:.5f} | ATR_entry={atr_at_entry:.6f} (FIXED, H1) | "
                    f"trailing_stop={trailing_stop_level:.5f} (lowest + 0.25×ATR) | hit={trailing_stop_hit}"
                )

            if trailing_stop_hit:
                exit_reason = (
                    f"Trailing stop hit | close={current_close:.5f}, "
                    f"stop={trailing_stop_level:.5f}, "
                    f"ATR_at_entry={atr_at_entry:.6f} (H1, fixed), "
                    f"mult={TRAILING_STOP_ATR_MULT}"
                )
                logger.info(f"[HLC-FX-EXIT] Position #{pos_id} | EXIT: trailing_stop (0.25x H1 ATR)")

                active_sigs = get_active_signals(strategy_name=STRATEGY_NAME, asset=asset)
                for sig in active_sigs:
                    close_signal(sig["id"], exit_reason, exit_price=current_close)
                close_position(STRATEGY_NAME, asset)
                closed_signals.append({
                    **pos,
                    "exit_price": current_close,
                    "exit_reason": "trailing_stop",
                    "atr_at_entry": atr_at_entry,
                })
            else:
                logger.info(f"[HLC-FX-EXIT] Position #{pos_id} | No exit triggered — holding")

        return closed_signals
