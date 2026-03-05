import logging
from datetime import datetime
from typing import Optional

import pytz
import pandas as pd

from trading_engine.strategies.base import BaseStrategy, SignalResult, Action, Direction
from trading_engine.indicators import IndicatorEngine
from trading_engine.cache_layer import CacheLayer
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
TARGET_SYMBOLS = ["EUR/USD", "USD/JPY"]
TIMEFRAME_D1 = "D1"
LOOKBACK_PERIODS = 20
ATR_PERIOD = 100
TRAILING_STOP_ATR_MULT = 0.25
TAKE_PROFIT_ATR_MULT = 6.0
MIN_D1_BARS = LOOKBACK_PERIODS + 1

EVAL_HOUR = 16
EVAL_MINUTE = 59
EVAL_WINDOW_MINUTES = 5

ET_ZONE = pytz.timezone("America/New_York")


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

        et_minutes = now_et.hour * 60 + now_et.minute
        eval_minutes = EVAL_HOUR * 60 + EVAL_MINUTE
        window_end = eval_minutes + EVAL_WINDOW_MINUTES

        in_window = eval_minutes <= et_minutes <= window_end

        logger.info(
            f"[HLC-FX] Timing check | now_ET={now_et.strftime('%H:%M')} {tz_abbr} | "
            f"pre-close=16:59 ET | window=16:59-17:04 ET | in_window={in_window} | "
            f"DST={'active' if is_dst else 'inactive'}"
        )
        return in_window

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
        logger.info(f"[HLC-FX] ====== Evaluating {asset} (N={LOOKBACK_PERIODS} close breakout) ======")

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
            logger.info(f"[HLC-FX] {asset} | Outside 4:59 PM ET pre-close window — skipping")
            return SignalResult()

        if df.empty or "close" not in df.columns:
            logger.warning(f"[HLC-FX] {asset} | D1 DataFrame is empty or missing columns")
            return SignalResult()

        logger.info(f"[HLC-FX] {asset} | D1 candles: {len(df)} (need {MIN_D1_BARS})")
        if len(df) < MIN_D1_BARS:
            logger.warning(f"[HLC-FX] {asset} | INSUFFICIENT D1 DATA — have {len(df)}, need {MIN_D1_BARS}")
            return SignalResult()

        d_closes = df["close"].tolist()
        d_highs = df["high"].tolist()
        d_lows = df["low"].tolist()

        prior_closes = d_closes[-(LOOKBACK_PERIODS + 1):-1]
        highest_close_n = max(prior_closes)
        lowest_close_n = min(prior_closes)

        d1_atr_values = IndicatorEngine.atr(d_highs, d_lows, d_closes, ATR_PERIOD)
        d1_atr_val = d1_atr_values[-1] if d1_atr_values and d1_atr_values[-1] is not None else None

        advance_quote = self._get_advance_price(asset)
        if advance_quote and advance_quote.get("close") is not None:
            current_price = float(advance_quote["close"])
            logger.info(f"[HLC-FX] {asset} | Using v4 advance pre-close: {current_price:.5f}")
        else:
            current_price = float(d_closes[-1])
            logger.info(f"[HLC-FX] {asset} | Using cached D1 close: {current_price:.5f}")

        signal_timestamp = now_et.strftime("%Y-%m-%dT%H:%M:%S")
        atr_str = f"{d1_atr_val:.6f}" if d1_atr_val is not None else "None"

        logger.info(
            f"[HLC-FX] {asset} | price={current_price:.5f} | "
            f"N({LOOKBACK_PERIODS}) highest_close={highest_close_n:.5f} | "
            f"N({LOOKBACK_PERIODS}) lowest_close={lowest_close_n:.5f} | "
            f"D1 ATR({ATR_PERIOD})={atr_str}"
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
            elif direction == "SELL":
                stored_lowest = open_position.get("lowest_price_since_entry") or open_position.get("entry_price", current_price)
                new_lowest = min(stored_lowest, current_price)
                if new_lowest < stored_lowest:
                    update_position_tracking(pos_id, lowest_price=new_lowest)
                    logger.info(f"[HLC-FX] {asset} | ACTIVE SHORT #{pos_id} | Trough updated: {stored_lowest:.5f} → {new_lowest:.5f}")

            logger.info(f"[HLC-FX] {asset} | IDEMPOTENCY: Open {direction} position #{pos_id} — skipping entry")
            return SignalResult()

        signal_data = None

        if current_price > highest_close_n:
            signal_data = {
                "direction": "BUY",
                "reason": (
                    f"N({LOOKBACK_PERIODS}) Close Breakout: price ({current_price:.5f}) > "
                    f"highest close ({highest_close_n:.5f}) at 4:59 PM pre-close"
                ),
            }
            logger.info(
                f"[HLC-FX] {asset} | BREAKOUT LONG: price {current_price:.5f} > "
                f"N({LOOKBACK_PERIODS}) highest close {highest_close_n:.5f}"
            )
        elif current_price < lowest_close_n:
            signal_data = {
                "direction": "SELL",
                "reason": (
                    f"N({LOOKBACK_PERIODS}) Close Breakout: price ({current_price:.5f}) < "
                    f"lowest close ({lowest_close_n:.5f}) at 4:59 PM pre-close"
                ),
            }
            logger.info(
                f"[HLC-FX] {asset} | BREAKOUT SHORT: price {current_price:.5f} < "
                f"N({LOOKBACK_PERIODS}) lowest close {lowest_close_n:.5f}"
            )

        if signal_data is None:
            logger.info(
                f"[HLC-FX] {asset} | No N({LOOKBACK_PERIODS}) close breakout at 4:59 PM — no action"
            )
            return SignalResult()

        direction = signal_data["direction"]

        if has_open_signal(STRATEGY_NAME, asset):
            logger.info(
                f"[HLC-FX] {asset} | IDEMPOTENCY: An OPEN signal already exists — duplicate blocked"
            )
            return SignalResult()

        if signal_exists(STRATEGY_NAME, asset, signal_timestamp):
            logger.info(f"[HLC-FX] {asset} | Signal already exists for timestamp {signal_timestamp} — blocked")
            return SignalResult()

        if d1_atr_val is None:
            logger.warning(f"[HLC-FX] {asset} | D1 ATR({ATR_PERIOD}) is None — cannot set trailing stop")
            return SignalResult()

        stop_distance = TRAILING_STOP_ATR_MULT * d1_atr_val
        if direction == "BUY":
            stop_loss = current_price - stop_distance
            take_profit = current_price + (TAKE_PROFIT_ATR_MULT * d1_atr_val)
        else:
            stop_loss = current_price + stop_distance
            take_profit = current_price - (TAKE_PROFIT_ATR_MULT * d1_atr_val)

        logger.info(
            f"[HLC-FX] {asset} | SIGNAL: {direction} @ {current_price:.5f} | "
            f"SL={stop_loss:.5f} ({TRAILING_STOP_ATR_MULT}x D1 ATR) | "
            f"TP={take_profit:.5f} ({TAKE_PROFIT_ATR_MULT}x D1 ATR) | "
            f"reason={signal_data['reason']}"
        )

        signal_direction = Direction.LONG if direction == "BUY" else Direction.SHORT

        return SignalResult(
            action=Action.ENTRY,
            direction=signal_direction,
            price=current_price,
            stop_loss=stop_loss,
            atr_at_entry=round(d1_atr_val, 6),
            metadata={
                "take_profit": take_profit,
                "reason": signal_data["reason"],
                "lookback_periods": LOOKBACK_PERIODS,
                "highest_close_n": highest_close_n,
                "lowest_close_n": lowest_close_n,
                "signal": {
                    "strategy_name": STRATEGY_NAME,
                    "asset": asset,
                    "direction": direction,
                    "entry_price": current_price,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    "atr_at_entry": round(d1_atr_val, 6),
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
            if advance_quote and advance_quote.get("close") is not None:
                current_close = float(advance_quote["close"])
                logger.info(f"[HLC-FX-EXIT] Position #{pos_id} | Using v4 advance close: {current_close:.5f}")
            else:
                try:
                    candles = self.cache.get_candles(asset, TIMEFRAME_D1, 200)
                except Exception as e:
                    logger.error(f"[HLC-FX-EXIT] Position #{pos_id} | Exception fetching candles: {e}")
                    continue
                if not candles:
                    logger.warning(f"[HLC-FX-EXIT] Position #{pos_id} | No candles available")
                    continue
                current_close = candles[-1]["close"]
                logger.info(f"[HLC-FX-EXIT] Position #{pos_id} | Using cached D1 close: {current_close:.5f}")

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
                    f"highest={highest_close:.5f} | ATR_entry={atr_at_entry:.6f} (FIXED, D1) | "
                    f"trailing_stop={trailing_stop_level:.5f} ({TRAILING_STOP_ATR_MULT}×ATR) | hit={trailing_stop_hit}"
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
                    f"lowest={lowest_close:.5f} | ATR_entry={atr_at_entry:.6f} (FIXED, D1) | "
                    f"trailing_stop={trailing_stop_level:.5f} ({TRAILING_STOP_ATR_MULT}×ATR) | hit={trailing_stop_hit}"
                )

            if trailing_stop_hit:
                exit_reason = (
                    f"Trailing stop hit | close={current_close:.5f}, "
                    f"stop={trailing_stop_level:.5f}, "
                    f"ATR_at_entry={atr_at_entry:.6f} (D1, fixed), "
                    f"mult={TRAILING_STOP_ATR_MULT}"
                )
                logger.info(f"[HLC-FX-EXIT] Position #{pos_id} | EXIT: trailing_stop")

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
