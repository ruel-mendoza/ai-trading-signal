import json
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Optional

from trading_engine.indicators import IndicatorEngine
from trading_engine.cache_layer import CacheLayer
from trading_engine.database import (
    signal_exists,
    insert_signal,
    get_active_signals,
    update_signal_tracking,
    close_signal,
)

logger = logging.getLogger("trading_engine.strategy.trend_forex")

STRATEGY_NAME = "trend_forex"
TARGET_SYMBOLS = ["EUR/USD", "USD/JPY", "GBP/USD"]
TIMEFRAME = "D"
SMA_FAST = 50
SMA_SLOW = 100
ATR_PERIOD = 100
LOOKBACK_DAYS = 50
TRAILING_STOP_ATR_MULT = 3.0
MIN_BARS_REQUIRED = ATR_PERIOD + 1

FOREX_CLOSE_HOUR = 17
FOREX_CLOSE_MINUTE = 0
EVAL_WINDOW_MINUTES = 30

ET_ZONE = ZoneInfo("America/New_York")


class ForexTrendFollowingStrategy:
    def __init__(self, cache: CacheLayer):
        self.cache = cache

    def _is_forex_close_window(self) -> bool:
        now_et = datetime.now(ET_ZONE)
        is_dst = bool(now_et.dst() and now_et.dst().total_seconds() > 0)
        tz_abbr = "EDT" if is_dst else "EST"

        et_minutes = now_et.hour * 60 + now_et.minute
        close_minutes = FOREX_CLOSE_HOUR * 60 + FOREX_CLOSE_MINUTE
        window_end = close_minutes + EVAL_WINDOW_MINUTES

        in_window = close_minutes <= et_minutes <= window_end

        logger.info(
            f"[TREND-FOREX] Timing check | now_ET={now_et.strftime('%H:%M')} {tz_abbr} | "
            f"forex_close=17:00 ET | window=17:00-17:30 ET | in_window={in_window}"
        )
        return in_window

    def _has_open_trade(self, symbol: str, direction: str) -> bool:
        active = get_active_signals(strategy=STRATEGY_NAME, symbol=symbol)
        for sig in active:
            if sig.get("direction") == direction:
                logger.info(
                    f"[TREND-FOREX] {symbol} | Existing open {direction.upper()} trade found (signal #{sig['id']})"
                )
                return True
        return False

    def evaluate(self, symbol: str) -> Optional[dict]:
        logger.info(f"[TREND-FOREX] ====== Evaluating {symbol} ======")

        if symbol not in TARGET_SYMBOLS:
            logger.info(f"[TREND-FOREX] {symbol} | Not a target asset - skipping")
            return None

        if not self._is_forex_close_window():
            logger.info(f"[TREND-FOREX] {symbol} | Outside 5:00 PM ET window - skipping")
            return None

        try:
            candles = self.cache.get_candles(symbol, TIMEFRAME, 300)
        except Exception as e:
            logger.error(f"[TREND-FOREX] {symbol} | Exception fetching candles: {e}")
            return None

        logger.info(f"[TREND-FOREX] {symbol} | Daily candles: {len(candles)} (need {MIN_BARS_REQUIRED})")
        if len(candles) < MIN_BARS_REQUIRED:
            logger.warning(
                f"[TREND-FOREX] {symbol} | INSUFFICIENT DATA - have {len(candles)}, need {MIN_BARS_REQUIRED}"
            )
            return None

        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]

        sma50_values = IndicatorEngine.sma(closes, SMA_FAST)
        sma100_values = IndicatorEngine.sma(closes, SMA_SLOW)
        atr_values = IndicatorEngine.atr(highs, lows, closes, ATR_PERIOD)

        current_close = closes[-1]
        sma50_val = sma50_values[-1]
        sma100_val = sma100_values[-1]
        atr_val = atr_values[-1]

        if any(v is None for v in [sma50_val, sma100_val, atr_val]):
            none_list = []
            if sma50_val is None: none_list.append("SMA50")
            if sma100_val is None: none_list.append("SMA100")
            if atr_val is None: none_list.append("ATR100")
            logger.warning(f"[TREND-FOREX] {symbol} | Indicators returned None: {none_list}")
            return None

        prior_closes = closes[-(LOOKBACK_DAYS + 1):-1]
        highest_50d = max(prior_closes)
        lowest_50d = min(prior_closes)

        sma50_above_sma100 = sma50_val > sma100_val
        close_above_highest = current_close > highest_50d
        close_below_lowest = current_close < lowest_50d

        now_et = datetime.now(ET_ZONE)
        is_dst = bool(now_et.dst() and now_et.dst().total_seconds() > 0)

        logger.info(f"[TREND-FOREX] {symbol} | close={current_close:.5f}")
        logger.info(f"[TREND-FOREX] {symbol} | SMA50={sma50_val:.5f} | SMA100={sma100_val:.5f} | ATR100={atr_val:.5f}")
        logger.info(f"[TREND-FOREX] {symbol} | 50-day highest={highest_50d:.5f} | 50-day lowest={lowest_50d:.5f}")
        logger.info(
            f"[TREND-FOREX] {symbol} | LONG check: close > highest_50d={close_above_highest} AND SMA50 > SMA100={sma50_above_sma100}"
        )
        logger.info(
            f"[TREND-FOREX] {symbol} | SHORT check: close < lowest_50d={close_below_lowest} AND SMA50 < SMA100={not sma50_above_sma100}"
        )

        trigger_candle_time = candles[-1]["open_time"]

        if close_above_highest and sma50_above_sma100:
            if self._has_open_trade(symbol, "long"):
                logger.info(f"[TREND-FOREX] {symbol} | Existing open LONG - skipping new entry")
                return None

            if signal_exists(STRATEGY_NAME, symbol, trigger_candle_time, TIMEFRAME):
                logger.info(f"[TREND-FOREX] {symbol} | Signal already exists for candle {trigger_candle_time} - skipping")
                return None

            stop_loss_distance = TRAILING_STOP_ATR_MULT * atr_val
            stop_loss = current_close - stop_loss_distance

            logger.info(f"[TREND-FOREX] {symbol} | ALL CONDITIONS MET: LONG")
            logger.info(
                f"[TREND-FOREX] {symbol} | GENERATING SIGNAL: LONG @ {current_close:.5f} | "
                f"SL={stop_loss:.5f} | ATR={atr_val:.5f}"
            )

            signal = {
                "strategy": STRATEGY_NAME,
                "symbol": symbol,
                "direction": "long",
                "entry_price": current_close,
                "stop_loss": stop_loss,
                "take_profit": None,
                "trailing_stop_atr_mult": TRAILING_STOP_ATR_MULT,
                "trigger_candle_time": trigger_candle_time,
                "trigger_timeframe": TIMEFRAME,
                "metadata": json.dumps({
                    "sma50": round(sma50_val, 6),
                    "sma100": round(sma100_val, 6),
                    "atr100_at_entry": round(atr_val, 6),
                    "highest_50d_close": round(highest_50d, 6),
                    "entry_price": round(current_close, 6),
                    "stop_loss_distance": round(stop_loss_distance, 6),
                    "dst_active": is_dst,
                }),
            }
            signal_id = insert_signal(signal)
            if signal_id:
                signal["id"] = signal_id
                signal["status"] = "new"
                logger.info(f"[TREND-FOREX] {symbol} | Signal stored with id={signal_id}")
                return signal

        elif close_below_lowest and not sma50_above_sma100:
            if self._has_open_trade(symbol, "short"):
                logger.info(f"[TREND-FOREX] {symbol} | Existing open SHORT - skipping new entry")
                return None

            if signal_exists(STRATEGY_NAME, symbol, trigger_candle_time, TIMEFRAME):
                logger.info(f"[TREND-FOREX] {symbol} | Signal already exists for candle {trigger_candle_time} - skipping")
                return None

            stop_loss_distance = TRAILING_STOP_ATR_MULT * atr_val
            stop_loss = current_close + stop_loss_distance

            logger.info(f"[TREND-FOREX] {symbol} | ALL CONDITIONS MET: SHORT")
            logger.info(
                f"[TREND-FOREX] {symbol} | GENERATING SIGNAL: SHORT @ {current_close:.5f} | "
                f"SL={stop_loss:.5f} | ATR={atr_val:.5f}"
            )

            signal = {
                "strategy": STRATEGY_NAME,
                "symbol": symbol,
                "direction": "short",
                "entry_price": current_close,
                "stop_loss": stop_loss,
                "take_profit": None,
                "trailing_stop_atr_mult": TRAILING_STOP_ATR_MULT,
                "trigger_candle_time": trigger_candle_time,
                "trigger_timeframe": TIMEFRAME,
                "metadata": json.dumps({
                    "sma50": round(sma50_val, 6),
                    "sma100": round(sma100_val, 6),
                    "atr100_at_entry": round(atr_val, 6),
                    "lowest_50d_close": round(lowest_50d, 6),
                    "entry_price": round(current_close, 6),
                    "stop_loss_distance": round(stop_loss_distance, 6),
                    "dst_active": is_dst,
                }),
            }
            signal_id = insert_signal(signal)
            if signal_id:
                signal["id"] = signal_id
                signal["status"] = "new"
                logger.info(f"[TREND-FOREX] {symbol} | Signal stored with id={signal_id}")
                return signal

        else:
            logger.info(f"[TREND-FOREX] {symbol} | ALL CONDITIONS MET: False")

        return None

    def check_exits(self) -> list[dict]:
        closed_signals = []
        active = get_active_signals(strategy=STRATEGY_NAME)
        logger.info(f"[TREND-FOREX-EXIT] ====== Checking exits | {len(active)} active signal(s) ======")

        if not active:
            return closed_signals

        for sig in active:
            symbol = sig["symbol"]
            sig_id = sig["id"]
            entry_price = sig["entry_price"]
            direction = sig["direction"]
            logger.info(f"[TREND-FOREX-EXIT] Signal #{sig_id} | {symbol} {direction.upper()} | entry={entry_price:.5f}")

            metadata = {}
            if sig.get("metadata"):
                try:
                    metadata = json.loads(sig["metadata"])
                except (json.JSONDecodeError, TypeError):
                    pass

            atr_at_entry = metadata.get("atr100_at_entry")
            if atr_at_entry is None:
                logger.warning(f"[TREND-FOREX-EXIT] Signal #{sig_id} | No atr100_at_entry in metadata - skipping")
                continue

            try:
                candles = self.cache.get_candles(symbol, TIMEFRAME, 300)
            except Exception as e:
                logger.error(f"[TREND-FOREX-EXIT] Signal #{sig_id} | Exception fetching candles: {e}")
                continue

            if len(candles) < 2:
                logger.warning(f"[TREND-FOREX-EXIT] Signal #{sig_id} | Insufficient candles: {len(candles)}")
                continue

            current_close = candles[-1]["close"]

            if direction == "long":
                stored_highest = sig.get("highest_price") or entry_price
                highest_close = max(stored_highest, current_close)
                update_signal_tracking(sig_id, highest_price=highest_close)

                trailing_stop = highest_close - (atr_at_entry * TRAILING_STOP_ATR_MULT)

                logger.info(
                    f"[TREND-FOREX-EXIT] Signal #{sig_id} | LONG | close={current_close:.5f} | "
                    f"highest={highest_close:.5f} | trailing_stop={trailing_stop:.5f} | "
                    f"ATR_entry={atr_at_entry:.6f}"
                )

                if current_close < trailing_stop:
                    exit_reason = (
                        f"Trailing stop hit | close={current_close:.5f}, "
                        f"stop={trailing_stop:.5f}, highest={highest_close:.5f}, "
                        f"ATR_entry={atr_at_entry:.6f}"
                    )
                    logger.info(f"[TREND-FOREX-EXIT] Signal #{sig_id} | EXIT: trailing_stop")
                    close_signal(sig_id, current_close, exit_reason)
                    closed_signals.append({**sig, "exit_price": current_close, "exit_reason": "trailing_stop"})
                else:
                    logger.info(f"[TREND-FOREX-EXIT] Signal #{sig_id} | Holding LONG position")

            elif direction == "short":
                stored_lowest = sig.get("lowest_price") or entry_price
                lowest_close = min(stored_lowest, current_close)
                update_signal_tracking(sig_id, lowest_price=lowest_close)

                trailing_stop = lowest_close + (atr_at_entry * TRAILING_STOP_ATR_MULT)

                logger.info(
                    f"[TREND-FOREX-EXIT] Signal #{sig_id} | SHORT | close={current_close:.5f} | "
                    f"lowest={lowest_close:.5f} | trailing_stop={trailing_stop:.5f} | "
                    f"ATR_entry={atr_at_entry:.6f}"
                )

                if current_close > trailing_stop:
                    exit_reason = (
                        f"Trailing stop hit | close={current_close:.5f}, "
                        f"stop={trailing_stop:.5f}, lowest={lowest_close:.5f}, "
                        f"ATR_entry={atr_at_entry:.6f}"
                    )
                    logger.info(f"[TREND-FOREX-EXIT] Signal #{sig_id} | EXIT: trailing_stop")
                    close_signal(sig_id, current_close, exit_reason)
                    closed_signals.append({**sig, "exit_price": current_close, "exit_reason": "trailing_stop"})
                else:
                    logger.info(f"[TREND-FOREX-EXIT] Signal #{sig_id} | Holding SHORT position")

        return closed_signals
