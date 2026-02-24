import logging
from datetime import datetime
import pytz
from typing import Optional

from trading_engine.indicators import IndicatorEngine
from trading_engine.cache_layer import CacheLayer
from trading_engine.database import (
    signal_exists,
    insert_signal,
    close_signal,
    open_position,
    get_open_position,
    get_all_open_positions,
    update_position_tracking,
    close_position,
    has_open_position,
    get_active_signals,
)

logger = logging.getLogger("trading_engine.strategy.trend_non_forex")

STRATEGY_NAME = "trend_non_forex"
TARGET_SYMBOLS = [
    "SPX",
    "NDX",
    "XAU/USD",
    "XAG/USD",
    "WTI/USD",
    "BTC/USD",
    "ETH/USD",
]
TIMEFRAME = "D1"
SMA_FAST = 50
SMA_SLOW = 100
ATR_PERIOD = 100
LOOKBACK_DAYS = 50
TRAILING_STOP_ATR_MULT = 3.0
MIN_BARS_REQUIRED = ATR_PERIOD + 1

EVAL_HOUR = 16
EVAL_MINUTE = 0
EVAL_WINDOW_MINUTES = 30

ET_ZONE = pytz.timezone("America/New_York")


class NonForexTrendFollowingStrategy:
    def __init__(self, cache: CacheLayer):
        self.cache = cache

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
            f"[TREND-NONFX] Timing check | now_ET={now_et.strftime('%H:%M')} {tz_abbr} | "
            f"eval_window=16:00-16:30 ET | in_window={in_window} | "
            f"DST={'active' if is_dst else 'inactive'}"
        )
        return in_window

    def _has_open_long(self, asset: str) -> bool:
        pos = get_open_position(STRATEGY_NAME, asset)
        if pos and pos["direction"] == "BUY":
            logger.info(
                f"[TREND-NONFX] {asset} | Existing open LONG trade found (position #{pos['id']})"
            )
            return True
        return False

    def evaluate(self, asset: str) -> Optional[dict]:
        logger.info(f"[TREND-NONFX] ====== Evaluating {asset} ======")

        if asset not in TARGET_SYMBOLS:
            logger.info(f"[TREND-NONFX] {asset} | Not a target asset - skipping")
            return None

        if not self._is_eval_window():
            logger.info(f"[TREND-NONFX] {asset} | Outside 4:00 PM ET window - skipping")
            return None

        try:
            candles = self.cache.get_candles(asset, TIMEFRAME, 300)
        except Exception as e:
            logger.error(f"[TREND-NONFX] {asset} | Exception fetching candles: {e}")
            return None

        logger.info(f"[TREND-NONFX] {asset} | Daily candles: {len(candles)} (need {MIN_BARS_REQUIRED})")
        if len(candles) < MIN_BARS_REQUIRED:
            logger.warning(
                f"[TREND-NONFX] {asset} | INSUFFICIENT DATA - have {len(candles)}, need {MIN_BARS_REQUIRED}"
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
            logger.warning(f"[TREND-NONFX] {asset} | Indicators returned None: {none_list}")
            return None

        prior_closes = closes[-(LOOKBACK_DAYS + 1):-1]
        highest_50d = max(prior_closes)

        sma50_above_sma100 = sma50_val > sma100_val
        close_above_highest = current_close > highest_50d

        logger.info(f"[TREND-NONFX] {asset} | close={current_close:.5f}")
        logger.info(f"[TREND-NONFX] {asset} | SMA(50)={sma50_val:.5f} | SMA(100)={sma100_val:.5f} | ATR(100)={atr_val:.5f}")
        logger.info(f"[TREND-NONFX] {asset} | 50-day highest close={highest_50d:.5f} (prior {LOOKBACK_DAYS} bars)")
        logger.info(
            f"[TREND-NONFX] {asset} | LONG check: close ({current_close:.5f}) > highest_50d ({highest_50d:.5f}) = {close_above_highest} "
            f"AND SMA50 ({sma50_val:.5f}) > SMA100 ({sma100_val:.5f}) = {sma50_above_sma100}"
        )

        existing_pos = get_open_position(STRATEGY_NAME, asset)
        if existing_pos:
            pos_atr = existing_pos.get("atr_at_entry")
            pos_highest = existing_pos.get("highest_price_since_entry") or existing_pos["entry_price"]
            pos_highest = max(pos_highest, current_close)
            if pos_atr is not None:
                trailing_stop = pos_highest - (pos_atr * TRAILING_STOP_ATR_MULT)
                logger.info(
                    f"[TREND-NONFX] {asset} | ACTIVE TRADE #{existing_pos['id']} | "
                    f"direction={existing_pos['direction']} | entry={existing_pos['entry_price']:.5f} | "
                    f"ATR_at_entry={pos_atr:.6f} (FIXED) | highest_since_entry={pos_highest:.5f} | "
                    f"current_trailing_stop={trailing_stop:.5f}"
                )
            else:
                logger.warning(
                    f"[TREND-NONFX] {asset} | ACTIVE TRADE #{existing_pos['id']} | "
                    f"ATR_at_entry=MISSING — trailing stop cannot be calculated"
                )

        signal_timestamp = candles[-1]["timestamp"]

        if close_above_highest and sma50_above_sma100:
            if self._has_open_long(asset):
                logger.info(f"[TREND-NONFX] {asset} | IDEMPOTENCY: Existing open LONG trade - skipping")
                return None

            if signal_exists(STRATEGY_NAME, asset, signal_timestamp):
                logger.info(
                    f"[TREND-NONFX] {asset} | IDEMPOTENCY: Signal already exists for "
                    f"signal_timestamp={signal_timestamp} (unique constraint: strategy+asset+timestamp) "
                    f"- duplicate blocked on re-run"
                )
                return None

            stop_loss_distance = TRAILING_STOP_ATR_MULT * atr_val
            stop_loss = current_close - stop_loss_distance

            logger.info(f"[TREND-NONFX] {asset} | ALL CONDITIONS MET: LONG")
            logger.info(
                f"[TREND-NONFX] {asset} | ATR({ATR_PERIOD}) at entry = {atr_val:.6f} "
                f"(FIXED for trade lifetime)"
            )
            logger.info(
                f"[TREND-NONFX] {asset} | GENERATING SIGNAL: BUY @ {current_close:.5f} | "
                f"initial_trailing_stop={stop_loss:.5f} (entry - {TRAILING_STOP_ATR_MULT}x ATR)"
            )

            signal = {
                "strategy_name": STRATEGY_NAME,
                "asset": asset,
                "direction": "BUY",
                "entry_price": current_close,
                "stop_loss": stop_loss,
                "take_profit": None,
                "atr_at_entry": round(atr_val, 6),
                "signal_timestamp": signal_timestamp,
            }
            signal_id = insert_signal(signal)
            if signal_id:
                open_position({
                    "asset": asset,
                    "strategy_name": STRATEGY_NAME,
                    "direction": "BUY",
                    "entry_price": current_close,
                    "atr_at_entry": round(atr_val, 6),
                })
                signal["id"] = signal_id
                signal["status"] = "OPEN"
                logger.info(f"[TREND-NONFX] {asset} | Signal stored with id={signal_id}")
                return signal
        else:
            logger.info(f"[TREND-NONFX] {asset} | LONG conditions not met — no action")

        return None

    def check_exits(self) -> list[dict]:
        closed_signals = []
        positions = get_all_open_positions(strategy_name=STRATEGY_NAME)
        logger.info(f"[TREND-NONFX-EXIT] ====== Checking exits | {len(positions)} open position(s) ======")

        if not positions:
            return closed_signals

        for pos in positions:
            asset = pos["asset"]
            pos_id = pos["id"]
            entry_price = pos["entry_price"]
            direction = pos["direction"]
            atr_at_entry = pos["atr_at_entry"]
            logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} {direction} | entry={entry_price:.5f}")

            if atr_at_entry is None:
                logger.warning(f"[TREND-NONFX-EXIT] Position #{pos_id} | No atr_at_entry - skipping")
                continue

            logger.info(
                f"[TREND-NONFX-EXIT] Position #{pos_id} | ATR locked at entry: {atr_at_entry:.6f}"
            )

            try:
                candles = self.cache.get_candles(asset, TIMEFRAME, 300)
            except Exception as e:
                logger.error(f"[TREND-NONFX-EXIT] Position #{pos_id} | Exception fetching candles: {e}")
                continue

            if len(candles) < 2:
                logger.warning(f"[TREND-NONFX-EXIT] Position #{pos_id} | Insufficient candles: {len(candles)}")
                continue

            current_close = candles[-1]["close"]

            stored_highest = pos.get("highest_price_since_entry") or entry_price
            highest_close = max(stored_highest, current_close)
            update_position_tracking(pos_id, highest_price=highest_close)

            trailing_stop = highest_close - (atr_at_entry * TRAILING_STOP_ATR_MULT)

            logger.info(
                f"[TREND-NONFX-EXIT] Position #{pos_id} | BUY tracking | "
                f"close={current_close:.5f} | prev_highest={stored_highest:.5f} | "
                f"new_highest={highest_close:.5f} | "
                f"trailing_stop = {highest_close:.5f} - ({atr_at_entry:.6f} x {TRAILING_STOP_ATR_MULT}) = {trailing_stop:.5f}"
            )
            logger.info(
                f"[TREND-NONFX-EXIT] Position #{pos_id} | Exit check: "
                f"close ({current_close:.5f}) < trailing_stop ({trailing_stop:.5f}) = "
                f"{current_close < trailing_stop}"
            )

            if current_close < trailing_stop:
                exit_reason = (
                    f"Trailing stop hit | close={current_close:.5f}, "
                    f"stop={trailing_stop:.5f}, highest_since_entry={highest_close:.5f}, "
                    f"ATR_at_entry={atr_at_entry:.6f} (fixed)"
                )
                logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | EXIT: trailing_stop")
                active_sigs = get_active_signals(strategy_name=STRATEGY_NAME, asset=asset)
                for sig in active_sigs:
                    close_signal(sig["id"], exit_reason)
                close_position(STRATEGY_NAME, asset)
                closed_signals.append({**pos, "exit_price": current_close, "exit_reason": "trailing_stop"})
            else:
                logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | Holding LONG position")

        return closed_signals
