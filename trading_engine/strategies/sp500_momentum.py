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

logger = logging.getLogger("trading_engine.strategy.sp500_momentum")

STRATEGY_NAME = "sp500_momentum"
SYMBOL = "SPX"
TIMEFRAME = "30m"
RSI_PERIOD = 20
ATR_PERIOD = 100
RSI_THRESHOLD = 70
TRAILING_STOP_ATR_MULT = 2.0
MIN_BARS_REQUIRED = max(RSI_PERIOD + 1, ATR_PERIOD + 1)

ARCA_SESSION_START_HOUR = 9
ARCA_SESSION_START_MIN = 30
ARCA_SESSION_END_HOUR = 16
ARCA_SESSION_END_MIN = 0
LAST_VALID_CANDLE_HOUR = 15
LAST_VALID_CANDLE_MIN = 30

ET_ZONE = ZoneInfo("America/New_York")


class SP500MomentumStrategy:
    def __init__(self, cache: CacheLayer):
        self.cache = cache

    def _is_within_arca_session(self, candle_time_str: str) -> bool:
        try:
            candle_utc = datetime.strptime(candle_time_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                candle_utc = datetime.strptime(candle_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                logger.warning(f"[SP500-MOM] Cannot parse candle time: {candle_time_str}")
                return False

        candle_et = candle_utc.astimezone(ET_ZONE)
        candle_et_minutes = candle_et.hour * 60 + candle_et.minute

        session_start_minutes = ARCA_SESSION_START_HOUR * 60 + ARCA_SESSION_START_MIN
        last_valid_minutes = LAST_VALID_CANDLE_HOUR * 60 + LAST_VALID_CANDLE_MIN

        in_session = session_start_minutes <= candle_et_minutes <= last_valid_minutes

        is_dst = candle_et.dst() and candle_et.dst().total_seconds() > 0
        tz_abbr = "EDT" if is_dst else "EST"

        logger.info(
            f"[SP500-MOM] Session filter | candle_utc={candle_time_str} | "
            f"candle_ET={candle_et.strftime('%H:%M')} {tz_abbr} | "
            f"ARCA window=09:30-15:30 ET (last valid) | "
            f"in_session={in_session}"
        )
        return in_session

    def _has_open_trade(self) -> bool:
        active = get_active_signals(strategy=STRATEGY_NAME, symbol=SYMBOL)
        has_open = len(active) > 0
        logger.info(f"[SP500-MOM] Open trade check | active_signals={len(active)} | has_open_trade={has_open}")
        return has_open

    def evaluate(self, symbol: str = SYMBOL) -> Optional[dict]:
        logger.info(f"[SP500-MOM] ====== Evaluating {symbol} ======")

        try:
            candles_30m = self.cache.get_candles(symbol, TIMEFRAME, 300)
        except Exception as e:
            logger.error(f"[SP500-MOM] {symbol} | Exception fetching candles: {e}")
            return None

        logger.info(f"[SP500-MOM] {symbol} | 30m candles: {len(candles_30m)} (need {MIN_BARS_REQUIRED})")
        if len(candles_30m) < MIN_BARS_REQUIRED:
            logger.warning(f"[SP500-MOM] {symbol} | INSUFFICIENT DATA - have {len(candles_30m)}, need {MIN_BARS_REQUIRED}")
            return None

        now_et = datetime.now(ET_ZONE)
        is_dst = now_et.dst() and now_et.dst().total_seconds() > 0
        et_offset = "-4" if is_dst else "-5"
        logger.info(f"[SP500-MOM] {symbol} | US DST active: {is_dst} | ET offset: UTC{et_offset}")

        latest_candle = candles_30m[-1]
        candle_time_str = latest_candle["open_time"]

        if not self._is_within_arca_session(candle_time_str):
            logger.info(f"[SP500-MOM] {symbol} | Outside ARCA session - skipping")
            return None

        closes = [c["close"] for c in candles_30m]
        highs = [c["high"] for c in candles_30m]
        lows = [c["low"] for c in candles_30m]

        rsi_values = IndicatorEngine.rsi(closes, RSI_PERIOD)
        atr_values = IndicatorEngine.atr(highs, lows, closes, ATR_PERIOD)

        current_rsi = rsi_values[-1]
        prev_rsi = rsi_values[-2] if len(rsi_values) >= 2 else None
        atr_val = atr_values[-1]
        current_price = closes[-1]

        logger.info(f"[SP500-MOM] {symbol} | price={current_price:.2f}")
        logger.info(f"[SP500-MOM] {symbol} | RSI({RSI_PERIOD}): current={current_rsi:.4f}, prev={prev_rsi:.4f}" if prev_rsi is not None else f"[SP500-MOM] {symbol} | RSI({RSI_PERIOD}): current={current_rsi}, prev=None")
        logger.info(f"[SP500-MOM] {symbol} | ATR({ATR_PERIOD}): {atr_val:.5f}" if atr_val is not None else f"[SP500-MOM] {symbol} | ATR({ATR_PERIOD}): None")

        if any(v is None for v in [current_rsi, prev_rsi, atr_val]):
            none_indicators = []
            if current_rsi is None: none_indicators.append("RSI_current")
            if prev_rsi is None: none_indicators.append("RSI_prev")
            if atr_val is None: none_indicators.append(f"ATR{ATR_PERIOD}")
            logger.warning(f"[SP500-MOM] {symbol} | Indicators returned None: {none_indicators}")
            return None

        cond_prev_below = prev_rsi < RSI_THRESHOLD
        cond_curr_above = current_rsi >= RSI_THRESHOLD
        rsi_crosses_above = cond_prev_below and cond_curr_above

        logger.info(f"[SP500-MOM] {symbol} | Condition 1 - Prev RSI ({prev_rsi:.4f}) < {RSI_THRESHOLD}: {cond_prev_below}")
        logger.info(f"[SP500-MOM] {symbol} | Condition 2 - Current RSI ({current_rsi:.4f}) >= {RSI_THRESHOLD}: {cond_curr_above}")
        logger.info(f"[SP500-MOM] {symbol} | RSI cross above {RSI_THRESHOLD}: {rsi_crosses_above}")

        if not rsi_crosses_above:
            logger.info(f"[SP500-MOM] {symbol} | ALL CONDITIONS MET: False")
            return None

        if self._has_open_trade():
            logger.info(f"[SP500-MOM] {symbol} | Existing open trade - skipping new entry")
            return None

        trigger_candle_time = candle_time_str
        if signal_exists(STRATEGY_NAME, symbol, trigger_candle_time, TIMEFRAME):
            logger.info(f"[SP500-MOM] {symbol} | Signal already exists for candle {trigger_candle_time} - idempotency check passed, skipping duplicate")
            return None

        stop_loss_distance = TRAILING_STOP_ATR_MULT * atr_val
        stop_loss = current_price - stop_loss_distance

        logger.info(f"[SP500-MOM] {symbol} | ALL CONDITIONS MET: True")
        logger.info(f"[SP500-MOM] {symbol} | GENERATING SIGNAL: LONG @ {current_price:.2f} | SL={stop_loss:.2f} | ATR={atr_val:.5f} (stored for trade duration)")

        signal = {
            "strategy": STRATEGY_NAME,
            "symbol": symbol,
            "direction": "long",
            "entry_price": current_price,
            "stop_loss": stop_loss,
            "take_profit": None,
            "trailing_stop_atr_mult": TRAILING_STOP_ATR_MULT,
            "trigger_candle_time": trigger_candle_time,
            "trigger_timeframe": TIMEFRAME,
            "metadata": json.dumps({
                "rsi20_current": round(current_rsi, 4),
                "rsi20_prev": round(prev_rsi, 4),
                "atr100_at_entry": round(atr_val, 6),
                "entry_price": round(current_price, 2),
                "stop_loss_distance": round(stop_loss_distance, 6),
                "session": "ARCA",
                "dst_active": is_dst,
            }),
        }
        signal_id = insert_signal(signal)
        if signal_id:
            signal["id"] = signal_id
            signal["status"] = "new"
            logger.info(f"[SP500-MOM] {symbol} | Signal stored with id={signal_id}")
            return signal

        return None

    def check_exits(self) -> list[dict]:
        closed_signals = []
        active = get_active_signals(strategy=STRATEGY_NAME)
        logger.info(f"[SP500-MOM-EXIT] ====== Checking exits | {len(active)} active signal(s) ======")

        if not active:
            return closed_signals

        for sig in active:
            symbol = sig["symbol"]
            sig_id = sig["id"]
            entry_price = sig["entry_price"]
            logger.info(f"[SP500-MOM-EXIT] Signal #{sig_id} | {symbol} | entry={entry_price:.2f}")

            metadata = {}
            if sig.get("metadata"):
                try:
                    metadata = json.loads(sig["metadata"])
                except (json.JSONDecodeError, TypeError):
                    pass

            atr_at_entry = metadata.get("atr100_at_entry")
            if atr_at_entry is None:
                logger.warning(f"[SP500-MOM-EXIT] Signal #{sig_id} | No atr100_at_entry in metadata - skipping")
                continue

            try:
                candles = self.cache.get_candles(symbol, TIMEFRAME, 300)
            except Exception as e:
                logger.error(f"[SP500-MOM-EXIT] Signal #{sig_id} | Exception fetching candles: {e}")
                continue

            if len(candles) < RSI_PERIOD + 1:
                logger.warning(f"[SP500-MOM-EXIT] Signal #{sig_id} | Insufficient candles: {len(candles)} (need {RSI_PERIOD + 1})")
                continue

            latest_candle = candles[-1]
            candle_time_str = latest_candle["open_time"]

            in_session = self._is_within_arca_session(candle_time_str)
            logger.info(f"[SP500-MOM-EXIT] Signal #{sig_id} | Session valid: {in_session}")

            closes = [c["close"] for c in candles]
            current_close = closes[-1]

            stored_highest = sig.get("highest_price") or entry_price
            highest_close = max(stored_highest, current_close)
            update_signal_tracking(sig_id, highest_price=highest_close)

            trailing_stop_level = highest_close - (atr_at_entry * TRAILING_STOP_ATR_MULT)

            rsi_values = IndicatorEngine.rsi(closes, RSI_PERIOD)
            current_rsi = rsi_values[-1] if rsi_values else None
            prev_rsi = rsi_values[-2] if rsi_values and len(rsi_values) >= 2 else None

            logger.info(
                f"[SP500-MOM-EXIT] Signal #{sig_id} | close={current_close:.2f} | "
                f"highest_close_since_entry={highest_close:.2f} | "
                f"ATR_at_entry={atr_at_entry:.6f} | "
                f"trailing_stop={trailing_stop_level:.2f}"
            )
            prev_rsi_str = f"{prev_rsi:.4f}" if prev_rsi is not None else "None"
            curr_rsi_str = f"{current_rsi:.4f}" if current_rsi is not None else "None"
            logger.info(
                f"[SP500-MOM-EXIT] Signal #{sig_id} | RSI({RSI_PERIOD}): "
                f"prev={prev_rsi_str}, current={curr_rsi_str}"
            )

            trailing_stop_hit = current_close < trailing_stop_level

            rsi_cross_down = False
            if current_rsi is not None and prev_rsi is not None:
                rsi_cross_down = prev_rsi >= RSI_THRESHOLD and current_rsi < RSI_THRESHOLD

            logger.info(
                f"[SP500-MOM-EXIT] Signal #{sig_id} | "
                f"trailing_stop_hit={trailing_stop_hit} (close {current_close:.2f} < stop {trailing_stop_level:.2f}) | "
                f"rsi_cross_down={rsi_cross_down} (prev {prev_rsi_str} >= {RSI_THRESHOLD} AND curr {curr_rsi_str} < {RSI_THRESHOLD})"
            )

            if trailing_stop_hit and rsi_cross_down:
                exit_reason = (
                    f"Trailing stop AND RSI cross-down on same candle | "
                    f"close={current_close:.2f}, stop={trailing_stop_level:.2f}, "
                    f"highest={highest_close:.2f}, ATR_entry={atr_at_entry:.6f}, "
                    f"RSI prev={prev_rsi:.4f} -> curr={current_rsi:.4f}"
                )
                logger.info(f"[SP500-MOM-EXIT] Signal #{sig_id} | BOTH triggers - producing single exit: trailing_stop+rsi")
                close_signal(sig_id, current_close, exit_reason)
                closed_signals.append({**sig, "exit_price": current_close, "exit_reason": "trailing_stop+rsi"})

            elif trailing_stop_hit:
                exit_reason = (
                    f"Trailing stop hit | close={current_close:.2f}, "
                    f"stop={trailing_stop_level:.2f}, highest={highest_close:.2f}, "
                    f"ATR_entry={atr_at_entry:.6f}"
                )
                logger.info(f"[SP500-MOM-EXIT] Signal #{sig_id} | EXIT: trailing_stop")
                close_signal(sig_id, current_close, exit_reason)
                closed_signals.append({**sig, "exit_price": current_close, "exit_reason": "trailing_stop"})

            elif rsi_cross_down:
                exit_reason = (
                    f"RSI cross below {RSI_THRESHOLD} | "
                    f"prev={prev_rsi:.4f}, curr={current_rsi:.4f}, "
                    f"close={current_close:.2f}"
                )
                logger.info(f"[SP500-MOM-EXIT] Signal #{sig_id} | EXIT: rsi_cross_down")
                close_signal(sig_id, current_close, exit_reason)
                closed_signals.append({**sig, "exit_price": current_close, "exit_reason": "rsi_cross_down"})

            else:
                logger.info(f"[SP500-MOM-EXIT] Signal #{sig_id} | No exit triggered - holding position")

        return closed_signals
