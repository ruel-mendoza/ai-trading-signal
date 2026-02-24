import json
import logging
from datetime import datetime
from typing import Optional

from trading_engine.indicators import IndicatorEngine
from trading_engine.cache_layer import CacheLayer
from trading_engine.database import (
    signal_exists,
    insert_signal,
    get_active_signals,
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


class SP500MomentumStrategy:
    def __init__(self, cache: CacheLayer):
        self.cache = cache

    def _is_us_dst(self, dt: datetime) -> bool:
        year = dt.year
        march_second_sunday = 8
        for d in range(8, 15):
            if datetime(year, 3, d).weekday() == 6:
                march_second_sunday = d
                break
        dst_start = datetime(year, 3, march_second_sunday, 7, 0)

        november_first_sunday = 1
        for d in range(1, 8):
            if datetime(year, 11, d).weekday() == 6:
                november_first_sunday = d
                break
        dst_end = datetime(year, 11, november_first_sunday, 6, 0)

        return dst_start <= dt < dst_end

    def _get_et_offset(self, dt: datetime) -> int:
        return -4 if self._is_us_dst(dt) else -5

    def _is_within_arca_session(self, candle_time_str: str, utc_now: datetime) -> bool:
        et_offset = self._get_et_offset(utc_now)

        try:
            candle_utc = datetime.strptime(candle_time_str, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            try:
                candle_utc = datetime.strptime(candle_time_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                logger.warning(f"[SP500-MOM] Cannot parse candle time: {candle_time_str}")
                return False

        candle_et_hour = (candle_utc.hour + et_offset) % 24
        candle_et_min = candle_utc.minute
        candle_et_minutes = candle_et_hour * 60 + candle_et_min

        session_start_minutes = ARCA_SESSION_START_HOUR * 60 + ARCA_SESSION_START_MIN
        last_valid_minutes = LAST_VALID_CANDLE_HOUR * 60 + LAST_VALID_CANDLE_MIN

        in_session = session_start_minutes <= candle_et_minutes <= last_valid_minutes

        logger.info(
            f"[SP500-MOM] Session filter | candle_utc={candle_time_str} | "
            f"candle_ET={candle_et_hour:02d}:{candle_et_min:02d} | "
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

        utc_now = datetime.utcnow()
        is_dst = self._is_us_dst(utc_now)
        et_offset = self._get_et_offset(utc_now)
        logger.info(f"[SP500-MOM] {symbol} | US DST active: {is_dst} | ET offset: UTC{et_offset}")

        latest_candle = candles_30m[-1]
        candle_time_str = latest_candle["open_time"]

        if not self._is_within_arca_session(candle_time_str, utc_now):
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
            logger.info(f"[SP500-MOM] {symbol} | Signal already exists for candle {trigger_candle_time} - skipping")
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
