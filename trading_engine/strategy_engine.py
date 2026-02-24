import json
import logging
from datetime import datetime, timedelta
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
from trading_engine.strategies.sp500_momentum import SP500MomentumStrategy

logger = logging.getLogger("trading_engine.strategy")

STRATEGY_MTF_EMA = "mtf_ema"
STRATEGY_TREND_FOLLOWING = "trend_following"
STRATEGY_SP500_MOMENTUM = "sp500_momentum"
STRATEGY_HIGHEST_LOWEST_FX = "highest_lowest_fx"


class StrategyEngine:
    def __init__(self, cache: CacheLayer):
        self.cache = cache
        self.sp500_strategy = SP500MomentumStrategy(cache)

    def evaluate_all(self, symbols: Optional[list[str]] = None) -> list[dict]:
        results = []

        forex_symbols = symbols or ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "NZD/USD", "USD/CAD", "USD/CHF", "EUR/GBP"]
        logger.info(f"[STRATEGY-ENGINE] evaluate_all called | symbols={forex_symbols}")

        for symbol in forex_symbols:
            mtf_result = self.evaluate_mtf_ema(symbol)
            if mtf_result:
                results.append(mtf_result)

            tf_result = self.evaluate_trend_following(symbol)
            if tf_result:
                results.append(tf_result)

        sp500_result = self.sp500_strategy.evaluate("SPX")
        if sp500_result:
            results.append(sp500_result)

        hlc_result = self.evaluate_highest_lowest_fx("EUR/USD")
        if hlc_result:
            results.append(hlc_result)

        return results

    def evaluate_mtf_ema(self, symbol: str) -> Optional[dict]:
        logger.info(f"[MTF-EMA] ====== Evaluating {symbol} ======")
        try:
            d1_candles = self.cache.get_candles(symbol, "D", 300)
            h4_candles = self.cache.get_candles(symbol, "4H", 300)
            h1_candles = self.cache.get_candles(symbol, "1H", 300)
        except Exception as e:
            logger.error(f"[MTF-EMA] {symbol} | Exception fetching candles: {e}")
            return None

        logger.info(f"[MTF-EMA] {symbol} | Candle counts: D1={len(d1_candles)} (need 200), H4={len(h4_candles)} (need 200), H1={len(h1_candles)} (need 20)")
        if len(d1_candles) < 200 or len(h4_candles) < 200 or len(h1_candles) < 20:
            logger.warning(f"[MTF-EMA] {symbol} | INSUFFICIENT DATA - D1: {'OK' if len(d1_candles) >= 200 else 'FAIL'}, H4: {'OK' if len(h4_candles) >= 200 else 'FAIL'}, H1: {'OK' if len(h1_candles) >= 20 else 'FAIL'}")
            return None

        d1_closes = [c["close"] for c in d1_candles]
        d1_ema200 = IndicatorEngine.ema(d1_closes, 200)
        d1_ema50 = IndicatorEngine.ema(d1_closes, 50)

        h4_closes = [c["close"] for c in h4_candles]
        h4_highs = [c["high"] for c in h4_candles]
        h4_lows = [c["low"] for c in h4_candles]
        h4_ema200 = IndicatorEngine.ema(h4_closes, 200)
        h4_ema50 = IndicatorEngine.ema(h4_closes, 50)
        h4_atr100 = IndicatorEngine.atr(h4_highs, h4_lows, h4_closes, 100)

        h1_closes = [c["close"] for c in h1_candles]
        h1_ema20 = IndicatorEngine.ema(h1_closes, 20)

        current_price = h1_closes[-1]
        d1_ema200_val = d1_ema200[-1]
        d1_ema50_val = d1_ema50[-1]
        h4_ema200_val = h4_ema200[-1]
        h4_ema200_prev = h4_ema200[-2] if len(h4_ema200) >= 2 else None
        h4_ema50_val = h4_ema50[-1]
        h4_atr_val = h4_atr100[-1]
        h1_ema20_val = h1_ema20[-1]

        if any(v is None for v in [d1_ema200_val, d1_ema50_val, h4_ema200_val, h4_ema200_prev, h4_ema50_val, h4_atr_val, h1_ema20_val]):
            none_indicators = []
            if d1_ema200_val is None: none_indicators.append("D1_EMA200")
            if d1_ema50_val is None: none_indicators.append("D1_EMA50")
            if h4_ema200_val is None: none_indicators.append("H4_EMA200")
            if h4_ema200_prev is None: none_indicators.append("H4_EMA200_prev")
            if h4_ema50_val is None: none_indicators.append("H4_EMA50")
            if h4_atr_val is None: none_indicators.append("H4_ATR100")
            if h1_ema20_val is None: none_indicators.append("H1_EMA20")
            logger.warning(f"[MTF-EMA] {symbol} | Indicators returned None: {none_indicators}")
            return None

        price_above_d1_emas = current_price > d1_ema200_val and current_price > d1_ema50_val
        h4_ema200_rising = h4_ema200_val > h4_ema200_prev
        dip_below_h4_50 = current_price < h4_ema50_val
        dip_within_1_atr = (h4_ema50_val - current_price) < h4_atr_val
        h1_closes_above_20_ema = current_price > h1_ema20_val

        logger.info(f"[MTF-EMA] {symbol} | price={current_price:.5f}")
        logger.info(f"[MTF-EMA] {symbol} | Condition 1 - Price > D1 EMA200 ({d1_ema200_val:.5f}) AND D1 EMA50 ({d1_ema50_val:.5f}): {price_above_d1_emas}")
        logger.info(f"[MTF-EMA] {symbol} | Condition 2 - H4 EMA200 rising ({h4_ema200_val:.5f} > {h4_ema200_prev:.5f}): {h4_ema200_rising}")
        logger.info(f"[MTF-EMA] {symbol} | Condition 3 - Price dips below H4 EMA50 ({h4_ema50_val:.5f}): {dip_below_h4_50}")
        logger.info(f"[MTF-EMA] {symbol} | Condition 4 - Dip within 1 ATR ({h4_atr_val:.5f}): {dip_within_1_atr}")
        logger.info(f"[MTF-EMA] {symbol} | Condition 5 - H1 closes above EMA20 ({h1_ema20_val:.5f}): {h1_closes_above_20_ema}")
        logger.info(f"[MTF-EMA] {symbol} | ALL CONDITIONS MET: {price_above_d1_emas and h4_ema200_rising and dip_below_h4_50 and dip_within_1_atr and h1_closes_above_20_ema}")

        if price_above_d1_emas and h4_ema200_rising and dip_below_h4_50 and dip_within_1_atr and h1_closes_above_20_ema:
            trigger_candle_time = h1_candles[-1]["open_time"]
            if signal_exists(STRATEGY_MTF_EMA, symbol, trigger_candle_time, "1H"):
                return None

            signal = {
                "strategy": STRATEGY_MTF_EMA,
                "symbol": symbol,
                "direction": "long",
                "entry_price": current_price,
                "stop_loss": current_price - (2 * h4_atr_val),
                "take_profit": current_price + (3 * h4_atr_val),
                "trailing_stop_atr_mult": None,
                "trigger_candle_time": trigger_candle_time,
                "trigger_timeframe": "1H",
                "metadata": json.dumps({
                    "d1_ema200": d1_ema200_val,
                    "d1_ema50": d1_ema50_val,
                    "h4_ema200": h4_ema200_val,
                    "h4_ema50": h4_ema50_val,
                    "h4_atr": h4_atr_val,
                    "h1_ema20": h1_ema20_val,
                }),
            }
            signal_id = insert_signal(signal)
            if signal_id:
                signal["id"] = signal_id
                signal["status"] = "new"
                return signal

        return None

    def evaluate_trend_following(self, symbol: str) -> Optional[dict]:
        logger.info(f"[TREND-FOLLOW] ====== Evaluating {symbol} ======")
        try:
            d_candles = self.cache.get_candles(symbol, "D", 300)
        except Exception as e:
            logger.error(f"[TREND-FOLLOW] {symbol} | Exception fetching candles: {e}")
            return None

        logger.info(f"[TREND-FOLLOW] {symbol} | Daily candles: {len(d_candles)} (need 100)")
        if len(d_candles) < 100:
            logger.warning(f"[TREND-FOLLOW] {symbol} | INSUFFICIENT DATA - have {len(d_candles)}, need 100")
            return None

        closes = [c["close"] for c in d_candles]
        highs = [c["high"] for c in d_candles]
        lows = [c["low"] for c in d_candles]

        sma50 = IndicatorEngine.sma(closes, 50)
        sma100 = IndicatorEngine.sma(closes, 100)
        atr100 = IndicatorEngine.atr(highs, lows, closes, 100)

        current_price = closes[-1]
        sma50_val = sma50[-1]
        sma100_val = sma100[-1]
        atr_val = atr100[-1]

        if any(v is None for v in [sma50_val, sma100_val, atr_val]):
            none_list = []
            if sma50_val is None: none_list.append("SMA50")
            if sma100_val is None: none_list.append("SMA100")
            if atr_val is None: none_list.append("ATR100")
            logger.warning(f"[TREND-FOLLOW] {symbol} | Indicators returned None: {none_list}")
            return None

        if len(closes) >= 50:
            highest_50 = max(closes[-50:])
        else:
            logger.warning(f"[TREND-FOLLOW] {symbol} | Not enough closes for 50-day high (have {len(closes)})")
            return None

        price_at_50d_high = current_price >= highest_50
        sma50_above_sma100 = sma50_val > sma100_val

        logger.info(f"[TREND-FOLLOW] {symbol} | price={current_price:.5f}")
        logger.info(f"[TREND-FOLLOW] {symbol} | Condition 1 - Price >= 50-day high ({highest_50:.5f}): {price_at_50d_high}")
        logger.info(f"[TREND-FOLLOW] {symbol} | Condition 2 - SMA50 ({sma50_val:.5f}) > SMA100 ({sma100_val:.5f}): {sma50_above_sma100}")
        logger.info(f"[TREND-FOLLOW] {symbol} | ALL CONDITIONS MET: {price_at_50d_high and sma50_above_sma100}")

        if price_at_50d_high and sma50_above_sma100:
            trigger_candle_time = d_candles[-1]["open_time"]
            if signal_exists(STRATEGY_TREND_FOLLOWING, symbol, trigger_candle_time, "D"):
                return None

            direction = "long"
            stop_loss = current_price - (3 * atr_val)
            trailing_mult = 3.0

            signal = {
                "strategy": STRATEGY_TREND_FOLLOWING,
                "symbol": symbol,
                "direction": direction,
                "entry_price": current_price,
                "stop_loss": stop_loss,
                "take_profit": None,
                "trailing_stop_atr_mult": trailing_mult,
                "trigger_candle_time": trigger_candle_time,
                "trigger_timeframe": "D",
                "metadata": json.dumps({
                    "sma50": sma50_val,
                    "sma100": sma100_val,
                    "atr100": atr_val,
                    "highest_50d_close": highest_50,
                    "trailing_stop_distance": 3 * atr_val,
                }),
            }
            signal_id = insert_signal(signal)
            if signal_id:
                signal["id"] = signal_id
                signal["status"] = "new"
                return signal

        return None

    @staticmethod
    def _is_us_dst(dt: datetime) -> bool:
        et_zone = ZoneInfo("America/New_York")
        if dt.tzinfo is None:
            aware_dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        else:
            aware_dt = dt
        et_dt = aware_dt.astimezone(et_zone)
        return bool(et_dt.dst() and et_dt.dst().total_seconds() > 0)

    def evaluate_sp500_momentum(self, symbol: str = "SPX") -> Optional[dict]:
        return self.sp500_strategy.evaluate(symbol)

    def evaluate_highest_lowest_fx(self, symbol: str = "EUR/USD") -> Optional[dict]:
        logger.info(f"[HLC-FX] ====== Evaluating {symbol} ======")
        try:
            h1_candles = self.cache.get_candles(symbol, "1H", 300)
            d_candles = self.cache.get_candles(symbol, "D", 60)
        except Exception as e:
            logger.error(f"[HLC-FX] {symbol} | Exception fetching candles: {e}")
            return None

        logger.info(f"[HLC-FX] {symbol} | Candle counts: H1={len(h1_candles)} (need 50), D={len(d_candles)} (need 50)")
        if len(h1_candles) < 50 or len(d_candles) < 50:
            logger.warning(f"[HLC-FX] {symbol} | INSUFFICIENT DATA")
            return None

        now = datetime.utcnow()
        tokyo_8am_utc = 23
        ny_8am_utc = 13

        current_hour = now.hour
        in_tokyo_window = (current_hour >= tokyo_8am_utc or current_hour < (tokyo_8am_utc + 2) % 24)
        in_ny_window = ny_8am_utc <= current_hour < ny_8am_utc + 2

        is_dst = self._is_us_dst(now)
        forex_close_5pm_et_utc = 21 if is_dst else 22
        logger.info(f"[TIMEZONE] UTC now: {now.strftime('%Y-%m-%d %H:%M:%S')} | hour={current_hour}")
        logger.info(f"[TIMEZONE] Tokyo 8am = UTC {tokyo_8am_utc}:00 (JST=UTC+9) | window: UTC {tokyo_8am_utc}:00-{(tokyo_8am_utc+2)%24}:00")
        logger.info(f"[TIMEZONE] NY 8am = UTC {ny_8am_utc}:00 (ET=UTC-5) | window: UTC {ny_8am_utc}:00-{ny_8am_utc+2}:00")
        logger.info(f"[TIMEZONE] US DST active: {is_dst} | Forex daily close (5pm ET) = UTC {forex_close_5pm_et_utc}:00")
        logger.info(f"[TIMEZONE] In Tokyo window: {in_tokyo_window} | In NY window: {in_ny_window}")

        if not (in_tokyo_window or in_ny_window):
            logger.info(f"[HLC-FX] {symbol} | Outside both Tokyo and NY windows - skipping")
            return None

        d_closes = [c["close"] for c in d_candles]
        d_highs = [c["high"] for c in d_candles]
        d_lows = [c["low"] for c in d_candles]

        atr100 = IndicatorEngine.atr(d_highs, d_lows, d_closes, 100)
        atr_val = atr100[-1] if atr100 and atr100[-1] is not None else None

        highest_50d = max(d_closes[-50:])
        lowest_50d = min(d_closes[-50:])

        current_price = h1_candles[-1]["close"]
        trigger_candle_time = h1_candles[-1]["open_time"]

        if signal_exists(STRATEGY_HIGHEST_LOWEST_FX, symbol, trigger_candle_time, "1H"):
            return None

        session = "tokyo" if in_tokyo_window else "new_york"
        signal_data = None

        if in_tokyo_window:
            if current_price >= highest_50d:
                signal_data = {
                    "direction": "long",
                    "reason": f"Price ({current_price:.5f}) at/above 50-day highest close ({highest_50d:.5f}) during Tokyo session",
                }
            elif current_price <= lowest_50d:
                signal_data = {
                    "direction": "short",
                    "reason": f"Price ({current_price:.5f}) at/below 50-day lowest close ({lowest_50d:.5f}) during Tokyo session",
                }

        if in_ny_window:
            if current_price >= highest_50d:
                signal_data = {
                    "direction": "long",
                    "reason": f"Price ({current_price:.5f}) at/above 50-day highest close ({highest_50d:.5f}) during NY session",
                }
            elif current_price <= lowest_50d:
                if current_price > lowest_50d * 0.998:
                    signal_data = {
                        "direction": "long",
                        "reason": f"Price ({current_price:.5f}) near 50-day lowest close ({lowest_50d:.5f}) - potential reversal during NY session",
                    }

        if signal_data is None:
            return None

        stop_distance = (2 * atr_val) if atr_val else 0.0050
        if signal_data["direction"] == "long":
            stop_loss = current_price - stop_distance
            take_profit = current_price + (3 * stop_distance)
        else:
            stop_loss = current_price + stop_distance
            take_profit = current_price - (3 * stop_distance)

        signal = {
            "strategy": STRATEGY_HIGHEST_LOWEST_FX,
            "symbol": symbol,
            "direction": signal_data["direction"],
            "entry_price": current_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trailing_stop_atr_mult": None,
            "trigger_candle_time": trigger_candle_time,
            "trigger_timeframe": "1H",
            "metadata": json.dumps({
                "session": session,
                "reason": signal_data["reason"],
                "highest_50d": highest_50d,
                "lowest_50d": lowest_50d,
                "atr100": atr_val,
            }),
        }
        signal_id = insert_signal(signal)
        if signal_id:
            signal["id"] = signal_id
            signal["status"] = "new"
            return signal

        return None

    def check_exit_conditions(self) -> list[dict]:
        closed_signals = []

        for strategy_name in [STRATEGY_TREND_FOLLOWING]:
            active = get_active_signals(strategy=strategy_name)
            for sig in active:
                symbol = sig["symbol"]
                trailing_mult = sig.get("trailing_stop_atr_mult")
                if not trailing_mult:
                    continue

                tf = sig["trigger_timeframe"]
                try:
                    candles = self.cache.get_candles(symbol, tf, 300)
                except Exception:
                    continue
                if not candles:
                    continue

                current_price = candles[-1]["close"]
                closes = [c["close"] for c in candles]
                highs = [c["high"] for c in candles]
                lows = [c["low"] for c in candles]
                atr100 = IndicatorEngine.atr(highs, lows, closes, 100)
                atr_val = atr100[-1] if atr100 and atr100[-1] is not None else None
                if atr_val is None:
                    continue

                direction = sig["direction"]

                if direction == "long":
                    update_signal_tracking(sig["id"], highest_price=current_price)
                    highest = max(sig.get("highest_price") or sig["entry_price"], current_price)
                    trailing_stop = highest - (trailing_mult * atr_val)
                    if current_price <= trailing_stop:
                        close_signal(sig["id"], current_price, f"Trailing stop hit at {current_price:.5f} (highest: {highest:.5f}, stop: {trailing_stop:.5f})")
                        closed_signals.append({**sig, "exit_price": current_price, "exit_reason": "trailing_stop"})
                elif direction == "short":
                    update_signal_tracking(sig["id"], lowest_price=current_price)
                    lowest = min(sig.get("lowest_price") or sig["entry_price"], current_price)
                    trailing_stop = lowest + (trailing_mult * atr_val)
                    if current_price >= trailing_stop:
                        close_signal(sig["id"], current_price, f"Trailing stop hit at {current_price:.5f} (lowest: {lowest:.5f}, stop: {trailing_stop:.5f})")
                        closed_signals.append({**sig, "exit_price": current_price, "exit_reason": "trailing_stop"})

        sp500_exits = self.sp500_strategy.check_exits()
        closed_signals.extend(sp500_exits)

        return closed_signals
