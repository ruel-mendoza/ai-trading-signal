import json
from datetime import datetime, timedelta
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

STRATEGY_MTF_EMA = "mtf_ema"
STRATEGY_TREND_FOLLOWING = "trend_following"
STRATEGY_SP500_MOMENTUM = "sp500_momentum"
STRATEGY_HIGHEST_LOWEST_FX = "highest_lowest_fx"


class StrategyEngine:
    def __init__(self, cache: CacheLayer):
        self.cache = cache

    def evaluate_all(self, symbols: Optional[list[str]] = None) -> list[dict]:
        results = []

        forex_symbols = symbols or ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "NZD/USD", "USD/CAD", "USD/CHF", "EUR/GBP"]

        for symbol in forex_symbols:
            mtf_result = self.evaluate_mtf_ema(symbol)
            if mtf_result:
                results.append(mtf_result)

            tf_result = self.evaluate_trend_following(symbol)
            if tf_result:
                results.append(tf_result)

        sp500_result = self.evaluate_sp500_momentum("SPX")
        if sp500_result:
            results.append(sp500_result)

        hlc_result = self.evaluate_highest_lowest_fx("EUR/USD")
        if hlc_result:
            results.append(hlc_result)

        return results

    def evaluate_mtf_ema(self, symbol: str) -> Optional[dict]:
        try:
            d1_candles = self.cache.get_candles(symbol, "D", 300)
            h4_candles = self.cache.get_candles(symbol, "4H", 300)
            h1_candles = self.cache.get_candles(symbol, "1H", 300)
        except Exception:
            return None

        if len(d1_candles) < 200 or len(h4_candles) < 200 or len(h1_candles) < 20:
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
            return None

        price_above_d1_emas = current_price > d1_ema200_val and current_price > d1_ema50_val
        h4_ema200_rising = h4_ema200_val > h4_ema200_prev
        dip_below_h4_50 = current_price < h4_ema50_val
        dip_within_1_atr = (h4_ema50_val - current_price) < h4_atr_val
        h1_closes_above_20_ema = current_price > h1_ema20_val

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
        try:
            d_candles = self.cache.get_candles(symbol, "D", 300)
        except Exception:
            return None

        if len(d_candles) < 100:
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
            return None

        if len(closes) >= 50:
            highest_50 = max(closes[-50:])
        else:
            return None

        price_at_50d_high = current_price >= highest_50
        sma50_above_sma100 = sma50_val > sma100_val

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

    def evaluate_sp500_momentum(self, symbol: str = "SPX") -> Optional[dict]:
        try:
            candles_30m = self.cache.get_candles(symbol, "30m", 300)
        except Exception:
            return None

        if len(candles_30m) < 21:
            return None

        closes = [c["close"] for c in candles_30m]
        highs = [c["high"] for c in candles_30m]
        lows = [c["low"] for c in candles_30m]

        rsi20 = IndicatorEngine.rsi(closes, 20)
        atr100 = IndicatorEngine.atr(highs, lows, closes, 100)

        current_rsi = rsi20[-1]
        prev_rsi = rsi20[-2] if len(rsi20) >= 2 else None
        atr_val = atr100[-1]
        current_price = closes[-1]

        if any(v is None for v in [current_rsi, prev_rsi]):
            return None

        rsi_crosses_above_70 = prev_rsi <= 70 and current_rsi > 70

        if rsi_crosses_above_70:
            trigger_candle_time = candles_30m[-1]["open_time"]
            if signal_exists(STRATEGY_SP500_MOMENTUM, symbol, trigger_candle_time, "30m"):
                return None

            trailing_mult = 2.0
            stop_loss_distance = (2 * atr_val) if atr_val else None
            stop_loss = (current_price - stop_loss_distance) if stop_loss_distance else None

            signal = {
                "strategy": STRATEGY_SP500_MOMENTUM,
                "symbol": symbol,
                "direction": "long",
                "entry_price": current_price,
                "stop_loss": stop_loss,
                "take_profit": None,
                "trailing_stop_atr_mult": trailing_mult,
                "trigger_candle_time": trigger_candle_time,
                "trigger_timeframe": "30m",
                "metadata": json.dumps({
                    "rsi20": current_rsi,
                    "prev_rsi20": prev_rsi,
                    "atr100": atr_val,
                    "trailing_stop_distance": stop_loss_distance,
                }),
            }
            signal_id = insert_signal(signal)
            if signal_id:
                signal["id"] = signal_id
                signal["status"] = "new"
                return signal

        return None

    def evaluate_highest_lowest_fx(self, symbol: str = "EUR/USD") -> Optional[dict]:
        try:
            h1_candles = self.cache.get_candles(symbol, "1H", 300)
            d_candles = self.cache.get_candles(symbol, "D", 60)
        except Exception:
            return None

        if len(h1_candles) < 50 or len(d_candles) < 50:
            return None

        now = datetime.utcnow()
        tokyo_8am_utc = 23
        ny_8am_utc = 13

        current_hour = now.hour
        in_tokyo_window = (tokyo_8am_utc <= current_hour or current_hour < tokyo_8am_utc + 2) if tokyo_8am_utc < 24 else (current_hour >= tokyo_8am_utc % 24 or current_hour < (tokyo_8am_utc % 24) + 2)
        in_ny_window = ny_8am_utc <= current_hour < ny_8am_utc + 2

        if not (in_tokyo_window or in_ny_window):
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

        for strategy_name in [STRATEGY_TREND_FOLLOWING, STRATEGY_SP500_MOMENTUM]:
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
                was_closed = False

                if direction == "long":
                    update_signal_tracking(sig["id"], highest_price=current_price)
                    highest = max(sig.get("highest_price") or sig["entry_price"], current_price)
                    trailing_stop = highest - (trailing_mult * atr_val)
                    if current_price <= trailing_stop:
                        close_signal(sig["id"], current_price, f"Trailing stop hit at {current_price:.5f} (highest: {highest:.5f}, stop: {trailing_stop:.5f})")
                        was_closed = True
                        closed_signals.append({**sig, "exit_price": current_price, "exit_reason": "trailing_stop"})
                elif direction == "short":
                    update_signal_tracking(sig["id"], lowest_price=current_price)
                    lowest = min(sig.get("lowest_price") or sig["entry_price"], current_price)
                    trailing_stop = lowest + (trailing_mult * atr_val)
                    if current_price >= trailing_stop:
                        close_signal(sig["id"], current_price, f"Trailing stop hit at {current_price:.5f} (lowest: {lowest:.5f}, stop: {trailing_stop:.5f})")
                        was_closed = True
                        closed_signals.append({**sig, "exit_price": current_price, "exit_reason": "trailing_stop"})

                if not was_closed and strategy_name == STRATEGY_SP500_MOMENTUM:
                    rsi20 = IndicatorEngine.rsi(closes, 20)
                    current_rsi = rsi20[-1] if rsi20 else None
                    if current_rsi is not None and current_rsi < 70:
                        close_signal(sig["id"], current_price, f"RSI below 70 ({current_rsi:.2f})")
                        closed_signals.append({**sig, "exit_price": current_price, "exit_reason": "rsi_exit"})

        return closed_signals
