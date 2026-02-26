import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional
import pandas as pd

from trading_engine.indicators import IndicatorEngine
from trading_engine.cache_layer import CacheLayer
from trading_engine.database import (
    signal_exists,
    insert_signal,
    get_active_signals,
    close_signal,
    open_position,
    get_open_position,
    get_all_open_positions,
    update_position_tracking,
    close_position,
    has_open_position,
)
from trading_engine.strategies.sp500_momentum import SP500MomentumStrategy
from trading_engine.strategies.highest_lowest import HighestLowestFXStrategy
from trading_engine.strategies.trend_forex import ForexTrendFollowingStrategy
from trading_engine.strategies.trend_non_forex import NonForexTrendFollowingStrategy

logger = logging.getLogger("trading_engine.strategy")

STRATEGY_MTF_EMA = "mtf_ema"
STRATEGY_TREND_FOLLOWING = "trend_following"
STRATEGY_SP500_MOMENTUM = "sp500_momentum"
STRATEGY_HIGHEST_LOWEST_FX = "highest_lowest_fx"
STRATEGY_TREND_FOREX = "trend_forex"
STRATEGY_TREND_NON_FOREX = "trend_non_forex"


class StrategyEngine:
    def __init__(self, cache: CacheLayer):
        self.cache = cache
        self.sp500_strategy = SP500MomentumStrategy(cache)
        self.highest_lowest_strategy = HighestLowestFXStrategy(cache)
        self.trend_forex_strategy = ForexTrendFollowingStrategy(cache)
        self.trend_non_forex_strategy = NonForexTrendFollowingStrategy(cache)

    def evaluate_all(self, symbols: Optional[list[str]] = None) -> list[dict]:
        results = []

        forex_symbols = symbols or ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "NZD/USD", "USD/CAD", "USD/CHF", "EUR/GBP"]
        logger.info(f"[STRATEGY-ENGINE] evaluate_all called | symbols={forex_symbols}")

        for asset in forex_symbols:
            mtf_result = self.evaluate_mtf_ema(asset)
            if mtf_result:
                results.append(mtf_result)

            tf_result = self.evaluate_trend_following(asset)
            if tf_result:
                results.append(tf_result)

        try:
            sp500_candles = self.cache.get_candles("SPX", "30m", 300)
            sp500_df = pd.DataFrame(sp500_candles) if sp500_candles else pd.DataFrame()
        except Exception as e:
            logger.error(f"[STRATEGY-ENGINE] Failed to fetch SPX 30m candles: {e}")
            sp500_df = pd.DataFrame()

        sp500_open_pos = get_open_position(STRATEGY_SP500_MOMENTUM, "SPX")
        sp500_result = self.sp500_strategy.evaluate("SPX", "30m", sp500_df, sp500_open_pos)
        if sp500_result.is_entry:
            results.append(sp500_result.metadata.get("signal", {}))

        try:
            h1_candles = self.cache.get_candles("EUR/USD", "1H", 300)
            hlc_df = pd.DataFrame(h1_candles) if h1_candles else pd.DataFrame()
        except Exception as e:
            logger.error(f"[STRATEGY-ENGINE] Failed to fetch EUR/USD 1H candles for HLC: {e}")
            hlc_df = pd.DataFrame()

        hlc_open_pos = get_open_position(STRATEGY_HIGHEST_LOWEST_FX, "EUR/USD")
        hlc_result = self.highest_lowest_strategy.evaluate("EUR/USD", "1H", hlc_df, hlc_open_pos)
        if hlc_result.is_entry:
            results.append(hlc_result.metadata.get("signal", {}))

        from trading_engine.strategies.trend_forex import TARGET_SYMBOLS as TREND_FOREX_SYMBOLS
        for asset in TREND_FOREX_SYMBOLS:
            tf_forex_result = self.trend_forex_strategy.evaluate(asset)
            if tf_forex_result:
                results.append(tf_forex_result)

        from trading_engine.strategies.trend_non_forex import TARGET_SYMBOLS as TREND_NON_FOREX_SYMBOLS, TIMEFRAME as TNF_TIMEFRAME
        for asset in TREND_NON_FOREX_SYMBOLS:
            try:
                candles = self.cache.get_candles(asset, TNF_TIMEFRAME, 300)
            except Exception:
                continue
            if not candles:
                continue
            df = pd.DataFrame(candles)
            open_pos = get_open_position(STRATEGY_TREND_NON_FOREX, asset)
            tnf_result = self.trend_non_forex_strategy.evaluate(asset, TNF_TIMEFRAME, df, open_pos)
            if tnf_result.is_entry:
                signal = tnf_result.metadata.get("signal")
                if signal:
                    results.append(signal)

        return results

    def evaluate_mtf_ema(self, asset: str) -> Optional[dict]:
        logger.info(f"[MTF-EMA] ====== Evaluating {asset} ======")
        try:
            d1_candles = self.cache.get_candles(asset, "D1", 300)
            h4_candles = self.cache.get_candles(asset, "4H", 300)
            h1_candles = self.cache.get_candles(asset, "1H", 300)
        except Exception as e:
            logger.error(f"[MTF-EMA] {asset} | Exception fetching candles: {e}")
            return None

        logger.info(f"[MTF-EMA] {asset} | Candle counts: D1={len(d1_candles)} (need 200), H4={len(h4_candles)} (need 200), H1={len(h1_candles)} (need 20)")
        if len(d1_candles) < 200 or len(h4_candles) < 200 or len(h1_candles) < 20:
            logger.warning(f"[MTF-EMA] {asset} | INSUFFICIENT DATA - D1: {'OK' if len(d1_candles) >= 200 else 'FAIL'}, H4: {'OK' if len(h4_candles) >= 200 else 'FAIL'}, H1: {'OK' if len(h1_candles) >= 20 else 'FAIL'}")
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
            logger.warning(f"[MTF-EMA] {asset} | Indicators returned None: {none_indicators}")
            return None

        price_above_d1_emas = current_price > d1_ema200_val and current_price > d1_ema50_val
        h4_ema200_rising = h4_ema200_val > h4_ema200_prev
        dip_below_h4_50 = current_price < h4_ema50_val
        dip_within_1_atr = (h4_ema50_val - current_price) < h4_atr_val
        h1_closes_above_20_ema = current_price > h1_ema20_val

        logger.info(f"[MTF-EMA] {asset} | price={current_price:.5f}")
        logger.info(f"[MTF-EMA] {asset} | Condition 1 - Price > D1 EMA200 ({d1_ema200_val:.5f}) AND D1 EMA50 ({d1_ema50_val:.5f}): {price_above_d1_emas}")
        logger.info(f"[MTF-EMA] {asset} | Condition 2 - H4 EMA200 rising ({h4_ema200_val:.5f} > {h4_ema200_prev:.5f}): {h4_ema200_rising}")
        logger.info(f"[MTF-EMA] {asset} | Condition 3 - Price dips below H4 EMA50 ({h4_ema50_val:.5f}): {dip_below_h4_50}")
        logger.info(f"[MTF-EMA] {asset} | Condition 4 - Dip within 1 ATR ({h4_atr_val:.5f}): {dip_within_1_atr}")
        logger.info(f"[MTF-EMA] {asset} | Condition 5 - H1 closes above EMA20 ({h1_ema20_val:.5f}): {h1_closes_above_20_ema}")
        logger.info(f"[MTF-EMA] {asset} | ALL CONDITIONS MET: {price_above_d1_emas and h4_ema200_rising and dip_below_h4_50 and dip_within_1_atr and h1_closes_above_20_ema}")

        if price_above_d1_emas and h4_ema200_rising and dip_below_h4_50 and dip_within_1_atr and h1_closes_above_20_ema:
            signal_timestamp = h1_candles[-1]["timestamp"]
            if signal_exists(STRATEGY_MTF_EMA, asset, signal_timestamp):
                return None

            signal = {
                "strategy_name": STRATEGY_MTF_EMA,
                "asset": asset,
                "direction": "BUY",
                "entry_price": current_price,
                "stop_loss": current_price - (2 * h4_atr_val),
                "take_profit": current_price + (3 * h4_atr_val),
                "atr_at_entry": round(h4_atr_val, 6),
                "signal_timestamp": signal_timestamp,
            }
            signal_id = insert_signal(signal)
            if signal_id:
                open_position({
                    "asset": asset,
                    "strategy_name": STRATEGY_MTF_EMA,
                    "direction": "BUY",
                    "entry_price": current_price,
                    "atr_at_entry": round(h4_atr_val, 6),
                })
                signal["id"] = signal_id
                signal["status"] = "OPEN"
                return signal

        return None

    def evaluate_trend_following(self, asset: str) -> Optional[dict]:
        logger.info(f"[TREND-FOLLOW] ====== Evaluating {asset} ======")
        try:
            d_candles = self.cache.get_candles(asset, "D1", 300)
        except Exception as e:
            logger.error(f"[TREND-FOLLOW] {asset} | Exception fetching candles: {e}")
            return None

        logger.info(f"[TREND-FOLLOW] {asset} | Daily candles: {len(d_candles)} (need 100)")
        if len(d_candles) < 100:
            logger.warning(f"[TREND-FOLLOW] {asset} | INSUFFICIENT DATA - have {len(d_candles)}, need 100")
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
            logger.warning(f"[TREND-FOLLOW] {asset} | Indicators returned None: {none_list}")
            return None

        if len(closes) >= 50:
            highest_50 = max(closes[-50:])
        else:
            logger.warning(f"[TREND-FOLLOW] {asset} | Not enough closes for 50-day high (have {len(closes)})")
            return None

        price_at_50d_high = current_price >= highest_50
        sma50_above_sma100 = sma50_val > sma100_val

        logger.info(f"[TREND-FOLLOW] {asset} | price={current_price:.5f}")
        logger.info(f"[TREND-FOLLOW] {asset} | Condition 1 - Price >= 50-day high ({highest_50:.5f}): {price_at_50d_high}")
        logger.info(f"[TREND-FOLLOW] {asset} | Condition 2 - SMA50 ({sma50_val:.5f}) > SMA100 ({sma100_val:.5f}): {sma50_above_sma100}")
        logger.info(f"[TREND-FOLLOW] {asset} | ALL CONDITIONS MET: {price_at_50d_high and sma50_above_sma100}")

        if price_at_50d_high and sma50_above_sma100:
            signal_timestamp = d_candles[-1]["timestamp"]
            if signal_exists(STRATEGY_TREND_FOLLOWING, asset, signal_timestamp):
                return None

            stop_loss = current_price - (3 * atr_val)

            signal = {
                "strategy_name": STRATEGY_TREND_FOLLOWING,
                "asset": asset,
                "direction": "BUY",
                "entry_price": current_price,
                "stop_loss": stop_loss,
                "take_profit": None,
                "atr_at_entry": round(atr_val, 6),
                "signal_timestamp": signal_timestamp,
            }
            signal_id = insert_signal(signal)
            if signal_id:
                open_position({
                    "asset": asset,
                    "strategy_name": STRATEGY_TREND_FOLLOWING,
                    "direction": "BUY",
                    "entry_price": current_price,
                    "atr_at_entry": round(atr_val, 6),
                })
                signal["id"] = signal_id
                signal["status"] = "OPEN"
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

    def evaluate_sp500_momentum(self, asset: str = "SPX") -> Optional[dict]:
        try:
            candles = self.cache.get_candles(asset, "30m", 300)
            df = pd.DataFrame(candles) if candles else pd.DataFrame()
        except Exception as e:
            logger.error(f"[STRATEGY-ENGINE] Failed to fetch {asset} 30m candles: {e}")
            return None

        open_pos = get_open_position(STRATEGY_SP500_MOMENTUM, asset)
        result = self.sp500_strategy.evaluate(asset, "30m", df, open_pos)
        if result.is_entry:
            return result.metadata.get("signal")
        return None

    def run_sp500_intraday_cycle(self, asset: str = "SPX") -> dict:
        logger.info(f"[SP500-INTRADAY] ====== 30m cycle start | {asset} ======")

        try:
            candles = self.cache.get_candles(asset, "30m", 300)
            df = pd.DataFrame(candles) if candles else pd.DataFrame()
        except Exception as e:
            logger.error(f"[SP500-INTRADAY] Failed to fetch {asset} 30m candles: {e}")
            return {"entry": None, "exits": [], "state_updated": False}

        logger.info(f"[SP500-INTRADAY] {asset} | Fetched {len(df)} candles for evaluation")

        open_pos = get_open_position(STRATEGY_SP500_MOMENTUM, asset)

        if open_pos:
            pos_id = open_pos.get("id")
            entry_price = open_pos.get("entry_price", 0)
            atr_at_entry = open_pos.get("atr_at_entry")
            stored_highest = open_pos.get("highest_price_since_entry") or entry_price
            direction = open_pos.get("direction", "BUY")

            logger.info(
                f"[SP500-INTRADAY] {asset} | OPEN POSITION #{pos_id} | "
                f"direction={direction} | entry={entry_price:.2f} | "
                f"atr_at_entry={atr_at_entry:.6f} (FIXED from DB) | "
                f"highest_since_entry={stored_highest:.2f}"
            )
            if atr_at_entry is not None:
                trailing_stop = stored_highest - (2.0 * atr_at_entry)
                logger.info(
                    f"[SP500-INTRADAY] {asset} | Current trailing stop = "
                    f"{stored_highest:.2f} - (2 × {atr_at_entry:.6f}) = {trailing_stop:.2f}"
                )
        else:
            logger.info(f"[SP500-INTRADAY] {asset} | No open position — evaluating for new entry")

        eval_result = self.sp500_strategy.evaluate(asset, "30m", df, open_pos)
        entry_signal = None
        if eval_result.is_entry:
            entry_signal = eval_result.metadata.get("signal")
            logger.info(
                f"[SP500-INTRADAY] {asset} | NEW ENTRY SIGNAL: "
                f"BUY @ {entry_signal.get('entry_price', 0):.2f} | "
                f"atr_at_entry={entry_signal.get('atr_at_entry', 0):.6f} | "
                f"initial_stop={entry_signal.get('stop_loss', 0):.2f}"
            )

        exits = self.sp500_strategy.check_exits()
        if exits:
            for ex in exits:
                logger.info(
                    f"[SP500-INTRADAY] {asset} | EXIT: {ex.get('exit_reason')} | "
                    f"exit_price={ex.get('exit_price', 0):.2f} | "
                    f"atr_at_entry={ex.get('atr_at_entry', 0):.6f}"
                )

        state_updated = open_pos is not None and eval_result.is_none
        logger.info(
            f"[SP500-INTRADAY] ====== 30m cycle complete | {asset} | "
            f"entry={'YES' if entry_signal else 'NO'} | "
            f"exits={len(exits)} | state_updated={state_updated} ======"
        )

        return {
            "entry": entry_signal,
            "exits": exits,
            "state_updated": state_updated,
        }

    def evaluate_highest_lowest_fx(self, asset: str = "EUR/USD") -> Optional[dict]:
        logger.info(f"[HLC-FX] ====== Evaluating {asset} ======")
        try:
            h1_candles = self.cache.get_candles(asset, "1H", 300)
            d_candles = self.cache.get_candles(asset, "D1", 60)
        except Exception as e:
            logger.error(f"[HLC-FX] {asset} | Exception fetching candles: {e}")
            return None

        logger.info(f"[HLC-FX] {asset} | Candle counts: H1={len(h1_candles)} (need 50), D1={len(d_candles)} (need 50)")
        if len(h1_candles) < 50 or len(d_candles) < 50:
            logger.warning(f"[HLC-FX] {asset} | INSUFFICIENT DATA")
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
            logger.info(f"[HLC-FX] {asset} | Outside both Tokyo and NY windows - skipping")
            return None

        d_closes = [c["close"] for c in d_candles]
        d_highs = [c["high"] for c in d_candles]
        d_lows = [c["low"] for c in d_candles]

        atr100 = IndicatorEngine.atr(d_highs, d_lows, d_closes, 100)
        atr_val = atr100[-1] if atr100 and atr100[-1] is not None else None

        highest_50d = max(d_closes[-50:])
        lowest_50d = min(d_closes[-50:])

        current_price = h1_candles[-1]["close"]
        signal_timestamp = h1_candles[-1]["timestamp"]

        if signal_exists(STRATEGY_HIGHEST_LOWEST_FX, asset, signal_timestamp):
            return None

        session = "tokyo" if in_tokyo_window else "new_york"
        signal_data = None

        if in_tokyo_window:
            if current_price >= highest_50d:
                signal_data = {
                    "direction": "BUY",
                    "reason": f"Price ({current_price:.5f}) at/above 50-day highest close ({highest_50d:.5f}) during Tokyo session",
                }
            elif current_price <= lowest_50d:
                signal_data = {
                    "direction": "SELL",
                    "reason": f"Price ({current_price:.5f}) at/below 50-day lowest close ({lowest_50d:.5f}) during Tokyo session",
                }

        if in_ny_window:
            if current_price >= highest_50d:
                signal_data = {
                    "direction": "BUY",
                    "reason": f"Price ({current_price:.5f}) at/above 50-day highest close ({highest_50d:.5f}) during NY session",
                }
            elif current_price <= lowest_50d:
                if current_price > lowest_50d * 0.998:
                    signal_data = {
                        "direction": "BUY",
                        "reason": f"Price ({current_price:.5f}) near 50-day lowest close ({lowest_50d:.5f}) - potential reversal during NY session",
                    }

        if signal_data is None:
            return None

        stop_distance = (2 * atr_val) if atr_val else 0.0050
        if signal_data["direction"] == "BUY":
            stop_loss = current_price - stop_distance
            take_profit = current_price + (3 * stop_distance)
        else:
            stop_loss = current_price + stop_distance
            take_profit = current_price - (3 * stop_distance)

        signal = {
            "strategy_name": STRATEGY_HIGHEST_LOWEST_FX,
            "asset": asset,
            "direction": signal_data["direction"],
            "entry_price": current_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "atr_at_entry": round(atr_val, 6) if atr_val else None,
            "signal_timestamp": signal_timestamp,
        }
        signal_id = insert_signal(signal)
        if signal_id:
            signal["id"] = signal_id
            signal["status"] = "OPEN"
            return signal

        return None

    def check_exit_conditions(self) -> list[dict]:
        closed_signals = []

        for strategy_name in [STRATEGY_TREND_FOLLOWING, STRATEGY_MTF_EMA]:
            positions = get_all_open_positions(strategy_name=strategy_name)
            for pos in positions:
                asset = pos["asset"]
                atr_at_entry = pos.get("atr_at_entry")
                if not atr_at_entry:
                    logger.warning(
                        f"[EXIT-CHECK] STATE LOCK MISSING: Position #{pos['id']} "
                        f"{strategy_name}/{asset} has no atr_at_entry — "
                        f"cannot compute trailing stop. Skipping."
                    )
                    continue

                try:
                    candles = self.cache.get_candles(asset, "D1", 300)
                except Exception:
                    continue
                if not candles:
                    continue

                current_price = candles[-1]["close"]
                direction = pos["direction"]
                trailing_mult = 2.0 if strategy_name == STRATEGY_MTF_EMA else 3.0

                if direction == "BUY":
                    update_position_tracking(pos["id"], highest_price=current_price)
                    highest = max(pos.get("highest_price_since_entry") or pos["entry_price"], current_price)
                    trailing_stop = highest - (trailing_mult * atr_at_entry)
                    if current_price <= trailing_stop:
                        exit_reason = f"Trailing stop hit at {current_price:.5f} (highest: {highest:.5f}, stop: {trailing_stop:.5f})"
                        active_sigs = get_active_signals(strategy_name=strategy_name, asset=asset)
                        for sig in active_sigs:
                            close_signal(sig["id"], exit_reason)
                        close_position(strategy_name, asset)
                        closed_signals.append({**pos, "exit_price": current_price, "exit_reason": "trailing_stop"})
                elif direction == "SELL":
                    update_position_tracking(pos["id"], lowest_price=current_price)
                    lowest = min(pos.get("lowest_price_since_entry") or pos["entry_price"], current_price)
                    trailing_stop = lowest + (trailing_mult * atr_at_entry)
                    if current_price >= trailing_stop:
                        exit_reason = f"Trailing stop hit at {current_price:.5f} (lowest: {lowest:.5f}, stop: {trailing_stop:.5f})"
                        active_sigs = get_active_signals(strategy_name=strategy_name, asset=asset)
                        for sig in active_sigs:
                            close_signal(sig["id"], exit_reason)
                        close_position(strategy_name, asset)
                        closed_signals.append({**pos, "exit_price": current_price, "exit_reason": "trailing_stop"})

        sp500_exits = self.sp500_strategy.check_exits()
        closed_signals.extend(sp500_exits)

        hlc_exits = self.highest_lowest_strategy.check_exits()
        closed_signals.extend(hlc_exits)

        trend_forex_exits = self.trend_forex_strategy.check_exits()
        closed_signals.extend(trend_forex_exits)

        trend_non_forex_exits = self.trend_non_forex_strategy.check_exits()
        closed_signals.extend(trend_non_forex_exits)

        return closed_signals
