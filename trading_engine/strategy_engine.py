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
    get_active_signals,
    close_signal,
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
from trading_engine.strategies.multi_timeframe import MultiTimeframeEMAStrategy

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
        self.mtf_ema_strategy = MultiTimeframeEMAStrategy(cache)

    def evaluate_all(self, symbols: Optional[list[str]] = None) -> list[dict]:
        results = []

        from trading_engine.strategies.multi_timeframe import get_all_mtf_assets
        mtf_symbols = symbols or get_all_mtf_assets()
        logger.info(f"[STRATEGY-ENGINE] evaluate_all called | mtf_symbols={mtf_symbols}")

        for asset in mtf_symbols:
            try:
                h1_candles = self.cache.get_candles(asset, "1H", 300)
                h1_df = pd.DataFrame(h1_candles) if h1_candles else pd.DataFrame()
            except Exception as e:
                logger.error(f"[STRATEGY-ENGINE] {asset} | Failed to fetch H1 candles: {e}")
                continue
            open_pos = get_open_position(STRATEGY_MTF_EMA, asset)
            mtf_result = self.mtf_ema_strategy.evaluate(asset, "1H", h1_df, open_pos)
            if mtf_result.is_entry:
                signal = mtf_result.metadata.get("signal")
                if signal:
                    results.append(signal)

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

        from trading_engine.strategies.trend_forex import TARGET_SYMBOLS as TREND_FOREX_SYMBOLS, TIMEFRAME as TF_TIMEFRAME
        for asset in TREND_FOREX_SYMBOLS:
            try:
                candles = self.cache.get_candles(asset, TF_TIMEFRAME, 300)
            except Exception:
                continue
            if not candles:
                continue
            df = pd.DataFrame(candles)
            open_pos = get_open_position(STRATEGY_TREND_FOREX, asset)
            tf_forex_result = self.trend_forex_strategy.evaluate(asset, TF_TIMEFRAME, df, open_pos)
            if tf_forex_result.is_entry:
                signal = tf_forex_result.metadata.get("signal")
                if signal:
                    results.append(signal)

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

    def check_exit_conditions(self) -> list[dict]:
        closed_signals = []

        for strategy_name in [STRATEGY_TREND_FOLLOWING]:
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
                trailing_mult = 3.0

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

        # MTF EMA was missing from this list — add standalone H1/EMA50 exit check
        try:
            mtf_exits = self.mtf_ema_strategy.check_exits()
            closed_signals.extend(mtf_exits)
            if mtf_exits:
                logger.info(
                    f"[STRATEGY-ENGINE] mtf_ema check_exits: "
                    f"{len(mtf_exits)} closed"
                )
        except Exception as e:
            logger.error(
                f"[STRATEGY-ENGINE] mtf_ema check_exits failed: {e}",
                exc_info=True,
            )

        return closed_signals
