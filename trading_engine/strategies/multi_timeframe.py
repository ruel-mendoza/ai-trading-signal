import logging
from typing import Optional
from dataclasses import dataclass

import pandas as pd

from trading_engine.strategies.base import BaseStrategy, SignalResult, Action, Direction
from trading_engine.indicators import IndicatorEngine
from trading_engine.cache_layer import CacheLayer
from trading_engine.database import (
    has_open_signal,
    has_open_position,
    update_position_tracking,
)

logger = logging.getLogger("trading_engine.strategy.multi_timeframe")

STRATEGY_NAME = "mtf_ema"

TARGET_ASSETS = {
    "indices": ["SPX", "NDX", "RUT"],
    "commodities": ["XAU/USD", "XAG/USD", "WTI/USD"],
    "crypto": ["BTC/USD", "ETH/USD"],
    "forex": ["EUR/USD", "USD/JPY", "GBP/USD", "AUD/USD"],
}

ALL_ASSETS = []
for group in TARGET_ASSETS.values():
    ALL_ASSETS.extend(group)

TIMEFRAME_D1 = "D1"
TIMEFRAME_H4 = "4H"
TIMEFRAME_H1 = "1H"
PRIMARY_TIMEFRAME = TIMEFRAME_H1

EMA_20 = 20
EMA_50 = 50
EMA_200 = 200
ATR_PERIOD = 100

MIN_D1_BARS = 200
MIN_H4_BARS = 200
MIN_H1_BARS = 20

SL_ATR_MULT = 0.5
TP_ATR_MULT = 3.0
TRAILING_STOP_ATR_MULT = 2.0
STRUCTURAL_LOOKBACK_H1 = 24
STRUCTURAL_PIP_BUFFER = 0.0002


@dataclass
class TimeframeData:
    timeframe: str
    df: pd.DataFrame
    closes: list[float]
    highs: list[float]
    lows: list[float]
    opens: list[float]


@dataclass
class MTFIndicators:
    d1_ema20: Optional[float] = None
    d1_ema50: Optional[float] = None
    d1_ema200: Optional[float] = None
    d1_ema200_prev: Optional[float] = None
    d1_atr100: Optional[float] = None

    h4_ema20: Optional[float] = None
    h4_ema50: Optional[float] = None
    h4_ema200: Optional[float] = None
    h4_ema200_prev: Optional[float] = None
    h4_ema200_earlier: Optional[float] = None
    h4_atr100: Optional[float] = None

    h1_ema20: Optional[float] = None
    h1_ema20_prev: Optional[float] = None
    h1_ema50: Optional[float] = None
    h1_ema200: Optional[float] = None
    h1_atr100: Optional[float] = None

    h1_close_current: Optional[float] = None
    h1_close_prev: Optional[float] = None
    h1_open_current: Optional[float] = None

    def all_required_present(self) -> bool:
        required = [
            self.d1_ema50,
            self.d1_ema200,
            self.d1_ema200_prev,
            self.h4_ema50,
            self.h4_ema200,
            self.h4_ema200_prev,
            self.h4_ema200_earlier,
            self.h4_atr100,
            self.h1_ema20,
            self.h1_ema20_prev,
            self.h1_close_current,
            self.h1_close_prev,
            self.h1_open_current,
        ]
        return all(v is not None for v in required)

    def missing_names(self) -> list[str]:
        checks = {
            "D1_EMA50": self.d1_ema50,
            "D1_EMA200": self.d1_ema200,
            "D1_EMA200_prev": self.d1_ema200_prev,
            "H4_EMA50": self.h4_ema50,
            "H4_EMA200": self.h4_ema200,
            "H4_EMA200_prev": self.h4_ema200_prev,
            "H4_EMA200_earlier": self.h4_ema200_earlier,
            "H4_ATR100": self.h4_atr100,
            "H1_EMA20": self.h1_ema20,
            "H1_EMA20_prev": self.h1_ema20_prev,
            "H1_close_current": self.h1_close_current,
            "H1_close_prev": self.h1_close_prev,
            "H1_open_current": self.h1_open_current,
        }
        return [name for name, val in checks.items() if val is None]


def _safe_last(values: list, offset: int = 0) -> Optional[float]:
    idx = -(1 + offset)
    if len(values) >= (1 + offset) and values[idx] is not None:
        return float(values[idx])
    return None


def _candles_to_lists(candles: list[dict]) -> tuple[list[float], list[float], list[float], list[float]]:
    closes = [float(c["close"]) for c in candles]
    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]
    opens = [float(c["open"]) for c in candles]
    return closes, highs, lows, opens


class MultiTimeframeEMAStrategy(BaseStrategy):
    def __init__(self, cache: CacheLayer):
        self.cache = cache

    @property
    def name(self) -> str:
        return STRATEGY_NAME

    def _fetch_timeframe(self, asset: str, timeframe: str, limit: int = 300) -> Optional[TimeframeData]:
        try:
            candles = self.cache.get_candles(asset, timeframe, limit)
        except Exception as e:
            logger.error(f"[MTF-EMA] {asset} | Exception fetching {timeframe} candles: {e}")
            return None

        if not candles:
            logger.warning(f"[MTF-EMA] {asset} | No {timeframe} candles returned")
            return None

        closes, highs, lows, opens = _candles_to_lists(candles)
        df = pd.DataFrame(candles)
        for col in ("open", "high", "low", "close"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return TimeframeData(
            timeframe=timeframe,
            df=df,
            closes=closes,
            highs=highs,
            lows=lows,
            opens=opens,
        )

    def _fetch_all_timeframes(self, asset: str, h1_df: pd.DataFrame) -> Optional[dict]:
        d1 = self._fetch_timeframe(asset, TIMEFRAME_D1, 300)
        h4 = self._fetch_timeframe(asset, TIMEFRAME_H4, 300)

        if h1_df.empty or "close" not in h1_df.columns:
            logger.warning(f"[MTF-EMA] {asset} | H1 DataFrame is empty or missing columns")
            return None

        h1_closes = h1_df["close"].tolist()
        h1_highs = h1_df["high"].tolist()
        h1_lows = h1_df["low"].tolist()
        h1_opens = h1_df["open"].tolist() if "open" in h1_df.columns else [0.0] * len(h1_closes)
        h1 = TimeframeData(
            timeframe=TIMEFRAME_H1,
            df=h1_df,
            closes=h1_closes,
            highs=h1_highs,
            lows=h1_lows,
            opens=h1_opens,
        )

        bar_status = (
            f"D1={'N/A' if d1 is None else len(d1.closes)} (need {MIN_D1_BARS}), "
            f"H4={'N/A' if h4 is None else len(h4.closes)} (need {MIN_H4_BARS}), "
            f"H1={len(h1.closes)} (need {MIN_H1_BARS})"
        )
        logger.info(f"[MTF-EMA] {asset} | Candle counts: {bar_status}")

        if d1 is None or len(d1.closes) < MIN_D1_BARS:
            d1_count = 0 if d1 is None else len(d1.closes)
            logger.warning(
                f"[MTF-EMA] {asset} | INSUFFICIENT D1 DATA — have {d1_count}, need {MIN_D1_BARS}"
            )
            return None

        if h4 is None or len(h4.closes) < MIN_H4_BARS:
            h4_count = 0 if h4 is None else len(h4.closes)
            logger.warning(
                f"[MTF-EMA] {asset} | INSUFFICIENT H4 DATA — have {h4_count}, need {MIN_H4_BARS}"
            )
            return None

        if len(h1.closes) < MIN_H1_BARS:
            logger.warning(
                f"[MTF-EMA] {asset} | INSUFFICIENT H1 DATA — have {len(h1.closes)}, need {MIN_H1_BARS}"
            )
            return None

        return {"d1": d1, "h4": h4, "h1": h1}

    def _compute_indicators(self, asset: str, tf_data: dict) -> Optional[MTFIndicators]:
        d1 = tf_data["d1"]
        h4 = tf_data["h4"]
        h1 = tf_data["h1"]

        d1_ema20_vals = IndicatorEngine.ema(d1.closes, EMA_20)
        d1_ema50_vals = IndicatorEngine.ema(d1.closes, EMA_50)
        d1_ema200_vals = IndicatorEngine.ema(d1.closes, EMA_200)
        d1_atr100_vals = IndicatorEngine.atr(d1.highs, d1.lows, d1.closes, ATR_PERIOD)

        h4_ema20_vals = IndicatorEngine.ema(h4.closes, EMA_20)
        h4_ema50_vals = IndicatorEngine.ema(h4.closes, EMA_50)
        h4_ema200_vals = IndicatorEngine.ema(h4.closes, EMA_200)
        h4_atr100_vals = IndicatorEngine.atr(h4.highs, h4.lows, h4.closes, ATR_PERIOD)

        h1_ema20_vals = IndicatorEngine.ema(h1.closes, EMA_20)
        h1_ema50_vals = IndicatorEngine.ema(h1.closes, EMA_50)
        h1_ema200_vals = IndicatorEngine.ema(h1.closes, EMA_200)
        h1_atr100_vals = IndicatorEngine.atr(h1.highs, h1.lows, h1.closes, ATR_PERIOD)

        indicators = MTFIndicators(
            d1_ema20=_safe_last(d1_ema20_vals),
            d1_ema50=_safe_last(d1_ema50_vals),
            d1_ema200=_safe_last(d1_ema200_vals),
            d1_ema200_prev=_safe_last(d1_ema200_vals, offset=1),
            d1_atr100=_safe_last(d1_atr100_vals),
            h4_ema20=_safe_last(h4_ema20_vals),
            h4_ema50=_safe_last(h4_ema50_vals),
            h4_ema200=_safe_last(h4_ema200_vals),
            h4_ema200_prev=_safe_last(h4_ema200_vals, offset=1),
            h4_ema200_earlier=_safe_last(h4_ema200_vals, offset=2),
            h4_atr100=_safe_last(h4_atr100_vals),
            h1_ema20=_safe_last(h1_ema20_vals),
            h1_ema20_prev=_safe_last(h1_ema20_vals, offset=1),
            h1_ema50=_safe_last(h1_ema50_vals),
            h1_ema200=_safe_last(h1_ema200_vals),
            h1_atr100=_safe_last(h1_atr100_vals),
            h1_close_current=_safe_last(h1.closes),
            h1_close_prev=_safe_last(h1.closes, offset=1),
            h1_open_current=_safe_last(h1.opens),
        )

        logger.info(
            f"[MTF-EMA] {asset} | D1 indicators: "
            f"EMA20={indicators.d1_ema20}, EMA50={indicators.d1_ema50}, "
            f"EMA200={indicators.d1_ema200}, EMA200_prev={indicators.d1_ema200_prev}, "
            f"ATR100={indicators.d1_atr100}"
        )
        logger.info(
            f"[MTF-EMA] {asset} | H4 indicators: "
            f"EMA20={indicators.h4_ema20}, EMA50={indicators.h4_ema50}, "
            f"EMA200={indicators.h4_ema200}, EMA200_prev={indicators.h4_ema200_prev}, "
            f"EMA200_earlier={indicators.h4_ema200_earlier}, ATR100={indicators.h4_atr100}"
        )
        logger.info(
            f"[MTF-EMA] {asset} | H1 indicators: "
            f"EMA20={indicators.h1_ema20}, EMA20_prev={indicators.h1_ema20_prev}, "
            f"EMA50={indicators.h1_ema50}, EMA200={indicators.h1_ema200}, "
            f"ATR100={indicators.h1_atr100}"
        )
        logger.info(
            f"[MTF-EMA] {asset} | H1 candle: "
            f"close_curr={indicators.h1_close_current}, close_prev={indicators.h1_close_prev}, "
            f"open_curr={indicators.h1_open_current}"
        )

        if not indicators.all_required_present():
            missing = indicators.missing_names()
            logger.warning(f"[MTF-EMA] {asset} | Missing required indicators: {missing}")
            return None

        return indicators

    def _check_long_conditions(self, asset: str, ind: MTFIndicators) -> bool:
        price = ind.h1_close_current

        cond1 = price > ind.d1_ema200 and price > ind.d1_ema50
        logger.info(
            f"[MTF-EMA] {asset} | LONG Cond 1 — D1 Trend: Price {price:.5f} > "
            f"D1 EMA200 ({ind.d1_ema200:.5f}) AND D1 EMA50 ({ind.d1_ema50:.5f}): {cond1}"
        )

        d1_rising = ind.d1_ema200 > ind.d1_ema200_prev
        h4_accel_now = ind.h4_ema200 - ind.h4_ema200_prev
        h4_accel_prev = ind.h4_ema200_prev - ind.h4_ema200_earlier
        h4_accelerating = h4_accel_now > h4_accel_prev
        cond2 = d1_rising and h4_accelerating
        logger.info(
            f"[MTF-EMA] {asset} | LONG Cond 2 — Slope Acceleration: "
            f"D1 EMA200 rising ({ind.d1_ema200:.5f} > {ind.d1_ema200_prev:.5f}): {d1_rising} | "
            f"H4 EMA200 accel ({h4_accel_now:.6f} > {h4_accel_prev:.6f}): {h4_accelerating} | "
            f"combined: {cond2}"
        )

        dip_below = price < ind.h4_ema50
        within_atr = (ind.h4_ema50 - price) < ind.h4_atr100
        cond3 = dip_below and within_atr
        logger.info(
            f"[MTF-EMA] {asset} | LONG Cond 3 — Pullback: "
            f"Price ({price:.5f}) < H4 EMA50 ({ind.h4_ema50:.5f}): {dip_below} | "
            f"Within 1× ATR ({ind.h4_atr100:.5f}): {within_atr} | "
            f"combined: {cond3}"
        )

        prev_below_ema20 = ind.h1_close_prev < ind.h1_ema20_prev
        curr_above_ema20 = ind.h1_close_current > ind.h1_ema20
        bullish_body = ind.h1_close_current > ind.h1_open_current
        cond4 = prev_below_ema20 and curr_above_ema20 and bullish_body
        logger.info(
            f"[MTF-EMA] {asset} | LONG Cond 4 — H1 Confirmation: "
            f"prev H1 close ({ind.h1_close_prev:.5f}) < prev EMA20 ({ind.h1_ema20_prev:.5f}): {prev_below_ema20} | "
            f"curr H1 close ({ind.h1_close_current:.5f}) > curr EMA20 ({ind.h1_ema20:.5f}): {curr_above_ema20} | "
            f"bullish body (C {ind.h1_close_current:.5f} > O {ind.h1_open_current:.5f}): {bullish_body} | "
            f"combined: {cond4}"
        )

        all_met = cond1 and cond2 and cond3 and cond4
        logger.info(f"[MTF-EMA] {asset} | LONG ALL CONDITIONS MET: {all_met}")
        return all_met

    def _check_short_conditions(self, asset: str, ind: MTFIndicators) -> bool:
        price = ind.h1_close_current

        cond1 = price < ind.d1_ema200 and price < ind.d1_ema50
        logger.info(
            f"[MTF-EMA] {asset} | SHORT Cond 1 — D1 Trend: Price {price:.5f} < "
            f"D1 EMA200 ({ind.d1_ema200:.5f}) AND D1 EMA50 ({ind.d1_ema50:.5f}): {cond1}"
        )

        d1_falling = ind.d1_ema200 < ind.d1_ema200_prev
        h4_accel_now = ind.h4_ema200_prev - ind.h4_ema200
        h4_accel_prev = ind.h4_ema200_earlier - ind.h4_ema200_prev
        h4_accelerating = h4_accel_now > h4_accel_prev
        cond2 = d1_falling and h4_accelerating
        logger.info(
            f"[MTF-EMA] {asset} | SHORT Cond 2 — Slope Acceleration: "
            f"D1 EMA200 falling ({ind.d1_ema200:.5f} < {ind.d1_ema200_prev:.5f}): {d1_falling} | "
            f"H4 EMA200 accel downward ({h4_accel_now:.6f} > {h4_accel_prev:.6f}): {h4_accelerating} | "
            f"combined: {cond2}"
        )

        dip_above = price > ind.h4_ema50
        within_atr = (price - ind.h4_ema50) < ind.h4_atr100
        cond3 = dip_above and within_atr
        logger.info(
            f"[MTF-EMA] {asset} | SHORT Cond 3 — Pullback: "
            f"Price ({price:.5f}) > H4 EMA50 ({ind.h4_ema50:.5f}): {dip_above} | "
            f"Within 1× ATR ({ind.h4_atr100:.5f}): {within_atr} | "
            f"combined: {cond3}"
        )

        prev_above_ema20 = ind.h1_close_prev > ind.h1_ema20_prev
        curr_below_ema20 = ind.h1_close_current < ind.h1_ema20
        bearish_body = ind.h1_close_current < ind.h1_open_current
        cond4 = prev_above_ema20 and curr_below_ema20 and bearish_body
        logger.info(
            f"[MTF-EMA] {asset} | SHORT Cond 4 — H1 Confirmation: "
            f"prev H1 close ({ind.h1_close_prev:.5f}) > prev EMA20 ({ind.h1_ema20_prev:.5f}): {prev_above_ema20} | "
            f"curr H1 close ({ind.h1_close_current:.5f}) < curr EMA20 ({ind.h1_ema20:.5f}): {curr_below_ema20} | "
            f"bearish body (C {ind.h1_close_current:.5f} < O {ind.h1_open_current:.5f}): {bearish_body} | "
            f"combined: {cond4}"
        )

        all_met = cond1 and cond2 and cond3 and cond4
        logger.info(f"[MTF-EMA] {asset} | SHORT ALL CONDITIONS MET: {all_met}")
        return all_met

    def _compute_structural_stop_long(
        self, asset: str, h1_lows: list[float], h4_ema50: float, entry_price: float
    ) -> Optional[float]:
        recent_lows = h1_lows[-STRUCTURAL_LOOKBACK_H1:]
        lows_below_ema = [lo for lo in recent_lows if lo < h4_ema50]

        if not lows_below_ema:
            logger.info(
                f"[MTF-EMA] {asset} | LONG structural stop: "
                f"no H1 lows below H4 EMA50 ({h4_ema50:.5f}) in last {STRUCTURAL_LOOKBACK_H1}h"
            )
            return None

        lowest = min(lows_below_ema)
        structural_sl = lowest - STRUCTURAL_PIP_BUFFER
        logger.info(
            f"[MTF-EMA] {asset} | LONG structural stop: "
            f"lowest H1 low below H4 EMA50 = {lowest:.5f} - {STRUCTURAL_PIP_BUFFER} pip buffer = {structural_sl:.5f} | "
            f"distance from entry = {entry_price - structural_sl:.5f}"
        )
        return structural_sl

    def _compute_structural_stop_short(
        self, asset: str, h1_highs: list[float], h4_ema50: float, entry_price: float
    ) -> Optional[float]:
        recent_highs = h1_highs[-STRUCTURAL_LOOKBACK_H1:]
        highs_above_ema = [hi for hi in recent_highs if hi > h4_ema50]

        if not highs_above_ema:
            logger.info(
                f"[MTF-EMA] {asset} | SHORT structural stop: "
                f"no H1 highs above H4 EMA50 ({h4_ema50:.5f}) in last {STRUCTURAL_LOOKBACK_H1}h"
            )
            return None

        highest = max(highs_above_ema)
        structural_sl = highest + STRUCTURAL_PIP_BUFFER
        logger.info(
            f"[MTF-EMA] {asset} | SHORT structural stop: "
            f"highest H1 high above H4 EMA50 = {highest:.5f} + {STRUCTURAL_PIP_BUFFER} pip buffer = {structural_sl:.5f} | "
            f"distance from entry = {structural_sl - entry_price:.5f}"
        )
        return structural_sl

    def _select_stop_loss(
        self, asset: str, direction: str, entry_price: float,
        structural_sl: Optional[float], atr_sl: float, atr_distance: float
    ) -> tuple[float, str]:
        if structural_sl is None:
            logger.info(
                f"[MTF-EMA] {asset} | {direction} SL selection: "
                f"no structural stop available — using ATR stop = {atr_sl:.5f} "
                f"(distance = {atr_distance:.5f})"
            )
            return atr_sl, "atr"

        if direction == "LONG":
            structural_distance = entry_price - structural_sl
        else:
            structural_distance = structural_sl - entry_price

        if structural_distance <= 0:
            logger.warning(
                f"[MTF-EMA] {asset} | {direction} SL selection: "
                f"structural stop on wrong side of entry (distance={structural_distance:.5f}) "
                f"— falling back to ATR stop = {atr_sl:.5f}"
            )
            return atr_sl, "atr"

        if structural_distance > atr_distance:
            logger.info(
                f"[MTF-EMA] {asset} | {direction} SL selection: "
                f"STRUCTURAL wins — structural={structural_sl:.5f} (dist={structural_distance:.5f}) "
                f"> ATR={atr_sl:.5f} (dist={atr_distance:.5f})"
            )
            return structural_sl, "structural"
        else:
            logger.info(
                f"[MTF-EMA] {asset} | {direction} SL selection: "
                f"ATR wins — ATR={atr_sl:.5f} (dist={atr_distance:.5f}) "
                f">= structural={structural_sl:.5f} (dist={structural_distance:.5f})"
            )
            return atr_sl, "atr"

    def _check_entry_conditions(
        self, asset: str, current_price: float, ind: MTFIndicators, tf_data: dict
    ) -> Optional[SignalResult]:
        logger.info(f"[MTF-EMA] {asset} | Checking entry conditions | price={current_price:.5f}")

        h1 = tf_data["h1"]

        long_triggered = self._check_long_conditions(asset, ind)
        if long_triggered:
            atr_distance = SL_ATR_MULT * ind.h4_atr100
            atr_sl = current_price - atr_distance

            structural_sl = self._compute_structural_stop_long(
                asset, h1.lows, ind.h4_ema50, current_price
            )

            stop_loss, sl_method = self._select_stop_loss(
                asset, "LONG", current_price, structural_sl, atr_sl, atr_distance
            )

            take_profit = current_price + (TP_ATR_MULT * ind.h4_atr100)
            h4_accel = (ind.h4_ema200 - ind.h4_ema200_prev) - (ind.h4_ema200_prev - ind.h4_ema200_earlier)

            return SignalResult(
                action=Action.ENTRY,
                direction=Direction.LONG,
                price=current_price,
                stop_loss=stop_loss,
                atr_at_entry=ind.h4_atr100,
                metadata={
                    "take_profit": take_profit,
                    "sl_method": sl_method,
                    "sl_atr_value": round(atr_sl, 6),
                    "sl_structural_value": round(structural_sl, 6) if structural_sl else None,
                    "sl_atr_mult": SL_ATR_MULT,
                    "tp_atr_mult": TP_ATR_MULT,
                    "h4_atr100": round(ind.h4_atr100, 6),
                    "d1_ema200": round(ind.d1_ema200, 6),
                    "d1_ema50": round(ind.d1_ema50, 6),
                    "h4_ema50": round(ind.h4_ema50, 6),
                    "h4_ema200": round(ind.h4_ema200, 6),
                    "h4_ema200_acceleration": round(h4_accel, 8),
                    "h1_ema20": round(ind.h1_ema20, 6),
                    "h1_crossover": "prev_below_curr_above",
                },
            )

        short_triggered = self._check_short_conditions(asset, ind)
        if short_triggered:
            atr_distance = SL_ATR_MULT * ind.h4_atr100
            atr_sl = current_price + atr_distance

            structural_sl = self._compute_structural_stop_short(
                asset, h1.highs, ind.h4_ema50, current_price
            )

            stop_loss, sl_method = self._select_stop_loss(
                asset, "SHORT", current_price, structural_sl, atr_sl, atr_distance
            )

            take_profit = current_price - (TP_ATR_MULT * ind.h4_atr100)
            h4_accel = (ind.h4_ema200_prev - ind.h4_ema200) - (ind.h4_ema200_earlier - ind.h4_ema200_prev)

            return SignalResult(
                action=Action.ENTRY,
                direction=Direction.SHORT,
                price=current_price,
                stop_loss=stop_loss,
                atr_at_entry=ind.h4_atr100,
                metadata={
                    "take_profit": take_profit,
                    "sl_method": sl_method,
                    "sl_atr_value": round(atr_sl, 6),
                    "sl_structural_value": round(structural_sl, 6) if structural_sl else None,
                    "sl_atr_mult": SL_ATR_MULT,
                    "tp_atr_mult": TP_ATR_MULT,
                    "h4_atr100": round(ind.h4_atr100, 6),
                    "d1_ema200": round(ind.d1_ema200, 6),
                    "d1_ema50": round(ind.d1_ema50, 6),
                    "h4_ema50": round(ind.h4_ema50, 6),
                    "h4_ema200": round(ind.h4_ema200, 6),
                    "h4_ema200_acceleration": round(h4_accel, 8),
                    "h1_ema20": round(ind.h1_ema20, 6),
                    "h1_crossover": "prev_above_curr_below",
                },
            )

        return None

    def _check_exit_conditions(
        self, asset: str, current_price: float, pos: dict, ind: MTFIndicators
    ) -> Optional[SignalResult]:
        pos_id = pos.get("id")
        direction = pos.get("direction")
        atr_at_entry = pos.get("atr_at_entry")

        if not atr_at_entry:
            logger.warning(
                f"[MTF-EMA] {asset} | Position #{pos_id} has no atr_at_entry — "
                f"cannot compute trailing stop"
            )
            return None

        if direction == "BUY":
            stored_highest = pos.get("highest_price_since_entry") or pos.get("entry_price", current_price)
            new_highest = max(stored_highest, current_price)
            if new_highest > stored_highest:
                update_position_tracking(pos_id, highest_price=new_highest)
                logger.info(
                    f"[MTF-EMA] {asset} | LONG #{pos_id} | Peak updated: "
                    f"{stored_highest:.5f} → {new_highest:.5f}"
                )

            trailing_stop = new_highest - (TRAILING_STOP_ATR_MULT * atr_at_entry)
            logger.info(
                f"[MTF-EMA] {asset} | LONG #{pos_id} | "
                f"trailing_stop={trailing_stop:.5f} (peak {new_highest:.5f} - "
                f"{TRAILING_STOP_ATR_MULT}×ATR {atr_at_entry:.5f}) | "
                f"price={current_price:.5f}"
            )

            if current_price < trailing_stop:
                logger.info(
                    f"[MTF-EMA] {asset} | EXIT LONG #{pos_id} — price {current_price:.5f} "
                    f"< trailing stop {trailing_stop:.5f}"
                )
                return SignalResult(
                    action=Action.EXIT,
                    direction=Direction.LONG,
                    price=current_price,
                    metadata={
                        "exit_reason": (
                            f"Trailing stop hit: price {current_price:.5f} < "
                            f"stop {trailing_stop:.5f} "
                            f"(peak {new_highest:.5f} - {TRAILING_STOP_ATR_MULT}×ATR)"
                        ),
                    },
                )

        elif direction == "SELL":
            stored_lowest = pos.get("lowest_price_since_entry") or pos.get("entry_price", current_price)
            new_lowest = min(stored_lowest, current_price)
            if new_lowest < stored_lowest:
                update_position_tracking(pos_id, lowest_price=new_lowest)
                logger.info(
                    f"[MTF-EMA] {asset} | SHORT #{pos_id} | Trough updated: "
                    f"{stored_lowest:.5f} → {new_lowest:.5f}"
                )

            trailing_stop = new_lowest + (TRAILING_STOP_ATR_MULT * atr_at_entry)
            logger.info(
                f"[MTF-EMA] {asset} | SHORT #{pos_id} | "
                f"trailing_stop={trailing_stop:.5f} (trough {new_lowest:.5f} + "
                f"{TRAILING_STOP_ATR_MULT}×ATR {atr_at_entry:.5f}) | "
                f"price={current_price:.5f}"
            )

            if current_price > trailing_stop:
                logger.info(
                    f"[MTF-EMA] {asset} | EXIT SHORT #{pos_id} — price {current_price:.5f} "
                    f"> trailing stop {trailing_stop:.5f}"
                )
                return SignalResult(
                    action=Action.EXIT,
                    direction=Direction.SHORT,
                    price=current_price,
                    metadata={
                        "exit_reason": (
                            f"Trailing stop hit: price {current_price:.5f} > "
                            f"stop {trailing_stop:.5f} "
                            f"(trough {new_lowest:.5f} + {TRAILING_STOP_ATR_MULT}×ATR)"
                        ),
                    },
                )

        return None

    def evaluate(
        self,
        asset: str,
        timeframe: str,
        df: pd.DataFrame,
        open_position: Optional[dict],
    ) -> SignalResult:
        logger.info(f"[MTF-EMA] ====== Evaluating {asset} ======")

        if df.empty or "close" not in df.columns:
            logger.warning(f"[MTF-EMA] {asset} | Primary H1 DataFrame is empty — skipping")
            return SignalResult()

        tf_data = self._fetch_all_timeframes(asset, df)
        if tf_data is None:
            return SignalResult()

        indicators = self._compute_indicators(asset, tf_data)
        if indicators is None:
            return SignalResult()

        current_price = float(df["close"].iloc[-1])

        if open_position and open_position.get("direction"):
            exit_result = self._check_exit_conditions(asset, current_price, open_position, indicators)
            if exit_result:
                return exit_result
            logger.info(f"[MTF-EMA] {asset} | Position open — no exit triggered")
            return SignalResult()

        if has_open_position(STRATEGY_NAME, asset):
            logger.info(f"[MTF-EMA] {asset} | Position already open — skipping entry check")
            return SignalResult()

        if has_open_signal(STRATEGY_NAME, asset):
            logger.info(f"[MTF-EMA] {asset} | Active signal exists — skipping entry check")
            return SignalResult()

        entry_result = self._check_entry_conditions(asset, current_price, indicators, tf_data)
        if entry_result:
            return entry_result

        return SignalResult()
