import logging
from typing import Optional
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd

from trading_engine.strategies.base import BaseStrategy, SignalResult, Action, Direction
from trading_engine.indicators import IndicatorEngine
from trading_engine.cache_layer import CacheLayer
from trading_engine.database import (
    has_open_signal,
    has_open_position,
)

logger = logging.getLogger("trading_engine.strategy.multi_timeframe")

STRATEGY_NAME = "mtf_ema"

TARGET_ASSETS = {
    "indices": ["SPX", "NDX", "RUT"],
    "commodities": ["XAU/USD", "XAG/USD", "OSX"],
    "crypto": ["BTC/USD", "ETH/USD"],
    "forex": ["EUR/USD", "USD/JPY", "GBP/USD", "AUD/USD"],
    "etfs": [
        "CORN", "SOYB", "WEAT", "CANE", "WOOD",
        "USO", "UNG", "UGA",
        "SGOL", "SIVR", "CPER", "PPLT", "PALL",
        "DBB", "SLX",
    ],
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

HISTORICAL_DIP_H4_BARS = 12

SL_ATR_MULT = 0.5
TP_ATR_MULT = 3.0
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
class DipInfo:
    found: bool = False
    max_breach: float = 0.0
    within_atr: bool = False
    recovered: bool = False
    dip_bar_index: Optional[int] = None
    dip_bar_timestamp: Optional[str] = None


@dataclass
class MTFIndicators:
    d1_ema20: Optional[float] = None
    d1_ema50: Optional[float] = None
    d1_ema200: Optional[float] = None
    d1_ema200_prev: Optional[float] = None
    d1_atr100: Optional[float] = None

    h4_ema50: Optional[float] = None
    h4_ema200: Optional[float] = None
    h4_ema200_prev: Optional[float] = None
    h4_atr100: Optional[float] = None
    h4_close_current: Optional[float] = None
    h4_close_prev: Optional[float] = None
    h4_closes_recent: Optional[list] = None
    h4_lows_recent: Optional[list] = None
    h4_highs_recent: Optional[list] = None
    h4_timestamps_recent: Optional[list] = None

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
            self.h4_ema50,
            self.h4_atr100,
            self.h4_close_current,
            self.h1_close_current,
        ]
        return all(v is not None for v in required)

    def missing_names(self) -> list[str]:
        checks = {
            "D1_EMA50": self.d1_ema50,
            "D1_EMA200": self.d1_ema200,
            "H4_EMA50": self.h4_ema50,
            "H4_ATR100": self.h4_atr100,
            "H4_close_current": self.h4_close_current,
            "H1_close_current": self.h1_close_current,
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

        h4_ema50_vals = IndicatorEngine.ema(h4.closes, EMA_50)
        h4_ema200_vals = IndicatorEngine.ema(h4.closes, EMA_200)
        h4_atr100_vals = IndicatorEngine.atr(h4.highs, h4.lows, h4.closes, ATR_PERIOD)

        h1_ema20_vals = IndicatorEngine.ema(h1.closes, EMA_20)
        h1_ema50_vals = IndicatorEngine.ema(h1.closes, EMA_50)
        h1_ema200_vals = IndicatorEngine.ema(h1.closes, EMA_200)
        h1_atr100_vals = IndicatorEngine.atr(h1.highs, h1.lows, h1.closes, ATR_PERIOD)

        h4_timestamps = []
        if "timestamp" in h4.df.columns:
            h4_timestamps = h4.df["timestamp"].tolist()
        elif "close_time" in h4.df.columns:
            h4_timestamps = h4.df["close_time"].tolist()
        elif "time" in h4.df.columns:
            h4_timestamps = h4.df["time"].tolist()

        n = HISTORICAL_DIP_H4_BARS
        h4_closes_recent = h4.closes[-(n + 1):-1] if len(h4.closes) > n else h4.closes[:-1] if len(h4.closes) > 1 else []
        h4_lows_recent = h4.lows[-(n + 1):-1] if len(h4.lows) > n else h4.lows[:-1] if len(h4.lows) > 1 else []
        h4_highs_recent = h4.highs[-(n + 1):-1] if len(h4.highs) > n else h4.highs[:-1] if len(h4.highs) > 1 else []
        h4_ts_recent = h4_timestamps[-(n + 1):-1] if len(h4_timestamps) > n else h4_timestamps[:-1] if len(h4_timestamps) > 1 else []

        indicators = MTFIndicators(
            d1_ema20=_safe_last(d1_ema20_vals),
            d1_ema50=_safe_last(d1_ema50_vals),
            d1_ema200=_safe_last(d1_ema200_vals),
            d1_ema200_prev=_safe_last(d1_ema200_vals, offset=1),
            d1_atr100=_safe_last(d1_atr100_vals),
            h4_ema50=_safe_last(h4_ema50_vals),
            h4_ema200=_safe_last(h4_ema200_vals),
            h4_ema200_prev=_safe_last(h4_ema200_vals, offset=1),
            h4_atr100=_safe_last(h4_atr100_vals),
            h4_close_current=_safe_last(h4.closes),
            h4_close_prev=_safe_last(h4.closes, offset=1),
            h4_closes_recent=h4_closes_recent,
            h4_lows_recent=h4_lows_recent,
            h4_highs_recent=h4_highs_recent,
            h4_timestamps_recent=h4_ts_recent,
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
            f"EMA50={indicators.d1_ema50}, EMA200={indicators.d1_ema200}, "
            f"ATR100={indicators.d1_atr100}"
        )
        logger.info(
            f"[MTF-EMA] {asset} | H4 indicators: "
            f"EMA50={indicators.h4_ema50}, EMA200={indicators.h4_ema200}, "
            f"ATR100={indicators.h4_atr100}, "
            f"close_curr={indicators.h4_close_current}, close_prev={indicators.h4_close_prev}"
        )
        logger.info(
            f"[MTF-EMA] {asset} | H1 indicators: "
            f"EMA20={indicators.h1_ema20}, EMA50={indicators.h1_ema50}, "
            f"EMA200={indicators.h1_ema200}, ATR100={indicators.h1_atr100}"
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

    def _check_historical_dip_long(self, asset: str, ind: MTFIndicators) -> DipInfo:
        closes = ind.h4_closes_recent or []
        timestamps = ind.h4_timestamps_recent or []
        h4_ema50 = ind.h4_ema50
        h4_atr100 = ind.h4_atr100

        dip_info = DipInfo()
        if not closes:
            return dip_info

        max_breach = 0.0
        deepest_idx = None
        for i, c in enumerate(closes):
            if c < h4_ema50:
                breach = h4_ema50 - c
                if breach > max_breach:
                    max_breach = breach
                    deepest_idx = i
                dip_info.found = True

        if not dip_info.found:
            return dip_info

        dip_info.max_breach = max_breach
        dip_info.within_atr = max_breach <= (1.0 * h4_atr100)
        dip_info.recovered = ind.h4_close_current is not None and ind.h4_close_current > h4_ema50
        dip_info.dip_bar_index = deepest_idx

        if deepest_idx is not None and deepest_idx < len(timestamps):
            ts = timestamps[deepest_idx]
            dip_info.dip_bar_timestamp = str(ts)

        return dip_info

    def _check_historical_dip_short(self, asset: str, ind: MTFIndicators) -> DipInfo:
        closes = ind.h4_closes_recent or []
        timestamps = ind.h4_timestamps_recent or []
        h4_ema50 = ind.h4_ema50
        h4_atr100 = ind.h4_atr100

        dip_info = DipInfo()
        if not closes:
            return dip_info

        max_breach = 0.0
        deepest_idx = None
        for i, c in enumerate(closes):
            if c > h4_ema50:
                breach = c - h4_ema50
                if breach > max_breach:
                    max_breach = breach
                    deepest_idx = i
                dip_info.found = True

        if not dip_info.found:
            return dip_info

        dip_info.max_breach = max_breach
        dip_info.within_atr = max_breach <= (1.0 * h4_atr100)
        dip_info.recovered = ind.h4_close_current is not None and ind.h4_close_current < h4_ema50
        dip_info.dip_bar_index = deepest_idx

        if deepest_idx is not None and deepest_idx < len(timestamps):
            ts = timestamps[deepest_idx]
            dip_info.dip_bar_timestamp = str(ts)

        return dip_info

    def _check_long_conditions(self, asset: str, ind: MTFIndicators) -> tuple[bool, Optional[DipInfo]]:
        cond1 = ind.d1_ema50 > ind.d1_ema200
        logger.info(
            f"[MTF-EMA] {asset} | LONG Cond 1 — D1 Trend Filter: "
            f"D1 EMA50 ({ind.d1_ema50:.5f}) > D1 EMA200 ({ind.d1_ema200:.5f}): {cond1}"
        )

        if not cond1:
            c = lambda v: "PASS" if v else "FAIL"
            logger.info(
                f"[MTF-EMA] {asset} | LONG SCORECARD: "
                f"D1Filter={c(cond1)} | HistDip=SKIP "
                f"=> NO SIGNAL (D1 filter failed, skipping dip check)"
            )
            return False, None

        dip = self._check_historical_dip_long(asset, ind)
        cond2 = dip.found and dip.within_atr and dip.recovered
        logger.info(
            f"[MTF-EMA] {asset} | LONG Cond 2 — Historical Dip (last {HISTORICAL_DIP_H4_BARS} H4 bars): "
            f"dip_below_H4_EMA50={dip.found} | max_breach={dip.max_breach:.5f} | "
            f"within 1.0x ATR100 ({ind.h4_atr100:.5f}): {dip.within_atr} | "
            f"recovered (H4 close {ind.h4_close_current} > EMA50 {ind.h4_ema50}): {dip.recovered} | "
            f"dip_timestamp={dip.dip_bar_timestamp or 'N/A'} | "
            f"combined: {cond2}"
        )

        all_met = cond1 and cond2
        c = lambda v: "PASS" if v else "FAIL"
        logger.info(
            f"[MTF-EMA] {asset} | LONG SCORECARD: "
            f"D1Filter={c(cond1)} | HistDip={c(cond2)} "
            f"=> {'SIGNAL' if all_met else 'NO SIGNAL'}"
        )
        return all_met, dip if all_met else None

    def _check_short_conditions(self, asset: str, ind: MTFIndicators) -> tuple[bool, Optional[DipInfo]]:
        cond1 = ind.d1_ema50 < ind.d1_ema200
        logger.info(
            f"[MTF-EMA] {asset} | SHORT Cond 1 — D1 Trend Filter: "
            f"D1 EMA50 ({ind.d1_ema50:.5f}) < D1 EMA200 ({ind.d1_ema200:.5f}): {cond1}"
        )

        if not cond1:
            c = lambda v: "PASS" if v else "FAIL"
            logger.info(
                f"[MTF-EMA] {asset} | SHORT SCORECARD: "
                f"D1Filter={c(cond1)} | HistDip=SKIP "
                f"=> NO SIGNAL (D1 filter failed, skipping dip check)"
            )
            return False, None

        dip = self._check_historical_dip_short(asset, ind)
        cond2 = dip.found and dip.within_atr and dip.recovered
        logger.info(
            f"[MTF-EMA] {asset} | SHORT Cond 2 — Historical Rally (last {HISTORICAL_DIP_H4_BARS} H4 bars): "
            f"rally_above_H4_EMA50={dip.found} | max_breach={dip.max_breach:.5f} | "
            f"within 1.0x ATR100 ({ind.h4_atr100:.5f}): {dip.within_atr} | "
            f"recovered (H4 close {ind.h4_close_current} < EMA50 {ind.h4_ema50}): {dip.recovered} | "
            f"dip_timestamp={dip.dip_bar_timestamp or 'N/A'} | "
            f"combined: {cond2}"
        )

        all_met = cond1 and cond2
        c = lambda v: "PASS" if v else "FAIL"
        logger.info(
            f"[MTF-EMA] {asset} | SHORT SCORECARD: "
            f"D1Filter={c(cond1)} | HistDip={c(cond2)} "
            f"=> {'SIGNAL' if all_met else 'NO SIGNAL'}"
        )
        return all_met, dip if all_met else None

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

        long_triggered, long_dip = self._check_long_conditions(asset, ind)
        if long_triggered and long_dip:
            atr_distance = SL_ATR_MULT * ind.h4_atr100
            atr_sl = current_price - atr_distance

            structural_sl = self._compute_structural_stop_long(
                asset, h1.lows, ind.h4_ema50, current_price
            )

            stop_loss, sl_method = self._select_stop_loss(
                asset, "LONG", current_price, structural_sl, atr_sl, atr_distance
            )

            take_profit = current_price + (TP_ATR_MULT * ind.h4_atr100)

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
                    "h4_ema200": round(ind.h4_ema200, 6) if ind.h4_ema200 else None,
                    "h1_ema20": round(ind.h1_ema20, 6) if ind.h1_ema20 else None,
                    "historical_dip_timestamp": long_dip.dip_bar_timestamp,
                    "historical_dip_max_breach": round(long_dip.max_breach, 6),
                    "historical_dip_bar_index": long_dip.dip_bar_index,
                },
            )

        short_triggered, short_dip = self._check_short_conditions(asset, ind)
        if short_triggered and short_dip:
            atr_distance = SL_ATR_MULT * ind.h4_atr100
            atr_sl = current_price + atr_distance

            structural_sl = self._compute_structural_stop_short(
                asset, h1.highs, ind.h4_ema50, current_price
            )

            stop_loss, sl_method = self._select_stop_loss(
                asset, "SHORT", current_price, structural_sl, atr_sl, atr_distance
            )

            take_profit = current_price - (TP_ATR_MULT * ind.h4_atr100)

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
                    "h4_ema200": round(ind.h4_ema200, 6) if ind.h4_ema200 else None,
                    "h1_ema20": round(ind.h1_ema20, 6) if ind.h1_ema20 else None,
                    "historical_dip_timestamp": short_dip.dip_bar_timestamp,
                    "historical_dip_max_breach": round(short_dip.max_breach, 6),
                    "historical_dip_bar_index": short_dip.dip_bar_index,
                },
            )

        return None

    def _check_h1_ema50_exit(
        self, asset: str, pos_id: int, direction: str, ind: MTFIndicators
    ) -> Optional[SignalResult]:
        h1_close = ind.h1_close_current
        h4_ema50 = ind.h4_ema50

        if h1_close is None or h4_ema50 is None:
            logger.warning(
                f"[MTF-EMA] {asset} | Position #{pos_id} | "
                f"Cannot check H1/H4-EMA50 exit — h1_close={h1_close}, h4_ema50={h4_ema50}"
            )
            return None

        if direction == "BUY" and h1_close < h4_ema50:
            breach = h4_ema50 - h1_close
            logger.info(
                f"[MTF-EMA] {asset} | EXIT LONG #{pos_id} — H1 close below H4 EMA50 | "
                f"H1 close={h1_close:.5f} < H4 EMA50={h4_ema50:.5f} (breach={breach:.5f})"
            )
            return SignalResult(
                action=Action.EXIT,
                direction=Direction.LONG,
                price=h1_close,
                metadata={
                    "exit_reason": (
                        f"H1/H4-EMA50 exit: H1 close {h1_close:.5f} < "
                        f"H4 EMA50 {h4_ema50:.5f} (breach={breach:.5f})"
                    ),
                    "exit_type": "h1_below_h4_ema50",
                    "h1_close": round(h1_close, 6),
                    "h4_ema50": round(h4_ema50, 6),
                    "breach_distance": round(breach, 6),
                },
            )

        if direction == "SELL" and h1_close > h4_ema50:
            breach = h1_close - h4_ema50
            logger.info(
                f"[MTF-EMA] {asset} | EXIT SHORT #{pos_id} — H1 close above H4 EMA50 | "
                f"H1 close={h1_close:.5f} > H4 EMA50={h4_ema50:.5f} (breach={breach:.5f})"
            )
            return SignalResult(
                action=Action.EXIT,
                direction=Direction.SHORT,
                price=h1_close,
                metadata={
                    "exit_reason": (
                        f"H1/H4-EMA50 exit: H1 close {h1_close:.5f} > "
                        f"H4 EMA50 {h4_ema50:.5f} (breach={breach:.5f})"
                    ),
                    "exit_type": "h1_above_h4_ema50",
                    "h1_close": round(h1_close, 6),
                    "h4_ema50": round(h4_ema50, 6),
                    "breach_distance": round(breach, 6),
                },
            )

        logger.info(
            f"[MTF-EMA] {asset} | Position #{pos_id} ({direction}) | "
            f"H1/H4-EMA50 exit NOT triggered — H1 close={h1_close:.5f}, "
            f"H4 EMA50={h4_ema50:.5f}"
        )
        return None

    def _check_exit_conditions(
        self, asset: str, current_price: float, pos: dict, ind: MTFIndicators
    ) -> Optional[SignalResult]:
        pos_id = pos.get("id")
        direction = pos.get("direction")

        exit_result = self._check_h1_ema50_exit(asset, pos_id, direction, ind)
        if exit_result:
            return exit_result

        logger.info(
            f"[MTF-EMA] {asset} | Position #{pos_id} ({direction}) | "
            f"No exit triggered — holding"
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
