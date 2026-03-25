import logging
from typing import Optional
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd

from trading_engine.strategies.base import BaseStrategy, SignalResult, Action, Direction
from trading_engine.indicators import IndicatorEngine
from trading_engine.cache_layer import CacheLayer
from datetime import datetime
from trading_engine.database import (
    has_open_signal,
    has_open_position,
    has_any_open_signal_for_asset,
    close_opposite_signal_if_exists,
    insert_signal,
    open_position as db_open_position,
    close_signal,
    close_position,
    get_active_signals,
    get_all_open_positions,
    signal_exists,
)

logger = logging.getLogger("trading_engine.strategy.multi_timeframe")

STRATEGY_NAME = "mtf_ema"

def _build_target_assets() -> dict[str, list[str]]:
    """Build TARGET_ASSETS from DB at module load time.
    Falls back to hardcoded dict only if DB raises an exception.
    """
    try:
        from trading_engine.database import get_strategy_assets_full
        rows = get_strategy_assets_full(STRATEGY_NAME)
        active = [
            r for r in rows
            if r.get("is_active") is True or r.get("is_active") == 1
        ]
        if active:
            grouped: dict[str, list[str]] = {}
            for r in active:
                key = r.get("sub_category") or r["asset_class"]
                grouped.setdefault(key, []).append(r["symbol"])
            return grouped
    except Exception:
        pass
    return {
        "indices": ["SPX", "NDX", "RUT", "DJI"],
        "commodities": ["XAU/USD", "XAG/USD", "OSX"],
        "crypto": ["BTC/USD", "ETH/USD"],
        "forex": ["GBP/USD", "AUD/USD"],
    }


TARGET_ASSETS = _build_target_assets()
ALL_ASSETS = [s for group in TARGET_ASSETS.values() for s in group]


def _load_target_assets() -> dict[str, list[str]]:
    """Read active MTF EMA assets from DB, grouped by sub_category.
    No hardcoded fallback — returns empty dict and warns if DB empty.
    """
    from trading_engine.database import get_strategy_assets_full
    rows = get_strategy_assets_full(STRATEGY_NAME)
    grouped: dict[str, list[str]] = {}
    for row in rows:
        if not (row.get("is_active") is True or row.get("is_active") == 1):
            continue
        key = row.get("sub_category") or row["asset_class"]
        grouped.setdefault(key, []).append(row["symbol"])
    if not grouped:
        logger.warning("[MTF-EMA] DB returned no active assets")
    return grouped


def get_all_mtf_assets() -> list[str]:
    """Return flat list of all active MTF EMA assets from DB.
    No hardcoded fallback — returns empty list and warns if DB empty.
    """
    from trading_engine.database import get_strategy_assets
    symbols = get_strategy_assets(STRATEGY_NAME, active_only=True)
    if not symbols:
        logger.warning("[MTF-EMA] DB returned no active assets")
    return symbols


FOREX_ASSETS = {"EUR/USD", "USD/JPY", "GBP/USD", "AUD/USD"}

# Indices and equity-like instruments — LONG only per QC algo
# (QC blocks shorts on SecurityType.EQUITY and SecurityType.FUTURE)
LONG_ONLY_ASSETS = {"SPX", "NDX", "RUT", "DJI", "OSX"}

TIMEFRAME_D1 = "D1"
TIMEFRAME_H4 = "4H"
TIMEFRAME_H1 = "1H"
PRIMARY_TIMEFRAME = TIMEFRAME_H1

EMA_20 = 20
EMA_50 = 50
EMA_200 = 200
ATR_PERIOD = 100

MIN_D1_BARS = 250
MIN_H4_BARS = 210
MIN_H1_BARS = 20

SL_ATR_MULT = 0.5
TP_ATR_MULT = 3.0
STRUCTURAL_LOOKBACK_H1 = 24


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

    h4_ema50: Optional[float] = None
    h4_ema200: Optional[float] = None
    h4_ema200_prev: Optional[float] = None
    h4_ema200_series: Optional[list] = None
    h4_atr100: Optional[float] = None
    h4_close_current: Optional[float] = None
    h4_close_prev: Optional[float] = None

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
            self.h4_atr100,
            self.h4_close_current,
            self.h1_ema20,
            self.h1_close_current,
            self.h1_close_prev,
        ]
        return all(v is not None for v in required)

    def missing_names(self) -> list[str]:
        checks = {
            "D1_EMA50": self.d1_ema50,
            "D1_EMA200": self.d1_ema200,
            "D1_EMA200_prev": self.d1_ema200_prev,
            "H4_EMA50": self.h4_ema50,
            "H4_ATR100": self.h4_atr100,
            "H4_close_current": self.h4_close_current,
            "H1_EMA20": self.h1_ema20,
            "H1_close_current": self.h1_close_current,
            "H1_close_prev": self.h1_close_prev,
        }
        return [name for name, val in checks.items() if val is None]


def _safe_last(values: list, offset: int = 0) -> Optional[float]:
    idx = -(1 + offset)
    if len(values) >= (1 + offset) and values[idx] is not None:
        return float(values[idx])
    return None


def _candles_to_lists(
    candles: list[dict],
) -> tuple[list[float], list[float], list[float], list[float]]:
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

    def _get_pip_buffer(self, asset: str) -> float:
        """2-pip buffer matching QC algo.
        forex: 0.0001 pip × 2 = 0.0002
        indices / crypto / commodities: 0.01 × 2 = 0.02
        """
        if asset in FOREX_ASSETS:
            return 0.0002
        return 0.02

    def _fetch_timeframe(
        self, asset: str, timeframe: str, limit: int = 300
    ) -> Optional[TimeframeData]:
        try:
            candles = self.cache.get_candles(asset, timeframe, limit)
        except Exception as e:
            logger.error(
                f"[MTF-EMA] {asset} | Exception fetching {timeframe} candles: {e}"
            )
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
            logger.warning(
                f"[MTF-EMA] {asset} | H1 DataFrame is empty or missing columns"
            )
            return None

        h1_closes = h1_df["close"].tolist()
        h1_highs = h1_df["high"].tolist()
        h1_lows = h1_df["low"].tolist()
        h1_opens = (
            h1_df["open"].tolist()
            if "open" in h1_df.columns
            else [0.0] * len(h1_closes)
        )
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

        indicators = MTFIndicators(
            d1_ema20=_safe_last(d1_ema20_vals),
            d1_ema50=_safe_last(d1_ema50_vals),
            d1_ema200=_safe_last(d1_ema200_vals),
            d1_ema200_prev=_safe_last(d1_ema200_vals, offset=1),
            d1_atr100=_safe_last(d1_atr100_vals),
            h4_ema50=_safe_last(h4_ema50_vals),
            h4_ema200=_safe_last(h4_ema200_vals),
            h4_ema200_prev=_safe_last(h4_ema200_vals, offset=1),
            h4_ema200_series=h4_ema200_vals if h4_ema200_vals else [],
            h4_atr100=_safe_last(h4_atr100_vals),
            h4_close_current=_safe_last(h4.closes),
            h4_close_prev=_safe_last(h4.closes, offset=1),
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
            f"EMA200_prev={indicators.d1_ema200_prev}, ATR100={indicators.d1_atr100}"
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
            logger.warning(
                f"[MTF-EMA] {asset} | Missing required indicators: {missing}"
            )
            return None

        return indicators

    def _compute_h4_ema200_slope(
        self, ind: MTFIndicators
    ) -> tuple[Optional[float], Optional[float]]:
        """Returns (recent_slope, previous_slope) from H4 EMA200 series.
        recent_slope   = ema200[-1] - ema200[-8]
        previous_slope = ema200[-2] - ema200[-9]
        Returns (None, None) if insufficient data.
        """
        series = ind.h4_ema200_series or []
        if len(series) < 9:
            return None, None
        try:
            recent_slope = float(series[-1]) - float(series[-8])
            previous_slope = float(series[-2]) - float(series[-9])
            return recent_slope, previous_slope
        except (TypeError, ValueError):
            return None, None

    def _check_long_conditions(self, asset: str, ind: MTFIndicators) -> bool:
        c = lambda v: "PASS" if v else "FAIL"

        # Cond 1: D1 Trend Filter — price > EMA50 > EMA200 (QC: price must be above both EMAs)
        price = ind.h1_close_current
        cond1 = (ind.d1_ema50 > ind.d1_ema200
                 and price > ind.d1_ema50
                 and price > ind.d1_ema200)
        logger.info(
            f"[MTF-EMA] {asset} | LONG Cond 1 — D1 Trend Filter: "
            f"EMA50 ({ind.d1_ema50:.5f}) > EMA200 ({ind.d1_ema200:.5f}) "
            f"AND price ({price:.5f}) > EMA50 AND price > EMA200: {cond1}"
        )
        if not cond1:
            logger.info(
                f"[MTF-EMA] {asset} | LONG SCORECARD: "
                f"D1Filter={c(cond1)} | D1Slope=SKIP | H4Slope=SKIP | Proximity=SKIP | H1Cross=SKIP "
                f"=> NO SIGNAL (D1 filter failed)"
            )
            return False

        # Cond 2: D1 EMA200 Slope Rising
        cond2 = ind.d1_ema200 > ind.d1_ema200_prev
        logger.info(
            f"[MTF-EMA] {asset} | LONG Cond 2 — D1 EMA200 slope rising: "
            f"{ind.d1_ema200:.5f} > {ind.d1_ema200_prev:.5f}: {cond2}"
        )
        if not cond2:
            logger.info(
                f"[MTF-EMA] {asset} | LONG SCORECARD: "
                f"D1Filter={c(cond1)} | D1Slope={c(cond2)} | H4Slope=SKIP | Proximity=SKIP | H1Cross=SKIP "
                f"=> NO SIGNAL (D1 EMA200 slope not rising)"
            )
            return False

        # Cond 3: H4 EMA200 Slope Accelerating
        recent_slope, previous_slope = self._compute_h4_ema200_slope(ind)
        if recent_slope is None or previous_slope is None:
            logger.warning(
                f"[MTF-EMA] {asset} | LONG Cond 3 — H4 EMA200 slope: insufficient data (need 9 values)"
            )
            logger.info(
                f"[MTF-EMA] {asset} | LONG SCORECARD: "
                f"D1Filter={c(cond1)} | D1Slope={c(cond2)} | H4Slope=SKIP | Proximity=SKIP | H1Cross=SKIP "
                f"=> NO SIGNAL (H4 EMA200 slope insufficient data)"
            )
            return False
        cond3 = recent_slope > 0 and recent_slope > previous_slope
        logger.info(
            f"[MTF-EMA] {asset} | LONG Cond 3 — H4 EMA200 slope accelerating: "
            f"recent={recent_slope:.5f} > prev={previous_slope:.5f}: {cond3}"
        )
        if not cond3:
            logger.info(
                f"[MTF-EMA] {asset} | LONG SCORECARD: "
                f"D1Filter={c(cond1)} | D1Slope={c(cond2)} | H4Slope={c(cond3)} | Proximity=SKIP | H1Cross=SKIP "
                f"=> NO SIGNAL (H4 EMA200 slope not accelerating)"
            )
            return False

        # Cond 4: H4 EMA50 Proximity — price below EMA50 but within 1 ATR
        price = ind.h1_close_current
        h4_ema50 = ind.h4_ema50
        h4_atr = ind.h4_atr100
        cond4 = price < h4_ema50 and (h4_ema50 - price) <= h4_atr
        logger.info(
            f"[MTF-EMA] {asset} | LONG Cond 4 — H4 EMA50 proximity: "
            f"price={price:.5f} < ema50={h4_ema50:.5f}, "
            f"distance={(h4_ema50 - price):.5f} <= ATR={h4_atr:.5f}: {cond4}"
        )
        if not cond4:
            logger.info(
                f"[MTF-EMA] {asset} | LONG SCORECARD: "
                f"D1Filter={c(cond1)} | D1Slope={c(cond2)} | H4Slope={c(cond3)} | Proximity={c(cond4)} | H1Cross=SKIP "
                f"=> NO SIGNAL (H4 EMA50 proximity not met)"
            )
            return False

        # Cond 5: H1 EMA20 Crossover — prev close below EMA20, current close above EMA20, bullish candle
        h1_ema20 = ind.h1_ema20
        h1_prev = ind.h1_close_prev
        h1_curr = ind.h1_close_current
        h1_open = ind.h1_open_current
        bullish_candle = h1_curr > h1_open
        cond5 = h1_prev < h1_ema20 and h1_curr > h1_ema20 and bullish_candle
        logger.info(
            f"[MTF-EMA] {asset} | LONG Cond 5 — H1 EMA20 crossover: "
            f"prev_close={h1_prev:.5f} < ema20={h1_ema20:.5f}, "
            f"curr_close={h1_curr:.5f} > ema20, bullish={bullish_candle}: {cond5}"
        )

        all_met = cond1 and cond2 and cond3 and cond4 and cond5
        logger.info(
            f"[MTF-EMA] {asset} | LONG SCORECARD: "
            f"D1Filter={c(cond1)} | D1Slope={c(cond2)} | H4Slope={c(cond3)} | "
            f"Proximity={c(cond4)} | H1Cross={c(cond5)} "
            f"=> {'SIGNAL' if all_met else 'NO SIGNAL'}"
        )
        return all_met

    def _check_short_conditions(self, asset: str, ind: MTFIndicators) -> bool:
        c = lambda v: "PASS" if v else "FAIL"

        # Cond 1: D1 Trend Filter — price < EMA50 < EMA200 (QC: price must be below both EMAs)
        price = ind.h1_close_current
        cond1 = (ind.d1_ema50 < ind.d1_ema200
                 and price < ind.d1_ema50
                 and price < ind.d1_ema200)
        logger.info(
            f"[MTF-EMA] {asset} | SHORT Cond 1 — D1 Trend Filter: "
            f"EMA50 ({ind.d1_ema50:.5f}) < EMA200 ({ind.d1_ema200:.5f}) "
            f"AND price ({price:.5f}) < EMA50 AND price < EMA200: {cond1}"
        )
        if not cond1:
            logger.info(
                f"[MTF-EMA] {asset} | SHORT SCORECARD: "
                f"D1Filter={c(cond1)} | D1Slope=SKIP | H4Slope=SKIP | Proximity=SKIP | H1Cross=SKIP "
                f"=> NO SIGNAL (D1 filter failed)"
            )
            return False

        # Cond 2: D1 EMA200 Slope Falling
        cond2 = ind.d1_ema200 < ind.d1_ema200_prev
        logger.info(
            f"[MTF-EMA] {asset} | SHORT Cond 2 — D1 EMA200 slope falling: "
            f"{ind.d1_ema200:.5f} < {ind.d1_ema200_prev:.5f}: {cond2}"
        )
        if not cond2:
            logger.info(
                f"[MTF-EMA] {asset} | SHORT SCORECARD: "
                f"D1Filter={c(cond1)} | D1Slope={c(cond2)} | H4Slope=SKIP | Proximity=SKIP | H1Cross=SKIP "
                f"=> NO SIGNAL (D1 EMA200 slope not falling)"
            )
            return False

        # Cond 3: H4 EMA200 Slope Accelerating Downward
        recent_slope, previous_slope = self._compute_h4_ema200_slope(ind)
        if recent_slope is None or previous_slope is None:
            logger.warning(
                f"[MTF-EMA] {asset} | SHORT Cond 3 — H4 EMA200 slope: insufficient data (need 9 values)"
            )
            logger.info(
                f"[MTF-EMA] {asset} | SHORT SCORECARD: "
                f"D1Filter={c(cond1)} | D1Slope={c(cond2)} | H4Slope=SKIP | Proximity=SKIP | H1Cross=SKIP "
                f"=> NO SIGNAL (H4 EMA200 slope insufficient data)"
            )
            return False
        cond3 = recent_slope < 0 and recent_slope < previous_slope
        logger.info(
            f"[MTF-EMA] {asset} | SHORT Cond 3 — H4 EMA200 slope accelerating downward: "
            f"recent={recent_slope:.5f} < prev={previous_slope:.5f}: {cond3}"
        )
        if not cond3:
            logger.info(
                f"[MTF-EMA] {asset} | SHORT SCORECARD: "
                f"D1Filter={c(cond1)} | D1Slope={c(cond2)} | H4Slope={c(cond3)} | Proximity=SKIP | H1Cross=SKIP "
                f"=> NO SIGNAL (H4 EMA200 slope not decelerating)"
            )
            return False

        # Cond 4: H4 EMA50 Proximity — price above EMA50 but within 1 ATR
        price = ind.h1_close_current
        h4_ema50 = ind.h4_ema50
        h4_atr = ind.h4_atr100
        cond4 = price > h4_ema50 and (price - h4_ema50) <= h4_atr
        logger.info(
            f"[MTF-EMA] {asset} | SHORT Cond 4 — H4 EMA50 proximity: "
            f"price={price:.5f} > ema50={h4_ema50:.5f}, "
            f"distance={(price - h4_ema50):.5f} <= ATR={h4_atr:.5f}: {cond4}"
        )
        if not cond4:
            logger.info(
                f"[MTF-EMA] {asset} | SHORT SCORECARD: "
                f"D1Filter={c(cond1)} | D1Slope={c(cond2)} | H4Slope={c(cond3)} | Proximity={c(cond4)} | H1Cross=SKIP "
                f"=> NO SIGNAL (H4 EMA50 proximity not met)"
            )
            return False

        # Cond 5: H1 EMA20 Crossover — prev close above EMA20, current close below EMA20, bearish candle
        h1_ema20 = ind.h1_ema20
        h1_prev = ind.h1_close_prev
        h1_curr = ind.h1_close_current
        h1_open = ind.h1_open_current
        bearish_candle = h1_curr < h1_open
        cond5 = h1_prev > h1_ema20 and h1_curr < h1_ema20 and bearish_candle
        logger.info(
            f"[MTF-EMA] {asset} | SHORT Cond 5 — H1 EMA20 crossover: "
            f"prev_close={h1_prev:.5f} > ema20={h1_ema20:.5f}, "
            f"curr_close={h1_curr:.5f} < ema20, bearish={bearish_candle}: {cond5}"
        )

        all_met = cond1 and cond2 and cond3 and cond4 and cond5
        logger.info(
            f"[MTF-EMA] {asset} | SHORT SCORECARD: "
            f"D1Filter={c(cond1)} | D1Slope={c(cond2)} | H4Slope={c(cond3)} | "
            f"Proximity={c(cond4)} | H1Cross={c(cond5)} "
            f"=> {'SIGNAL' if all_met else 'NO SIGNAL'}"
        )
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
        pip_buffer = self._get_pip_buffer(asset)
        structural_sl = lowest - pip_buffer
        logger.info(
            f"[MTF-EMA] {asset} | LONG structural stop: "
            f"lowest H1 low below H4 EMA50 = {lowest:.5f} - {pip_buffer} pip buffer = {structural_sl:.5f} | "
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
        pip_buffer = self._get_pip_buffer(asset)
        structural_sl = highest + pip_buffer
        logger.info(
            f"[MTF-EMA] {asset} | SHORT structural stop: "
            f"highest H1 high above H4 EMA50 = {highest:.5f} + {pip_buffer} pip buffer = {structural_sl:.5f} | "
            f"distance from entry = {structural_sl - entry_price:.5f}"
        )
        return structural_sl

    def _select_stop_loss(
        self,
        asset: str,
        direction: str,
        entry_price: float,
        structural_sl: Optional[float],
        atr_sl: float,
        atr_distance: float,
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

    def _persist_entry(
        self,
        asset: str,
        direction: str,
        current_price: float,
        stop_loss: float,
        take_profit: float,
        atr_at_entry: float,
        entry_metadata: dict,
    ) -> Optional[dict]:
        try:
            from pytz import timezone as pytz_timezone

            now_et = datetime.now(pytz_timezone("US/Eastern"))
        except Exception:
            now_et = datetime.utcnow()
        signal_timestamp = now_et.strftime("%Y-%m-%dT%H:%M:%S")

        if signal_exists(STRATEGY_NAME, asset, signal_timestamp):
            logger.warning(
                f"[MTF-EMA] {asset} | Duplicate signal blocked for "
                f"signal_timestamp={signal_timestamp}"
            )
            return None

        signal = {
            "strategy_name": STRATEGY_NAME,
            "asset": asset,
            "direction": direction,
            "action": "ENTRY",
            "entry_price": current_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "atr_at_entry": round(atr_at_entry, 6),
            "signal_timestamp": signal_timestamp,
        }
        close_opposite_signal_if_exists(STRATEGY_NAME, asset, direction)
        signal_id = insert_signal(signal)
        if signal_id:
            db_open_position(
                {
                    "asset": asset,
                    "strategy_name": STRATEGY_NAME,
                    "direction": direction,
                    "entry_price": current_price,
                    "atr_at_entry": round(atr_at_entry, 6),
                }
            )
            signal["id"] = signal_id
            signal["status"] = "OPEN"
            logger.info(
                f"[MTF-EMA] {asset} | Signal + position persisted with id={signal_id}"
            )
            return signal
        else:
            logger.error(f"[MTF-EMA] {asset} | Failed to persist signal to DB")
            return None

    def _check_entry_conditions(
        self, asset: str, current_price: float, ind: MTFIndicators, tf_data: dict
    ) -> Optional[SignalResult]:
        logger.info(
            f"[MTF-EMA] {asset} | Checking entry conditions | price={current_price:.5f}"
        )

        h1 = tf_data["h1"]
        recent_slope, previous_slope = self._compute_h4_ema200_slope(ind)

        if self._check_long_conditions(asset, ind):
            atr_distance = SL_ATR_MULT * ind.h4_atr100
            atr_sl = current_price - atr_distance

            structural_sl = self._compute_structural_stop_long(
                asset, h1.lows, ind.h4_ema50, current_price
            )

            stop_loss, sl_method = self._select_stop_loss(
                asset, "LONG", current_price, structural_sl, atr_sl, atr_distance
            )

            take_profit = current_price + (TP_ATR_MULT * ind.h4_atr100)

            entry_metadata = {
                "take_profit": take_profit,
                "sl_method": sl_method,
                "sl_atr_value": round(atr_sl, 6),
                "sl_structural_value": round(structural_sl, 6)
                if structural_sl
                else None,
                "sl_atr_mult": SL_ATR_MULT,
                "tp_atr_mult": TP_ATR_MULT,
                "h4_atr100": round(ind.h4_atr100, 6),
                "d1_ema200": round(ind.d1_ema200, 6),
                "d1_ema50": round(ind.d1_ema50, 6),
                "h4_ema50": round(ind.h4_ema50, 6),
                "h4_ema200": round(ind.h4_ema200, 6) if ind.h4_ema200 else None,
                "h1_ema20_at_entry": round(ind.h1_ema20, 6) if ind.h1_ema20 else None,
                "h4_ema200_recent_slope": round(recent_slope, 6)
                if recent_slope is not None
                else None,
                "h4_ema200_previous_slope": round(previous_slope, 6)
                if previous_slope is not None
                else None,
                "d1_ema200_slope_rising": ind.d1_ema200 > ind.d1_ema200_prev,
                "h1_cross_confirmed": True,
            }

            persisted = self._persist_entry(
                asset,
                "BUY",
                current_price,
                stop_loss,
                take_profit,
                ind.h4_atr100,
                entry_metadata,
            )
            if not persisted:
                return None

            return SignalResult(
                action=Action.ENTRY,
                direction=Direction.LONG,
                price=current_price,
                stop_loss=stop_loss,
                atr_at_entry=ind.h4_atr100,
                metadata={"signal": persisted, **entry_metadata},
            )

        if self._check_short_conditions(asset, ind):
            atr_distance = SL_ATR_MULT * ind.h4_atr100
            atr_sl = current_price + atr_distance

            structural_sl = self._compute_structural_stop_short(
                asset, h1.highs, ind.h4_ema50, current_price
            )

            stop_loss, sl_method = self._select_stop_loss(
                asset, "SHORT", current_price, structural_sl, atr_sl, atr_distance
            )

            take_profit = current_price - (TP_ATR_MULT * ind.h4_atr100)

            entry_metadata = {
                "take_profit": take_profit,
                "sl_method": sl_method,
                "sl_atr_value": round(atr_sl, 6),
                "sl_structural_value": round(structural_sl, 6)
                if structural_sl
                else None,
                "sl_atr_mult": SL_ATR_MULT,
                "tp_atr_mult": TP_ATR_MULT,
                "h4_atr100": round(ind.h4_atr100, 6),
                "d1_ema200": round(ind.d1_ema200, 6),
                "d1_ema50": round(ind.d1_ema50, 6),
                "h4_ema50": round(ind.h4_ema50, 6),
                "h4_ema200": round(ind.h4_ema200, 6) if ind.h4_ema200 else None,
                "h1_ema20_at_entry": round(ind.h1_ema20, 6) if ind.h1_ema20 else None,
                "h4_ema200_recent_slope": round(recent_slope, 6)
                if recent_slope is not None
                else None,
                "h4_ema200_previous_slope": round(previous_slope, 6)
                if previous_slope is not None
                else None,
                "d1_ema200_slope_rising": False,
                "h1_cross_confirmed": True,
            }

            persisted = self._persist_entry(
                asset,
                "SELL",
                current_price,
                stop_loss,
                take_profit,
                ind.h4_atr100,
                entry_metadata,
            )
            if not persisted:
                return None

            return SignalResult(
                action=Action.ENTRY,
                direction=Direction.SHORT,
                price=current_price,
                stop_loss=stop_loss,
                atr_at_entry=ind.h4_atr100,
                metadata={"signal": persisted, **entry_metadata},
            )

        return None

    def _persist_exit(self, asset: str, exit_reason: str):
        active_sigs = get_active_signals(strategy_name=STRATEGY_NAME, asset=asset)
        for sig in active_sigs:
            close_signal(sig["id"], exit_reason)
            logger.info(
                f"[MTF-EMA] {asset} | Closed signal #{sig['id']}: {exit_reason}"
            )
        close_position(STRATEGY_NAME, asset)
        logger.info(f"[MTF-EMA] {asset} | Position closed in DB")

    def _check_h1_ema20_exit(
        self, asset: str, pos_id: int, direction: str, ind: MTFIndicators
    ) -> Optional[SignalResult]:
        h1_close = ind.h1_close_current
        h1_ema20 = ind.h1_ema20

        if h1_close is None or h1_ema20 is None:
            logger.warning(
                f"[MTF-EMA] {asset} | Position #{pos_id} | "
                f"Cannot check H1 EMA20 exit — h1_close={h1_close}, h1_ema20={h1_ema20}"
            )
            return None

        if direction == "BUY" and h1_close < h1_ema20:
            breach = h1_ema20 - h1_close
            exit_reason = (
                f"H1/H1-EMA20 exit: H1 close {h1_close:.5f} < "
                f"H1 EMA20 {h1_ema20:.5f} (breach={breach:.5f})"
            )
            logger.info(
                f"[MTF-EMA] {asset} | EXIT LONG #{pos_id} — H1 close below H1 EMA20 | "
                f"H1 close={h1_close:.5f} < H1 EMA20={h1_ema20:.5f} (breach={breach:.5f})"
            )
            self._persist_exit(asset, exit_reason)
            return SignalResult(
                action=Action.EXIT,
                direction=Direction.LONG,
                price=h1_close,
                metadata={
                    "exit_reason": exit_reason,
                    "exit_type": "h1_below_h1_ema20",
                    "h1_close": round(h1_close, 6),
                    "h1_ema20": round(h1_ema20, 6),
                    "breach_distance": round(breach, 6),
                },
            )

        if direction == "SELL" and h1_close > h1_ema20:
            breach = h1_close - h1_ema20
            exit_reason = (
                f"H1/H1-EMA20 exit: H1 close {h1_close:.5f} > "
                f"H1 EMA20 {h1_ema20:.5f} (breach={breach:.5f})"
            )
            logger.info(
                f"[MTF-EMA] {asset} | EXIT SHORT #{pos_id} — H1 close above H1 EMA20 | "
                f"H1 close={h1_close:.5f} > H1 EMA20={h1_ema20:.5f} (breach={breach:.5f})"
            )
            self._persist_exit(asset, exit_reason)
            return SignalResult(
                action=Action.EXIT,
                direction=Direction.SHORT,
                price=h1_close,
                metadata={
                    "exit_reason": exit_reason,
                    "exit_type": "h1_above_h1_ema20",
                    "h1_close": round(h1_close, 6),
                    "h1_ema20": round(h1_ema20, 6),
                    "breach_distance": round(breach, 6),
                },
            )

        logger.info(
            f"[MTF-EMA] {asset} | Position #{pos_id} ({direction}) | "
            f"H1 EMA20 exit NOT triggered — H1 close={h1_close:.5f}, "
            f"H1 EMA20={h1_ema20:.5f}"
        )
        return None

    def _check_exit_conditions(
        self, asset: str, current_price: float, pos: dict, ind: MTFIndicators
    ) -> Optional[SignalResult]:
        pos_id = pos.get("id")
        direction = pos.get("direction")

        exit_result = self._check_h1_ema20_exit(asset, pos_id, direction, ind)
        if exit_result:
            return exit_result

        logger.info(
            f"[MTF-EMA] {asset} | Position #{pos_id} ({direction}) | "
            f"No exit triggered — holding"
        )
        return None

    def _check_long_only_entry(
        self,
        asset: str,
        current_price: float,
        ind: MTFIndicators,
        tf_data: dict,
    ) -> Optional[SignalResult]:
        """LONG-only entry check for index assets (SPX, NDX, RUT, DJI, OSX).

        Mirrors _check_entry_conditions() but skips the short path entirely,
        matching QC algo behavior where EQUITY and FUTURE types cannot go short.
        """
        if self._check_long_conditions(asset, ind):
            h1 = tf_data["h1"]
            atr_distance = SL_ATR_MULT * ind.h4_atr100
            atr_sl = current_price - atr_distance

            structural_sl = self._compute_structural_stop_long(
                asset, h1.lows, ind.h4_ema50, current_price
            )
            stop_loss, sl_method = self._select_stop_loss(
                asset, "LONG", current_price, structural_sl, atr_sl, atr_distance
            )
            take_profit = current_price + (TP_ATR_MULT * ind.h4_atr100)
            recent_slope, previous_slope = self._compute_h4_ema200_slope(ind)

            entry_metadata = {
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
                "h1_ema20_at_entry": round(ind.h1_ema20, 6) if ind.h1_ema20 else None,
                "h4_ema200_recent_slope": round(recent_slope, 6) if recent_slope is not None else None,
                "h4_ema200_previous_slope": round(previous_slope, 6) if previous_slope is not None else None,
                "d1_ema200_slope_rising": ind.d1_ema200 > ind.d1_ema200_prev,
                "h1_cross_confirmed": True,
                "long_only_asset": True,
            }
            persisted = self._persist_entry(
                asset, "BUY", current_price, stop_loss, take_profit,
                ind.h4_atr100, entry_metadata,
            )
            if not persisted:
                return None
            return SignalResult(
                action=Action.ENTRY,
                direction=Direction.LONG,
                price=current_price,
                stop_loss=stop_loss,
                atr_at_entry=ind.h4_atr100,
                metadata={"signal": persisted, **entry_metadata},
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
            logger.warning(
                f"[MTF-EMA] {asset} | Primary H1 DataFrame is empty — skipping"
            )
            return SignalResult()

        tf_data = self._fetch_all_timeframes(asset, df)
        if tf_data is None:
            return SignalResult()

        indicators = self._compute_indicators(asset, tf_data)
        if indicators is None:
            return SignalResult()

        current_price = float(df["close"].iloc[-1])

        if open_position and open_position.get("direction"):
            exit_result = self._check_exit_conditions(
                asset, current_price, open_position, indicators
            )
            if exit_result:
                return exit_result
            logger.info(f"[MTF-EMA] {asset} | Position open — no exit triggered")
            return SignalResult()

        if has_open_position(STRATEGY_NAME, asset):
            logger.info(
                f"[MTF-EMA] {asset} | Position already open — skipping entry check"
            )
            return SignalResult()

        if has_any_open_signal_for_asset(
            asset,
            exclude_strategies=["sp500_momentum"],
        ):
            logger.info(
                f"[MTF-EMA] {asset} | IDEMPOTENCY BLOCK: "
                f"An OPEN signal already exists for this asset "
                f"(cross-strategy check) — entry skipped"
            )
            return SignalResult()

        if has_open_signal(STRATEGY_NAME, asset):
            logger.info(
                f"[MTF-EMA] {asset} | Active signal exists — skipping entry check"
            )
            return SignalResult()

        # ── D1 EMA Trend Filter (hard pre-screen gate) ──────────────────────
        # Requirement: D1 EMA50 > D1 EMA200 for LONG; D1 EMA50 < D1 EMA200 for SHORT.
        # This is an explicit early-return gate that runs BEFORE all 5 entry conditions.
        d1_ema50 = indicators.d1_ema50
        d1_ema200 = indicators.d1_ema200
        if d1_ema50 is None or d1_ema200 is None:
            logger.warning(
                f"[MTF-EMA] {asset} | D1 EMA Trend Filter: "
                f"EMA50={d1_ema50} / EMA200={d1_ema200} — one or both unavailable, "
                f"entry blocked until D1 data is sufficient (need {MIN_D1_BARS} bars)"
            )
            return SignalResult(action=Action.NONE)

        d1_bull = d1_ema50 > d1_ema200
        logger.info(
            f"[MTF-EMA] {asset} | D1 EMA Trend Filter: "
            f"EMA50={d1_ema50:.5f} {'>' if d1_bull else '<'} EMA200={d1_ema200:.5f} "
            f"=> Regime={'BULL — LONG entries eligible' if d1_bull else 'BEAR — SHORT entries eligible (LONG blocked)'}"
        )
        if asset in LONG_ONLY_ASSETS and not d1_bull:
            logger.info(
                f"[MTF-EMA] {asset} | D1 EMA Trend Filter: BEAR regime on LONG_ONLY asset "
                f"(D1 EMA50 {d1_ema50:.5f} < EMA200 {d1_ema200:.5f}) => NO ENTRY"
            )
            return SignalResult(action=Action.NONE)
        # ── End D1 EMA Trend Filter ──────────────────────────────────────────

        if asset in LONG_ONLY_ASSETS:
            logger.info(
                f"[MTF-EMA] {asset} | LONG_ONLY asset — "
                f"short entry blocked per QC algo (equity/index restriction)"
            )
            entry_result = self._check_long_only_entry(
                asset, current_price, indicators, tf_data
            )
        else:
            entry_result = self._check_entry_conditions(
                asset, current_price, indicators, tf_data
            )
        if entry_result:
            return entry_result

        return SignalResult()

    def _close_orphaned_signals(self) -> list[dict]:
        """Close any OPEN signals that have no corresponding open_positions record.

        Handles post-restart desync where open_positions is cleared but signals
        remain OPEN.

        Exit rule (mirrors QC algo): close only when H1 close has crossed the
        H1 EMA20 exit threshold. If candles are unavailable or EMA20 cannot be
        computed, close unconditionally — a position with no tracking record
        cannot be managed safely.
        """
        orphans = get_active_signals(strategy_name=STRATEGY_NAME)
        if not orphans:
            return []

        positions = get_all_open_positions(strategy_name=STRATEGY_NAME)
        position_assets = {p["asset"] for p in positions}

        closed: list[dict] = []
        for sig in orphans:
            asset = sig["asset"]
            if asset in position_assets:
                continue

            sig_id = sig["id"]
            direction = sig.get("direction", "")
            entry_price = sig.get("entry_price", 0)
            ts_raw = sig.get("signal_timestamp") or ""

            hours_open = None
            try:
                entry_time = datetime.strptime(str(ts_raw)[:19], "%Y-%m-%dT%H:%M:%S")
                import pytz as _pytz

                entry_time = _pytz.timezone("America/New_York").localize(entry_time)
                now_et = datetime.now(_pytz.utc).astimezone(
                    _pytz.timezone("America/New_York")
                )
                hours_open = (now_et - entry_time).total_seconds() / 3600
            except (ValueError, TypeError):
                pass

            hours_str = (
                f"{hours_open:.1f}h" if hours_open is not None else "unknown duration"
            )
            logger.warning(
                f"[MTF-EMA-EXIT] ORPHAN SIGNAL detected | id={sig_id} | {asset} {direction} | "
                f"entry={entry_price} | hours_open={hours_str} | "
                f"No open_positions record — checking H1 EMA20 exit rule"
            )

            # ── Attempt QC-aligned exit check ──────────────────────────────
            # Mirror the live check_exits() logic: close only when the H1/EMA20
            # threshold has been crossed.  If we cannot compute the indicator,
            # fall back to an unconditional close with a clear reason tag.
            exit_price = entry_price
            ema_exit_triggered = False
            ema_exit_details = "EMA20 check skipped — insufficient data"

            try:
                h1_candles = self.cache.get_candles(asset, TIMEFRAME_H1, 300)
                if h1_candles and len(h1_candles) >= EMA_20 + 1:
                    h1_closes = [float(c["close"]) for c in h1_candles]
                    h1_ema20_series = IndicatorEngine.ema(h1_closes, EMA_20)
                    h1_ema20 = (
                        h1_ema20_series[-1]
                        if h1_ema20_series and h1_ema20_series[-1] is not None
                        else None
                    )
                    h1_close = float(h1_candles[-1]["close"])
                    exit_price = h1_close

                    if h1_ema20 is not None:
                        if direction == "BUY":
                            ema_exit_triggered = h1_close < h1_ema20
                        elif direction == "SELL":
                            ema_exit_triggered = h1_close > h1_ema20

                        ema_exit_details = (
                            f"H1_close={h1_close:.5f} "
                            f"{'<' if direction == 'BUY' else '>'} "
                            f"H1_EMA20={h1_ema20:.5f} → "
                            f"exit_triggered={ema_exit_triggered}"
                        )
                    else:
                        ema_exit_details = (
                            "H1 EMA20 returned None — closing unconditionally"
                        )
                        ema_exit_triggered = True  # cannot manage without the indicator
                else:
                    candle_count = len(h1_candles) if h1_candles else 0
                    ema_exit_details = (
                        f"Insufficient H1 candles ({candle_count} < {EMA_20 + 1}) "
                        f"— closing unconditionally"
                    )
                    ema_exit_triggered = True  # cannot manage without candles
            except Exception as e:
                logger.warning(
                    f"[MTF-EMA-EXIT] ORPHAN id={sig_id} | EMA20 check failed: {e}"
                )
                ema_exit_details = (
                    f"EMA20 check exception ({e}) — closing unconditionally"
                )
                ema_exit_triggered = True

            if not ema_exit_triggered:
                logger.info(
                    f"[MTF-EMA-EXIT] ORPHAN id={sig_id} | {asset} {direction} | "
                    f"EMA20 exit NOT triggered — holding orphan open | {ema_exit_details}"
                )
                continue

            exit_reason = (
                f"Orphaned signal close | No open_positions record (cleared on restart) | "
                f"open for {hours_str} | {ema_exit_details}"
            )
            close_signal(sig_id, exit_reason, exit_price=exit_price)
            closed.append(
                {
                    "asset": asset,
                    "direction": direction,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "exit_reason": "orphaned_no_position_record",
                }
            )
            logger.info(
                f"[MTF-EMA-EXIT] ORPHAN SIGNAL closed | id={sig_id} | {asset} {direction} | "
                f"exit_price={exit_price} | {ema_exit_details}"
            )

        return closed

    def check_exits(self) -> list[dict]:
        """Standalone exit checker called by the scheduler after the per-asset evaluation loop.

        Ensures H1 EMA20 exits fire even if evaluate() was skipped for an asset.

        Exit rules (same as _check_h1_ema20_exit):
          LONG:  close when H1 close < H1 EMA20
          SHORT: close when H1 close > H1 EMA20

        IMPORTANT: only closes positions via the strategy's own exit logic.
        Never closes a position just because conditions changed or because the signal is old.
        """
        closed: list[dict] = []

        # Safety net: close any OPEN signals with no position record
        orphan_closes = self._close_orphaned_signals()
        closed.extend(orphan_closes)
        if orphan_closes:
            logger.warning(
                f"[MTF-EMA-EXIT] {len(orphan_closes)} orphaned signal(s) closed — "
                f"open_positions was out of sync with signals table"
            )

        positions = get_all_open_positions(strategy_name=STRATEGY_NAME)

        if not positions:
            logger.info("[MTF-EMA-EXIT] ====== check_exits | 0 open positions ======")
            return closed

        logger.info(
            f"[MTF-EMA-EXIT] ====== check_exits | "
            f"{len(positions)} open position(s) ======"
        )

        for pos in positions:
            asset: str = pos["asset"]
            pos_id: int = pos["id"]
            direction: str = pos["direction"]

            logger.info(
                f"[MTF-EMA-EXIT] Position #{pos_id} | {asset} | "
                f"{direction} | entry={pos['entry_price']}"
            )

            # Fetch H1 candles for current price and EMA20
            try:
                h1_candles = self.cache.get_candles(asset, TIMEFRAME_H1, 300)
            except Exception as e:
                logger.error(
                    f"[MTF-EMA-EXIT] Position #{pos_id} | {asset} | "
                    f"Exception fetching H1 candles: {e}"
                )
                continue

            if not h1_candles or len(h1_candles) < EMA_20 + 1:
                logger.warning(
                    f"[MTF-EMA-EXIT] Position #{pos_id} | {asset} | "
                    f"Insufficient H1 candles for EMA20: {len(h1_candles) if h1_candles else 0}"
                )
                continue

            h1_closes: list[float] = [float(c["close"]) for c in h1_candles]
            h1_ema20_series = IndicatorEngine.ema(h1_closes, EMA_20)
            h1_ema20: Optional[float] = (
                h1_ema20_series[-1]
                if h1_ema20_series and h1_ema20_series[-1] is not None
                else None
            )

            if h1_ema20 is None:
                logger.warning(
                    f"[MTF-EMA-EXIT] Position #{pos_id} | {asset} | "
                    f"H1 EMA20 returned None — skipping"
                )
                continue

            h1_close = float(h1_candles[-1]["close"])

            logger.info(
                f"[MTF-EMA-EXIT] Position #{pos_id} | {asset} | {direction} | "
                f"H1_close={h1_close:.5f} | H1_EMA20={h1_ema20:.5f} | "
                f"LONG_exit(H1<EMA20)={h1_close < h1_ema20} | "
                f"SHORT_exit(H1>EMA20)={h1_close > h1_ema20}"
            )

            exit_triggered = False
            exit_reason = ""

            if direction == "BUY" and h1_close < h1_ema20:
                breach = h1_ema20 - h1_close
                exit_reason = (
                    f"H1/H1-EMA20 exit: H1 close {h1_close:.5f} < "
                    f"H1 EMA20 {h1_ema20:.5f} (breach={breach:.5f})"
                )
                exit_triggered = True
                logger.info(
                    f"[MTF-EMA-EXIT] Position #{pos_id} | EXIT LONG | "
                    f"H1 {h1_close:.5f} < H1 EMA20 {h1_ema20:.5f}"
                )

            elif direction == "SELL" and h1_close > h1_ema20:
                breach = h1_close - h1_ema20
                exit_reason = (
                    f"H1/H1-EMA20 exit: H1 close {h1_close:.5f} > "
                    f"H1 EMA20 {h1_ema20:.5f} (breach={breach:.5f})"
                )
                exit_triggered = True
                logger.info(
                    f"[MTF-EMA-EXIT] Position #{pos_id} | EXIT SHORT | "
                    f"H1 {h1_close:.5f} > H1 EMA20 {h1_ema20:.5f}"
                )

            if exit_triggered:
                self._persist_exit(asset, exit_reason)
                closed.append(
                    {
                        **pos,
                        "exit_price": h1_close,
                        "exit_reason": "h1_ema20_cross",
                    }
                )
            else:
                # ── Priority 2: trailing stop (peak/trough − 2× ATR at entry) ──
                atr_at_entry = pos.get("atr_at_entry")
                if atr_at_entry is not None and atr_at_entry > 0:
                    highest = pos.get("highest_price_since_entry") or pos["entry_price"]
                    lowest  = pos.get("lowest_price_since_entry")  or pos["entry_price"]

                    if direction == "BUY":
                        trail_stop = highest - (2.0 * atr_at_entry)
                        trail_hit  = h1_close <= trail_stop
                        logger.info(
                            f"[MTF-EMA-EXIT] Position #{pos_id} | {asset} | "
                            f"Trailing stop check (LONG): "
                            f"h1_close={h1_close:.5f} | peak={highest:.5f} | "
                            f"stop={trail_stop:.5f} (peak - 2×{atr_at_entry:.6f}) | "
                            f"hit={trail_hit}"
                        )
                        if trail_hit:
                            trail_reason = (
                                f"Trailing stop hit (LONG) | "
                                f"h1_close={h1_close:.5f} <= "
                                f"peak({highest:.5f}) - 2×ATR({atr_at_entry:.6f}) = {trail_stop:.5f}"
                            )
                            logger.info(
                                f"[MTF-EMA-EXIT] Position #{pos_id} | EXIT LONG (trailing stop) | "
                                f"{asset}"
                            )
                            self._persist_exit(asset, trail_reason)
                            closed.append({**pos, "exit_price": h1_close, "exit_reason": "trailing_stop"})
                            continue

                    elif direction == "SELL":
                        trail_stop = lowest + (2.0 * atr_at_entry)
                        trail_hit  = h1_close >= trail_stop
                        logger.info(
                            f"[MTF-EMA-EXIT] Position #{pos_id} | {asset} | "
                            f"Trailing stop check (SHORT): "
                            f"h1_close={h1_close:.5f} | trough={lowest:.5f} | "
                            f"stop={trail_stop:.5f} (trough + 2×{atr_at_entry:.6f}) | "
                            f"hit={trail_hit}"
                        )
                        if trail_hit:
                            trail_reason = (
                                f"Trailing stop hit (SHORT) | "
                                f"h1_close={h1_close:.5f} >= "
                                f"trough({lowest:.5f}) + 2×ATR({atr_at_entry:.6f}) = {trail_stop:.5f}"
                            )
                            logger.info(
                                f"[MTF-EMA-EXIT] Position #{pos_id} | EXIT SHORT (trailing stop) | "
                                f"{asset}"
                            )
                            self._persist_exit(asset, trail_reason)
                            closed.append({**pos, "exit_price": h1_close, "exit_reason": "trailing_stop"})
                            continue

                    logger.info(
                        f"[MTF-EMA-EXIT] Position #{pos_id} | {asset} | "
                        f"Holding {direction} — neither EMA20 nor trailing stop triggered"
                    )
                else:
                    logger.warning(
                        f"[MTF-EMA-EXIT] Position #{pos_id} | {asset} | "
                        f"atr_at_entry missing — trailing stop cannot be evaluated, holding"
                    )

        logger.info(
            f"[MTF-EMA-EXIT] ====== check_exits complete | {len(closed)} closed ======"
        )
        return closed
