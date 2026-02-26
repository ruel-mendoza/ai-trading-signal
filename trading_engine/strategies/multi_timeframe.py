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

SL_ATR_MULT = 2.0
TP_ATR_MULT = 3.0
TRAILING_STOP_ATR_MULT = 2.0


@dataclass
class TimeframeData:
    timeframe: str
    df: pd.DataFrame
    closes: list[float]
    highs: list[float]
    lows: list[float]


@dataclass
class MTFIndicators:
    d1_ema20: Optional[float] = None
    d1_ema50: Optional[float] = None
    d1_ema200: Optional[float] = None
    d1_atr100: Optional[float] = None

    h4_ema20: Optional[float] = None
    h4_ema50: Optional[float] = None
    h4_ema200: Optional[float] = None
    h4_ema200_prev: Optional[float] = None
    h4_atr100: Optional[float] = None

    h1_ema20: Optional[float] = None
    h1_ema50: Optional[float] = None
    h1_ema200: Optional[float] = None
    h1_atr100: Optional[float] = None

    def all_required_present(self) -> bool:
        required = [
            self.d1_ema50,
            self.d1_ema200,
            self.h4_ema50,
            self.h4_ema200,
            self.h4_ema200_prev,
            self.h4_atr100,
            self.h1_ema20,
        ]
        return all(v is not None for v in required)

    def missing_names(self) -> list[str]:
        checks = {
            "D1_EMA50": self.d1_ema50,
            "D1_EMA200": self.d1_ema200,
            "H4_EMA50": self.h4_ema50,
            "H4_EMA200": self.h4_ema200,
            "H4_EMA200_prev": self.h4_ema200_prev,
            "H4_ATR100": self.h4_atr100,
            "H1_EMA20": self.h1_ema20,
        }
        return [name for name, val in checks.items() if val is None]


def _safe_last(values: list, offset: int = 0) -> Optional[float]:
    idx = -(1 + offset)
    if len(values) >= (1 + offset) and values[idx] is not None:
        return float(values[idx])
    return None


def _candles_to_lists(candles: list[dict]) -> tuple[list[float], list[float], list[float]]:
    closes = [float(c["close"]) for c in candles]
    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]
    return closes, highs, lows


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

        closes, highs, lows = _candles_to_lists(candles)
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
        h1 = TimeframeData(
            timeframe=TIMEFRAME_H1,
            df=h1_df,
            closes=h1_closes,
            highs=h1_highs,
            lows=h1_lows,
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
            d1_atr100=_safe_last(d1_atr100_vals),
            h4_ema20=_safe_last(h4_ema20_vals),
            h4_ema50=_safe_last(h4_ema50_vals),
            h4_ema200=_safe_last(h4_ema200_vals),
            h4_ema200_prev=_safe_last(h4_ema200_vals, offset=1),
            h4_atr100=_safe_last(h4_atr100_vals),
            h1_ema20=_safe_last(h1_ema20_vals),
            h1_ema50=_safe_last(h1_ema50_vals),
            h1_ema200=_safe_last(h1_ema200_vals),
            h1_atr100=_safe_last(h1_atr100_vals),
        )

        logger.info(
            f"[MTF-EMA] {asset} | D1 indicators: "
            f"EMA20={indicators.d1_ema20}, EMA50={indicators.d1_ema50}, "
            f"EMA200={indicators.d1_ema200}, ATR100={indicators.d1_atr100}"
        )
        logger.info(
            f"[MTF-EMA] {asset} | H4 indicators: "
            f"EMA20={indicators.h4_ema20}, EMA50={indicators.h4_ema50}, "
            f"EMA200={indicators.h4_ema200}, EMA200_prev={indicators.h4_ema200_prev}, "
            f"ATR100={indicators.h4_atr100}"
        )
        logger.info(
            f"[MTF-EMA] {asset} | H1 indicators: "
            f"EMA20={indicators.h1_ema20}, EMA50={indicators.h1_ema50}, "
            f"EMA200={indicators.h1_ema200}, ATR100={indicators.h1_atr100}"
        )

        if not indicators.all_required_present():
            missing = indicators.missing_names()
            logger.warning(f"[MTF-EMA] {asset} | Missing required indicators: {missing}")
            return None

        return indicators

    def _check_entry_conditions(self, asset: str, current_price: float, ind: MTFIndicators) -> Optional[SignalResult]:
        cond1 = current_price > ind.d1_ema200 and current_price > ind.d1_ema50
        cond2 = ind.h4_ema200 > ind.h4_ema200_prev
        cond3 = current_price < ind.h4_ema50
        cond4 = (ind.h4_ema50 - current_price) < ind.h4_atr100
        cond5 = current_price > ind.h1_ema20

        logger.info(f"[MTF-EMA] {asset} | price={current_price:.5f}")
        logger.info(
            f"[MTF-EMA] {asset} | Cond 1 — Price > D1 EMA200 ({ind.d1_ema200:.5f}) "
            f"AND D1 EMA50 ({ind.d1_ema50:.5f}): {cond1}"
        )
        logger.info(
            f"[MTF-EMA] {asset} | Cond 2 — H4 EMA200 rising "
            f"({ind.h4_ema200:.5f} > {ind.h4_ema200_prev:.5f}): {cond2}"
        )
        logger.info(
            f"[MTF-EMA] {asset} | Cond 3 — Price dips below H4 EMA50 ({ind.h4_ema50:.5f}): {cond3}"
        )
        logger.info(
            f"[MTF-EMA] {asset} | Cond 4 — Dip within 1× H4 ATR ({ind.h4_atr100:.5f}): {cond4}"
        )
        logger.info(
            f"[MTF-EMA] {asset} | Cond 5 — H1 closes above EMA20 ({ind.h1_ema20:.5f}): {cond5}"
        )

        all_met = cond1 and cond2 and cond3 and cond4 and cond5
        logger.info(f"[MTF-EMA] {asset} | ALL ENTRY CONDITIONS MET: {all_met}")

        if not all_met:
            return None

        stop_loss = current_price - (SL_ATR_MULT * ind.h4_atr100)
        take_profit = current_price + (TP_ATR_MULT * ind.h4_atr100)

        return SignalResult(
            action=Action.ENTRY,
            direction=Direction.LONG,
            price=current_price,
            stop_loss=stop_loss,
            atr_at_entry=ind.h4_atr100,
            metadata={
                "take_profit": take_profit,
                "sl_atr_mult": SL_ATR_MULT,
                "tp_atr_mult": TP_ATR_MULT,
                "h4_atr100": round(ind.h4_atr100, 6),
                "d1_ema200": round(ind.d1_ema200, 6),
                "d1_ema50": round(ind.d1_ema50, 6),
                "h4_ema50": round(ind.h4_ema50, 6),
                "h4_ema200": round(ind.h4_ema200, 6),
                "h1_ema20": round(ind.h1_ema20, 6),
            },
        )

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

        entry_result = self._check_entry_conditions(asset, current_price, indicators)
        if entry_result:
            return entry_result

        return SignalResult()
