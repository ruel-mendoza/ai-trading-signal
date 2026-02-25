import logging
import numpy as np
import pandas as pd
from typing import Optional

from trading_engine.indicators.validation import check_data_length, InsufficientDataError
from trading_engine.indicators.ema_slope import ema as ema_series, calculate_slope, calculate_slope_series
from trading_engine.indicators.sma import SMA
from trading_engine.indicators.ema import EMA
from trading_engine.indicators.atr import ATR
from trading_engine.indicators.rsi import RSI

logger = logging.getLogger("trading_engine.indicators")


class IndicatorEngine:
    @staticmethod
    def ema(closes: list[float], period: int) -> list[Optional[float]]:
        if len(closes) < period:
            return [None] * len(closes)

        arr = pd.Series(closes, dtype=np.float64)
        ema_vals = arr.ewm(span=period, adjust=False).mean()

        result: list[Optional[float]] = [None] * (period - 1)
        result.extend(ema_vals.iloc[period - 1:].tolist())
        return result

    @staticmethod
    def sma(closes: list[float], period: int) -> list[Optional[float]]:
        if len(closes) < period:
            return [None] * len(closes)

        arr = pd.Series(closes, dtype=np.float64)
        sma_vals = arr.rolling(window=period).mean()

        result: list[Optional[float]] = []
        for v in sma_vals:
            result.append(None if pd.isna(v) else float(v))
        return result

    @staticmethod
    def atr(highs: list[float], lows: list[float], closes: list[float], period: int = 100) -> list[Optional[float]]:
        if len(closes) < 2 or len(closes) < period + 1:
            return [None] * len(closes)

        h = np.array(highs, dtype=np.float64)
        l = np.array(lows, dtype=np.float64)
        c = np.array(closes, dtype=np.float64)

        prev_c = np.empty_like(c)
        prev_c[0] = np.nan
        prev_c[1:] = c[:-1]

        tr = np.maximum(
            h - l,
            np.maximum(
                np.abs(h - prev_c),
                np.abs(l - prev_c),
            ),
        )
        tr[0] = h[0] - l[0]

        tr_series = pd.Series(tr, dtype=np.float64)
        atr_vals = tr_series.ewm(alpha=1.0 / period, adjust=False).mean()

        result: list[Optional[float]] = [None] * period
        result.extend(atr_vals.iloc[period:].tolist())
        return result

    @staticmethod
    def rsi(closes: list[float], period: int = 20) -> list[Optional[float]]:
        if len(closes) < period + 1:
            return [None] * len(closes)

        arr = pd.Series(closes, dtype=np.float64)
        delta = arr.diff()
        gains = delta.clip(lower=0)
        losses = (-delta).clip(lower=0)

        avg_gain = gains.ewm(alpha=1.0 / period, adjust=False).mean()
        avg_loss = losses.ewm(alpha=1.0 / period, adjust=False).mean()

        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))

        result: list[Optional[float]] = [None] * period
        result.extend(rsi.iloc[period:].tolist())
        return result

    @classmethod
    def calculate_all(cls, candles: list[dict]) -> dict:
        if not candles:
            logger.warning("[INDICATORS] calculate_all called with 0 candles")
            return {}

        num_bars = len(candles)
        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]

        logger.info(f"[INDICATOR-READINESS] Bars available: {num_bars}")
        logger.info(f"[INDICATOR-READINESS] EMA 20:  {'READY' if num_bars >= 20 else f'INSUFFICIENT (need 20, have {num_bars})'}")
        logger.info(f"[INDICATOR-READINESS] EMA 50:  {'READY' if num_bars >= 50 else f'INSUFFICIENT (need 50, have {num_bars})'}")
        logger.info(f"[INDICATOR-READINESS] EMA 200: {'READY' if num_bars >= 200 else f'INSUFFICIENT (need 200, have {num_bars})'}")
        logger.info(f"[INDICATOR-READINESS] SMA 50:  {'READY' if num_bars >= 50 else f'INSUFFICIENT (need 50, have {num_bars})'}")
        logger.info(f"[INDICATOR-READINESS] SMA 100: {'READY' if num_bars >= 100 else f'INSUFFICIENT (need 100, have {num_bars})'}")
        logger.info(f"[INDICATOR-READINESS] ATR 100: {'READY' if num_bars >= 101 else f'INSUFFICIENT (need 101, have {num_bars})'}")
        logger.info(f"[INDICATOR-READINESS] RSI 20:  {'READY' if num_bars >= 21 else f'INSUFFICIENT (need 21, have {num_bars})'}")

        return {
            "ema_20": cls.ema(closes, 20),
            "ema_50": cls.ema(closes, 50),
            "ema_200": cls.ema(closes, 200),
            "sma_50": cls.sma(closes, 50),
            "sma_100": cls.sma(closes, 100),
            "atr_100": cls.atr(highs, lows, closes, 100),
            "rsi_20": cls.rsi(closes, 20),
        }

    @classmethod
    def get_latest(cls, candles: list[dict]) -> dict:
        all_indicators = cls.calculate_all(candles)
        latest = {}
        for key, values in all_indicators.items():
            latest[key] = values[-1] if values and values[-1] is not None else None
        return latest
