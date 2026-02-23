import numpy as np
from typing import Optional


class IndicatorEngine:
    @staticmethod
    def ema(closes: list[float], period: int) -> list[Optional[float]]:
        if len(closes) < period:
            return [None] * len(closes)

        result: list[Optional[float]] = [None] * (period - 1)
        multiplier = 2.0 / (period + 1)
        sma_initial = float(np.mean(closes[:period]))
        result.append(sma_initial)

        prev = sma_initial
        for i in range(period, len(closes)):
            val = (closes[i] - prev) * multiplier + prev
            result.append(val)
            prev = val

        return result

    @staticmethod
    def sma(closes: list[float], period: int) -> list[Optional[float]]:
        if len(closes) < period:
            return [None] * len(closes)

        result: list[Optional[float]] = [None] * (period - 1)
        arr = np.array(closes, dtype=float)

        cumsum = np.cumsum(arr)
        cumsum_shifted = np.concatenate(([0], cumsum[:-1]))
        sma_values = (cumsum[period - 1:] - cumsum_shifted[:len(cumsum) - period + 1]) / period

        result.extend(sma_values.tolist())
        return result

    @staticmethod
    def atr(highs: list[float], lows: list[float], closes: list[float], period: int = 100) -> list[Optional[float]]:
        if len(closes) < 2 or len(closes) < period + 1:
            return [None] * len(closes)

        true_ranges: list[float] = [highs[0] - lows[0]]
        for i in range(1, len(closes)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            true_ranges.append(tr)

        result: list[Optional[float]] = [None] * (period)
        first_atr = float(np.mean(true_ranges[:period]))
        result.append(first_atr)

        prev_atr = first_atr
        for i in range(period + 1, len(true_ranges)):
            atr_val = (prev_atr * (period - 1) + true_ranges[i]) / period
            result.append(atr_val)
            prev_atr = atr_val

        return result

    @staticmethod
    def rsi(closes: list[float], period: int = 20) -> list[Optional[float]]:
        if len(closes) < period + 1:
            return [None] * len(closes)

        deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains = [max(d, 0) for d in deltas]
        losses = [abs(min(d, 0)) for d in deltas]

        avg_gain = float(np.mean(gains[:period]))
        avg_loss = float(np.mean(losses[:period]))

        result: list[Optional[float]] = [None] * period

        if avg_loss == 0:
            result.append(100.0)
        else:
            rs = avg_gain / avg_loss
            result.append(100.0 - (100.0 / (1.0 + rs)))

        for i in range(period, len(deltas)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

            if avg_loss == 0:
                result.append(100.0)
            else:
                rs = avg_gain / avg_loss
                result.append(100.0 - (100.0 / (1.0 + rs)))

        return result

    @classmethod
    def calculate_all(cls, candles: list[dict]) -> dict:
        if not candles:
            return {}

        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]

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
