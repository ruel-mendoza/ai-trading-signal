import pytest
import pandas as pd
import numpy as np
from trading_engine.indicators.ema import EMA
from trading_engine.indicators.sma import SMA
from trading_engine.indicators.atr import ATR
from trading_engine.indicators.rsi import RSI
from trading_engine.indicators.ema_slope import ema as ema_slope_fn, calculate_slope
from trading_engine.indicators.validation import check_data_length, InsufficientDataError


KNOWN_DATA = [
    44.0, 44.3, 44.1, 43.6, 44.3,
    44.8, 45.1, 45.4, 45.8, 46.2,
    46.5, 46.8, 47.2, 47.5, 47.0,
    46.5, 46.0, 46.8, 47.5, 48.2,
]

TIMESTAMPS = pd.date_range("2025-01-01", periods=len(KNOWN_DATA), freq="h", tz="UTC")


def _known_series() -> pd.Series:
    return pd.Series(KNOWN_DATA, index=TIMESTAMPS, name="close")


def _known_ohlc_df() -> pd.DataFrame:
    close = _known_series()
    return pd.DataFrame(
        {
            "high": close + 0.3,
            "low": close - 0.3,
            "close": close,
        },
        index=TIMESTAMPS,
    )


class TestRSICrossDetection:
    def test_cross_70_on_known_dataset(self):
        data = _known_series()
        rsi, cross_70 = RSI(data, period=5)

        assert isinstance(rsi, pd.Series)
        assert isinstance(cross_70, pd.Series)
        assert cross_70.dtype == bool
        assert rsi.index.equals(data.index)
        assert cross_70.index.equals(data.index)

        valid_rsi = rsi.dropna()
        assert (valid_rsi >= 0).all() and (valid_rsi <= 100).all()

        assert cross_70.any(), "Known data should produce at least one RSI cross-70 event"

        for i in cross_70.index[cross_70]:
            pos = rsi.index.get_loc(i)
            assert pos > 0, "Cross cannot happen at index 0"
            assert rsi.iloc[pos] >= 70, "RSI must be >= 70 at cross point"
            assert rsi.iloc[pos - 1] < 70, "RSI must be < 70 before cross point"

    def test_no_false_cross_on_flat_data(self):
        flat = pd.Series([50.0] * 30, index=pd.date_range("2025-01-01", periods=30, freq="h", tz="UTC"))
        rsi, cross_70 = RSI(flat, period=5)
        assert not cross_70.any(), "Flat data should produce no cross-70 events"

    def test_rsi_values_deterministic(self):
        data = _known_series()
        rsi1, _ = RSI(data, period=5)
        rsi2, _ = RSI(data, period=5)
        assert rsi1.equals(rsi2)


class TestEMADeterminism:
    def test_ema_equals_on_same_input(self):
        data = _known_series()
        ema1 = EMA(data, period=5)
        ema2 = EMA(data, period=5)
        assert ema1.equals(ema2), "EMA must be deterministic — two runs on identical data must be .equals()"

    def test_ema_slope_deterministic(self):
        data = _known_series()
        e1 = ema_slope_fn(data, 5)
        e2 = ema_slope_fn(data, 5)
        assert e1.equals(e2)
        s1 = calculate_slope(e1)
        s2 = calculate_slope(e2)
        assert s1.equals(s2)

    def test_sma_deterministic(self):
        data = _known_series()
        sma1 = SMA(data, period=5)
        sma2 = SMA(data, period=5)
        assert sma1.equals(sma2)

    def test_atr_deterministic(self):
        df = _known_ohlc_df()
        atr1 = ATR(df, period=5)
        atr2 = ATR(df, period=5)
        assert atr1.equals(atr2)


class TestOffByOne:
    def test_sma_uses_only_past_and_current(self):
        data = _known_series()
        sma = SMA(data, period=5)

        for i in range(4, len(data)):
            expected = np.mean(KNOWN_DATA[i - 4 : i + 1])
            assert abs(sma.iloc[i] - expected) < 1e-10, (
                f"SMA at index {i} ({data.index[i]}) = {sma.iloc[i]:.6f}, "
                f"expected mean of data[{i-4}:{i+1}] = {expected:.6f}"
            )

        assert sma.index.equals(data.index), "SMA index must exactly match input index"

    def test_sma_nan_before_window(self):
        data = _known_series()
        sma = SMA(data, period=5)
        assert pd.isna(sma.iloc[:4]).all(), "First period-1 values must be NaN"
        assert pd.notna(sma.iloc[4]), "Value at index period-1 must be valid"

    def test_ema_no_forward_looking(self):
        data = _known_series()
        ema_full = EMA(data, period=5)

        data_short = data.iloc[:15]
        ema_short = EMA(data_short, period=5)

        for i in range(len(data_short)):
            assert abs(ema_full.iloc[i] - ema_short.iloc[i]) < 1e-10, (
                f"EMA at index {i}: full={ema_full.iloc[i]:.6f} vs short={ema_short.iloc[i]:.6f}. "
                f"EMA must not use future data."
            )

    def test_rsi_no_forward_looking(self):
        data = _known_series()
        rsi_full, _ = RSI(data, period=5)

        data_short = data.iloc[:15]
        rsi_short, _ = RSI(data_short, period=5)

        for i in range(len(data_short)):
            if pd.isna(rsi_full.iloc[i]) and pd.isna(rsi_short.iloc[i]):
                continue
            assert abs(rsi_full.iloc[i] - rsi_short.iloc[i]) < 1e-10, (
                f"RSI at index {i}: full={rsi_full.iloc[i]:.6f} vs short={rsi_short.iloc[i]:.6f}. "
                f"RSI must not use future data."
            )

    def test_atr_no_forward_looking(self):
        df = _known_ohlc_df()
        atr_full = ATR(df, period=5)

        df_short = df.iloc[:15]
        atr_short = ATR(df_short, period=5)

        for i in range(len(df_short)):
            if pd.isna(atr_full.iloc[i]) and pd.isna(atr_short.iloc[i]):
                continue
            assert abs(atr_full.iloc[i] - atr_short.iloc[i]) < 1e-10, (
                f"ATR at index {i}: full={atr_full.iloc[i]:.6f} vs short={atr_short.iloc[i]:.6f}. "
                f"ATR must not use future data."
            )

    def test_indicator_timestamps_match_candles(self):
        data = _known_series()
        df = _known_ohlc_df()

        ema = EMA(data, period=5)
        sma = SMA(data, period=5)
        atr = ATR(df, period=5)
        rsi, cross = RSI(data, period=5)

        for name, result in [("EMA", ema), ("SMA", sma), ("ATR", atr), ("RSI", rsi), ("cross_70", cross)]:
            assert result.index.equals(TIMESTAMPS), (
                f"{name} index does not match input timestamps — possible off-by-one shift"
            )
            assert str(result.index.tz) == "UTC", f"{name} lost timezone info"


class TestValidation:
    def test_insufficient_data_sma(self):
        with pytest.raises(InsufficientDataError):
            SMA(pd.Series([1.0, 2.0]), period=5)

    def test_insufficient_data_ema(self):
        with pytest.raises(InsufficientDataError):
            EMA(pd.Series([1.0, 2.0]), period=5)

    def test_insufficient_data_atr(self):
        df = pd.DataFrame({"high": [1, 2], "low": [0.5, 1.5], "close": [0.8, 1.8]})
        with pytest.raises(InsufficientDataError):
            ATR(df, period=5)

    def test_insufficient_data_rsi(self):
        with pytest.raises(InsufficientDataError):
            RSI(pd.Series([1.0, 2.0]), period=5)

    def test_check_data_length_exact_boundary(self):
        s = pd.Series(range(10))
        check_data_length(s, 10, "boundary")
        with pytest.raises(InsufficientDataError):
            check_data_length(s, 11, "boundary")
