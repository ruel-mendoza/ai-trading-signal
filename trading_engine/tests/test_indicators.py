import pytest
import pandas as pd
import numpy as np
from trading_engine.indicators.ema import EMA, EMA20, EMA50, EMA200, latest as ema_latest
from trading_engine.indicators.sma import SMA, SMA50, SMA100, latest as sma_latest
from trading_engine.indicators.atr import ATR, true_range, latest as atr_latest
from trading_engine.indicators.rsi import RSI, latest as rsi_latest
from trading_engine.indicators.ema_slope import ema as ema_slope_fn, calculate_slope, calculate_slope_series
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
        rsi, cross_70, cross_70_down = RSI(data, period=5)

        assert isinstance(rsi, pd.Series)
        assert isinstance(cross_70, pd.Series)
        assert isinstance(cross_70_down, pd.Series)
        assert cross_70.dtype == bool
        assert cross_70_down.dtype == bool
        assert rsi.index.equals(data.index)
        assert cross_70.index.equals(data.index)
        assert cross_70_down.index.equals(data.index)

        valid_rsi = rsi.dropna()
        assert (valid_rsi >= 0).all() and (valid_rsi <= 100).all()

        assert cross_70.any(), "Known data should produce at least one RSI cross-70 event"

        for i in cross_70.index[cross_70]:
            pos = rsi.index.get_loc(i)
            assert pos > 0, "Cross cannot happen at index 0"
            assert rsi.iloc[pos] >= 70, "RSI must be >= 70 at cross point"
            assert rsi.iloc[pos - 1] < 70, "RSI must be < 70 before cross point"

    def test_cross_down_70_detection(self):
        prices = [50.0] * 10 + [50 + i * 2.0 for i in range(10)] + [50 - i * 2.0 for i in range(10)]
        ts = pd.date_range("2025-01-01", periods=len(prices), freq="h", tz="UTC")
        data = pd.Series(prices, index=ts, name="close")
        rsi, cross_up, cross_down = RSI(data, period=5)

        for i in cross_down.index[cross_down]:
            pos = rsi.index.get_loc(i)
            assert pos > 0, "Cross-down cannot happen at index 0"
            assert rsi.iloc[pos] < 70, "RSI must be < 70 after cross-down"
            assert rsi.iloc[pos - 1] >= 70, "RSI must be >= 70 before cross-down"

    def test_no_false_cross_on_flat_data(self):
        flat = pd.Series([50.0] * 30, index=pd.date_range("2025-01-01", periods=30, freq="h", tz="UTC"))
        rsi, cross_70, cross_70_down = RSI(flat, period=5)
        assert not cross_70.any(), "Flat data should produce no cross-70 events"
        assert not cross_70_down.any(), "Flat data should produce no cross-70-down events"

    def test_rsi_values_deterministic(self):
        data = _known_series()
        rsi1, _, _ = RSI(data, period=5)
        rsi2, _, _ = RSI(data, period=5)
        assert rsi1.equals(rsi2)


class TestRSILatest:
    def test_latest_returns_float(self):
        data = _known_series()
        val = rsi_latest(data, period=5)
        assert isinstance(val, float)
        assert 0 <= val <= 100


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
        assert s1 == s2
        assert isinstance(s1, float)

    def test_calculate_slope_series_deterministic(self):
        data = _known_series()
        e1 = ema_slope_fn(data, 5)
        ss1 = calculate_slope_series(e1)
        ss2 = calculate_slope_series(e1)
        assert ss1.equals(ss2)
        assert isinstance(ss1, pd.Series)

    def test_calculate_slope_returns_current_minus_previous(self):
        s = pd.Series([10.0, 20.0, 35.0])
        slope = calculate_slope(s)
        assert slope == 15.0

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


class TestConvenienceFunctions:
    def test_sma50_equals_sma_50(self):
        data = pd.Series(range(100), dtype=float)
        assert SMA50(data).equals(SMA(data, 50))

    def test_sma100_equals_sma_100(self):
        data = pd.Series(range(150), dtype=float)
        assert SMA100(data).equals(SMA(data, 100))

    def test_ema20_equals_ema_20(self):
        data = pd.Series(range(50), dtype=float)
        assert EMA20(data).equals(EMA(data, 20))

    def test_ema50_equals_ema_50(self):
        data = pd.Series(range(100), dtype=float)
        assert EMA50(data).equals(EMA(data, 50))

    def test_ema200_equals_ema_200(self):
        data = pd.Series(range(250), dtype=float)
        assert EMA200(data).equals(EMA(data, 200))

    def test_sma_latest_returns_float(self):
        data = pd.Series(range(100), dtype=float)
        val = sma_latest(data, period=50)
        assert isinstance(val, float)
        expected = float(data.iloc[-50:].mean())
        assert abs(val - expected) < 1e-10

    def test_ema_latest_returns_float(self):
        data = pd.Series(range(50), dtype=float)
        val = ema_latest(data, period=20)
        assert isinstance(val, float)

    def test_atr_latest_returns_float(self):
        df = _known_ohlc_df()
        val = atr_latest(df, period=5)
        assert isinstance(val, float)


class TestATRSimpleMovingAverage:
    def test_atr_is_sma_of_true_range(self):
        df = _known_ohlc_df()
        period = 5

        tr = true_range(df)
        expected_atr = tr.rolling(window=period).mean()
        actual_atr = ATR(df, period=period)

        valid = expected_atr.dropna()
        for i in valid.index:
            assert abs(actual_atr.loc[i] - expected_atr.loc[i]) < 1e-10, (
                f"ATR at {i} = {actual_atr.loc[i]:.6f}, expected SMA(TR) = {expected_atr.loc[i]:.6f}"
            )

    def test_true_range_formula(self):
        df = pd.DataFrame({
            "high":  [12.0, 12.5, 13.0],
            "low":   [10.0, 11.0, 11.5],
            "close": [11.0, 12.0, 12.5],
        })
        tr = true_range(df)
        assert tr.iloc[0] == 2.0
        expected_tr1 = max(12.5 - 11.0, abs(12.5 - 11.0), abs(11.0 - 11.0))
        assert abs(tr.iloc[1] - expected_tr1) < 1e-10
        expected_tr2 = max(13.0 - 11.5, abs(13.0 - 12.0), abs(11.5 - 12.0))
        assert abs(tr.iloc[2] - expected_tr2) < 1e-10

    def test_atr_nan_count_matches_rolling_window(self):
        df = _known_ohlc_df()
        period = 5
        atr = ATR(df, period=period)
        nan_count = atr.isna().sum()
        assert nan_count == period - 1


class TestSeriesNames:
    def test_sma_series_name(self):
        data = _known_series()
        assert SMA(data, 5).name == "SMA_5"

    def test_ema_series_name(self):
        data = _known_series()
        assert EMA(data, 5).name == "EMA_5"

    def test_atr_series_name(self):
        df = _known_ohlc_df()
        assert ATR(df, 5).name == "ATR_5"

    def test_rsi_series_names(self):
        data = _known_series()
        rsi, cross_up, cross_down = RSI(data, 5)
        assert rsi.name == "RSI_5"
        assert cross_up.name == "RSI_5_cross_70"
        assert cross_down.name == "RSI_5_cross_70_down"


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
        rsi_full, _, _ = RSI(data, period=5)

        data_short = data.iloc[:15]
        rsi_short, _, _ = RSI(data_short, period=5)

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
        rsi, cross_up, cross_down = RSI(data, period=5)

        for name, result in [("EMA", ema), ("SMA", sma), ("ATR", atr), ("RSI", rsi), ("cross_70", cross_up), ("cross_70_down", cross_down)]:
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
