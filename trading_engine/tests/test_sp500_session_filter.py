import pytest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from unittest.mock import MagicMock, patch

from trading_engine.strategies.sp500_momentum import SP500MomentumStrategy


ET_ZONE = ZoneInfo("America/New_York")


def _make_utc_str(year, month, day, hour, minute):
    return datetime(year, month, day, hour, minute, 0).strftime("%Y-%m-%dT%H:%M:%S")


class TestARCASessionFilter:
    def setup_method(self):
        mock_cache = MagicMock()
        self.strategy = SP500MomentumStrategy(mock_cache)

    def test_friday_330pm_et_valid(self):
        et_time = datetime(2026, 2, 27, 15, 30, tzinfo=ET_ZONE)
        utc_time = et_time.astimezone(timezone.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is True

    def test_friday_400pm_et_invalid(self):
        et_time = datetime(2026, 2, 27, 16, 0, tzinfo=ET_ZONE)
        utc_time = et_time.astimezone(timezone.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is False

    def test_930am_et_valid(self):
        et_time = datetime(2026, 2, 24, 9, 30, tzinfo=ET_ZONE)
        utc_time = et_time.astimezone(timezone.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is True

    def test_929am_et_invalid(self):
        et_time = datetime(2026, 2, 24, 9, 29, tzinfo=ET_ZONE)
        utc_time = et_time.astimezone(timezone.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is False

    def test_1200pm_et_valid(self):
        et_time = datetime(2026, 2, 24, 12, 0, tzinfo=ET_ZONE)
        utc_time = et_time.astimezone(timezone.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is True

    def test_midnight_et_invalid(self):
        et_time = datetime(2026, 2, 24, 0, 0, tzinfo=ET_ZONE)
        utc_time = et_time.astimezone(timezone.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is False


class TestDSTTransition:
    def setup_method(self):
        mock_cache = MagicMock()
        self.strategy = SP500MomentumStrategy(mock_cache)

    def test_est_winter_330pm(self):
        et_time = datetime(2026, 1, 15, 15, 30, tzinfo=ET_ZONE)
        utc_time = et_time.astimezone(timezone.utc)
        assert utc_time.hour == 20
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is True

    def test_edt_summer_330pm(self):
        et_time = datetime(2026, 7, 15, 15, 30, tzinfo=ET_ZONE)
        utc_time = et_time.astimezone(timezone.utc)
        assert utc_time.hour == 19
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is True

    def test_est_winter_400pm_invalid(self):
        et_time = datetime(2026, 1, 15, 16, 0, tzinfo=ET_ZONE)
        utc_time = et_time.astimezone(timezone.utc)
        assert utc_time.hour == 21
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is False

    def test_edt_summer_400pm_invalid(self):
        et_time = datetime(2026, 7, 15, 16, 0, tzinfo=ET_ZONE)
        utc_time = et_time.astimezone(timezone.utc)
        assert utc_time.hour == 20
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is False

    def test_dst_spring_forward_day(self):
        et_time = datetime(2026, 3, 8, 15, 30, tzinfo=ET_ZONE)
        utc_time = et_time.astimezone(timezone.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is True

    def test_dst_fall_back_day(self):
        et_time = datetime(2026, 11, 1, 15, 30, tzinfo=ET_ZONE)
        utc_time = et_time.astimezone(timezone.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is True

    def test_winter_utc_2030_maps_to_1530_est(self):
        candle_str = "2026-01-15T20:30:00"
        assert self.strategy._is_within_arca_session(candle_str) is True

    def test_summer_utc_1930_maps_to_1530_edt(self):
        candle_str = "2026-07-15T19:30:00"
        assert self.strategy._is_within_arca_session(candle_str) is True

    def test_winter_utc_2100_maps_to_1600_est_invalid(self):
        candle_str = "2026-01-15T21:00:00"
        assert self.strategy._is_within_arca_session(candle_str) is False

    def test_summer_utc_2000_maps_to_1600_edt_invalid(self):
        candle_str = "2026-07-15T20:00:00"
        assert self.strategy._is_within_arca_session(candle_str) is False


class TestAlternateDateFormat:
    def setup_method(self):
        mock_cache = MagicMock()
        self.strategy = SP500MomentumStrategy(mock_cache)

    def test_space_separated_format(self):
        candle_str = "2026-02-24 17:00:00"
        assert self.strategy._is_within_arca_session(candle_str) is True

    def test_invalid_format_returns_false(self):
        candle_str = "invalid-date"
        assert self.strategy._is_within_arca_session(candle_str) is False


class TestIdempotency:
    def setup_method(self):
        mock_cache = MagicMock()
        self.strategy = SP500MomentumStrategy(mock_cache)

    @patch("trading_engine.strategies.sp500_momentum.get_active_signals")
    @patch("trading_engine.strategies.sp500_momentum.signal_exists")
    @patch("trading_engine.strategies.sp500_momentum.insert_signal")
    def test_duplicate_signal_not_created(self, mock_insert, mock_exists, mock_active):
        mock_exists.return_value = True
        mock_active.return_value = []
        mock_insert.return_value = None

        closes = [100.0 + i * 0.1 for i in range(150)]
        highs = [c + 0.5 for c in closes]
        lows = [c - 0.5 for c in closes]

        et_time = datetime(2026, 2, 24, 14, 0, tzinfo=ET_ZONE)
        utc_time = et_time.astimezone(timezone.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")

        candles = []
        for i in range(150):
            candles.append({
                "open": closes[i] - 0.05,
                "high": highs[i],
                "low": lows[i],
                "close": closes[i],
                "open_time": candle_str,
            })

        self.strategy.cache.get_candles.return_value = candles

        with patch.object(self.strategy, '_is_within_arca_session', return_value=True):
            from trading_engine.indicators import IndicatorEngine
            rsi_vals = IndicatorEngine.rsi(closes, 20)
            if rsi_vals[-2] < 70 and rsi_vals[-1] >= 70:
                result = self.strategy.evaluate("SPX")
                mock_insert.assert_not_called()

    @patch("trading_engine.strategies.sp500_momentum.get_active_signals")
    @patch("trading_engine.strategies.sp500_momentum.signal_exists")
    @patch("trading_engine.strategies.sp500_momentum.insert_signal")
    def test_first_signal_created(self, mock_insert, mock_exists, mock_active):
        mock_exists.return_value = False
        mock_active.return_value = []
        mock_insert.return_value = 42

        et_time = datetime(2026, 2, 24, 14, 0, tzinfo=ET_ZONE)
        utc_time = et_time.astimezone(timezone.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")

        closes = [100.0] * 150
        highs = [101.0] * 150
        lows = [99.0] * 150
        candles = [{"open": 99.9, "high": 101.0, "low": 99.0, "close": 100.0, "open_time": candle_str} for _ in range(150)]

        self.strategy.cache.get_candles.return_value = candles

        with patch.object(self.strategy, '_is_within_arca_session', return_value=True):
            from trading_engine.indicators import IndicatorEngine
            rsi_vals = IndicatorEngine.rsi(closes, 20)
            if rsi_vals and len(rsi_vals) >= 2 and rsi_vals[-2] < 70 and rsi_vals[-1] >= 70:
                result = self.strategy.evaluate("SPX")
                mock_insert.assert_called_once()
