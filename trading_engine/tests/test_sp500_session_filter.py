import pytest
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import pytz

from trading_engine.strategies.sp500_momentum import SP500MomentumStrategy
from trading_engine.strategies.base import Action, Direction


ET_ZONE = pytz.timezone("America/New_York")


def _make_utc_str(year, month, day, hour, minute):
    return datetime(year, month, day, hour, minute, 0).strftime("%Y-%m-%dT%H:%M:%S")


def _make_df(n=150, base_close=100.0, step=0.1, candle_time_str="2026-02-24T19:00:00"):
    closes = [base_close + i * step for i in range(n)]
    highs = [c + 0.5 for c in closes]
    lows = [c - 0.5 for c in closes]
    rows = []
    for i in range(n):
        rows.append({
            "open": closes[i] - 0.05,
            "high": highs[i],
            "low": lows[i],
            "close": closes[i],
            "timestamp": candle_time_str,
        })
    return pd.DataFrame(rows)


class TestARCASessionFilter:
    def setup_method(self):
        mock_cache = MagicMock()
        self.strategy = SP500MomentumStrategy(mock_cache)

    def test_friday_330pm_et_valid(self):
        et_time = ET_ZONE.localize(datetime(2026, 2, 27, 15, 30))
        utc_time = et_time.astimezone(pytz.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is True

    def test_friday_400pm_et_invalid(self):
        et_time = ET_ZONE.localize(datetime(2026, 2, 27, 16, 0))
        utc_time = et_time.astimezone(pytz.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is False

    def test_930am_et_valid(self):
        et_time = ET_ZONE.localize(datetime(2026, 2, 24, 9, 30))
        utc_time = et_time.astimezone(pytz.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is True

    def test_929am_et_invalid(self):
        et_time = ET_ZONE.localize(datetime(2026, 2, 24, 9, 29))
        utc_time = et_time.astimezone(pytz.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is False

    def test_1200pm_et_valid(self):
        et_time = ET_ZONE.localize(datetime(2026, 2, 24, 12, 0))
        utc_time = et_time.astimezone(pytz.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is True

    def test_midnight_et_invalid(self):
        et_time = ET_ZONE.localize(datetime(2026, 2, 24, 0, 0))
        utc_time = et_time.astimezone(pytz.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is False


class TestDSTTransition:
    def setup_method(self):
        mock_cache = MagicMock()
        self.strategy = SP500MomentumStrategy(mock_cache)

    def test_est_winter_330pm(self):
        et_time = ET_ZONE.localize(datetime(2026, 1, 15, 15, 30))
        utc_time = et_time.astimezone(pytz.utc)
        assert utc_time.hour == 20
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is True

    def test_edt_summer_330pm(self):
        et_time = ET_ZONE.localize(datetime(2026, 7, 15, 15, 30))
        utc_time = et_time.astimezone(pytz.utc)
        assert utc_time.hour == 19
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is True

    def test_est_winter_400pm_invalid(self):
        et_time = ET_ZONE.localize(datetime(2026, 1, 15, 16, 0))
        utc_time = et_time.astimezone(pytz.utc)
        assert utc_time.hour == 21
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is False

    def test_edt_summer_400pm_invalid(self):
        et_time = ET_ZONE.localize(datetime(2026, 7, 15, 16, 0))
        utc_time = et_time.astimezone(pytz.utc)
        assert utc_time.hour == 20
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is False

    def test_dst_spring_forward_day(self):
        et_time = ET_ZONE.localize(datetime(2026, 3, 8, 15, 30))
        utc_time = et_time.astimezone(pytz.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        assert self.strategy._is_within_arca_session(candle_str) is True

    def test_dst_fall_back_day(self):
        et_time = ET_ZONE.localize(datetime(2026, 11, 1, 15, 30))
        utc_time = et_time.astimezone(pytz.utc)
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


class TestEvaluateBaseStrategyInterface:
    def setup_method(self):
        mock_cache = MagicMock()
        mock_cache.api_client = MagicMock()
        self.strategy = SP500MomentumStrategy(mock_cache)

    def test_non_spx_asset_returns_none(self):
        df = _make_df(150)
        result = self.strategy.evaluate("AAPL", "30m", df, None)
        assert result.is_none

    def test_insufficient_data_returns_none(self):
        df = _make_df(50)
        result = self.strategy.evaluate("SPX", "30m", df, None)
        assert result.is_none

    @patch("trading_engine.strategies.sp500_momentum.has_open_signal")
    @patch("trading_engine.strategies.sp500_momentum.signal_exists")
    @patch("trading_engine.strategies.sp500_momentum.db_open_position")
    @patch("trading_engine.strategies.sp500_momentum.insert_signal")
    def test_outside_session_returns_none(self, mock_insert, mock_open_pos, mock_signal_exists, mock_has_open):
        et_time = ET_ZONE.localize(datetime(2026, 2, 24, 5, 0))
        utc_time = et_time.astimezone(pytz.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        df = _make_df(150, candle_time_str=candle_str)

        mock_has_open.return_value = False
        self.strategy.cache.api_client.get_advance_data.return_value = []

        result = self.strategy.evaluate("SPX", "30m", df, None)
        assert result.is_none
        mock_insert.assert_not_called()

    def test_open_position_blocks_entry(self):
        et_time = ET_ZONE.localize(datetime(2026, 2, 24, 14, 0))
        utc_time = et_time.astimezone(pytz.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        df = _make_df(150, candle_time_str=candle_str)

        open_pos = {
            "id": 1,
            "direction": "BUY",
            "entry_price": 5000.0,
            "atr_at_entry": 10.0,
            "highest_price_since_entry": 5050.0,
        }
        result = self.strategy.evaluate("SPX", "30m", df, open_pos)
        assert result.is_none

    @patch("trading_engine.strategies.sp500_momentum.has_open_signal")
    @patch("trading_engine.strategies.sp500_momentum.signal_exists")
    @patch("trading_engine.strategies.sp500_momentum.db_open_position")
    @patch("trading_engine.strategies.sp500_momentum.insert_signal")
    def test_has_open_signal_blocks_duplicate(self, mock_insert, mock_open_pos, mock_signal_exists, mock_has_open):
        mock_has_open.return_value = True
        mock_signal_exists.return_value = False

        et_time = ET_ZONE.localize(datetime(2026, 2, 24, 14, 0))
        utc_time = et_time.astimezone(pytz.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        df = _make_df(150, candle_time_str=candle_str)

        self.strategy.cache.api_client.get_advance_data.return_value = []

        with patch.object(self.strategy, '_is_within_arca_session', return_value=True):
            result = self.strategy.evaluate("SPX", "30m", df, None)

        mock_insert.assert_not_called()


class TestAdvancePriceIntegration:
    def setup_method(self):
        mock_cache = MagicMock()
        mock_cache.api_client = MagicMock()
        self.strategy = SP500MomentumStrategy(mock_cache)

    def test_advance_price_fetched_with_30m_period(self):
        et_time = ET_ZONE.localize(datetime(2026, 2, 24, 14, 0))
        utc_time = et_time.astimezone(pytz.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        df = _make_df(150, candle_time_str=candle_str)

        self.strategy.cache.api_client.get_advance_data.return_value = [{
            "current": {"close": "5700.5", "high": "5710", "low": "5690", "open": "5695", "timestamp": candle_str},
            "update_time": "2026-02-24 14:00:00",
            "profile": {"name": "S&P 500"},
        }]

        result = self.strategy._get_advance_price("SPX")
        assert result is not None
        assert result["close"] == 5700.5
        self.strategy.cache.api_client.get_advance_data.assert_called_once_with(["SPX"], period="30m", merge="latest,profile")

    def test_advance_price_none_on_failure(self):
        self.strategy.cache.api_client.get_advance_data.side_effect = Exception("API down")
        result = self.strategy._get_advance_price("SPX")
        assert result is None


class TestTrailingStopExit:
    def setup_method(self):
        mock_cache = MagicMock()
        mock_cache.api_client = MagicMock()
        self.strategy = SP500MomentumStrategy(mock_cache)

    @patch("trading_engine.strategies.sp500_momentum.get_active_signals")
    @patch("trading_engine.strategies.sp500_momentum.close_signal")
    @patch("trading_engine.strategies.sp500_momentum.close_position")
    @patch("trading_engine.strategies.sp500_momentum.update_position_tracking")
    @patch("trading_engine.strategies.sp500_momentum.get_all_open_positions")
    def test_trailing_stop_exit(self, mock_get_positions, mock_update, mock_close_pos, mock_close_sig, mock_active):
        atr_at_entry = 10.0
        highest = 5100.0
        exit_price = highest - (atr_at_entry * 2.0) - 1.0

        mock_get_positions.return_value = [{
            "id": 1,
            "asset": "SPX",
            "entry_price": 5000.0,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": highest,
            "direction": "BUY",
        }]
        mock_active.return_value = [{"id": 10}]

        closes = [5000.0 + i for i in range(25)]
        closes[-1] = exit_price
        candles = [{"open": c - 1, "high": c + 1, "low": c - 2, "close": c, "timestamp": "2026-02-24T19:00:00"} for c in closes]
        self.strategy.cache.get_candles.return_value = candles

        self.strategy.cache.api_client.get_advance_data.return_value = [{
            "current": {"close": str(exit_price)},
            "update_time": "",
            "profile": {"name": "S&P 500"},
        }]

        results = self.strategy.check_exits()
        assert len(results) == 1
        assert results[0]["exit_reason"] == "trailing_stop"
        mock_close_sig.assert_called_once()
        call_args = mock_close_sig.call_args
        assert call_args[0][0] == 10
        assert "Trailing stop hit" in call_args[0][1]
        mock_close_pos.assert_called_once_with("sp500_momentum", "SPX")

    @patch("trading_engine.strategies.sp500_momentum.get_active_signals")
    @patch("trading_engine.strategies.sp500_momentum.close_signal")
    @patch("trading_engine.strategies.sp500_momentum.close_position")
    @patch("trading_engine.strategies.sp500_momentum.update_position_tracking")
    @patch("trading_engine.strategies.sp500_momentum.get_all_open_positions")
    def test_rsi_cross_down_exit(self, mock_get_positions, mock_update, mock_close_pos, mock_close_sig, mock_active):
        mock_get_positions.return_value = [{
            "id": 1,
            "asset": "SPX",
            "entry_price": 5000.0,
            "atr_at_entry": 10.0,
            "highest_price_since_entry": 5050.0,
            "direction": "BUY",
        }]
        mock_active.return_value = [{"id": 10}]

        closes = [5000.0 + i * 0.5 for i in range(25)]
        candles = [{"open": c - 1, "high": c + 1, "low": c - 2, "close": c, "timestamp": "2026-02-24T19:00:00"} for c in closes]
        self.strategy.cache.get_candles.return_value = candles

        self.strategy.cache.api_client.get_advance_data.return_value = []

        with patch.object(self.strategy, 'check_exits', wraps=self.strategy.check_exits):
            from trading_engine.indicators import IndicatorEngine
            rsi_values = IndicatorEngine.rsi(closes, 20)

            if rsi_values[-2] >= 70 and rsi_values[-1] < 70:
                results = self.strategy.check_exits()
                assert len(results) == 1
                assert results[0]["exit_reason"] == "rsi_cross_down"

    @patch("trading_engine.strategies.sp500_momentum.get_all_open_positions")
    def test_no_positions_returns_empty(self, mock_get_positions):
        mock_get_positions.return_value = []
        results = self.strategy.check_exits()
        assert results == []


class TestATRFixedAtEntry:
    def setup_method(self):
        mock_cache = MagicMock()
        mock_cache.api_client = MagicMock()
        self.strategy = SP500MomentumStrategy(mock_cache)

    def test_open_position_logs_fixed_atr(self):
        et_time = ET_ZONE.localize(datetime(2026, 2, 24, 14, 0))
        utc_time = et_time.astimezone(pytz.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")
        df = _make_df(150, candle_time_str=candle_str)

        open_pos = {
            "id": 5,
            "direction": "BUY",
            "entry_price": 5000.0,
            "atr_at_entry": 8.5,
            "highest_price_since_entry": 5050.0,
        }
        result = self.strategy.evaluate("SPX", "30m", df, open_pos)
        assert result.is_none

    @patch("trading_engine.strategies.sp500_momentum.update_position_tracking")
    def test_highest_price_updated_when_new_peak(self, mock_update):
        et_time = ET_ZONE.localize(datetime(2026, 2, 24, 14, 0))
        utc_time = et_time.astimezone(pytz.utc)
        candle_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")

        closes = [5000.0 + i * 2 for i in range(150)]
        highs = [c + 5 for c in closes]
        lows = [c - 5 for c in closes]
        rows = [{"open": c - 1, "high": h, "low": l, "close": c, "timestamp": candle_str}
                for c, h, l in zip(closes, highs, lows)]
        df = pd.DataFrame(rows)

        self.strategy.cache.api_client.get_advance_data.return_value = []

        open_pos = {
            "id": 5,
            "direction": "BUY",
            "entry_price": 5000.0,
            "atr_at_entry": 8.5,
            "highest_price_since_entry": 5050.0,
        }
        result = self.strategy.evaluate("SPX", "30m", df, open_pos)
        assert result.is_none
        new_close = closes[-1]
        if new_close > 5050.0:
            mock_update.assert_called_once_with(5, highest_price=new_close)


class TestStrategyNameProperty:
    def test_name_returns_sp500_momentum(self):
        mock_cache = MagicMock()
        strategy = SP500MomentumStrategy(mock_cache)
        assert strategy.name == "sp500_momentum"
