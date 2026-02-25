import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import MagicMock, patch, call
import pytz

from trading_engine.strategies.sp500_momentum import (
    SP500MomentumStrategy,
    STRATEGY_NAME,
    RSI_PERIOD,
    ATR_PERIOD,
    RSI_THRESHOLD,
    TRAILING_STOP_ATR_MULT,
    MIN_BARS_REQUIRED,
)
from trading_engine.strategies.base import Action, Direction, SignalResult


ET_ZONE = pytz.timezone("America/New_York")


def _et_to_utc_str(year, month, day, hour, minute):
    et_time = ET_ZONE.localize(datetime(year, month, day, hour, minute, 0))
    utc_time = et_time.astimezone(pytz.utc)
    return utc_time.strftime("%Y-%m-%dT%H:%M:%S")


def _make_df(n=150, base_close=5000.0, step=1.0, candle_time_str="2026-02-24T19:00:00"):
    closes = [base_close + i * step for i in range(n)]
    highs = [c + 5.0 for c in closes]
    lows = [c - 5.0 for c in closes]
    rows = []
    for i in range(n):
        rows.append({
            "open": closes[i] - 1.0,
            "high": highs[i],
            "low": lows[i],
            "close": closes[i],
            "timestamp": candle_time_str,
        })
    return pd.DataFrame(rows)


def _make_strategy():
    mock_cache = MagicMock()
    mock_cache.api_client = MagicMock()
    mock_cache.api_client.get_advance_data.return_value = []
    return SP500MomentumStrategy(mock_cache)


class TestTimeBoundary:
    """Verify that a 3:30 PM ET candle generates a signal, but a 4:00 PM ET candle
    returns None regardless of RSI value."""

    def setup_method(self):
        self.strategy = _make_strategy()

    @patch("trading_engine.strategies.sp500_momentum.insert_signal", return_value=42)
    @patch("trading_engine.strategies.sp500_momentum.db_open_position")
    @patch("trading_engine.strategies.sp500_momentum.signal_exists", return_value=False)
    @patch("trading_engine.strategies.sp500_momentum.has_open_signal", return_value=False)
    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    @patch("trading_engine.indicators.IndicatorEngine.atr")
    def test_330pm_et_candle_generates_signal(
        self, mock_atr, mock_rsi, mock_has_open, mock_sig_exists,
        mock_db_open, mock_insert
    ):
        candle_ts = _et_to_utc_str(2026, 2, 24, 15, 30)
        df = _make_df(n=MIN_BARS_REQUIRED, candle_time_str=candle_ts)

        mock_rsi.return_value = [50.0] * (MIN_BARS_REQUIRED - 1) + [69.9, 70.1]
        mock_atr.return_value = [10.0] * MIN_BARS_REQUIRED

        result = self.strategy.evaluate("SPX", "30m", df, None)

        assert result.is_entry
        assert result.direction == Direction.LONG
        assert result.atr_at_entry == 10.0
        mock_insert.assert_called_once()
        mock_db_open.assert_called_once()

    @patch("trading_engine.strategies.sp500_momentum.insert_signal")
    @patch("trading_engine.strategies.sp500_momentum.db_open_position")
    @patch("trading_engine.strategies.sp500_momentum.signal_exists")
    @patch("trading_engine.strategies.sp500_momentum.has_open_signal")
    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    @patch("trading_engine.indicators.IndicatorEngine.atr")
    def test_400pm_et_candle_returns_none(
        self, mock_atr, mock_rsi, mock_has_open, mock_sig_exists,
        mock_db_open, mock_insert
    ):
        candle_ts = _et_to_utc_str(2026, 2, 24, 16, 0)
        df = _make_df(n=MIN_BARS_REQUIRED, candle_time_str=candle_ts)

        mock_rsi.return_value = [50.0] * (MIN_BARS_REQUIRED - 1) + [69.9, 70.1]
        mock_atr.return_value = [10.0] * MIN_BARS_REQUIRED

        result = self.strategy.evaluate("SPX", "30m", df, None)

        assert result.is_none
        mock_insert.assert_not_called()
        mock_db_open.assert_not_called()

    @patch("trading_engine.strategies.sp500_momentum.insert_signal")
    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    @patch("trading_engine.indicators.IndicatorEngine.atr")
    def test_400pm_et_blocked_even_with_perfect_rsi_cross(
        self, mock_atr, mock_rsi, mock_insert
    ):
        candle_ts = _et_to_utc_str(2026, 7, 15, 16, 0)
        df = _make_df(n=MIN_BARS_REQUIRED, candle_time_str=candle_ts)

        mock_rsi.return_value = [50.0] * (MIN_BARS_REQUIRED - 1) + [69.9, 70.1]
        mock_atr.return_value = [10.0] * MIN_BARS_REQUIRED

        result = self.strategy.evaluate("SPX", "30m", df, None)

        assert result.is_none
        mock_insert.assert_not_called()

    @patch("trading_engine.strategies.sp500_momentum.insert_signal", return_value=99)
    @patch("trading_engine.strategies.sp500_momentum.db_open_position")
    @patch("trading_engine.strategies.sp500_momentum.signal_exists", return_value=False)
    @patch("trading_engine.strategies.sp500_momentum.has_open_signal", return_value=False)
    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    @patch("trading_engine.indicators.IndicatorEngine.atr")
    def test_330pm_et_summer_edt_generates_signal(
        self, mock_atr, mock_rsi, mock_has_open, mock_sig_exists,
        mock_db_open, mock_insert
    ):
        candle_ts = _et_to_utc_str(2026, 7, 15, 15, 30)
        df = _make_df(n=MIN_BARS_REQUIRED, candle_time_str=candle_ts)

        mock_rsi.return_value = [50.0] * (MIN_BARS_REQUIRED - 1) + [69.9, 70.1]
        mock_atr.return_value = [10.0] * MIN_BARS_REQUIRED

        result = self.strategy.evaluate("SPX", "30m", df, None)

        assert result.is_entry
        mock_insert.assert_called_once()


class TestRSICrossAccuracy:
    """Provide mock data where RSI moves from 69.9 to 70.1 → verify entry.
    Move from 70.1 to 69.9 → verify exit."""

    def setup_method(self):
        self.strategy = _make_strategy()

    @patch("trading_engine.strategies.sp500_momentum.insert_signal", return_value=1)
    @patch("trading_engine.strategies.sp500_momentum.db_open_position")
    @patch("trading_engine.strategies.sp500_momentum.signal_exists", return_value=False)
    @patch("trading_engine.strategies.sp500_momentum.has_open_signal", return_value=False)
    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    @patch("trading_engine.indicators.IndicatorEngine.atr")
    def test_rsi_cross_up_69_9_to_70_1_triggers_entry(
        self, mock_atr, mock_rsi, mock_has_open, mock_sig_exists,
        mock_db_open, mock_insert
    ):
        candle_ts = _et_to_utc_str(2026, 2, 24, 14, 0)
        df = _make_df(n=MIN_BARS_REQUIRED, candle_time_str=candle_ts)

        rsi_series = [50.0] * (MIN_BARS_REQUIRED - 2) + [69.9, 70.1]
        mock_rsi.return_value = rsi_series
        mock_atr.return_value = [10.0] * MIN_BARS_REQUIRED

        result = self.strategy.evaluate("SPX", "30m", df, None)

        assert result.is_entry
        assert result.action == Action.ENTRY
        assert result.direction == Direction.LONG
        mock_insert.assert_called_once()
        signal_arg = mock_insert.call_args[0][0]
        assert signal_arg["direction"] == "BUY"
        assert signal_arg["strategy_name"] == "sp500_momentum"
        assert signal_arg["asset"] == "SPX"

    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    @patch("trading_engine.indicators.IndicatorEngine.atr")
    def test_rsi_no_cross_both_below_70_no_entry(self, mock_atr, mock_rsi):
        candle_ts = _et_to_utc_str(2026, 2, 24, 14, 0)
        df = _make_df(n=MIN_BARS_REQUIRED, candle_time_str=candle_ts)

        rsi_series = [50.0] * (MIN_BARS_REQUIRED - 2) + [68.0, 69.5]
        mock_rsi.return_value = rsi_series
        mock_atr.return_value = [10.0] * MIN_BARS_REQUIRED

        result = self.strategy.evaluate("SPX", "30m", df, None)

        assert result.is_none

    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    @patch("trading_engine.indicators.IndicatorEngine.atr")
    def test_rsi_both_above_70_no_cross_no_entry(self, mock_atr, mock_rsi):
        candle_ts = _et_to_utc_str(2026, 2, 24, 14, 0)
        df = _make_df(n=MIN_BARS_REQUIRED, candle_time_str=candle_ts)

        rsi_series = [50.0] * (MIN_BARS_REQUIRED - 2) + [71.0, 72.0]
        mock_rsi.return_value = rsi_series
        mock_atr.return_value = [10.0] * MIN_BARS_REQUIRED

        result = self.strategy.evaluate("SPX", "30m", df, None)

        assert result.is_none

    @patch("trading_engine.strategies.sp500_momentum.get_active_signals")
    @patch("trading_engine.strategies.sp500_momentum.close_signal")
    @patch("trading_engine.strategies.sp500_momentum.close_position")
    @patch("trading_engine.strategies.sp500_momentum.update_position_tracking")
    @patch("trading_engine.strategies.sp500_momentum.get_all_open_positions")
    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    def test_rsi_cross_down_70_1_to_69_9_triggers_exit(
        self, mock_rsi, mock_get_positions, mock_update,
        mock_close_pos, mock_close_sig, mock_active
    ):
        entry_price = 5000.0
        atr_at_entry = 10.0
        highest = 5200.0
        current_price = 5195.0

        mock_get_positions.return_value = [{
            "id": 1,
            "asset": "SPX",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": highest,
            "direction": "BUY",
        }]
        mock_active.return_value = [{"id": 10}]

        closes = [5000.0 + i * 0.5 for i in range(25)]
        closes[-1] = current_price
        candles = [{"open": c - 1, "high": c + 1, "low": c - 2, "close": c,
                     "timestamp": "2026-02-24T19:00:00"} for c in closes]
        self.strategy.cache.get_candles.return_value = candles

        rsi_series = [50.0] * 23 + [70.1, 69.9]
        mock_rsi.return_value = rsi_series

        results = self.strategy.check_exits()

        assert len(results) == 1
        assert results[0]["exit_reason"] == "rsi_cross_down"
        assert results[0]["exit_price"] == current_price
        mock_close_sig.assert_called_once()
        sig_id, reason = mock_close_sig.call_args[0]
        assert sig_id == 10
        assert f"RSI cross below {RSI_THRESHOLD}" in reason
        assert "prev=70.1" in reason
        assert "curr=69.9" in reason
        mock_close_pos.assert_called_once_with("sp500_momentum", "SPX")

    @patch("trading_engine.strategies.sp500_momentum.get_active_signals")
    @patch("trading_engine.strategies.sp500_momentum.close_signal")
    @patch("trading_engine.strategies.sp500_momentum.close_position")
    @patch("trading_engine.strategies.sp500_momentum.update_position_tracking")
    @patch("trading_engine.strategies.sp500_momentum.get_all_open_positions")
    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    def test_rsi_stays_above_70_no_exit(
        self, mock_rsi, mock_get_positions, mock_update,
        mock_close_pos, mock_close_sig, mock_active
    ):
        mock_get_positions.return_value = [{
            "id": 1,
            "asset": "SPX",
            "entry_price": 5000.0,
            "atr_at_entry": 10.0,
            "highest_price_since_entry": 5200.0,
            "direction": "BUY",
        }]

        closes = [5190.0 + i * 0.5 for i in range(25)]
        candles = [{"open": c - 1, "high": c + 1, "low": c - 2, "close": c,
                     "timestamp": "2026-02-24T19:00:00"} for c in closes]
        self.strategy.cache.get_candles.return_value = candles

        rsi_series = [50.0] * 23 + [72.0, 73.0]
        mock_rsi.return_value = rsi_series

        results = self.strategy.check_exits()

        assert len(results) == 0
        mock_close_sig.assert_not_called()
        mock_close_pos.assert_not_called()


class TestTrailingStopHit:
    """Mock a price drop that exceeds 2 * ATR_at_entry. Verify exit even if
    RSI is still above 70."""

    def setup_method(self):
        self.strategy = _make_strategy()

    @patch("trading_engine.strategies.sp500_momentum.get_active_signals")
    @patch("trading_engine.strategies.sp500_momentum.close_signal")
    @patch("trading_engine.strategies.sp500_momentum.close_position")
    @patch("trading_engine.strategies.sp500_momentum.update_position_tracking")
    @patch("trading_engine.strategies.sp500_momentum.get_all_open_positions")
    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    def test_trailing_stop_exit_with_rsi_above_70(
        self, mock_rsi, mock_get_positions, mock_update,
        mock_close_pos, mock_close_sig, mock_active
    ):
        entry_price = 5000.0
        atr_at_entry = 10.0
        highest = 5100.0
        trailing_stop = highest - (TRAILING_STOP_ATR_MULT * atr_at_entry)
        exit_price = trailing_stop - 1.0

        mock_get_positions.return_value = [{
            "id": 1,
            "asset": "SPX",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": highest,
            "direction": "BUY",
        }]
        mock_active.return_value = [{"id": 10}]

        closes = [5050.0 + i * 0.1 for i in range(25)]
        closes[-1] = exit_price
        candles = [{"open": c - 1, "high": c + 1, "low": c - 2, "close": c,
                     "timestamp": "2026-02-24T19:00:00"} for c in closes]
        self.strategy.cache.get_candles.return_value = candles

        rsi_series = [50.0] * 23 + [72.0, 73.0]
        mock_rsi.return_value = rsi_series

        self.strategy.cache.api_client.get_advance_data.return_value = [{
            "current": {"close": str(exit_price)},
            "update_time": "",
            "profile": {"name": "S&P 500"},
        }]

        results = self.strategy.check_exits()

        assert len(results) == 1
        assert results[0]["exit_reason"] == "trailing_stop"
        assert results[0]["exit_price"] == exit_price
        assert results[0]["atr_at_entry"] == atr_at_entry
        mock_close_sig.assert_called_once()
        reason = mock_close_sig.call_args[0][1]
        assert "Trailing stop hit" in reason
        mock_close_pos.assert_called_once_with("sp500_momentum", "SPX")

    @patch("trading_engine.strategies.sp500_momentum.get_active_signals")
    @patch("trading_engine.strategies.sp500_momentum.close_signal")
    @patch("trading_engine.strategies.sp500_momentum.close_position")
    @patch("trading_engine.strategies.sp500_momentum.update_position_tracking")
    @patch("trading_engine.strategies.sp500_momentum.get_all_open_positions")
    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    def test_trailing_stop_exact_boundary_no_exit(
        self, mock_rsi, mock_get_positions, mock_update,
        mock_close_pos, mock_close_sig, mock_active
    ):
        entry_price = 5000.0
        atr_at_entry = 10.0
        highest = 5100.0
        trailing_stop = highest - (TRAILING_STOP_ATR_MULT * atr_at_entry)
        price_at_boundary = trailing_stop

        mock_get_positions.return_value = [{
            "id": 1,
            "asset": "SPX",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": highest,
            "direction": "BUY",
        }]

        closes = [5050.0 + i * 0.1 for i in range(25)]
        closes[-1] = price_at_boundary
        candles = [{"open": c - 1, "high": c + 1, "low": c - 2, "close": c,
                     "timestamp": "2026-02-24T19:00:00"} for c in closes]
        self.strategy.cache.get_candles.return_value = candles

        rsi_series = [50.0] * 23 + [72.0, 73.0]
        mock_rsi.return_value = rsi_series

        results = self.strategy.check_exits()

        assert len(results) == 0
        mock_close_sig.assert_not_called()

    @patch("trading_engine.strategies.sp500_momentum.get_active_signals")
    @patch("trading_engine.strategies.sp500_momentum.close_signal")
    @patch("trading_engine.strategies.sp500_momentum.close_position")
    @patch("trading_engine.strategies.sp500_momentum.update_position_tracking")
    @patch("trading_engine.strategies.sp500_momentum.get_all_open_positions")
    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    def test_trailing_stop_uses_fixed_atr_not_current(
        self, mock_rsi, mock_get_positions, mock_update,
        mock_close_pos, mock_close_sig, mock_active
    ):
        atr_at_entry = 10.0
        highest = 5100.0
        trailing_stop = highest - (TRAILING_STOP_ATR_MULT * atr_at_entry)
        exit_price = trailing_stop - 0.5

        mock_get_positions.return_value = [{
            "id": 1,
            "asset": "SPX",
            "entry_price": 5000.0,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": highest,
            "direction": "BUY",
        }]
        mock_active.return_value = [{"id": 10}]

        closes = [5050.0 + i * 0.1 for i in range(25)]
        closes[-1] = exit_price
        candles = [{"open": c - 1, "high": c + 1, "low": c - 2, "close": c,
                     "timestamp": "2026-02-24T19:00:00"} for c in closes]
        self.strategy.cache.get_candles.return_value = candles

        rsi_series = [50.0] * 23 + [72.0, 73.0]
        mock_rsi.return_value = rsi_series

        results = self.strategy.check_exits()

        assert len(results) == 1
        assert results[0]["exit_reason"] == "trailing_stop"
        assert results[0]["atr_at_entry"] == atr_at_entry
        reason = mock_close_sig.call_args[0][1]
        assert f"ATR_at_entry={atr_at_entry:.6f}" in reason

    @patch("trading_engine.strategies.sp500_momentum.get_active_signals")
    @patch("trading_engine.strategies.sp500_momentum.close_signal")
    @patch("trading_engine.strategies.sp500_momentum.close_position")
    @patch("trading_engine.strategies.sp500_momentum.update_position_tracking")
    @patch("trading_engine.strategies.sp500_momentum.get_all_open_positions")
    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    def test_both_trailing_stop_and_rsi_cross_down_single_exit(
        self, mock_rsi, mock_get_positions, mock_update,
        mock_close_pos, mock_close_sig, mock_active
    ):
        atr_at_entry = 10.0
        highest = 5100.0
        trailing_stop = highest - (TRAILING_STOP_ATR_MULT * atr_at_entry)
        exit_price = trailing_stop - 2.0

        mock_get_positions.return_value = [{
            "id": 1,
            "asset": "SPX",
            "entry_price": 5000.0,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": highest,
            "direction": "BUY",
        }]
        mock_active.return_value = [{"id": 10}]

        closes = [5050.0 + i * 0.1 for i in range(25)]
        closes[-1] = exit_price
        candles = [{"open": c - 1, "high": c + 1, "low": c - 2, "close": c,
                     "timestamp": "2026-02-24T19:00:00"} for c in closes]
        self.strategy.cache.get_candles.return_value = candles

        rsi_series = [50.0] * 23 + [70.1, 69.9]
        mock_rsi.return_value = rsi_series

        results = self.strategy.check_exits()

        assert len(results) == 1
        assert results[0]["exit_reason"] == "trailing_stop+rsi"
        mock_close_sig.assert_called_once()
        mock_close_pos.assert_called_once()


class TestIdempotency:
    """Run the engine twice on the same 30m candle and verify the database
    Signals table only contains one entry."""

    def setup_method(self):
        self.strategy = _make_strategy()

    @patch("trading_engine.strategies.sp500_momentum.insert_signal")
    @patch("trading_engine.strategies.sp500_momentum.db_open_position")
    @patch("trading_engine.strategies.sp500_momentum.signal_exists", return_value=False)
    @patch("trading_engine.strategies.sp500_momentum.has_open_signal")
    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    @patch("trading_engine.indicators.IndicatorEngine.atr")
    def test_second_run_blocked_by_has_open_signal(
        self, mock_atr, mock_rsi, mock_has_open, mock_sig_exists,
        mock_db_open, mock_insert
    ):
        candle_ts = _et_to_utc_str(2026, 2, 24, 14, 0)
        df = _make_df(n=MIN_BARS_REQUIRED, candle_time_str=candle_ts)

        rsi_series = [50.0] * (MIN_BARS_REQUIRED - 2) + [69.9, 70.1]
        mock_rsi.return_value = rsi_series
        mock_atr.return_value = [10.0] * MIN_BARS_REQUIRED

        mock_has_open.return_value = False
        mock_insert.return_value = 1

        result1 = self.strategy.evaluate("SPX", "30m", df, None)
        assert result1.is_entry
        assert mock_insert.call_count == 1
        assert mock_db_open.call_count == 1

        mock_has_open.return_value = True

        result2 = self.strategy.evaluate("SPX", "30m", df, None)
        assert result2.is_none
        assert mock_insert.call_count == 1
        assert mock_db_open.call_count == 1

    @patch("trading_engine.strategies.sp500_momentum.insert_signal")
    @patch("trading_engine.strategies.sp500_momentum.db_open_position")
    @patch("trading_engine.strategies.sp500_momentum.signal_exists")
    @patch("trading_engine.strategies.sp500_momentum.has_open_signal", return_value=False)
    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    @patch("trading_engine.indicators.IndicatorEngine.atr")
    def test_second_run_blocked_by_signal_exists(
        self, mock_atr, mock_rsi, mock_has_open, mock_sig_exists,
        mock_db_open, mock_insert
    ):
        candle_ts = _et_to_utc_str(2026, 2, 24, 14, 0)
        df = _make_df(n=MIN_BARS_REQUIRED, candle_time_str=candle_ts)

        rsi_series = [50.0] * (MIN_BARS_REQUIRED - 2) + [69.9, 70.1]
        mock_rsi.return_value = rsi_series
        mock_atr.return_value = [10.0] * MIN_BARS_REQUIRED

        mock_sig_exists.return_value = False
        mock_insert.return_value = 1

        result1 = self.strategy.evaluate("SPX", "30m", df, None)
        assert result1.is_entry

        mock_sig_exists.return_value = True

        result2 = self.strategy.evaluate("SPX", "30m", df, None)
        assert result2.is_none
        assert mock_insert.call_count == 1

    @patch("trading_engine.strategies.sp500_momentum.insert_signal")
    @patch("trading_engine.strategies.sp500_momentum.db_open_position")
    @patch("trading_engine.strategies.sp500_momentum.signal_exists", return_value=False)
    @patch("trading_engine.strategies.sp500_momentum.has_open_signal", return_value=False)
    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    @patch("trading_engine.indicators.IndicatorEngine.atr")
    def test_open_position_data_blocks_second_entry(
        self, mock_atr, mock_rsi, mock_has_open, mock_sig_exists,
        mock_db_open, mock_insert
    ):
        candle_ts = _et_to_utc_str(2026, 2, 24, 14, 0)
        df = _make_df(n=MIN_BARS_REQUIRED, candle_time_str=candle_ts)

        rsi_series = [50.0] * (MIN_BARS_REQUIRED - 2) + [69.9, 70.1]
        mock_rsi.return_value = rsi_series
        mock_atr.return_value = [10.0] * MIN_BARS_REQUIRED

        mock_insert.return_value = 1
        result1 = self.strategy.evaluate("SPX", "30m", df, None)
        assert result1.is_entry

        open_pos = {
            "id": 1,
            "direction": "BUY",
            "entry_price": 5100.0,
            "atr_at_entry": 10.0,
            "highest_price_since_entry": 5100.0,
        }
        result2 = self.strategy.evaluate("SPX", "30m", df, open_pos)
        assert result2.is_none
        assert mock_insert.call_count == 1

    @patch("trading_engine.strategies.sp500_momentum.insert_signal")
    @patch("trading_engine.strategies.sp500_momentum.db_open_position")
    @patch("trading_engine.strategies.sp500_momentum.signal_exists", return_value=False)
    @patch("trading_engine.strategies.sp500_momentum.has_open_signal", return_value=False)
    @patch("trading_engine.indicators.IndicatorEngine.rsi")
    @patch("trading_engine.indicators.IndicatorEngine.atr")
    def test_triple_guard_all_three_layers(
        self, mock_atr, mock_rsi, mock_has_open, mock_sig_exists,
        mock_db_open, mock_insert
    ):
        candle_ts = _et_to_utc_str(2026, 2, 24, 14, 0)
        df = _make_df(n=MIN_BARS_REQUIRED, candle_time_str=candle_ts)

        rsi_series = [50.0] * (MIN_BARS_REQUIRED - 2) + [69.9, 70.1]
        mock_rsi.return_value = rsi_series
        mock_atr.return_value = [10.0] * MIN_BARS_REQUIRED

        mock_insert.return_value = 1
        result1 = self.strategy.evaluate("SPX", "30m", df, None)
        assert result1.is_entry
        assert mock_insert.call_count == 1

        open_pos = {
            "id": 1, "direction": "BUY", "entry_price": 5100.0,
            "atr_at_entry": 10.0, "highest_price_since_entry": 5100.0,
        }
        result2 = self.strategy.evaluate("SPX", "30m", df, open_pos)
        assert result2.is_none
        assert mock_insert.call_count == 1

        mock_has_open.return_value = True
        result3 = self.strategy.evaluate("SPX", "30m", df, None)
        assert result3.is_none
        assert mock_insert.call_count == 1

        mock_has_open.return_value = False
        mock_sig_exists.return_value = True
        result4 = self.strategy.evaluate("SPX", "30m", df, None)
        assert result4.is_none
        assert mock_insert.call_count == 1
