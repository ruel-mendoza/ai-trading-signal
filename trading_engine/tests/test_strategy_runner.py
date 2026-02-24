import pytest
import logging
import pandas as pd
from unittest.mock import patch, MagicMock, call
from trading_engine.strategies.base import Action, Direction, SignalResult
from trading_engine.strategy_runner import (
    _candles_to_dataframe,
    _update_trailing_stop,
    _handle_entry,
    _handle_exit,
    _log,
    run_strategy,
    run_all,
    STRATEGY_ASSET_CONFIG,
)


def _make_candles(n=10, base_close=1.0):
    return [
        {
            "timestamp": f"2025-01-{i+1:02d}T00:00:00",
            "open": base_close + i * 0.001,
            "high": base_close + i * 0.001 + 0.005,
            "low": base_close + i * 0.001 - 0.005,
            "close": base_close + (i + 1) * 0.001,
        }
        for i in range(n)
    ]


class TestCandlesToDataframe:
    def test_empty_candles(self):
        df = _candles_to_dataframe([])
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert "close" in df.columns

    def test_normal_candles(self):
        candles = _make_candles(5)
        df = _candles_to_dataframe(candles)
        assert len(df) == 5
        assert df["close"].dtype in ("float64",)
        assert df["timestamp"].iloc[0] == "2025-01-01T00:00:00"


class TestUpdateTrailingStop:
    @patch("trading_engine.strategy_runner.update_position_tracking")
    def test_buy_updates_highest(self, mock_update):
        pos = {
            "id": 1,
            "direction": "BUY",
            "entry_price": 1.0,
            "highest_price_since_entry": 1.05,
        }
        _update_trailing_stop("test_strat", "EUR/USD", pos, 1.08)
        mock_update.assert_called_once_with(1, highest_price=1.08)

    @patch("trading_engine.strategy_runner.update_position_tracking")
    def test_buy_no_update_when_lower(self, mock_update):
        pos = {
            "id": 1,
            "direction": "BUY",
            "entry_price": 1.0,
            "highest_price_since_entry": 1.10,
        }
        _update_trailing_stop("test_strat", "EUR/USD", pos, 1.05)
        mock_update.assert_not_called()

    @patch("trading_engine.strategy_runner.update_position_tracking")
    def test_sell_updates_lowest(self, mock_update):
        pos = {
            "id": 2,
            "direction": "SELL",
            "entry_price": 1.10,
            "lowest_price_since_entry": 1.05,
        }
        _update_trailing_stop("test_strat", "EUR/USD", pos, 1.02)
        mock_update.assert_called_once_with(2, lowest_price=1.02)

    @patch("trading_engine.strategy_runner.update_position_tracking")
    def test_sell_no_update_when_higher(self, mock_update):
        pos = {
            "id": 2,
            "direction": "SELL",
            "entry_price": 1.10,
            "lowest_price_since_entry": 1.02,
        }
        _update_trailing_stop("test_strat", "EUR/USD", pos, 1.05)
        mock_update.assert_not_called()


class TestHandleEntry:
    @patch("trading_engine.strategy_runner.open_position", return_value=10)
    @patch("trading_engine.strategy_runner.insert_signal", return_value=42)
    @patch("trading_engine.strategy_runner.signal_exists", return_value=False)
    def test_creates_signal_and_position(self, mock_exists, mock_insert, mock_open):
        result = SignalResult(
            action=Action.ENTRY,
            direction=Direction.LONG,
            price=1.085,
            stop_loss=1.080,
            atr_at_entry=0.00125,
        )
        sig = _handle_entry("test_strat", "EUR/USD", "1H", result, "2025-01-10T00:00:00")
        assert sig is not None
        assert sig["id"] == 42
        assert sig["direction"] == "BUY"
        assert sig["entry_price"] == 1.085
        assert sig["atr_at_entry"] == 0.00125
        mock_insert.assert_called_once()
        mock_open.assert_called_once()
        open_call_args = mock_open.call_args[0][0]
        assert open_call_args["atr_at_entry"] == 0.00125

    @patch("trading_engine.strategy_runner.signal_exists", return_value=True)
    def test_idempotency_skip(self, mock_exists):
        result = SignalResult(
            action=Action.ENTRY,
            direction=Direction.LONG,
            price=1.085,
            stop_loss=1.080,
        )
        sig = _handle_entry("test_strat", "EUR/USD", "1H", result, "2025-01-10T00:00:00")
        assert sig is None

    @patch("trading_engine.strategy_runner.insert_signal", return_value=None)
    @patch("trading_engine.strategy_runner.signal_exists", return_value=False)
    def test_insert_failure(self, mock_exists, mock_insert):
        result = SignalResult(
            action=Action.ENTRY,
            direction=Direction.SHORT,
            price=1.085,
            stop_loss=1.090,
        )
        sig = _handle_entry("test_strat", "EUR/USD", "1H", result, "2025-01-10T00:00:00")
        assert sig is None

    @patch("trading_engine.strategy_runner.open_position", return_value=10)
    @patch("trading_engine.strategy_runner.insert_signal", return_value=42)
    @patch("trading_engine.strategy_runner.signal_exists", return_value=False)
    def test_atr_locked_to_position(self, mock_exists, mock_insert, mock_open):
        result = SignalResult(
            action=Action.ENTRY,
            direction=Direction.LONG,
            price=1.085,
            stop_loss=1.080,
            atr_at_entry=0.004567,
        )
        sig = _handle_entry("test_strat", "EUR/USD", "1H", result, "2025-01-10T00:00:00")
        insert_args = mock_insert.call_args[0][0]
        open_args = mock_open.call_args[0][0]
        assert insert_args["atr_at_entry"] == 0.004567
        assert open_args["atr_at_entry"] == 0.004567
        assert sig["atr_at_entry"] == 0.004567


class TestHandleExit:
    @patch("trading_engine.strategy_runner.close_position")
    @patch("trading_engine.strategy_runner.close_signal")
    @patch("trading_engine.strategy_runner.get_active_signals", return_value=[{"id": 100}, {"id": 101}])
    def test_closes_position_and_signals(self, mock_active, mock_close_sig, mock_close_pos):
        pos = {"id": 5, "asset": "EUR/USD", "entry_price": 1.08, "direction": "BUY"}
        result = SignalResult(
            action=Action.EXIT,
            price=1.10,
            metadata={"exit_reason": "Trailing stop hit"},
        )
        out = _handle_exit("test_strat", "EUR/USD", pos, result)
        assert out["exit_price"] == 1.10
        assert out["exit_reason"] == "Trailing stop hit"
        assert mock_close_sig.call_count == 2
        mock_close_sig.assert_any_call(100, "Trailing stop hit", exit_price=1.10)
        mock_close_sig.assert_any_call(101, "Trailing stop hit", exit_price=1.10)
        mock_close_pos.assert_called_once_with("test_strat", "EUR/USD")


class TestRunStrategy:
    @patch("trading_engine.strategy_runner.get_open_position", return_value=None)
    @patch("trading_engine.strategy_runner.get_candles")
    def test_no_candles_returns_none(self, mock_candles, mock_pos):
        mock_candles.return_value = []
        strategy = MagicMock()
        result = run_strategy(strategy, "test_strat", "EUR/USD", "1H")
        assert result is None
        strategy.evaluate.assert_not_called()

    @patch("trading_engine.strategy_runner.get_open_position", return_value=None)
    @patch("trading_engine.strategy_runner.get_candles")
    def test_none_result_returns_none(self, mock_candles, mock_pos):
        mock_candles.return_value = _make_candles(5)
        strategy = MagicMock()
        strategy.evaluate.return_value = SignalResult()
        result = run_strategy(strategy, "test_strat", "EUR/USD", "1H")
        assert result is None

    @patch("trading_engine.strategy_runner.get_open_position", return_value=None)
    @patch("trading_engine.strategy_runner.get_candles")
    def test_evaluate_exception_caught(self, mock_candles, mock_pos):
        mock_candles.return_value = _make_candles(5)
        strategy = MagicMock()
        strategy.evaluate.side_effect = ZeroDivisionError("math error in RSI")
        result = run_strategy(strategy, "test_strat", "EUR/USD", "1H")
        assert result is None

    @patch("trading_engine.strategy_runner._handle_entry", return_value={"id": 1, "status": "OPEN"})
    @patch("trading_engine.strategy_runner.get_open_position", return_value=None)
    @patch("trading_engine.strategy_runner.get_candles")
    def test_entry_signal_no_position(self, mock_candles, mock_pos, mock_entry):
        mock_candles.return_value = _make_candles(5)
        strategy = MagicMock()
        strategy.evaluate.return_value = SignalResult(
            action=Action.ENTRY,
            direction=Direction.LONG,
            price=1.005,
            stop_loss=1.000,
            atr_at_entry=0.001,
        )
        result = run_strategy(strategy, "test_strat", "EUR/USD", "1H")
        assert result is not None
        mock_entry.assert_called_once()

    @patch("trading_engine.strategy_runner.get_open_position")
    @patch("trading_engine.strategy_runner.get_candles")
    def test_entry_signal_with_existing_position_ignored(self, mock_candles, mock_pos):
        mock_candles.return_value = _make_candles(5)
        mock_pos.return_value = {
            "id": 99, "direction": "BUY", "entry_price": 1.0,
            "highest_price_since_entry": 1.0, "asset": "EUR/USD",
            "strategy_name": "test_strat", "atr_at_entry": 0.001,
        }
        strategy = MagicMock()
        strategy.evaluate.return_value = SignalResult(
            action=Action.ENTRY,
            direction=Direction.LONG,
            price=1.005,
        )
        result = run_strategy(strategy, "test_strat", "EUR/USD", "1H")
        assert result is None

    @patch("trading_engine.strategy_runner._handle_exit", return_value={"exit_price": 1.10})
    @patch("trading_engine.strategy_runner.get_open_position")
    @patch("trading_engine.strategy_runner.get_candles")
    def test_exit_signal_with_position(self, mock_candles, mock_pos, mock_exit):
        mock_candles.return_value = _make_candles(5)
        pos = {
            "id": 5, "direction": "BUY", "entry_price": 1.0,
            "highest_price_since_entry": 1.0, "asset": "EUR/USD",
            "strategy_name": "test_strat", "atr_at_entry": 0.001,
        }
        mock_pos.return_value = pos
        strategy = MagicMock()
        strategy.evaluate.return_value = SignalResult(
            action=Action.EXIT,
            price=1.10,
            metadata={"exit_reason": "stop hit"},
        )
        result = run_strategy(strategy, "test_strat", "EUR/USD", "1H")
        assert result is not None
        mock_exit.assert_called_once()

    @patch("trading_engine.strategy_runner.get_open_position", return_value=None)
    @patch("trading_engine.strategy_runner.get_candles")
    def test_exit_signal_no_position_ignored(self, mock_candles, mock_pos):
        mock_candles.return_value = _make_candles(5)
        strategy = MagicMock()
        strategy.evaluate.return_value = SignalResult(
            action=Action.EXIT,
            price=1.10,
        )
        result = run_strategy(strategy, "test_strat", "EUR/USD", "1H")
        assert result is None

    @patch("trading_engine.strategy_runner.get_open_position")
    @patch("trading_engine.strategy_runner.get_candles")
    def test_atr_lock_verified_on_open_position(self, mock_candles, mock_pos):
        mock_candles.return_value = _make_candles(5)
        pos = {
            "id": 10, "direction": "BUY", "entry_price": 1.0,
            "highest_price_since_entry": 1.0, "asset": "EUR/USD",
            "strategy_name": "test_strat", "atr_at_entry": 0.0035,
        }
        mock_pos.return_value = pos
        strategy = MagicMock()
        strategy.evaluate.return_value = SignalResult()
        run_strategy(strategy, "test_strat", "EUR/USD", "1H")
        eval_args = strategy.evaluate.call_args
        passed_pos = eval_args[0][3]
        assert passed_pos["atr_at_entry"] == 0.0035

    @patch("trading_engine.strategy_runner.get_open_position")
    @patch("trading_engine.strategy_runner.get_candles")
    def test_atr_lock_missing_warns(self, mock_candles, mock_pos, caplog):
        mock_candles.return_value = _make_candles(5)
        pos = {
            "id": 10, "direction": "BUY", "entry_price": 1.0,
            "highest_price_since_entry": 1.0, "asset": "EUR/USD",
            "strategy_name": "test_strat", "atr_at_entry": None,
        }
        mock_pos.return_value = pos
        strategy = MagicMock()
        strategy.evaluate.return_value = SignalResult()
        with caplog.at_level(logging.WARNING):
            run_strategy(strategy, "test_strat", "EUR/USD", "1H")
        assert any("ATR_LOCK_MISSING" in r.message for r in caplog.records)


class TestRunAll:
    @patch("trading_engine.strategy_runner.run_strategy")
    def test_iterates_all_strategies(self, mock_run):
        mock_run.return_value = None

        strategies = {
            "strat_a": MagicMock(),
            "strat_b": MagicMock(),
        }
        config = {
            "strat_a": {"assets": ["EUR/USD"], "timeframe": "1H"},
            "strat_b": {"assets": ["SPX", "GBP/USD"], "timeframe": "30m"},
        }
        results = run_all(strategies, config)
        assert mock_run.call_count == 3
        assert results == []

    @patch("trading_engine.strategy_runner.run_strategy")
    def test_collects_results(self, mock_run):
        mock_run.side_effect = [
            {"id": 1, "status": "OPEN"},
            None,
            {"id": 2, "exit_price": 1.10},
        ]
        strategies = {"s1": MagicMock()}
        config = {"s1": {"assets": ["A", "B", "C"], "timeframe": "D1"}}
        results = run_all(strategies, config)
        assert len(results) == 2

    @patch("trading_engine.strategy_runner.run_strategy")
    def test_exception_in_one_does_not_stop_others(self, mock_run):
        mock_run.side_effect = [
            RuntimeError("boom"),
            {"id": 1, "status": "OPEN"},
        ]
        strategies = {"s1": MagicMock()}
        config = {"s1": {"assets": ["A", "B"], "timeframe": "D1"}}
        results = run_all(strategies, config)
        assert len(results) == 1

    def test_missing_strategy_skipped(self):
        strategies = {"s1": MagicMock()}
        config = {
            "s1": {"assets": ["A"], "timeframe": "D1"},
            "s2": {"assets": ["B"], "timeframe": "1H"},
        }
        with patch("trading_engine.strategy_runner.run_strategy", return_value=None):
            results = run_all(strategies, config)
        assert results == []


class TestStructuredLogging:
    def test_log_format(self, caplog):
        with caplog.at_level(logging.INFO):
            _log("sp500_momentum", "SPX", "ENTRY", "BUY @ 5400.00")
        assert len(caplog.records) == 1
        msg = caplog.records[0].message
        assert "[sp500_momentum]" in msg
        assert "[SPX]" in msg
        assert "[ENTRY]" in msg
        assert "BUY @ 5400.00" in msg
        assert msg.startswith("[")

    def test_log_format_no_detail(self, caplog):
        with caplog.at_level(logging.INFO):
            _log("trend_forex", "EUR/USD", "NO_ACTION")
        msg = caplog.records[0].message
        assert "[trend_forex]" in msg
        assert "[EUR/USD]" in msg
        assert "[NO_ACTION]" in msg


class TestIdempotencyDefenseInDepth:
    @patch("trading_engine.strategy_runner.open_position", return_value=10)
    @patch("trading_engine.strategy_runner.insert_signal")
    @patch("trading_engine.strategy_runner.signal_exists", return_value=False)
    def test_insert_returns_none_blocks_position_open(self, mock_exists, mock_insert, mock_open):
        mock_insert.return_value = None
        result = SignalResult(
            action=Action.ENTRY,
            direction=Direction.LONG,
            price=1.085,
            stop_loss=1.080,
            atr_at_entry=0.001,
        )
        sig = _handle_entry("test_strat", "EUR/USD", "1H", result, "2025-01-10T00:00:00")
        assert sig is None
        mock_open.assert_not_called()
