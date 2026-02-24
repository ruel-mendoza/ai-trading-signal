import pytest
from unittest.mock import patch, MagicMock
from trading_engine.strategies.trend_non_forex import (
    NonForexTrendFollowingStrategy,
    STRATEGY_NAME,
    TARGET_SYMBOLS,
    TIMEFRAME,
    MIN_BARS_REQUIRED,
    TRAILING_STOP_ATR_MULT,
)


def _make_candles(n=150, base_close=100.0, increment=0.5):
    return [
        {
            "timestamp": f"2025-01-{(i % 28) + 1:02d}T00:00:00",
            "open": base_close + i * increment,
            "high": base_close + i * increment + 2.0,
            "low": base_close + i * increment - 2.0,
            "close": base_close + (i + 1) * increment,
        }
        for i in range(n)
    ]


def _make_flat_candles(n=150, price=100.0):
    return [
        {
            "timestamp": f"2025-06-{(i % 28) + 1:02d}T00:00:00",
            "open": price,
            "high": price + 0.5,
            "low": price - 0.5,
            "close": price,
        }
        for i in range(n)
    ]


class TestTargetSymbols:
    def test_all_expected_assets_present(self):
        expected = ["SPX", "NDX", "XAU/USD", "XAG/USD", "WTI/USD", "BTC/USD", "ETH/USD"]
        assert TARGET_SYMBOLS == expected

    def test_strategy_name(self):
        assert STRATEGY_NAME == "trend_non_forex"

    def test_timeframe(self):
        assert TIMEFRAME == "D1"


class TestEvalWindow:
    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_inside_window(self, mock_dt):
        import pytz
        from datetime import datetime
        mock_now = datetime(2025, 6, 15, 20, 5, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        result = strat._is_eval_window()
        assert result is True

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_outside_window(self, mock_dt):
        import pytz
        from datetime import datetime
        mock_now = datetime(2025, 6, 15, 14, 0, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        result = strat._is_eval_window()
        assert result is False


class TestEvaluateEntry:
    @patch("trading_engine.strategies.trend_non_forex.open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal", return_value=42)
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.get_open_position", return_value=None)
    def test_long_entry_conditions_met(self, mock_pos, mock_exists, mock_insert, mock_open):
        cache = MagicMock()
        candles = _make_candles(150, base_close=100.0, increment=0.5)
        cache.get_candles.return_value = candles

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True):
            result = strat.evaluate("SPX")

        assert result is not None
        assert result["direction"] == "BUY"
        assert result["strategy_name"] == STRATEGY_NAME
        assert result["id"] == 42
        assert result["atr_at_entry"] is not None
        mock_insert.assert_called_once()
        mock_open.assert_called_once()
        open_args = mock_open.call_args[0][0]
        assert open_args["atr_at_entry"] == result["atr_at_entry"]

    @patch("trading_engine.strategies.trend_non_forex.get_open_position", return_value=None)
    def test_flat_market_no_signal(self, mock_pos):
        cache = MagicMock()
        candles = _make_flat_candles(150, price=100.0)
        cache.get_candles.return_value = candles

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True):
            result = strat.evaluate("SPX")

        assert result is None

    def test_non_target_asset_skipped(self):
        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True):
            result = strat.evaluate("EUR/USD")
        assert result is None
        cache.get_candles.assert_not_called()

    def test_outside_window_skipped(self):
        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=False):
            result = strat.evaluate("SPX")
        assert result is None
        cache.get_candles.assert_not_called()

    @patch("trading_engine.strategies.trend_non_forex.get_open_position", return_value=None)
    def test_insufficient_data(self, mock_pos):
        cache = MagicMock()
        cache.get_candles.return_value = _make_candles(50)

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True):
            result = strat.evaluate("BTC/USD")
        assert result is None


class TestIdempotency:
    @patch("trading_engine.strategies.trend_non_forex.get_open_position")
    def test_existing_long_blocks_entry(self, mock_pos):
        mock_pos.return_value = {"id": 99, "direction": "BUY"}
        cache = MagicMock()
        candles = _make_candles(150, base_close=100.0, increment=0.5)
        cache.get_candles.return_value = candles

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True):
            result = strat.evaluate("SPX")
        assert result is None

    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=True)
    @patch("trading_engine.strategies.trend_non_forex.get_open_position", return_value=None)
    def test_duplicate_signal_blocked(self, mock_pos, mock_exists):
        cache = MagicMock()
        candles = _make_candles(150, base_close=100.0, increment=0.5)
        cache.get_candles.return_value = candles

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True):
            result = strat.evaluate("SPX")
        assert result is None


class TestCheckExits:
    @patch("trading_engine.strategies.trend_non_forex.close_position")
    @patch("trading_engine.strategies.trend_non_forex.close_signal")
    @patch("trading_engine.strategies.trend_non_forex.get_active_signals", return_value=[{"id": 50}])
    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions")
    def test_trailing_stop_exit(self, mock_positions, mock_update, mock_active, mock_close_sig, mock_close_pos):
        mock_positions.return_value = [{
            "id": 1,
            "asset": "XAU/USD",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": 2000.0,
            "atr_at_entry": 20.0,
            "highest_price_since_entry": 2100.0,
        }]
        cache = MagicMock()
        cache.get_candles.return_value = [
            {"timestamp": "2025-01-01T00:00:00", "open": 2020, "high": 2030, "low": 2010, "close": 2020},
            {"timestamp": "2025-01-02T00:00:00", "open": 2020, "high": 2025, "low": 2030, "close": 2030},
        ]

        strat = NonForexTrendFollowingStrategy(cache)
        exits = strat.check_exits()

        assert len(exits) == 1
        assert exits[0]["exit_reason"] == "trailing_stop"
        mock_close_sig.assert_called_once()
        mock_close_pos.assert_called_once_with(STRATEGY_NAME, "XAU/USD")

    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions")
    def test_no_exit_when_above_trailing_stop(self, mock_positions, mock_update):
        mock_positions.return_value = [{
            "id": 1,
            "asset": "SPX",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": 5000.0,
            "atr_at_entry": 50.0,
            "highest_price_since_entry": 5200.0,
        }]
        cache = MagicMock()
        cache.get_candles.return_value = [
            {"timestamp": "2025-01-01T00:00:00", "open": 5100, "high": 5150, "low": 5080, "close": 5100},
            {"timestamp": "2025-01-02T00:00:00", "open": 5100, "high": 5250, "low": 5090, "close": 5200},
        ]

        strat = NonForexTrendFollowingStrategy(cache)
        exits = strat.check_exits()
        assert len(exits) == 0

    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions", return_value=[])
    def test_no_positions_no_exits(self, mock_positions):
        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        exits = strat.check_exits()
        assert exits == []

    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions")
    def test_no_atr_at_entry_skips(self, mock_positions, mock_update):
        mock_positions.return_value = [{
            "id": 1,
            "asset": "BTC/USD",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": 60000.0,
            "atr_at_entry": None,
            "highest_price_since_entry": 62000.0,
        }]
        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        exits = strat.check_exits()
        assert exits == []
