import pytest
import math
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, call, PropertyMock
import pytz
import pandas as pd

@pytest.fixture(autouse=True)
def _allow_all_symbols():
    with patch("trading_engine.fcsapi_client.is_symbol_supported", return_value=True):
        yield

from trading_engine.strategies.trend_non_forex import (
    NonForexTrendFollowingStrategy,
    STRATEGY_NAME,
    TARGET_SYMBOLS,
    TIMEFRAME,
    MIN_BARS_REQUIRED,
    TRAILING_STOP_ATR_MULT,
    SMA_FAST,
    SMA_SLOW,
    ATR_PERIOD,
    LOOKBACK_DAYS,
    EVAL_HOUR,
    EVAL_MINUTE,
    EVAL_WINDOW_MINUTES,
    ET_ZONE,
)
from trading_engine.strategies.base import Action, Direction, SignalResult
from trading_engine.indicators import IndicatorEngine


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


def _make_spx_backtest_candles(n_days=400, start_price=4000.0, trend_pct=0.0003, volatility=20.0):
    import random
    random.seed(42)
    candles = []
    price = start_price
    base_date = datetime(2024, 1, 1)

    for i in range(n_days):
        day = base_date + timedelta(days=i)
        ts = day.strftime("%Y-%m-%dT16:00:00")

        daily_return = trend_pct + random.gauss(0, volatility / price)
        open_price = price
        intraday_high = price * (1 + abs(random.gauss(0, 0.005)))
        intraday_low = price * (1 - abs(random.gauss(0, 0.005)))
        close_price = price * (1 + daily_return)

        candles.append({
            "timestamp": ts,
            "open": round(open_price, 2),
            "high": round(max(open_price, close_price, intraday_high), 2),
            "low": round(min(open_price, close_price, intraday_low), 2),
            "close": round(close_price, 2),
        })
        price = close_price

    return candles


def _make_advance_quote(close_price, timestamp="2026-02-24T16:00:00"):
    return {
        "close": close_price,
        "high": close_price + 5.0,
        "low": close_price - 5.0,
        "open": close_price - 1.0,
        "change": 0.5,
        "change_pct": 0.01,
        "timestamp": timestamp,
        "update_time": timestamp,
    }


def _candles_to_df(candles):
    return pd.DataFrame(candles)


class TestBaseStrategyInterface:
    def test_extends_base_strategy(self):
        from trading_engine.strategies.base import BaseStrategy
        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert isinstance(strat, BaseStrategy)

    def test_name_property(self):
        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat.name == "trend_non_forex"

    def test_evaluate_returns_signal_result(self):
        candles = _make_flat_candles(150, price=100.0)
        df = _candles_to_df(candles)
        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(100.0)):
            result = strat.evaluate("SPX", TIMEFRAME, df, None)
        assert isinstance(result, SignalResult)


class TestTargetSymbols:
    def test_all_expected_assets_present(self):
        expected = ["SPX", "NDX", "XAU/USD", "XAG/USD", "OSX", "BTC/USD", "ETH/USD"]
        assert TARGET_SYMBOLS == expected

    def test_strategy_name(self):
        assert STRATEGY_NAME == "trend_non_forex"

    def test_timeframe(self):
        assert TIMEFRAME == "D1"


class TestEvalWindow:
    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_inside_window(self, mock_dt):
        mock_now = datetime(2025, 6, 15, 20, 5, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now
        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is True

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_outside_window(self, mock_dt):
        mock_now = datetime(2025, 6, 15, 14, 0, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now
        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is False


class TestAdvanceEndpoint:
    def test_get_advance_price_success(self):
        cache = MagicMock()
        mock_client = MagicMock()
        mock_client.get_advance_data.return_value = [{
            "symbol": "SPX",
            "ticker": "CBOE:SPX",
            "current": {"close": 6837.75, "high": 6840.0, "low": 6830.0, "open": 6835.0, "change": 7.5, "change_pct": 0.11},
            "profile": {"name": "S&P 500 Index"},
            "update_time": "2026-02-24 16:00:00",
        }]
        cache.api_client = mock_client

        strat = NonForexTrendFollowingStrategy(cache)
        result = strat._get_advance_price("SPX")

        assert result is not None
        assert result["close"] == 6837.75
        mock_client.get_advance_data.assert_called_once_with(["SPX"], period="1d", merge="latest,profile")

    def test_get_advance_price_empty_response(self):
        cache = MagicMock()
        mock_client = MagicMock()
        mock_client.get_advance_data.return_value = []
        cache.api_client = mock_client

        strat = NonForexTrendFollowingStrategy(cache)
        result = strat._get_advance_price("SPX")
        assert result is None

    def test_get_advance_price_exception(self):
        cache = MagicMock()
        mock_client = MagicMock()
        mock_client.get_advance_data.side_effect = Exception("API error")
        cache.api_client = mock_client

        strat = NonForexTrendFollowingStrategy(cache)
        result = strat._get_advance_price("SPX")
        assert result is None

    def test_get_advance_price_null_close(self):
        cache = MagicMock()
        mock_client = MagicMock()
        mock_client.get_advance_data.return_value = [{
            "symbol": "SPX",
            "current": {"close": None, "high": 100.0, "low": 99.0, "open": 99.5},
            "profile": {},
            "update_time": "",
        }]
        cache.api_client = mock_client

        strat = NonForexTrendFollowingStrategy(cache)
        result = strat._get_advance_price("SPX")
        assert result is None


class TestBacktest400DaysSPX:
    def _compute_indicators(self, candles):
        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]
        sma50 = IndicatorEngine.sma(closes, SMA_FAST)
        sma100 = IndicatorEngine.sma(closes, SMA_SLOW)
        atr100 = IndicatorEngine.atr(highs, lows, closes, ATR_PERIOD)
        return closes, sma50, sma100, atr100

    @patch("trading_engine.strategies.trend_non_forex.db_open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal")
    @patch("trading_engine.strategies.trend_non_forex.has_open_signal", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    def test_backtest_entries_only_above_50d_high(self, mock_exists, mock_open_sig, mock_insert, mock_open):
        mock_insert.side_effect = lambda sig: 1

        all_candles = _make_spx_backtest_candles(400, start_price=4000.0, trend_pct=0.0003)

        entries = []
        no_entries = []

        for day_idx in range(MIN_BARS_REQUIRED, len(all_candles)):
            window = all_candles[:day_idx + 1]

            closes, sma50, sma100, atr100 = self._compute_indicators(window)

            current_close = closes[-1]
            sma50_val = sma50[-1]
            sma100_val = sma100[-1]
            atr_val = atr100[-1]

            if any(v is None for v in [sma50_val, sma100_val, atr_val]):
                continue

            prior_closes = closes[-(LOOKBACK_DAYS + 1):-1]
            highest_50d = max(prior_closes)

            should_entry = current_close > highest_50d and sma50_val > sma100_val

            cache = MagicMock()
            df = _candles_to_df(window)

            strat = NonForexTrendFollowingStrategy(cache)
            with patch.object(strat, "_is_eval_window", return_value=True), \
                 patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(current_close)):
                result = strat.evaluate("SPX", TIMEFRAME, df, None)

            if should_entry:
                assert result.is_entry, (
                    f"Day {day_idx}: Expected ENTRY | close={current_close:.2f} > "
                    f"50d_high={highest_50d:.2f}, SMA50={sma50_val:.2f} > SMA100={sma100_val:.2f}"
                )
                assert result.direction == Direction.LONG
                assert result.atr_at_entry is not None
                entries.append(day_idx)
            else:
                assert result.is_none, (
                    f"Day {day_idx}: Expected NO ENTRY | close={current_close:.2f}, "
                    f"50d_high={highest_50d:.2f}, SMA50={sma50_val:.2f}, SMA100={sma100_val:.2f}"
                )
                no_entries.append(day_idx)

            mock_exists.reset_mock()
            mock_insert.reset_mock()
            mock_open.reset_mock()
            mock_insert.side_effect = lambda sig: 1

        assert len(entries) > 0, "Uptrending data should produce at least one entry"
        assert len(no_entries) > 0, "Not every day should trigger an entry"

    @patch("trading_engine.strategies.trend_non_forex.db_open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal")
    @patch("trading_engine.strategies.trend_non_forex.has_open_signal", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    def test_backtest_no_entry_in_flat_market(self, mock_exists, mock_open_sig, mock_insert, mock_open):
        flat_candles = _make_flat_candles(200, price=5000.0)
        df = _candles_to_df(flat_candles)

        cache = MagicMock()

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(5000.0)):
            result = strat.evaluate("SPX", TIMEFRAME, df, None)

        assert result.is_none, "Flat market should never trigger an entry"

    @patch("trading_engine.strategies.trend_non_forex.db_open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal")
    @patch("trading_engine.strategies.trend_non_forex.has_open_signal", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    def test_backtest_no_short_entries(self, mock_exists, mock_open_sig, mock_insert, mock_open):
        mock_insert.side_effect = lambda sig: 1

        downtrend_candles = _make_spx_backtest_candles(200, start_price=5000.0, trend_pct=-0.002)
        last_close = downtrend_candles[-1]["close"]
        df = _candles_to_df(downtrend_candles)

        cache = MagicMock()

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(last_close)):
            result = strat.evaluate("SPX", TIMEFRAME, df, None)

        if result.is_entry:
            assert result.direction == Direction.LONG, "Strategy must never produce SHORT signals"


class TestEntryValidation50DayHigh:
    @patch("trading_engine.strategies.trend_non_forex.db_open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal", return_value=100)
    @patch("trading_engine.strategies.trend_non_forex.has_open_signal", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    def test_entry_when_close_exceeds_50d_high(self, mock_exists, mock_open_sig, mock_insert, mock_open):
        candles = _make_candles(150, base_close=100.0, increment=0.5)

        closes = [c["close"] for c in candles]
        prior_closes = closes[-(LOOKBACK_DAYS + 1):-1]
        highest_50d = max(prior_closes)
        current_close = closes[-1]
        assert current_close > highest_50d, "Test data must have close > 50-day high"

        sma50 = IndicatorEngine.sma(closes, SMA_FAST)
        sma100 = IndicatorEngine.sma(closes, SMA_SLOW)
        assert sma50[-1] > sma100[-1], "Test data must have SMA50 > SMA100"

        df = _candles_to_df(candles)
        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(current_close)):
            result = strat.evaluate("SPX", TIMEFRAME, df, None)

        assert result.is_entry
        assert result.direction == Direction.LONG
        assert result.price == current_close
        assert result.atr_at_entry is not None
        signal = result.metadata.get("signal")
        assert signal is not None
        assert signal["action"] == "ENTRY"

    def test_no_entry_when_close_equals_50d_high(self):
        n = 150
        candles = []
        for i in range(n):
            candles.append({
                "timestamp": f"2025-03-{(i % 28) + 1:02d}T16:00:00",
                "open": 5000.0,
                "high": 5005.0,
                "low": 4995.0,
                "close": 5000.0 + (i * 0.001),
            })

        closes = [c["close"] for c in candles]
        prior_closes = closes[-(LOOKBACK_DAYS + 1):-1]
        highest_50d = max(prior_closes)

        advance_close = highest_50d
        df = _candles_to_df(candles)

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(advance_close)):
            result = strat.evaluate("SPX", TIMEFRAME, df, None)

        assert result.is_none, "Entry requires close > highest_50d (strict), not >="

    def test_no_entry_when_sma50_below_sma100(self):
        n = 150
        candles = []
        for i in range(n):
            if i < 100:
                price = 5000.0 + i * 2.0
            else:
                price = 5000.0 + 100 * 2.0 - (i - 100) * 0.5

            final_close = price
            if i == n - 1:
                final_close = price + 500

            candles.append({
                "timestamp": f"2025-04-{(i % 28) + 1:02d}T16:00:00",
                "open": price - 1.0,
                "high": max(price, final_close) + 2.0,
                "low": min(price, final_close) - 2.0,
                "close": final_close if i == n - 1 else price,
            })

        closes = [c["close"] for c in candles]
        sma50 = IndicatorEngine.sma(closes, SMA_FAST)
        sma100 = IndicatorEngine.sma(closes, SMA_SLOW)
        df = _candles_to_df(candles)

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(closes[-1])):
            result = strat.evaluate("SPX", TIMEFRAME, df, None)

        if sma50[-1] is not None and sma100[-1] is not None and sma50[-1] <= sma100[-1]:
            assert result.is_none, "No entry when SMA50 <= SMA100 even if close > 50d high"

    @patch("trading_engine.strategies.trend_non_forex.db_open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal", return_value=100)
    @patch("trading_engine.strategies.trend_non_forex.has_open_signal", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    def test_no_entry_when_advance_unavailable(self, mock_exists, mock_open_sig, mock_insert, mock_open):
        candles = _make_candles(150, base_close=100.0, increment=0.5)
        df = _candles_to_df(candles)

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=None):
            result = strat.evaluate("SPX", TIMEFRAME, df, None)

        assert result.is_none, "Should skip when advance price is unavailable"


class TestTrailingStopPrecision:
    @patch("trading_engine.strategies.trend_non_forex.close_position")
    @patch("trading_engine.strategies.trend_non_forex.close_signal")
    @patch("trading_engine.strategies.trend_non_forex.get_active_signals", return_value=[{"id": 1}])
    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions")
    def test_exit_at_exactly_3x_atr_below_peak(self, mock_positions, mock_update, mock_active, mock_close_sig, mock_close_pos):
        entry_price = 5000.0
        atr_at_entry = 50.0
        peak_price = 5300.0

        trailing_stop = peak_price - (TRAILING_STOP_ATR_MULT * atr_at_entry)
        assert trailing_stop == 5150.0

        exit_price = trailing_stop - 1.0

        mock_positions.return_value = [{
            "id": 1,
            "asset": "SPX",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": peak_price,
        }]

        cache = MagicMock()
        cache.get_candles.return_value = [
            {"timestamp": "2025-06-01T16:00:00", "open": 5200, "high": 5210, "low": 5100, "close": 5200},
            {"timestamp": "2025-06-02T16:00:00", "open": 5200, "high": 5210, "low": exit_price - 5, "close": exit_price},
        ]

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(exit_price)):
            exits = strat.check_exits()

        assert len(exits) == 1
        assert exits[0]["exit_reason"] == "trailing_stop"
        assert exits[0]["exit_price"] == exit_price
        mock_close_pos.assert_called_once_with(STRATEGY_NAME, "SPX")

    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions")
    def test_no_exit_at_exactly_3x_atr_boundary(self, mock_positions, mock_update):
        entry_price = 5000.0
        atr_at_entry = 50.0
        peak_price = 5300.0

        trailing_stop = peak_price - (TRAILING_STOP_ATR_MULT * atr_at_entry)
        boundary_price = trailing_stop

        mock_positions.return_value = [{
            "id": 1,
            "asset": "SPX",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": peak_price,
        }]

        cache = MagicMock()
        cache.get_candles.return_value = [
            {"timestamp": "2025-06-01T16:00:00", "open": 5200, "high": 5210, "low": 5150, "close": 5200},
            {"timestamp": "2025-06-02T16:00:00", "open": 5200, "high": 5200, "low": 5150, "close": boundary_price},
        ]

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(boundary_price)):
            exits = strat.check_exits()

        assert len(exits) == 0, (
            f"Exit should NOT trigger when close ({boundary_price}) == trailing_stop ({trailing_stop}). "
            f"Exit condition is strictly close < trailing_stop."
        )

    @patch("trading_engine.strategies.trend_non_forex.close_position")
    @patch("trading_engine.strategies.trend_non_forex.close_signal")
    @patch("trading_engine.strategies.trend_non_forex.get_active_signals", return_value=[{"id": 1}])
    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions")
    def test_4x_atr_drop_triggers_exit_at_3x(self, mock_positions, mock_update, mock_active, mock_close_sig, mock_close_pos):
        entry_price = 5000.0
        atr_at_entry = 50.0
        peak_price = 5300.0

        trailing_stop_3x = peak_price - (3.0 * atr_at_entry)
        drop_4x = peak_price - (4.0 * atr_at_entry)

        assert drop_4x < trailing_stop_3x

        mock_positions.return_value = [{
            "id": 1,
            "asset": "SPX",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": peak_price,
        }]

        cache = MagicMock()

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(drop_4x)):
            exits = strat.check_exits()

        assert len(exits) == 1, "Must exit — 4x ATR drop is well past the 3x trailing stop"
        assert exits[0]["exit_price"] == drop_4x

    @patch("trading_engine.strategies.trend_non_forex.close_position")
    @patch("trading_engine.strategies.trend_non_forex.close_signal")
    @patch("trading_engine.strategies.trend_non_forex.get_active_signals", return_value=[{"id": 1}])
    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions")
    def test_trailing_stop_rises_with_new_highs(self, mock_positions, mock_update, mock_active, mock_close_sig, mock_close_pos):
        entry_price = 5000.0
        atr_at_entry = 50.0
        old_peak = 5200.0
        new_peak_close = 5400.0

        old_stop = old_peak - (3.0 * atr_at_entry)
        new_stop = new_peak_close - (3.0 * atr_at_entry)
        assert new_stop > old_stop

        mock_positions.return_value = [{
            "id": 1,
            "asset": "NDX",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": old_peak,
        }]

        cache = MagicMock()

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(new_peak_close)):
            exits = strat.check_exits()

        assert len(exits) == 0, "No exit — close is above new trailing stop"
        mock_update.assert_called_with(1, highest_price=new_peak_close)

    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions")
    def test_atr_never_recalculated(self, mock_positions, mock_update):
        entry_atr = 50.0
        peak = 5300.0
        hold_price = peak - (entry_atr * 3) + 10

        mock_positions.return_value = [{
            "id": 1,
            "asset": "XAU/USD",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": 5000.0,
            "atr_at_entry": entry_atr,
            "highest_price_since_entry": peak,
        }]

        cache = MagicMock()

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(hold_price)):
            exits = strat.check_exits()

        assert len(exits) == 0, (
            "If ATR were recalculated from volatile data, the trailing stop would be much wider. "
            "Using the fixed entry ATR of 50, the stop at 5150 holds. Close at 5160 = no exit."
        )

    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions")
    def test_exit_falls_back_to_candles_when_advance_fails(self, mock_positions, mock_update):
        entry_price = 5000.0
        atr_at_entry = 50.0
        peak_price = 5300.0
        hold_price = 5200.0

        mock_positions.return_value = [{
            "id": 1,
            "asset": "SPX",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": peak_price,
        }]

        cache = MagicMock()
        cache.get_candles.return_value = [
            {"timestamp": "2025-06-01T16:00:00", "open": 5200, "high": 5210, "low": 5190, "close": 5200},
            {"timestamp": "2025-06-02T16:00:00", "open": 5200, "high": 5210, "low": 5190, "close": hold_price},
        ]

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_get_advance_price", return_value=None):
            exits = strat.check_exits()

        assert len(exits) == 0
        cache.get_candles.assert_called_once()


class TestTimezone4PMET:
    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_friday_4pm_et_in_est(self, mock_dt):
        mock_now = datetime(2025, 1, 10, 21, 0, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is True

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_friday_4pm_et_in_edt(self, mock_dt):
        mock_now = datetime(2025, 7, 11, 20, 0, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is True

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_friday_430pm_et_still_in_window(self, mock_dt):
        mock_now = datetime(2025, 1, 10, 21, 30, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is True

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_friday_431pm_et_outside_window(self, mock_dt):
        mock_now = datetime(2025, 1, 10, 21, 31, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is False

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_sunday_4pm_et_crypto_open(self, mock_dt):
        mock_now = datetime(2025, 1, 12, 21, 0, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is True

    @patch("trading_engine.strategies.trend_non_forex.db_open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal", return_value=99)
    @patch("trading_engine.strategies.trend_non_forex.has_open_signal", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_sunday_btc_entry_fires(self, mock_dt, mock_exists, mock_open_sig, mock_insert, mock_open):
        mock_now = datetime(2025, 1, 12, 21, 0, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        candles = _make_candles(150, base_close=60000.0, increment=100.0)
        last_close = candles[-1]["close"]
        df = _candles_to_df(candles)

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(last_close)):
            result = strat.evaluate("BTC/USD", TIMEFRAME, df, None)

        closes = [c["close"] for c in candles]
        prior_closes = closes[-(LOOKBACK_DAYS + 1):-1]
        highest_50d = max(prior_closes)
        sma50 = IndicatorEngine.sma(closes, SMA_FAST)
        sma100 = IndicatorEngine.sma(closes, SMA_SLOW)

        if last_close > highest_50d and sma50[-1] > sma100[-1]:
            assert result.is_entry
            signal = result.metadata.get("signal")
            assert signal["direction"] == "BUY"
        else:
            assert result.is_none

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_saturday_4pm_et_no_window_issue(self, mock_dt):
        mock_now = datetime(2025, 1, 11, 21, 0, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is True

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_dst_transition_march(self, mock_dt):
        mock_now = datetime(2025, 3, 9, 20, 0, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is True

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_dst_transition_november(self, mock_dt):
        mock_now = datetime(2025, 11, 2, 21, 0, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is True


class TestPeakTrackingInEvaluate:
    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    def test_peak_updated_when_advance_close_exceeds_stored_highest(self, mock_update):
        open_pos = {
            "id": 5,
            "direction": "BUY",
            "entry_price": 5000.0,
            "atr_at_entry": 50.0,
            "highest_price_since_entry": 5200.0,
        }
        new_close = 5350.0

        candles = _make_candles(150, base_close=100.0, increment=0.5)
        df = _candles_to_df(candles)

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(new_close)):
            strat.evaluate("SPX", TIMEFRAME, df, open_pos)

        mock_update.assert_called_once_with(5, highest_price=new_close)

    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    def test_peak_not_updated_when_close_below_stored_highest(self, mock_update):
        open_pos = {
            "id": 5,
            "direction": "BUY",
            "entry_price": 5000.0,
            "atr_at_entry": 50.0,
            "highest_price_since_entry": 5200.0,
        }
        lower_close = 5100.0

        candles = _make_candles(150, base_close=100.0, increment=0.5)
        df = _candles_to_df(candles)

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(lower_close)):
            strat.evaluate("SPX", TIMEFRAME, df, open_pos)

        mock_update.assert_not_called()

    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    def test_peak_uses_entry_price_when_no_stored_highest(self, mock_update):
        open_pos = {
            "id": 7,
            "direction": "BUY",
            "entry_price": 5000.0,
            "atr_at_entry": 50.0,
            "highest_price_since_entry": None,
        }
        new_close = 5100.0

        candles = _make_candles(150, base_close=100.0, increment=0.5)
        df = _candles_to_df(candles)

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(new_close)):
            strat.evaluate("SPX", TIMEFRAME, df, open_pos)

        mock_update.assert_called_once_with(7, highest_price=new_close)


class TestATRPersistence:
    @patch("trading_engine.strategies.trend_non_forex.db_open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal", return_value=42)
    @patch("trading_engine.strategies.trend_non_forex.has_open_signal", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    def test_atr_saved_to_db_on_entry(self, mock_exists, mock_open_sig, mock_insert, mock_open_pos):
        candles = _make_candles(150, base_close=100.0, increment=0.5)
        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]
        atr_values = IndicatorEngine.atr(highs, lows, closes, ATR_PERIOD)
        expected_atr = round(atr_values[-1], 6)

        current_close = closes[-1]
        df = _candles_to_df(candles)

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(current_close)):
            result = strat.evaluate("SPX", TIMEFRAME, df, None)

        if result.is_entry:
            mock_open_pos.assert_called_once()
            call_args = mock_open_pos.call_args[0][0]
            assert call_args["atr_at_entry"] == expected_atr
            assert result.atr_at_entry == expected_atr

    def test_trailing_stop_uses_db_atr_not_recalculated(self):
        entry_atr = 75.0
        open_pos = {
            "id": 10,
            "direction": "BUY",
            "entry_price": 5000.0,
            "atr_at_entry": entry_atr,
            "highest_price_since_entry": 5500.0,
        }
        current_close = 5400.0
        expected_stop = 5500.0 - (entry_atr * TRAILING_STOP_ATR_MULT)

        candles = _make_candles(150, base_close=100.0, increment=0.5)
        df = _candles_to_df(candles)

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(current_close)), \
             patch("trading_engine.strategies.trend_non_forex.update_position_tracking"):
            result = strat.evaluate("SPX", TIMEFRAME, df, open_pos)

        assert result.is_none
        assert expected_stop == 5500.0 - (75.0 * 3.0)


class TestIdempotency:
    def test_existing_long_blocks_entry(self):
        open_pos = {"id": 1, "direction": "BUY", "entry_price": 100.0, "atr_at_entry": 5.0, "highest_price_since_entry": 110.0}

        candles = _make_candles(150, base_close=100.0, increment=0.5)
        last_close = candles[-1]["close"]
        df = _candles_to_df(candles)

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(last_close)):
            result = strat.evaluate("SPX", TIMEFRAME, df, open_pos)

        assert result.is_none

    @patch("trading_engine.strategies.trend_non_forex.has_open_signal", return_value=True)
    def test_duplicate_signal_blocked_by_has_open_signal(self, mock_open_sig):
        candles = _make_candles(150, base_close=100.0, increment=0.5)
        last_close = candles[-1]["close"]
        df = _candles_to_df(candles)

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(last_close)):
            result = strat.evaluate("SPX", TIMEFRAME, df, None)

        assert result.is_none

    @patch("trading_engine.strategies.trend_non_forex.db_open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal", return_value=50)
    @patch("trading_engine.strategies.trend_non_forex.has_open_signal", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    def test_signal_timestamp_uses_current_et_time(self, mock_exists, mock_open_sig, mock_insert, mock_open):
        candles = _make_candles(150, base_close=100.0, increment=0.5)
        last_close = candles[-1]["close"]
        df = _candles_to_df(candles)

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(last_close)):
            result = strat.evaluate("SPX", TIMEFRAME, df, None)

        if result.is_entry:
            signal = result.metadata.get("signal")
            assert "T" in signal["signal_timestamp"], "Signal timestamp should be ISO format with T separator"
            assert signal["action"] == "ENTRY"


class TestCheckExits:
    @patch("trading_engine.strategies.trend_non_forex.close_position")
    @patch("trading_engine.strategies.trend_non_forex.close_signal")
    @patch("trading_engine.strategies.trend_non_forex.get_active_signals", return_value=[{"id": 1}])
    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions")
    def test_trailing_stop_exit(self, mock_positions, mock_update, mock_active, mock_close_sig, mock_close_pos):
        exit_price = 4800.0
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
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(exit_price)):
            exits = strat.check_exits()

        assert len(exits) == 1
        assert exits[0]["exit_reason"] == "trailing_stop"

    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions", return_value=[])
    def test_no_positions_no_exits(self, mock_positions):
        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        exits = strat.check_exits()
        assert len(exits) == 0

    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions")
    def test_no_atr_at_entry_skips(self, mock_positions):
        mock_positions.return_value = [{
            "id": 1,
            "asset": "SPX",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": 5000.0,
            "atr_at_entry": None,
            "highest_price_since_entry": 5200.0,
        }]

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        exits = strat.check_exits()
        assert len(exits) == 0


class TestBreakoutEntry:
    """Breakout Test: Price exceeds the 50-day high with bullish SMAs → LONG ENTRY."""

    @patch("trading_engine.strategies.trend_non_forex.db_open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal", return_value=200)
    @patch("trading_engine.strategies.trend_non_forex.has_open_signal", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    def test_breakout_generates_long_entry(self, mock_exists, mock_open_sig, mock_insert, mock_open_pos):
        candles = _make_candles(200, base_close=5000.0, increment=5.0)

        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]

        prior_closes = closes[-(LOOKBACK_DAYS + 1):-1]
        highest_50d = max(prior_closes)
        advance_close = closes[-1]
        assert advance_close > highest_50d, (
            f"Precondition: advance close ({advance_close}) must exceed "
            f"50-day highest close ({highest_50d})"
        )

        sma50 = IndicatorEngine.sma(closes, SMA_FAST)
        sma100 = IndicatorEngine.sma(closes, SMA_SLOW)
        assert sma50[-1] > sma100[-1], (
            f"Precondition: SMA50 ({sma50[-1]}) must be above SMA100 ({sma100[-1]})"
        )

        atr_values = IndicatorEngine.atr(highs, lows, closes, ATR_PERIOD)
        expected_atr = round(atr_values[-1], 6)
        expected_stop = advance_close - (TRAILING_STOP_ATR_MULT * atr_values[-1])

        df = _candles_to_df(candles)
        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(advance_close)):
            result = strat.evaluate("SPX", TIMEFRAME, df, None)

        assert result.is_entry, "Must generate ENTRY when close > 50-day high AND SMA50 > SMA100"
        assert result.action == Action.ENTRY
        assert result.direction == Direction.LONG
        assert result.price == advance_close
        assert result.atr_at_entry == expected_atr

        signal = result.metadata["signal"]
        assert signal["direction"] == "BUY"
        assert signal["action"] == "ENTRY"
        assert signal["entry_price"] == advance_close
        assert signal["atr_at_entry"] == expected_atr
        assert signal["strategy_name"] == STRATEGY_NAME
        assert abs(signal["stop_loss"] - expected_stop) < 1e-6, (
            f"Initial stop loss ({signal['stop_loss']}) must equal "
            f"entry ({advance_close}) - {TRAILING_STOP_ATR_MULT} × ATR ({expected_stop})"
        )

        mock_insert.assert_called_once()
        mock_open_pos.assert_called_once()
        pos_args = mock_open_pos.call_args[0][0]
        assert pos_args["atr_at_entry"] == expected_atr
        assert pos_args["direction"] == "BUY"
        assert pos_args["entry_price"] == advance_close


class TestTrailingStopExit:
    """Trailing Stop Test: Price drops > 3×ATR from peak → EXIT signal."""

    @patch("trading_engine.strategies.trend_non_forex.close_position")
    @patch("trading_engine.strategies.trend_non_forex.close_signal")
    @patch("trading_engine.strategies.trend_non_forex.get_active_signals", return_value=[{"id": 10}])
    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions")
    def test_exit_when_price_drops_beyond_3x_atr_from_peak(
        self, mock_positions, mock_update, mock_active, mock_close_sig, mock_close_pos
    ):
        entry_price = 5000.0
        atr_at_entry = 40.0
        peak_price = 5500.0

        trailing_stop = peak_price - (TRAILING_STOP_ATR_MULT * atr_at_entry)
        assert trailing_stop == 5500.0 - (3.0 * 40.0)
        assert trailing_stop == 5380.0

        exit_close = 5370.0
        assert exit_close < trailing_stop, (
            f"Precondition: exit close ({exit_close}) must be below "
            f"trailing stop ({trailing_stop})"
        )

        mock_positions.return_value = [{
            "id": 10,
            "asset": "NDX",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": peak_price,
        }]

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(exit_close)):
            exits = strat.check_exits()

        assert len(exits) == 1, "Must generate exactly one exit"
        assert exits[0]["exit_reason"] == "trailing_stop"
        assert exits[0]["exit_price"] == exit_close
        assert exits[0]["asset"] == "NDX"
        assert exits[0]["atr_at_entry"] == atr_at_entry

        mock_close_sig.assert_called_once_with(10, pytest.approx(
            f"Trailing stop hit | close={exit_close:.5f}, "
            f"stop={trailing_stop:.5f}, highest_since_entry={peak_price:.5f}, "
            f"ATR_at_entry={atr_at_entry:.6f} (fixed)",
            abs=0,
        ))
        mock_close_pos.assert_called_once_with(STRATEGY_NAME, "NDX")

    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions")
    def test_no_exit_when_price_above_trailing_stop(self, mock_positions, mock_update):
        entry_price = 5000.0
        atr_at_entry = 40.0
        peak_price = 5500.0

        trailing_stop = peak_price - (TRAILING_STOP_ATR_MULT * atr_at_entry)
        hold_close = trailing_stop + 5.0

        mock_positions.return_value = [{
            "id": 10,
            "asset": "NDX",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": peak_price,
        }]

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(hold_close)):
            exits = strat.check_exits()

        assert len(exits) == 0, (
            f"No exit when close ({hold_close}) is above trailing stop ({trailing_stop})"
        )


class TestIdempotencySameTimestamp:
    """Idempotency Test: Execute twice for the same 4PM timestamp → only one signal saved."""

    @patch("trading_engine.strategies.trend_non_forex.db_open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal")
    @patch("trading_engine.strategies.trend_non_forex.has_open_signal")
    @patch("trading_engine.strategies.trend_non_forex.signal_exists")
    def test_second_run_blocked_by_has_open_signal(self, mock_ts_exists, mock_open_sig, mock_insert, mock_open_pos):
        candles = _make_candles(200, base_close=5000.0, increment=5.0)
        closes = [c["close"] for c in candles]
        advance_close = closes[-1]
        df = _candles_to_df(candles)

        sma50 = IndicatorEngine.sma(closes, SMA_FAST)
        sma100 = IndicatorEngine.sma(closes, SMA_SLOW)
        prior_closes = closes[-(LOOKBACK_DAYS + 1):-1]
        highest_50d = max(prior_closes)
        assert advance_close > highest_50d and sma50[-1] > sma100[-1], \
            "Test data must satisfy entry conditions"

        mock_ts_exists.return_value = False
        mock_open_sig.return_value = False
        mock_insert.return_value = 300

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)

        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(advance_close)):
            result1 = strat.evaluate("SPX", TIMEFRAME, df, None)

        assert result1.is_entry, "First run must produce an ENTRY signal"
        assert mock_insert.call_count == 1, "First run must call insert_signal exactly once"

        mock_open_sig.return_value = True
        mock_insert.reset_mock()

        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(advance_close)):
            result2 = strat.evaluate("SPX", TIMEFRAME, df, None)

        assert result2.is_none, (
            "Second run must be blocked by has_open_signal — "
            "an OPEN signal already exists for this strategy+asset"
        )
        assert mock_insert.call_count == 0, (
            "insert_signal must NOT be called on the second run — "
            "has_open_signal returned True, blocking the duplicate"
        )

    @patch("trading_engine.strategies.trend_non_forex.db_open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal")
    @patch("trading_engine.strategies.trend_non_forex.has_open_signal", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    def test_open_position_blocks_duplicate_even_without_timestamp_match(
        self, mock_ts_exists, mock_open_sig, mock_insert, mock_open_pos
    ):
        mock_insert.return_value = 301

        candles = _make_candles(200, base_close=5000.0, increment=5.0)
        closes = [c["close"] for c in candles]
        advance_close = closes[-1]
        df = _candles_to_df(candles)

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)

        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(advance_close)):
            result1 = strat.evaluate("SPX", TIMEFRAME, df, None)

        assert result1.is_entry

        open_pos = {
            "id": 301,
            "direction": "BUY",
            "entry_price": advance_close,
            "atr_at_entry": result1.atr_at_entry,
            "highest_price_since_entry": advance_close,
        }
        mock_insert.reset_mock()

        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(advance_close)), \
             patch("trading_engine.strategies.trend_non_forex.update_position_tracking"):
            result2 = strat.evaluate("SPX", TIMEFRAME, df, open_pos)

        assert result2.is_none, "Must not generate a second signal when open position exists"
        assert mock_insert.call_count == 0


class TestATRConsistencyEntryToExit:
    """ATR Consistency: The ATR used for exit must be identical to the ATR recorded at entry,
    regardless of current market volatility."""

    @patch("trading_engine.strategies.trend_non_forex.close_position")
    @patch("trading_engine.strategies.trend_non_forex.close_signal")
    @patch("trading_engine.strategies.trend_non_forex.get_active_signals", return_value=[{"id": 500}])
    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions")
    @patch("trading_engine.strategies.trend_non_forex.db_open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal", return_value=500)
    @patch("trading_engine.strategies.trend_non_forex.has_open_signal", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    def test_entry_atr_equals_exit_atr_despite_volatility_change(
        self,
        mock_exists,
        mock_open_sig,
        mock_insert,
        mock_open_pos,
        mock_all_positions,
        mock_update,
        mock_active,
        mock_close_sig,
        mock_close_pos,
    ):
        entry_candles = _make_candles(200, base_close=5000.0, increment=5.0)
        closes = [c["close"] for c in entry_candles]
        highs = [c["high"] for c in entry_candles]
        lows = [c["low"] for c in entry_candles]
        entry_close = closes[-1]
        df_entry = _candles_to_df(entry_candles)

        entry_atr_values = IndicatorEngine.atr(highs, lows, closes, ATR_PERIOD)
        entry_atr = round(entry_atr_values[-1], 6)

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True), \
             patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(entry_close)):
            entry_result = strat.evaluate("SPX", TIMEFRAME, df_entry, None)

        assert entry_result.is_entry, "Must produce an ENTRY signal"
        assert entry_result.atr_at_entry == entry_atr

        pos_args = mock_open_pos.call_args[0][0]
        db_stored_atr = pos_args["atr_at_entry"]
        assert db_stored_atr == entry_atr, (
            f"ATR stored to DB ({db_stored_atr}) must equal calculated ATR ({entry_atr})"
        )

        peak_price = entry_close + 200.0
        exit_close = peak_price - (db_stored_atr * TRAILING_STOP_ATR_MULT) - 1.0

        volatile_candles = []
        for i in range(200):
            volatile_candles.append({
                "timestamp": f"2025-09-{(i % 28) + 1:02d}T16:00:00",
                "open": 5500.0,
                "high": 5500.0 + 100.0,
                "low": 5500.0 - 100.0,
                "close": 5500.0,
            })
        volatile_closes = [c["close"] for c in volatile_candles]
        volatile_highs = [c["high"] for c in volatile_candles]
        volatile_lows = [c["low"] for c in volatile_candles]
        new_atr_values = IndicatorEngine.atr(volatile_highs, volatile_lows, volatile_closes, ATR_PERIOD)
        new_market_atr = new_atr_values[-1]
        assert new_market_atr != entry_atr, (
            f"Precondition: current market ATR ({new_market_atr}) must differ from "
            f"entry ATR ({entry_atr}) to prove the strategy doesn't recalculate"
        )

        mock_all_positions.return_value = [{
            "id": 500,
            "asset": "SPX",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": entry_close,
            "atr_at_entry": db_stored_atr,
            "highest_price_since_entry": peak_price,
        }]

        with patch.object(strat, "_get_advance_price", return_value=_make_advance_quote(exit_close)):
            exits = strat.check_exits()

        assert len(exits) == 1, "Must exit — price below trailing stop"

        expected_trailing_stop = peak_price - (db_stored_atr * TRAILING_STOP_ATR_MULT)
        wrong_trailing_stop = peak_price - (new_market_atr * TRAILING_STOP_ATR_MULT)

        assert exit_close < expected_trailing_stop, (
            f"Exit close ({exit_close}) must be below the correct trailing stop ({expected_trailing_stop}) "
            f"computed from the ENTRY ATR ({db_stored_atr})"
        )

        assert exits[0]["atr_at_entry"] == db_stored_atr, (
            f"Exit signal's atr_at_entry ({exits[0]['atr_at_entry']}) must equal "
            f"the DB-stored entry ATR ({db_stored_atr}), NOT the current market ATR ({new_market_atr})"
        )

        assert db_stored_atr == entry_atr, "Full chain: computed → stored → used for exit — all identical"
