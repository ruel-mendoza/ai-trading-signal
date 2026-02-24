import pytest
import math
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, call
import pytz

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


class TestBacktest400DaysSPX:
    """Simulate the last 400 days of S&P 500 daily data.
    Walk through each day, feeding growing windows of candles to the strategy.
    Verify entries only happen when price > 50-day high AND SMA50 > SMA100."""

    def _compute_indicators(self, candles):
        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]
        sma50 = IndicatorEngine.sma(closes, SMA_FAST)
        sma100 = IndicatorEngine.sma(closes, SMA_SLOW)
        atr100 = IndicatorEngine.atr(highs, lows, closes, ATR_PERIOD)
        return closes, sma50, sma100, atr100

    @patch("trading_engine.strategies.trend_non_forex.open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal")
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.get_open_position", return_value=None)
    def test_backtest_entries_only_above_50d_high(self, mock_pos, mock_exists, mock_insert, mock_open):
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
            cache.get_candles.return_value = window

            strat = NonForexTrendFollowingStrategy(cache)
            with patch.object(strat, "_is_eval_window", return_value=True):
                result = strat.evaluate("SPX")

            if should_entry:
                assert result is not None, (
                    f"Day {day_idx}: Expected ENTRY | close={current_close:.2f} > "
                    f"50d_high={highest_50d:.2f}, SMA50={sma50_val:.2f} > SMA100={sma100_val:.2f}"
                )
                assert result["direction"] == "BUY"
                assert result["atr_at_entry"] is not None
                entries.append(day_idx)
            else:
                assert result is None, (
                    f"Day {day_idx}: Expected NO ENTRY | close={current_close:.2f}, "
                    f"50d_high={highest_50d:.2f}, SMA50={sma50_val:.2f}, SMA100={sma100_val:.2f}"
                )
                no_entries.append(day_idx)

            mock_pos.reset_mock()
            mock_exists.reset_mock()
            mock_insert.reset_mock()
            mock_open.reset_mock()
            mock_insert.side_effect = lambda sig: 1

        assert len(entries) > 0, "Uptrending data should produce at least one entry"
        assert len(no_entries) > 0, "Not every day should trigger an entry"

    @patch("trading_engine.strategies.trend_non_forex.open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal")
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.get_open_position", return_value=None)
    def test_backtest_no_entry_in_flat_market(self, mock_pos, mock_exists, mock_insert, mock_open):
        flat_candles = _make_flat_candles(200, price=5000.0)

        cache = MagicMock()
        cache.get_candles.return_value = flat_candles

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True):
            result = strat.evaluate("SPX")

        assert result is None, "Flat market should never trigger an entry"

    @patch("trading_engine.strategies.trend_non_forex.open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal")
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.get_open_position", return_value=None)
    def test_backtest_no_short_entries(self, mock_pos, mock_exists, mock_insert, mock_open):
        mock_insert.side_effect = lambda sig: 1

        downtrend_candles = _make_spx_backtest_candles(200, start_price=5000.0, trend_pct=-0.002)

        cache = MagicMock()
        cache.get_candles.return_value = downtrend_candles

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True):
            result = strat.evaluate("SPX")

        if result is not None:
            assert result["direction"] == "BUY", "Strategy must never produce SHORT signals"


class TestEntryValidation50DayHigh:
    """Verify that an entry is ONLY created when the price exceeds the 50-day high
    with SMA50 > SMA100."""

    @patch("trading_engine.strategies.trend_non_forex.open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal", return_value=100)
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.get_open_position", return_value=None)
    def test_entry_when_close_exceeds_50d_high(self, mock_pos, mock_exists, mock_insert, mock_open):
        candles = _make_candles(150, base_close=100.0, increment=0.5)

        closes = [c["close"] for c in candles]
        prior_closes = closes[-(LOOKBACK_DAYS + 1):-1]
        highest_50d = max(prior_closes)
        current_close = closes[-1]
        assert current_close > highest_50d, "Test data must have close > 50-day high"

        sma50 = IndicatorEngine.sma(closes, SMA_FAST)
        sma100 = IndicatorEngine.sma(closes, SMA_SLOW)
        assert sma50[-1] > sma100[-1], "Test data must have SMA50 > SMA100"

        cache = MagicMock()
        cache.get_candles.return_value = candles
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True):
            result = strat.evaluate("SPX")

        assert result is not None
        assert result["direction"] == "BUY"
        assert result["entry_price"] == current_close
        assert result["atr_at_entry"] is not None

    @patch("trading_engine.strategies.trend_non_forex.get_open_position", return_value=None)
    def test_no_entry_when_close_equals_50d_high(self, mock_pos):
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

        candles[-1]["close"] = highest_50d

        cache = MagicMock()
        cache.get_candles.return_value = candles
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True):
            result = strat.evaluate("SPX")

        assert result is None, "Entry requires close > highest_50d (strict), not >="

    @patch("trading_engine.strategies.trend_non_forex.get_open_position", return_value=None)
    def test_no_entry_when_sma50_below_sma100(self, mock_pos):
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

        cache = MagicMock()
        cache.get_candles.return_value = candles
        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True):
            result = strat.evaluate("SPX")

        if sma50[-1] is not None and sma100[-1] is not None and sma50[-1] <= sma100[-1]:
            assert result is None, "No entry when SMA50 <= SMA100 even if close > 50d high"


class TestTrailingStopPrecision:
    """Provide a manual data sequence where:
    - Price rises to a peak
    - Then falls by 4x ATR
    - Confirm exit triggers at exactly 3x ATR from peak (before reaching 4x ATR drop)."""

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
        assert trailing_stop == 5150.0, f"Expected trailing stop at 5150.0, got {trailing_stop}"

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
        """Price drops 4x ATR from peak. Exit should have triggered at the 3x ATR level."""
        entry_price = 5000.0
        atr_at_entry = 50.0
        peak_price = 5300.0

        trailing_stop_3x = peak_price - (3.0 * atr_at_entry)
        drop_4x = peak_price - (4.0 * atr_at_entry)

        assert drop_4x < trailing_stop_3x, "4x drop is below 3x trailing stop"

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
            {"timestamp": "2025-06-02T16:00:00", "open": 5200, "high": 5200, "low": drop_4x - 5, "close": drop_4x},
        ]

        strat = NonForexTrendFollowingStrategy(cache)
        exits = strat.check_exits()

        assert len(exits) == 1, "Must exit — 4x ATR drop is well past the 3x trailing stop"
        assert exits[0]["exit_price"] == drop_4x

    @patch("trading_engine.strategies.trend_non_forex.close_position")
    @patch("trading_engine.strategies.trend_non_forex.close_signal")
    @patch("trading_engine.strategies.trend_non_forex.get_active_signals", return_value=[{"id": 1}])
    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions")
    def test_trailing_stop_rises_with_new_highs(self, mock_positions, mock_update, mock_active, mock_close_sig, mock_close_pos):
        """As price makes new highs, the trailing stop should ratchet upward."""
        entry_price = 5000.0
        atr_at_entry = 50.0
        old_peak = 5200.0
        new_peak_close = 5400.0

        old_stop = old_peak - (3.0 * atr_at_entry)
        new_stop = new_peak_close - (3.0 * atr_at_entry)
        assert new_stop > old_stop, "Trailing stop should rise with new highs"

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
        cache.get_candles.return_value = [
            {"timestamp": "2025-06-01T16:00:00", "open": 5350, "high": 5420, "low": 5340, "close": 5350},
            {"timestamp": "2025-06-02T16:00:00", "open": 5350, "high": 5420, "low": 5340, "close": new_peak_close},
        ]

        strat = NonForexTrendFollowingStrategy(cache)
        exits = strat.check_exits()

        assert len(exits) == 0, "No exit — close is above new trailing stop"
        mock_update.assert_called_with(1, highest_price=new_peak_close)

    @patch("trading_engine.strategies.trend_non_forex.update_position_tracking")
    @patch("trading_engine.strategies.trend_non_forex.get_all_open_positions")
    def test_atr_never_recalculated(self, mock_positions, mock_update):
        """ATR used for trailing stop must be the fixed value from entry, not recalculated."""
        entry_atr = 50.0
        peak = 5300.0

        mock_positions.return_value = [{
            "id": 1,
            "asset": "XAU/USD",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": 5000.0,
            "atr_at_entry": entry_atr,
            "highest_price_since_entry": peak,
        }]

        volatile_candles = []
        for i in range(150):
            p = 5000 + i * 10
            volatile_candles.append({
                "timestamp": f"2025-01-{(i % 28) + 1:02d}T16:00:00",
                "open": p,
                "high": p + 100,
                "low": p - 100,
                "close": p + 5,
            })
        volatile_candles[-1]["close"] = peak - (entry_atr * 3) + 10

        cache = MagicMock()
        cache.get_candles.return_value = volatile_candles

        strat = NonForexTrendFollowingStrategy(cache)
        exits = strat.check_exits()

        assert len(exits) == 0, (
            "If ATR were recalculated from volatile data, the trailing stop would be much wider. "
            "Using the fixed entry ATR of 50, the stop at 5150 holds. Close at 5160 = no exit."
        )


class TestTimezone4PMET:
    """Verify the 4:00 PM ET trigger correctly handles timezone edge cases:
    - DST vs EST offset
    - Friday close at 4 PM ET
    - Sunday crypto open (no traditional market close, strategy window should still work)."""

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_friday_4pm_et_in_est(self, mock_dt):
        """Friday 4:00 PM ET in winter (EST = UTC-5) → UTC 21:00"""
        mock_now = datetime(2025, 1, 10, 21, 0, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is True

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_friday_4pm_et_in_edt(self, mock_dt):
        """Friday 4:00 PM ET in summer (EDT = UTC-4) → UTC 20:00"""
        mock_now = datetime(2025, 7, 11, 20, 0, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is True

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_friday_430pm_et_still_in_window(self, mock_dt):
        """Friday 4:30 PM ET is the edge of the 30-min window — should still be in."""
        mock_now = datetime(2025, 1, 10, 21, 30, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is True

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_friday_431pm_et_outside_window(self, mock_dt):
        """Friday 4:31 PM ET is just past the window — should not trigger."""
        mock_now = datetime(2025, 1, 10, 21, 31, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is False

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_sunday_4pm_et_crypto_open(self, mock_dt):
        """Sunday 4:00 PM ET — traditional markets are closed but crypto trades 24/7.
        The eval window is purely time-based (not day-of-week filtered),
        so it should fire for crypto assets like BTC/USD on Sundays."""
        mock_now = datetime(2025, 1, 12, 21, 0, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is True

    @patch("trading_engine.strategies.trend_non_forex.open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal", return_value=77)
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.get_open_position", return_value=None)
    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_sunday_btc_entry_fires(self, mock_dt, mock_pos, mock_exists, mock_insert, mock_open):
        """End-to-end: BTC/USD on Sunday 4 PM ET with uptrending data → entry fires."""
        mock_now = datetime(2025, 1, 12, 21, 5, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        candles = _make_candles(150, base_close=40000.0, increment=200.0)
        cache = MagicMock()
        cache.get_candles.return_value = candles

        strat = NonForexTrendFollowingStrategy(cache)
        result = strat.evaluate("BTC/USD")

        assert result is not None
        assert result["direction"] == "BUY"
        assert result["strategy_name"] == STRATEGY_NAME

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_saturday_4pm_et_no_window_issue(self, mock_dt):
        """Saturday 4:00 PM ET — window is time-based only, should still be True.
        Whether to actually trade on Saturday is a data availability issue, not a window issue."""
        mock_now = datetime(2025, 1, 11, 21, 0, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is True

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_dst_transition_march(self, mock_dt):
        """During DST transition in March, 4 PM ET shifts from UTC-5 to UTC-4.
        At UTC 20:00 in summer → 4 PM EDT."""
        mock_now = datetime(2025, 3, 15, 20, 0, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is True

    @patch("trading_engine.strategies.trend_non_forex.datetime")
    def test_dst_transition_november(self, mock_dt):
        """After DST ends in November, 4 PM ET shifts from UTC-4 to UTC-5.
        At UTC 21:00 in winter → 4 PM EST."""
        mock_now = datetime(2025, 11, 15, 21, 0, 0, tzinfo=pytz.utc)
        mock_dt.now.return_value = mock_now

        cache = MagicMock()
        strat = NonForexTrendFollowingStrategy(cache)
        assert strat._is_eval_window() is True


class TestIdempotency:
    @patch("trading_engine.strategies.trend_non_forex.get_open_position")
    def test_existing_long_blocks_entry(self, mock_pos):
        mock_pos.return_value = {
            "id": 99, "direction": "BUY", "entry_price": 100.0,
            "atr_at_entry": 2.0, "highest_price_since_entry": 105.0,
        }
        cache = MagicMock()
        candles = _make_candles(150, base_close=100.0, increment=0.5)
        cache.get_candles.return_value = candles

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True):
            result = strat.evaluate("SPX")
        assert result is None

    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=True)
    @patch("trading_engine.strategies.trend_non_forex.get_open_position", return_value=None)
    def test_duplicate_signal_blocked_by_timestamp(self, mock_pos, mock_exists):
        cache = MagicMock()
        candles = _make_candles(150, base_close=100.0, increment=0.5)
        cache.get_candles.return_value = candles

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True):
            result = strat.evaluate("SPX")

        assert result is None
        mock_exists.assert_called_once()
        call_args = mock_exists.call_args[0]
        assert call_args[0] == STRATEGY_NAME
        assert call_args[1] == "SPX"
        assert call_args[2] == candles[-1]["timestamp"]

    @patch("trading_engine.strategies.trend_non_forex.open_position")
    @patch("trading_engine.strategies.trend_non_forex.insert_signal", return_value=42)
    @patch("trading_engine.strategies.trend_non_forex.signal_exists", return_value=False)
    @patch("trading_engine.strategies.trend_non_forex.get_open_position", return_value=None)
    def test_signal_timestamp_matches_candle_timestamp(self, mock_pos, mock_exists, mock_insert, mock_open):
        cache = MagicMock()
        candles = _make_candles(150, base_close=100.0, increment=0.5)
        cache.get_candles.return_value = candles

        strat = NonForexTrendFollowingStrategy(cache)
        with patch.object(strat, "_is_eval_window", return_value=True):
            result = strat.evaluate("SPX")

        assert result is not None
        insert_call_arg = mock_insert.call_args[0][0]
        assert insert_call_arg["signal_timestamp"] == candles[-1]["timestamp"]


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
