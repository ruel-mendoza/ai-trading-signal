import pytest
import json
import csv
import os
from datetime import datetime, timezone, timedelta
import pytz
from unittest.mock import MagicMock, patch

from trading_engine.strategies.trend_forex import (
    ForexTrendFollowingStrategy,
    STRATEGY_NAME,
    TARGET_SYMBOLS,
    TIMEFRAME,
    TRAILING_STOP_ATR_MULT,
    ATR_PERIOD,
    SMA_FAST,
    SMA_SLOW,
    LOOKBACK_DAYS,
    ET_ZONE,
)
from trading_engine.indicators import IndicatorEngine


ET = pytz.timezone("America/New_York")
FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


def _load_csv_candles(filename: str) -> list[dict]:
    filepath = os.path.join(FIXTURES_DIR, filename)
    candles = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            candles.append({
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "timestamp": row["date"] + "T00:00:00",
            })
    return candles


def _generate_daily_candles(
    base_price: float,
    count: int,
    trend: str = "flat",
    volatility: float = 0.001,
    start_date: str = "2025-01-01",
):
    candles = []
    price = base_price
    dt = datetime.strptime(start_date, "%Y-%m-%d")

    for i in range(count):
        if trend == "up":
            price *= (1 + volatility)
        elif trend == "down":
            price *= (1 - volatility)

        o = round(price, 6)
        h = round(price * 1.002, 6)
        l = round(price * 0.998, 6)
        c = round(price, 6)

        candle_dt = dt + timedelta(days=i)
        candles.append({
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "timestamp": candle_dt.strftime("%Y-%m-%dT00:00:00"),
        })

    return candles


def _generate_breakout_candles(base_price: float, count: int, breakout_close: float):
    candles = _generate_daily_candles(base_price, count - 1, trend="flat", volatility=0.0001)
    last_dt = datetime.strptime(candles[-1]["timestamp"], "%Y-%m-%dT%H:%M:%S") + timedelta(days=1)
    candles.append({
        "open": round(breakout_close * 0.999, 6),
        "high": round(breakout_close * 1.001, 6),
        "low": round(breakout_close * 0.998, 6),
        "close": breakout_close,
        "timestamp": last_dt.strftime("%Y-%m-%dT00:00:00"),
    })
    return candles


class TestIdempotency:
    def setup_method(self):
        self.mock_cache = MagicMock()
        self.strategy = ForexTrendFollowingStrategy(self.mock_cache)

    @patch("trading_engine.strategies.trend_forex.datetime")
    @patch("trading_engine.strategies.trend_forex.signal_exists")
    @patch("trading_engine.strategies.trend_forex.get_open_position")
    @patch("trading_engine.strategies.trend_forex.open_position")
    @patch("trading_engine.strategies.trend_forex.insert_signal")
    def test_first_run_generates_signal(self, mock_insert, mock_open_pos, mock_get_pos, mock_exists, mock_dt):
        et_now = ET.localize(datetime(2026, 3, 10, 17, 5))
        mock_dt.now.return_value = et_now
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        candles = _generate_breakout_candles(1.0800, 200, 1.1200)
        self.mock_cache.get_candles.return_value = candles

        mock_exists.return_value = False
        mock_get_pos.return_value = None
        mock_insert.return_value = 42

        result = self.strategy.evaluate("EUR/USD")
        assert result is not None
        assert result["id"] == 42
        assert result["direction"] == "BUY"
        mock_insert.assert_called_once()

    @patch("trading_engine.strategies.trend_forex.datetime")
    @patch("trading_engine.strategies.trend_forex.signal_exists")
    @patch("trading_engine.strategies.trend_forex.get_open_position")
    @patch("trading_engine.strategies.trend_forex.insert_signal")
    def test_rerun_at_515pm_blocked_by_signal_exists(self, mock_insert, mock_get_pos, mock_exists, mock_dt):
        et_now = ET.localize(datetime(2026, 3, 10, 17, 15))
        mock_dt.now.return_value = et_now
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        candles = _generate_breakout_candles(1.0800, 200, 1.1200)
        self.mock_cache.get_candles.return_value = candles

        mock_exists.return_value = True
        mock_get_pos.return_value = None

        result = self.strategy.evaluate("EUR/USD")
        assert result is None
        mock_insert.assert_not_called()

    @patch("trading_engine.strategies.trend_forex.datetime")
    @patch("trading_engine.strategies.trend_forex.signal_exists")
    @patch("trading_engine.strategies.trend_forex.get_open_position")
    @patch("trading_engine.strategies.trend_forex.insert_signal")
    def test_rerun_blocked_by_open_trade(self, mock_insert, mock_get_pos, mock_exists, mock_dt):
        et_now = ET.localize(datetime(2026, 3, 10, 17, 15))
        mock_dt.now.return_value = et_now
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        candles = _generate_breakout_candles(1.0800, 200, 1.1200)
        self.mock_cache.get_candles.return_value = candles

        mock_get_pos.return_value = {"id": 99, "direction": "BUY", "asset": "EUR/USD"}

        result = self.strategy.evaluate("EUR/USD")
        assert result is None
        mock_insert.assert_not_called()


class TestDSTHandling:
    def test_pytz_handles_est(self):
        winter = ET.localize(datetime(2026, 1, 15, 17, 0))
        assert winter.dst().total_seconds() == 0
        utc_offset = winter.utcoffset().total_seconds() / 3600
        assert utc_offset == -5.0

    def test_pytz_handles_edt(self):
        summer = ET.localize(datetime(2026, 7, 15, 17, 0))
        assert summer.dst().total_seconds() == 3600
        utc_offset = summer.utcoffset().total_seconds() / 3600
        assert utc_offset == -4.0

    def test_dst_spring_forward_transition(self):
        before = ET.localize(datetime(2026, 3, 7, 17, 0))
        assert before.dst().total_seconds() == 0

        after = ET.localize(datetime(2026, 3, 9, 17, 0))
        assert after.dst().total_seconds() == 3600

        before_utc = before.astimezone(pytz.utc)
        after_utc = after.astimezone(pytz.utc)
        assert before_utc.hour == 22
        assert after_utc.hour == 21

    def test_dst_fall_back_transition(self):
        before = ET.localize(datetime(2026, 10, 31, 17, 0))
        assert before.dst().total_seconds() == 3600

        after = ET.localize(datetime(2026, 11, 2, 17, 0))
        assert after.dst().total_seconds() == 0

    def test_eval_window_check_works_in_est(self):
        mock_cache = MagicMock()
        strategy = ForexTrendFollowingStrategy(mock_cache)

        with patch("trading_engine.strategies.trend_forex.datetime") as mock_dt:
            winter_5pm = ET.localize(datetime(2026, 1, 15, 17, 5))
            mock_dt.now.return_value = winter_5pm
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            assert strategy._is_forex_close_window() is True

    def test_eval_window_check_works_in_edt(self):
        mock_cache = MagicMock()
        strategy = ForexTrendFollowingStrategy(mock_cache)

        with patch("trading_engine.strategies.trend_forex.datetime") as mock_dt:
            summer_5pm = ET.localize(datetime(2026, 7, 15, 17, 5))
            mock_dt.now.return_value = summer_5pm
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            assert strategy._is_forex_close_window() is True

    def test_outside_window_rejected(self):
        mock_cache = MagicMock()
        strategy = ForexTrendFollowingStrategy(mock_cache)

        with patch("trading_engine.strategies.trend_forex.datetime") as mock_dt:
            morning = ET.localize(datetime(2026, 1, 15, 10, 0))
            mock_dt.now.return_value = morning
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            assert strategy._is_forex_close_window() is False


class TestLongExitTrailingStop:
    def setup_method(self):
        self.mock_cache = MagicMock()
        self.strategy = ForexTrendFollowingStrategy(self.mock_cache)

    @patch("trading_engine.strategies.trend_forex.close_position")
    @patch("trading_engine.strategies.trend_forex.close_signal")
    @patch("trading_engine.strategies.trend_forex.update_position_tracking")
    @patch("trading_engine.database.get_active_signals")
    @patch("trading_engine.strategies.trend_forex.get_all_open_positions")
    def test_long_exit_triggers_at_3x_atr_below_peak(self, mock_positions, mock_active_sigs, mock_tracking, mock_close, mock_close_pos):
        entry_price = 1.10000
        atr_at_entry = 0.00500
        highest_close_since_entry = 1.12000
        trailing_stop = highest_close_since_entry - (atr_at_entry * TRAILING_STOP_ATR_MULT)

        current_close = trailing_stop - 0.00010

        mock_positions.return_value = [{
            "id": 1,
            "asset": "EUR/USD",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": highest_close_since_entry,
            "lowest_price_since_entry": None,
            "opened_at": "2026-01-15T17:00:00",
        }]

        mock_active_sigs.return_value = [{"id": 1}]

        exit_candles = _generate_daily_candles(current_close, 5, trend="flat")
        exit_candles[-1]["close"] = current_close
        self.mock_cache.get_candles.return_value = exit_candles

        closed = self.strategy.check_exits()

        assert len(closed) == 1
        assert closed[0]["exit_reason"] == "trailing_stop"
        assert closed[0]["exit_price"] == current_close
        mock_close.assert_called_once()

        call_args = mock_close.call_args
        assert call_args[0][0] == 1
        assert "Trailing stop hit" in call_args[0][1]

    @patch("trading_engine.strategies.trend_forex.close_position")
    @patch("trading_engine.strategies.trend_forex.close_signal")
    @patch("trading_engine.strategies.trend_forex.update_position_tracking")
    @patch("trading_engine.database.get_active_signals")
    @patch("trading_engine.strategies.trend_forex.get_all_open_positions")
    def test_long_holds_when_above_trailing_stop(self, mock_positions, mock_active_sigs, mock_tracking, mock_close, mock_close_pos):
        entry_price = 1.10000
        atr_at_entry = 0.00500
        highest_close_since_entry = 1.12000
        trailing_stop = highest_close_since_entry - (atr_at_entry * TRAILING_STOP_ATR_MULT)

        current_close = trailing_stop + 0.00100

        mock_positions.return_value = [{
            "id": 2,
            "asset": "EUR/USD",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": highest_close_since_entry,
            "lowest_price_since_entry": None,
            "opened_at": "2026-01-15T17:00:00",
        }]

        exit_candles = _generate_daily_candles(current_close, 5, trend="flat")
        exit_candles[-1]["close"] = current_close
        self.mock_cache.get_candles.return_value = exit_candles

        closed = self.strategy.check_exits()

        assert len(closed) == 0
        mock_close.assert_not_called()

    @patch("trading_engine.strategies.trend_forex.close_position")
    @patch("trading_engine.strategies.trend_forex.close_signal")
    @patch("trading_engine.strategies.trend_forex.update_position_tracking")
    @patch("trading_engine.database.get_active_signals")
    @patch("trading_engine.strategies.trend_forex.get_all_open_positions")
    def test_long_exit_exactly_at_trailing_stop_holds(self, mock_positions, mock_active_sigs, mock_tracking, mock_close, mock_close_pos):
        entry_price = 1.10000
        atr_at_entry = 0.00500
        highest_close_since_entry = 1.12000
        trailing_stop = highest_close_since_entry - (atr_at_entry * TRAILING_STOP_ATR_MULT)

        current_close = trailing_stop

        mock_positions.return_value = [{
            "id": 3,
            "asset": "EUR/USD",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": highest_close_since_entry,
            "lowest_price_since_entry": None,
            "opened_at": "2026-01-15T17:00:00",
        }]

        exit_candles = _generate_daily_candles(current_close, 5, trend="flat")
        exit_candles[-1]["close"] = current_close
        self.mock_cache.get_candles.return_value = exit_candles

        closed = self.strategy.check_exits()

        assert len(closed) == 0
        mock_close.assert_not_called()

    @patch("trading_engine.strategies.trend_forex.close_position")
    @patch("trading_engine.strategies.trend_forex.close_signal")
    @patch("trading_engine.strategies.trend_forex.update_position_tracking")
    @patch("trading_engine.database.get_active_signals")
    @patch("trading_engine.strategies.trend_forex.get_all_open_positions")
    def test_long_exit_uses_fixed_atr_not_current(self, mock_positions, mock_active_sigs, mock_tracking, mock_close, mock_close_pos):
        entry_price = 1.10000
        atr_at_entry = 0.00500
        highest_close_since_entry = 1.13000

        trailing_stop = highest_close_since_entry - (atr_at_entry * 3.0)
        current_close = trailing_stop - 0.00001

        mock_positions.return_value = [{
            "id": 4,
            "asset": "EUR/USD",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": highest_close_since_entry,
            "lowest_price_since_entry": None,
            "opened_at": "2026-01-15T17:00:00",
        }]

        mock_active_sigs.return_value = [{"id": 4}]

        exit_candles = _generate_daily_candles(current_close, 5, trend="flat")
        exit_candles[-1]["close"] = current_close
        self.mock_cache.get_candles.return_value = exit_candles

        closed = self.strategy.check_exits()
        assert len(closed) == 1

        exit_reason = mock_close.call_args[0][1]
        assert str(round(atr_at_entry, 6)) in exit_reason

    @patch("trading_engine.strategies.trend_forex.close_position")
    @patch("trading_engine.strategies.trend_forex.close_signal")
    @patch("trading_engine.strategies.trend_forex.update_position_tracking")
    @patch("trading_engine.database.get_active_signals")
    @patch("trading_engine.strategies.trend_forex.get_all_open_positions")
    def test_highest_price_updates_with_new_high(self, mock_positions, mock_active_sigs, mock_tracking, mock_close, mock_close_pos):
        entry_price = 1.10000
        atr_at_entry = 0.00500
        stored_highest = 1.11000
        new_close = 1.11500

        mock_positions.return_value = [{
            "id": 5,
            "asset": "EUR/USD",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": stored_highest,
            "lowest_price_since_entry": None,
            "opened_at": "2026-01-15T17:00:00",
        }]

        exit_candles = _generate_daily_candles(new_close, 5, trend="flat")
        exit_candles[-1]["close"] = new_close
        self.mock_cache.get_candles.return_value = exit_candles

        self.strategy.check_exits()

        mock_tracking.assert_called_once_with(5, highest_price=new_close)


class TestShortExitTrailingStop:
    def setup_method(self):
        self.mock_cache = MagicMock()
        self.strategy = ForexTrendFollowingStrategy(self.mock_cache)

    @patch("trading_engine.strategies.trend_forex.close_position")
    @patch("trading_engine.strategies.trend_forex.close_signal")
    @patch("trading_engine.strategies.trend_forex.update_position_tracking")
    @patch("trading_engine.database.get_active_signals")
    @patch("trading_engine.strategies.trend_forex.get_all_open_positions")
    def test_short_exit_triggers_at_3x_atr_above_trough(self, mock_positions, mock_active_sigs, mock_tracking, mock_close, mock_close_pos):
        entry_price = 1.10000
        atr_at_entry = 0.00500
        lowest_close_since_entry = 1.08000
        trailing_stop = lowest_close_since_entry + (atr_at_entry * TRAILING_STOP_ATR_MULT)

        current_close = trailing_stop + 0.00010

        mock_positions.return_value = [{
            "id": 10,
            "asset": "EUR/USD",
            "strategy_name": STRATEGY_NAME,
            "direction": "SELL",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": None,
            "lowest_price_since_entry": lowest_close_since_entry,
            "opened_at": "2026-01-20T17:00:00",
        }]

        mock_active_sigs.return_value = [{"id": 10}]

        exit_candles = _generate_daily_candles(current_close, 5, trend="flat")
        exit_candles[-1]["close"] = current_close
        self.mock_cache.get_candles.return_value = exit_candles

        closed = self.strategy.check_exits()

        assert len(closed) == 1
        assert closed[0]["exit_reason"] == "trailing_stop"
        mock_close.assert_called_once()

    @patch("trading_engine.strategies.trend_forex.close_position")
    @patch("trading_engine.strategies.trend_forex.close_signal")
    @patch("trading_engine.strategies.trend_forex.update_position_tracking")
    @patch("trading_engine.database.get_active_signals")
    @patch("trading_engine.strategies.trend_forex.get_all_open_positions")
    def test_short_holds_when_below_trailing_stop(self, mock_positions, mock_active_sigs, mock_tracking, mock_close, mock_close_pos):
        entry_price = 1.10000
        atr_at_entry = 0.00500
        lowest_close_since_entry = 1.08000
        trailing_stop = lowest_close_since_entry + (atr_at_entry * TRAILING_STOP_ATR_MULT)

        current_close = trailing_stop - 0.00200

        mock_positions.return_value = [{
            "id": 11,
            "asset": "EUR/USD",
            "strategy_name": STRATEGY_NAME,
            "direction": "SELL",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": None,
            "lowest_price_since_entry": lowest_close_since_entry,
            "opened_at": "2026-01-20T17:00:00",
        }]

        exit_candles = _generate_daily_candles(current_close, 5, trend="flat")
        exit_candles[-1]["close"] = current_close
        self.mock_cache.get_candles.return_value = exit_candles

        closed = self.strategy.check_exits()

        assert len(closed) == 0
        mock_close.assert_not_called()


class TestMissingMetadata:
    def setup_method(self):
        self.mock_cache = MagicMock()
        self.strategy = ForexTrendFollowingStrategy(self.mock_cache)

    @patch("trading_engine.strategies.trend_forex.close_position")
    @patch("trading_engine.strategies.trend_forex.close_signal")
    @patch("trading_engine.strategies.trend_forex.update_position_tracking")
    @patch("trading_engine.database.get_active_signals")
    @patch("trading_engine.strategies.trend_forex.get_all_open_positions")
    def test_missing_atr_metadata_skips_exit_check(self, mock_positions, mock_active_sigs, mock_tracking, mock_close, mock_close_pos):
        mock_positions.return_value = [{
            "id": 20,
            "asset": "EUR/USD",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": 1.10000,
            "atr_at_entry": None,
            "highest_price_since_entry": 1.12000,
            "lowest_price_since_entry": None,
            "opened_at": "2026-01-15T17:00:00",
        }]

        closed = self.strategy.check_exits()

        assert len(closed) == 0
        mock_close.assert_not_called()
        mock_tracking.assert_not_called()


class TestLongExitWithCSVData:
    def setup_method(self):
        self.mock_cache = MagicMock()
        self.strategy = ForexTrendFollowingStrategy(self.mock_cache)

    @patch("trading_engine.strategies.trend_forex.close_position")
    @patch("trading_engine.strategies.trend_forex.close_signal")
    @patch("trading_engine.strategies.trend_forex.update_position_tracking")
    @patch("trading_engine.database.get_active_signals")
    @patch("trading_engine.strategies.trend_forex.get_all_open_positions")
    def test_long_exit_triggers_from_csv_history(self, mock_positions, mock_active_sigs, mock_tracking, mock_close, mock_close_pos):
        csv_candles = _load_csv_candles("eurusd_daily_exit_test.csv")
        assert len(csv_candles) >= 120, f"CSV has {len(csv_candles)} candles, need 120+"

        closes = [c["close"] for c in csv_candles]
        highs = [c["high"] for c in csv_candles]
        lows = [c["low"] for c in csv_candles]

        atr_values = IndicatorEngine.atr(highs, lows, closes, 100)
        atr_at_entry_idx = -3
        atr_at_entry = atr_values[atr_at_entry_idx]
        assert atr_at_entry is not None, "ATR should not be None for the entry candle"

        entry_price = closes[atr_at_entry_idx]
        highest_close_after_entry = max(closes[atr_at_entry_idx:])

        final_close = closes[-1]
        trailing_stop = highest_close_after_entry - (atr_at_entry * TRAILING_STOP_ATR_MULT)

        should_exit = final_close < trailing_stop

        mock_positions.return_value = [{
            "id": 100,
            "asset": "EUR/USD",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": highest_close_after_entry,
            "lowest_price_since_entry": None,
            "opened_at": "2025-09-15T17:00:00",
        }]

        mock_active_sigs.return_value = [{"id": 100}]

        self.mock_cache.get_candles.return_value = csv_candles

        closed = self.strategy.check_exits()

        if should_exit:
            assert len(closed) == 1, (
                f"Expected exit: close={final_close:.5f} < trailing_stop={trailing_stop:.5f} "
                f"(highest={highest_close_after_entry:.5f} - ATR*3={atr_at_entry * 3:.5f})"
            )
            assert closed[0]["exit_reason"] == "trailing_stop"
            assert closed[0]["exit_price"] == final_close

            call_args = mock_close.call_args
            assert call_args[0][0] == 100
            assert "Trailing stop hit" in call_args[0][1]
            assert str(round(atr_at_entry, 6)) in call_args[0][1]
        else:
            assert len(closed) == 0, (
                f"Expected hold: close={final_close:.5f} >= trailing_stop={trailing_stop:.5f}"
            )

    @patch("trading_engine.strategies.trend_forex.close_position")
    @patch("trading_engine.strategies.trend_forex.close_signal")
    @patch("trading_engine.strategies.trend_forex.update_position_tracking")
    @patch("trading_engine.database.get_active_signals")
    @patch("trading_engine.strategies.trend_forex.get_all_open_positions")
    def test_csv_atr_remains_fixed_throughout_trade(self, mock_positions, mock_active_sigs, mock_tracking, mock_close, mock_close_pos):
        csv_candles = _load_csv_candles("eurusd_daily_exit_test.csv")

        closes = [c["close"] for c in csv_candles]
        highs = [c["high"] for c in csv_candles]
        lows = [c["low"] for c in csv_candles]

        atr_values = IndicatorEngine.atr(highs, lows, closes, 100)

        entry_idx = 105
        atr_at_entry = atr_values[entry_idx]
        entry_price = closes[entry_idx]

        atr_at_exit = atr_values[-1]
        assert atr_at_entry != atr_at_exit, "ATR should change over time in real data"

        highest_since = max(closes[entry_idx:])

        mock_positions.return_value = [{
            "id": 101,
            "asset": "EUR/USD",
            "strategy_name": STRATEGY_NAME,
            "direction": "BUY",
            "entry_price": entry_price,
            "atr_at_entry": atr_at_entry,
            "highest_price_since_entry": highest_since,
            "lowest_price_since_entry": None,
            "opened_at": "2025-08-15T17:00:00",
        }]

        mock_active_sigs.return_value = [{"id": 101}]

        self.mock_cache.get_candles.return_value = csv_candles
        self.strategy.check_exits()

        if mock_close.called:
            exit_reason = mock_close.call_args[0][1]
            assert str(round(atr_at_entry, 6)) in exit_reason
            assert str(round(atr_at_exit, 6)) not in exit_reason or atr_at_entry == atr_at_exit
