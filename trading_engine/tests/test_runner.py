import pytest
from unittest.mock import MagicMock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from trading_engine.models import Base, Signal, OpenPosition, Candle
from trading_engine.strategies.base import Action, Direction, SignalResult
import trading_engine.database as db_module
from trading_engine.strategy_runner import run_strategy


@pytest.fixture(autouse=True)
def _isolated_db(monkeypatch):
    mem_engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(mem_engine)
    TestSession = sessionmaker(bind=mem_engine, expire_on_commit=False)

    monkeypatch.setattr(db_module, "engine", mem_engine)
    monkeypatch.setattr(db_module, "SessionFactory", TestSession)

    with TestSession() as session:
        for i in range(10):
            session.add(Candle(
                asset="EUR/USD",
                timeframe="1H",
                timestamp=f"2025-06-{i+1:02d}T00:00:00",
                open=1.08 + i * 0.001,
                high=1.085 + i * 0.001,
                low=1.075 + i * 0.001,
                close=1.081 + i * 0.001,
            ))
        session.commit()

    yield TestSession


def _make_entry_strategy(price=1.09, atr=0.003):
    strat = MagicMock()
    strat.evaluate.return_value = SignalResult(
        action=Action.ENTRY,
        direction=Direction.LONG,
        price=price,
        stop_loss=price - atr * 2,
        atr_at_entry=atr,
    )
    return strat


def _make_exit_strategy(price=1.095):
    strat = MagicMock()
    strat.evaluate.return_value = SignalResult(
        action=Action.EXIT,
        price=price,
        metadata={"exit_reason": "Trailing stop hit"},
    )
    return strat


class TestTradeLifecycle:
    def test_scenario1_entry_creates_position_and_signal(self, _isolated_db):
        strat = _make_entry_strategy(price=1.09, atr=0.003)

        result = run_strategy(strat, "test_strat", "EUR/USD", "1H")

        assert result is not None
        assert result["direction"] == "BUY"
        assert result["entry_price"] == 1.09
        assert result["atr_at_entry"] == 0.003
        assert result["status"] == "OPEN"

        with _isolated_db() as session:
            positions = session.query(OpenPosition).all()
            assert len(positions) == 1
            assert positions[0].asset == "EUR/USD"
            assert positions[0].strategy_name == "test_strat"
            assert positions[0].direction == "BUY"
            assert positions[0].entry_price == 1.09
            assert positions[0].atr_at_entry == 0.003
            assert positions[0].highest_price_since_entry == 1.09

            signals = session.query(Signal).all()
            assert len(signals) == 1
            assert signals[0].status == "OPEN"
            assert signals[0].atr_at_entry == 0.003

    def test_scenario2_duplicate_entry_blocked(self, _isolated_db):
        strat = _make_entry_strategy(price=1.09, atr=0.003)

        result1 = run_strategy(strat, "test_strat", "EUR/USD", "1H")
        assert result1 is not None

        result2 = run_strategy(strat, "test_strat", "EUR/USD", "1H")
        assert result2 is None

        with _isolated_db() as session:
            positions = session.query(OpenPosition).all()
            assert len(positions) == 1

            signals = session.query(Signal).all()
            assert len(signals) == 1

    def test_scenario3_trailing_stop_updates_highest_price(self, _isolated_db):
        strat = _make_entry_strategy(price=1.09, atr=0.003)
        run_strategy(strat, "test_strat", "EUR/USD", "1H")

        with _isolated_db() as session:
            pos = session.query(OpenPosition).first()
            assert pos.highest_price_since_entry == 1.09

        higher_close = 1.12
        with _isolated_db() as session:
            session.add(Candle(
                asset="EUR/USD",
                timeframe="1H",
                timestamp="2025-06-11T00:00:00",
                open=1.10,
                high=1.13,
                low=1.09,
                close=higher_close,
            ))
            session.commit()

        no_action = MagicMock()
        no_action.evaluate.return_value = SignalResult()
        run_strategy(no_action, "test_strat", "EUR/USD", "1H")

        with _isolated_db() as session:
            pos = session.query(OpenPosition).first()
            assert pos is not None
            assert pos.highest_price_since_entry == higher_close

    def test_scenario4_exit_closes_position_and_signals(self, _isolated_db):
        entry_strat = _make_entry_strategy(price=1.09, atr=0.003)
        run_strategy(entry_strat, "test_strat", "EUR/USD", "1H")

        with _isolated_db() as session:
            assert session.query(OpenPosition).count() == 1
            assert session.query(Signal).filter_by(status="OPEN").count() == 1

        exit_strat = _make_exit_strategy(price=1.095)
        result = run_strategy(exit_strat, "test_strat", "EUR/USD", "1H")

        assert result is not None
        assert result["exit_price"] == 1.095
        assert result["exit_reason"] == "Trailing stop hit"

        with _isolated_db() as session:
            positions = session.query(OpenPosition).all()
            assert len(positions) == 0

            signals = session.query(Signal).all()
            assert len(signals) == 1
            assert signals[0].status == "CLOSED"
            assert signals[0].exit_price == 1.095
            assert signals[0].exit_reason == "Trailing stop hit"
