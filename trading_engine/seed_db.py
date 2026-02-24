#!/usr/bin/env python3
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from trading_engine.database import (
    upsert_candles,
    get_candles,
    insert_signal,
    signal_exists,
    get_active_signals,
    open_position,
    get_open_position,
    has_open_position,
)

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger("seed_db")


def seed_and_verify():
    logger.info("=== Seeding test data ===")

    test_candle = {
        "timestamp": "2026-02-24T00:00:00",
        "open": 1.0850,
        "high": 1.0900,
        "low": 1.0800,
        "close": 1.0875,
    }
    upsert_candles("EUR/USD", "D1", [test_candle])
    logger.info("Inserted test candle: EUR/USD D1 2026-02-24")

    rows = get_candles("EUR/USD", "D1", limit=5)
    assert len(rows) >= 1, "Expected at least 1 candle"
    assert rows[-1]["close"] == 1.0875, f"Unexpected close: {rows[-1]['close']}"
    logger.info(f"  Verified: {len(rows)} candle(s) returned, close={rows[-1]['close']}")

    test_signal = {
        "strategy_name": "trend_forex",
        "asset": "EUR/USD",
        "direction": "BUY",
        "entry_price": 1.0875,
        "stop_loss": 1.0750,
        "take_profit": 1.1000,
        "atr_at_entry": 0.00500,
        "signal_timestamp": "2026-02-24T00:00:00",
    }
    sig_id = insert_signal(test_signal)
    assert sig_id is not None, "Signal insert returned None"
    logger.info(f"Inserted test signal #{sig_id}: trend_forex BUY EUR/USD")

    exists = signal_exists("trend_forex", "EUR/USD", "2026-02-24T00:00:00")
    assert exists, "signal_exists should return True"
    logger.info("  Verified: signal_exists() = True")

    active = get_active_signals(strategy_name="trend_forex", asset="EUR/USD")
    assert len(active) >= 1, "Expected at least 1 active signal"
    assert active[0]["status"] == "OPEN"
    logger.info(f"  Verified: {len(active)} active signal(s), status=OPEN")

    logger.info("=== Testing UniqueConstraint (idempotency) ===")
    dup_id = insert_signal(test_signal)
    assert dup_id is None, "Duplicate signal should return None (constraint violation)"
    logger.info("  Verified: duplicate signal rejected by UniqueConstraint")

    logger.info("=== Testing CheckConstraint (direction) ===")
    bad_signal = test_signal.copy()
    bad_signal["direction"] = "INVALID"
    bad_signal["signal_timestamp"] = "2026-02-25T00:00:00"
    bad_id = insert_signal(bad_signal)
    assert bad_id is None, "Invalid direction should return None (constraint violation)"
    logger.info("  Verified: invalid direction rejected by CheckConstraint")

    logger.info("=== Testing OpenPosition ===")
    pos_data = {
        "asset": "EUR/USD",
        "strategy_name": "trend_forex",
        "direction": "BUY",
        "entry_price": 1.0875,
        "atr_at_entry": 0.00500,
    }
    pos_id = open_position(pos_data)
    assert pos_id is not None, "Position insert returned None"
    logger.info(f"Opened test position #{pos_id}: trend_forex BUY EUR/USD")

    has_pos = has_open_position("trend_forex", "EUR/USD")
    assert has_pos, "has_open_position should return True"
    logger.info("  Verified: has_open_position() = True")

    pos = get_open_position("trend_forex", "EUR/USD")
    assert pos is not None
    assert pos["atr_at_entry"] == 0.00500
    logger.info(f"  Verified: position data correct, atr_at_entry={pos['atr_at_entry']}")

    dup_pos_id = open_position(pos_data)
    assert dup_pos_id == pos_id, "Duplicate position should return existing id"
    logger.info("  Verified: duplicate position returns existing id (no duplicate)")

    logger.info("=== Testing candle upsert (idempotent update) ===")
    updated_candle = test_candle.copy()
    updated_candle["close"] = 1.0900
    upsert_candles("EUR/USD", "D1", [updated_candle])
    rows2 = get_candles("EUR/USD", "D1", limit=5)
    latest = [r for r in rows2 if r["timestamp"] == "2026-02-24T00:00:00"]
    assert len(latest) == 1, "Upsert should not create duplicate candle"
    assert latest[0]["close"] == 1.0900, f"Upsert should update close to 1.0900, got {latest[0]['close']}"
    logger.info("  Verified: candle upsert updated close without creating duplicate")

    logger.info("=== All seed tests passed ===")


if __name__ == "__main__":
    seed_and_verify()
