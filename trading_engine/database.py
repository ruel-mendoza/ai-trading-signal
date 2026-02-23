import sqlite3
import os
from datetime import datetime
from typing import Optional

DB_PATH = os.path.join(os.path.dirname(__file__), "trading_data.db")

VALID_TIMEFRAMES = ["30m", "1H", "4H", "D"]

def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ohlc_candles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL CHECK(timeframe IN ('30m', '1H', '4H', 'D')),
            open_time TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL DEFAULT 0,
            is_closed INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(symbol, timeframe, open_time)
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_ohlc_symbol_tf_time
        ON ohlc_candles(symbol, timeframe, open_time)
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cache_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            last_fetched TEXT NOT NULL,
            last_candle_close TEXT,
            UNIQUE(symbol, timeframe)
        )
    """)
    conn.commit()
    conn.close()

def upsert_candles(symbol: str, timeframe: str, candles: list[dict]):
    conn = get_connection()
    cursor = conn.cursor()
    for c in candles:
        cursor.execute("""
            INSERT INTO ohlc_candles (symbol, timeframe, open_time, open, high, low, close, volume, is_closed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, timeframe, open_time)
            DO UPDATE SET
                open = excluded.open,
                high = excluded.high,
                low = excluded.low,
                close = excluded.close,
                volume = excluded.volume,
                is_closed = excluded.is_closed
        """, (
            symbol, timeframe, c["open_time"],
            c["open"], c["high"], c["low"], c["close"],
            c.get("volume", 0), c.get("is_closed", 1)
        ))
    conn.commit()
    conn.close()

def get_candles(symbol: str, timeframe: str, limit: int = 300) -> list[dict]:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT open_time, open, high, low, close, volume, is_closed
        FROM ohlc_candles
        WHERE symbol = ? AND timeframe = ?
        ORDER BY open_time DESC
        LIMIT ?
    """, (symbol, timeframe, limit))
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in reversed(rows)]

def update_cache_metadata(symbol: str, timeframe: str, last_candle_close: Optional[str] = None):
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()
    cursor.execute("""
        INSERT INTO cache_metadata (symbol, timeframe, last_fetched, last_candle_close)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(symbol, timeframe)
        DO UPDATE SET
            last_fetched = excluded.last_fetched,
            last_candle_close = COALESCE(excluded.last_candle_close, cache_metadata.last_candle_close)
    """, (symbol, timeframe, now, last_candle_close))
    conn.commit()
    conn.close()

def get_cache_metadata(symbol: str, timeframe: str) -> Optional[dict]:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT last_fetched, last_candle_close
        FROM cache_metadata
        WHERE symbol = ? AND timeframe = ?
    """, (symbol, timeframe))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def get_candle_count(symbol: str, timeframe: str) -> int:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) as cnt FROM ohlc_candles
        WHERE symbol = ? AND timeframe = ?
    """, (symbol, timeframe))
    row = cursor.fetchone()
    conn.close()
    return row["cnt"]
