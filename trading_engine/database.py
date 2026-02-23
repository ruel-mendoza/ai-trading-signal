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
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategy_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL CHECK(direction IN ('long', 'short')),
            entry_price REAL NOT NULL,
            stop_loss REAL,
            take_profit REAL,
            trailing_stop_atr_mult REAL,
            trigger_candle_time TEXT NOT NULL,
            trigger_timeframe TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active', 'closed', 'expired')),
            highest_price REAL,
            lowest_price REAL,
            exit_price REAL,
            exit_reason TEXT,
            metadata TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(strategy, symbol, trigger_candle_time, trigger_timeframe)
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_strategy_signals_lookup
        ON strategy_signals(strategy, symbol, trigger_candle_time, trigger_timeframe)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_strategy_signals_active
        ON strategy_signals(strategy, symbol, status)
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS api_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            endpoint TEXT NOT NULL,
            symbol TEXT,
            timeframe TEXT,
            credits_used INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_api_usage_created
        ON api_usage(created_at)
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT DEFAULT (datetime('now'))
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

def signal_exists(strategy: str, symbol: str, trigger_candle_time: str, trigger_timeframe: str) -> bool:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 1 FROM strategy_signals
        WHERE strategy = ? AND symbol = ? AND trigger_candle_time = ? AND trigger_timeframe = ?
    """, (strategy, symbol, trigger_candle_time, trigger_timeframe))
    row = cursor.fetchone()
    conn.close()
    return row is not None

def insert_signal(signal: dict) -> Optional[int]:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO strategy_signals
                (strategy, symbol, direction, entry_price, stop_loss, take_profit,
                 trailing_stop_atr_mult, trigger_candle_time, trigger_timeframe,
                 status, highest_price, lowest_price, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)
        """, (
            signal["strategy"], signal["symbol"], signal["direction"],
            signal["entry_price"], signal.get("stop_loss"),
            signal.get("take_profit"), signal.get("trailing_stop_atr_mult"),
            signal["trigger_candle_time"], signal["trigger_timeframe"],
            signal["entry_price"] if signal["direction"] == "long" else None,
            signal["entry_price"] if signal["direction"] == "short" else None,
            signal.get("metadata"),
        ))
        conn.commit()
        signal_id = cursor.lastrowid
    except sqlite3.IntegrityError:
        signal_id = None
    conn.close()
    return signal_id

def get_active_signals(strategy: Optional[str] = None, symbol: Optional[str] = None) -> list[dict]:
    conn = get_connection()
    cursor = conn.cursor()
    query = "SELECT * FROM strategy_signals WHERE status = 'active'"
    params: list = []
    if strategy:
        query += " AND strategy = ?"
        params.append(strategy)
    if symbol:
        query += " AND symbol = ?"
        params.append(symbol)
    query += " ORDER BY created_at DESC"
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def update_signal_tracking(signal_id: int, highest_price: Optional[float] = None, lowest_price: Optional[float] = None):
    conn = get_connection()
    cursor = conn.cursor()
    if highest_price is not None:
        cursor.execute("UPDATE strategy_signals SET highest_price = MAX(COALESCE(highest_price, 0), ?) WHERE id = ?", (highest_price, signal_id))
    if lowest_price is not None:
        cursor.execute("UPDATE strategy_signals SET lowest_price = MIN(COALESCE(lowest_price, 999999), ?) WHERE id = ?", (lowest_price, signal_id))
    conn.commit()
    conn.close()

def close_signal(signal_id: int, exit_price: float, exit_reason: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE strategy_signals
        SET status = 'closed', exit_price = ?, exit_reason = ?
        WHERE id = ?
    """, (exit_price, exit_reason, signal_id))
    conn.commit()
    conn.close()

def get_all_signals(strategy: Optional[str] = None, symbol: Optional[str] = None, status: Optional[str] = None, limit: int = 100) -> list[dict]:
    conn = get_connection()
    cursor = conn.cursor()
    query = "SELECT * FROM strategy_signals WHERE 1=1"
    params: list = []
    if strategy:
        query += " AND strategy = ?"
        params.append(strategy)
    if symbol:
        query += " AND symbol = ?"
        params.append(symbol)
    if status:
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_setting(key: str) -> Optional[str]:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM app_settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    return row["value"] if row else None

def set_setting(key: str, value: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')
    """, (key, value))
    conn.commit()
    conn.close()

def log_api_usage(endpoint: str, symbol: Optional[str] = None, timeframe: Optional[str] = None, credits_used: int = 1):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO api_usage (endpoint, symbol, timeframe, credits_used)
        VALUES (?, ?, ?, ?)
    """, (endpoint, symbol, timeframe, credits_used))
    conn.commit()
    conn.close()

def get_api_usage_stats() -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COALESCE(SUM(credits_used), 0) as total
        FROM api_usage
        WHERE created_at >= datetime('now', 'start of month')
    """)
    monthly_total = cursor.fetchone()["total"]
    cursor.execute("""
        SELECT COALESCE(SUM(credits_used), 0) as total
        FROM api_usage
        WHERE created_at >= datetime('now', '-1 day')
    """)
    daily_total = cursor.fetchone()["total"]
    cursor.execute("""
        SELECT endpoint, COUNT(*) as count, SUM(credits_used) as credits
        FROM api_usage
        WHERE created_at >= datetime('now', 'start of month')
        GROUP BY endpoint ORDER BY credits DESC
    """)
    by_endpoint = [dict(row) for row in cursor.fetchall()]
    cursor.execute("""
        SELECT date(created_at) as day, SUM(credits_used) as credits
        FROM api_usage
        WHERE created_at >= datetime('now', '-30 days')
        GROUP BY date(created_at) ORDER BY day DESC LIMIT 30
    """)
    daily_history = [dict(row) for row in cursor.fetchall()]
    conn.close()

    monthly_limit = 500000
    usage_pct = (monthly_total / monthly_limit) * 100 if monthly_limit > 0 else 0
    alert_level = None
    if usage_pct >= 90:
        alert_level = "critical"
    elif usage_pct >= 75:
        alert_level = "warning"
    elif usage_pct >= 60:
        alert_level = "caution"

    return {
        "monthly_total": monthly_total,
        "monthly_limit": monthly_limit,
        "usage_percentage": round(usage_pct, 2),
        "daily_total": daily_total,
        "alert_level": alert_level,
        "by_endpoint": by_endpoint,
        "daily_history": daily_history,
    }

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
