import os
import hashlib
import secrets
import logging
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import (
    create_engine,
    event,
    text,
    inspect,
)
from sqlalchemy.orm import (
    sessionmaker,
    Session,
)
from sqlalchemy.pool import QueuePool


def _invalidate_signal_cache():
    try:
        from trading_engine.api_v1 import invalidate_signal_caches

        invalidate_signal_caches()
    except Exception:
        pass


def _ws_broadcast_new(signal_dict: dict):
    try:
        from trading_engine.websocket import broadcaster

        broadcaster.broadcast_signal_new(signal_dict)
    except Exception:
        pass


def _ws_broadcast_closed(signal_id: int, exit_reason: str = "", exit_price=None):
    try:
        from trading_engine.websocket import broadcaster

        broadcaster.broadcast_signal_closed(signal_id, exit_reason, exit_price)
    except Exception:
        pass


from trading_engine.models import (
    Base,
    Candle,
    Signal,
    OpenPosition,
    APIUsageLog,
    CacheMetadata,
    AppSetting,
    AdminUser,
    AdminSession,
    PartnerApiKey,
    SchedulerJobLog,
    SignalMetrics,
    SignalCmsPost,
    UserCmsConfig,
    StrategyExecutionLog,
    RecoveryNotification,
    HistoricalDailyClose,
    StrategyAsset,
    StockAlgo2Position,
    VALID_TIMEFRAMES,
)

logger = logging.getLogger("trading_engine.database")

_ASSET_CLASS_MAP: dict[str, str] = {
    # ── Forex (pairs) ──────────────────────────────────
    "EUR/USD": "forex",
    "GBP/USD": "forex",
    "USD/JPY": "forex",
    "USD/CAD": "forex",
    "AUD/USD": "forex",
    "NZD/USD": "forex",
    "USD/CHF": "forex",
    "EUR/GBP": "forex",
    # ── Forex (spot commodities) ───────────────────────
    "XAU/USD": "forex",
    "XAG/USD": "forex",
    "XPT/USD": "forex",
    "XPD/USD": "forex",
    "XCU/USD": "forex",
    "OSX": "forex",
    "NATGAS/USD": "forex",
    "CORN/USD": "forex",
    "SOYBEAN/USD": "forex",
    "WHEAT/USD": "forex",
    "SUGAR/USD": "forex",
    # ── Forex (commodity ETFs) ─────────────────────────
    "USO": "forex",
    "UNG": "forex",
    "UGA": "forex",
    "DBB": "forex",
    "SLX": "forex",
    "SGOL": "forex",
    "SIVR": "forex",
    "CPER": "forex",
    "PPLT": "forex",
    "PALL": "forex",
    "CORN": "forex",
    "SOYB": "forex",
    "WEAT": "forex",
    "CANE": "forex",
    "WOOD": "forex",
    # ── Forex (indices) ────────────────────────────────
    "SPX": "forex",
    "NDX": "forex",
    "RUT": "forex",
    "DJI": "forex",
    # ── Crypto ─────────────────────────────────────────
    "BTC/USD": "crypto",
    "ETH/USD": "crypto",
    "LTC/USD": "crypto",
    "XRP/USD": "crypto",
    "BNB/USD": "crypto",
    "ETHUSD": "crypto",
    "BTCUSD": "crypto",
    # Altcoins — Tier 1
    "SOL/USD": "crypto",
    "DOGE/USD": "crypto",
    "ADA/USD": "crypto",
    "AVAX/USD": "crypto",
    "LINK/USD": "crypto",
    "DOT/USD": "crypto",
    "BCH/USD": "crypto",
    "XLM/USD": "crypto",
    "ATOM/USD": "crypto",
    "UNI/USD": "crypto",
    # Altcoins — Tier 2
    "TON/USD": "crypto",
    "SHIB/USD": "crypto",
    "HBAR/USD": "crypto",
    "NEAR/USD": "crypto",
    "ICP/USD": "crypto",
    "CRO/USD": "crypto",
    # Altcoins — Tier 3
    "APT/USD": "crypto",
    "ARB/USD": "crypto",
    "OP/USD": "crypto",
    "SUI/USD": "crypto",
    "INJ/USD": "crypto",
    "TRX/USD": "crypto",
}

# ---------------------------------------------------------------------------
# Full display names for all tracked assets — populated once at startup,
# reused for every signal insert. No FCSAPI calls needed.
# ---------------------------------------------------------------------------
_ASSET_NAME_MAP: dict[str, str] = {
    # Forex pairs
    "EUR/USD": "Euro / US Dollar",
    "GBP/USD": "British Pound / US Dollar",
    "USD/JPY": "US Dollar / Japanese Yen",
    "AUD/USD": "Australian Dollar / US Dollar",
    "USD/CAD": "US Dollar / Canadian Dollar",
    "USD/CHF": "US Dollar / Swiss Franc",
    "NZD/USD": "New Zealand Dollar / US Dollar",
    "EUR/GBP": "Euro / British Pound",
    # Crypto
    "BTC/USD": "Bitcoin",
    "ETH/USD": "Ethereum",
    "BNB/USD": "BNB",
    "XRP/USD": "XRP",
    "SOL/USD": "Solana",
    "TRX/USD": "TRON",
    "DOGE/USD": "Dogecoin",
    "ADA/USD": "Cardano",
    "TON/USD": "Toncoin",
    "SHIB/USD": "Shiba Inu",
    "AVAX/USD": "Avalanche",
    "LINK/USD": "Chainlink",
    "LTC/USD": "Litecoin",
    "DOT/USD": "Polkadot",
    "BCH/USD": "Bitcoin Cash",
    "UNI/USD": "Uniswap",
    "ATOM/USD": "Cosmos",
    "XLM/USD": "Stellar",
    "HBAR/USD": "Hedera",
    "ICP/USD": "Internet Computer",
    "APT/USD": "Aptos",
    "NEAR/USD": "NEAR Protocol",
    "ARB/USD": "Arbitrum",
    "OP/USD": "Optimism",
    "SUI/USD": "Sui",
    "INJ/USD": "Injective",
    "CRO/USD": "Cronos",
    # Commodities
    "XAU/USD": "Gold",
    "XAG/USD": "Silver",
    "OSX": "Oil Services Index",
    # Indices
    "SPX": "S&P 500",
    "NDX": "NASDAQ 100",
    "RUT": "Russell 2000",
    "DJI": "Dow Jones Industrial Average",
    # Commodity ETFs
    "CORN": "Teucrium Corn Fund",
    "SOYB": "Teucrium Soybean Fund",
    "WEAT": "Teucrium Wheat Fund",
    "CANE": "Teucrium Sugar Fund",
    "WOOD": "iShares Global Timber & Forestry ETF",
    "USO": "United States Oil Fund",
    "UNG": "United States Natural Gas Fund",
    "UGA": "United States Gasoline Fund",
    "SGOL": "Aberdeen Standard Physical Gold Shares",
    "SIVR": "Aberdeen Standard Physical Silver Shares",
    "CPER": "United States Copper Index Fund",
    "PPLT": "Aberdeen Standard Physical Platinum Shares",
    "PALL": "Aberdeen Standard Physical Palladium Shares",
    "DBB": "Invesco DB Base Metals Fund",
    "SLX": "VanEck Steel ETF",
}


def get_full_name_for_asset(symbol: str) -> Optional[str]:
    """Look up the full display name for a ticker or pair symbol.

    Priority:
    1. Static _ASSET_NAME_MAP (forex, crypto, commodities, ETFs, indices)
    2. _NDX100_COMPANY_NAMES (NASDAQ 100 stocks, from nasdaq_sync)
    3. strategy_assets.full_name DB column (fallback for any remaining symbols)
    4. None — callers should handle gracefully
    """
    name = _ASSET_NAME_MAP.get(symbol)
    if name:
        return name
    # Lazy import to avoid circular dependency at module load time
    try:
        from trading_engine.utils.nasdaq_sync import _NDX100_COMPANY_NAMES
        name = _NDX100_COMPANY_NAMES.get(symbol)
        if name:
            return name
    except Exception:
        pass
    try:
        with _get_session() as session:
            row = (
                session.query(StrategyAsset.full_name)
                .filter(
                    StrategyAsset.symbol == symbol,
                    StrategyAsset.full_name.isnot(None),
                )
                .first()
            )
            if row and row[0]:
                return row[0]
    except Exception:
        pass
    return None


def _get_asset_class(symbol: str) -> str:
    """Return the asset class label for a symbol. Falls back to strategy_assets table, then other."""
    if symbol in _ASSET_CLASS_MAP:
        return _ASSET_CLASS_MAP[symbol]
    try:
        with _get_session() as session:
            row = (
                session.query(StrategyAsset.asset_class)
                .filter_by(symbol=symbol, is_active=1)
                .first()
            )
            if row:
                return row[0]
    except Exception:
        pass
    return "other"


DB_PATH = os.path.join(os.path.dirname(__file__), "trading_data.db")
DATABASE_URL = os.environ.get("TRADING_ENGINE_DB_URL", f"sqlite:///{DB_PATH}")

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_pre_ping=True,
    echo=False,
    connect_args={"check_same_thread": False}
    if DATABASE_URL.startswith("sqlite")
    else {},
)

SessionFactory = sessionmaker(bind=engine, expire_on_commit=False)


@event.listens_for(engine, "connect")
def _on_connect(dbapi_conn, connection_record):
    logger.info("[DB] New connection established (pool checkout)")
    if DATABASE_URL.startswith("sqlite"):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


@event.listens_for(engine, "checkout")
def _on_checkout(dbapi_conn, connection_record, connection_proxy):
    logger.debug("[DB] Connection checked out from pool")


@event.listens_for(engine, "checkin")
def _on_checkin(dbapi_conn, connection_record):
    logger.debug("[DB] Connection returned to pool")


@event.listens_for(SessionFactory, "after_begin")
def _after_begin(session, transaction, connection):
    logger.debug("[DB] Session transaction started")


@event.listens_for(SessionFactory, "after_commit")
def _after_commit(session):
    logger.debug("[DB] Session transaction committed")


@event.listens_for(SessionFactory, "after_rollback")
def _after_rollback(session):
    logger.warning("[DB] Session transaction rolled back")


def _get_session() -> Session:
    return SessionFactory()


def check_db_health() -> dict:
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()

        insp = inspect(engine)
        tables = insp.get_table_names()

        pool_status = {
            "pool_size": engine.pool.size(),
            "checked_in": engine.pool.checkedin(),
            "checked_out": engine.pool.checkedout(),
            "overflow": engine.pool.overflow(),
        }

        status = {
            "status": "healthy",
            "database_url": DATABASE_URL.split("///")[-1]
            if "sqlite" in DATABASE_URL
            else "(configured)",
            "tables": tables,
            "table_count": len(tables),
            "pool": pool_status,
        }
        logger.info(
            f"[DB] Health check passed: {len(tables)} tables, pool={pool_status}"
        )
        return status
    except Exception as e:
        logger.error(f"[DB] Health check FAILED: {e}")
        return {"status": "unhealthy", "error": str(e)}


def _migrate_schema():
    migrations = [
        ("signals", "exit_price", "REAL"),
        ("signals", "exit_reason", "TEXT"),
        ("signals", "asset_class", "TEXT DEFAULT 'other'"),
        ("admin_users", "role", "TEXT DEFAULT 'CUSTOMER'"),
        ("admin_users", "email", "TEXT"),
        ("admin_users", "full_name", "TEXT"),
        ("open_positions", "n_period_high_close", "REAL"),
        ("open_positions", "n_period_low_close", "REAL"),
        ("strategy_assets", "sub_category", "TEXT"),
        ("signals", "full_name", "TEXT"),
        ("strategy_assets", "full_name", "TEXT"),
    ]
    with engine.connect() as conn:
        for table, column, col_type in migrations:
            try:
                conn.execute(text(f"SELECT {column} FROM {table} LIMIT 1"))
            except Exception:
                logger.info(
                    f"[DB] MIGRATE: Adding column {table}.{column} ({col_type})"
                )
                conn.execute(
                    text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                )
                conn.commit()

        try:
            result = conn.execute(
                text("SELECT id FROM admin_users WHERE role IS NULL")
            ).fetchall()
            if result:
                conn.execute(
                    text("UPDATE admin_users SET role = 'ADMIN' WHERE role IS NULL")
                )
                conn.commit()
                logger.info(
                    f"[DB] MIGRATE: Set {len(result)} existing user(s) to ADMIN role"
                )
        except Exception:
            pass

        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_signal_status_ts ON signals(status, signal_timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_signal_strategy_asset_status ON signals(strategy_name, asset, status)",
        ]:
            try:
                conn.execute(text(idx_sql))
                conn.commit()
            except Exception:
                pass


def _purge_unsupported_symbols():
    from trading_engine.fcsapi_client import UNSUPPORTED_SYMBOLS

    if not UNSUPPORTED_SYMBOLS:
        return
    with _get_session() as session:
        try:
            for sym in UNSUPPORTED_SYMBOLS:
                c_del = session.query(Candle).filter_by(asset=sym).delete()
                s_del = session.query(Signal).filter(Signal.asset == sym).delete()
                p_del = (
                    session.query(OpenPosition)
                    .filter(OpenPosition.asset == sym)
                    .delete()
                )
                m_del = session.query(CacheMetadata).filter_by(asset=sym).delete()
                if c_del or s_del or p_del or m_del:
                    logger.info(
                        f"[DB] Purged unsupported symbol {sym}: "
                        f"candles={c_del}, signals={s_del}, positions={p_del}, cache={m_del}"
                    )
            session.commit()
        except Exception as e:
            session.rollback()
            logger.warning(f"[DB] Error purging unsupported symbols: {e}")


def purge_matic_usd():
    """Permanently remove MATIC/USD from all DB tables.
    Called on every startup — idempotent, no error if not found.
    """
    SYMBOL = "MATIC/USD"
    with _get_session() as session:
        try:
            # Step 1: Close any OPEN signals
            open_sigs = (
                session.query(Signal)
                .filter(
                    Signal.asset == SYMBOL,
                    Signal.status == "OPEN",
                )
                .all()
            )
            for sig in open_sigs:
                sig.status = "CLOSED"
                sig.exit_reason = (
                    "Symbol permanently removed — MATIC/USD not supported by FCSAPI"
                )
            if open_sigs:
                session.commit()
                logger.info(
                    f"[DB] purge_matic_usd: closed {len(open_sigs)} open signal(s)"
                )

            # Step 2: Delete open positions
            pos = (
                session.query(OpenPosition)
                .filter(OpenPosition.asset == SYMBOL)
                .delete(synchronize_session=False)
            )
            if pos:
                session.commit()
                logger.info(f"[DB] purge_matic_usd: deleted {pos} open position(s)")

            # Step 3: Hard delete all strategy_assets rows
            sa = (
                session.query(StrategyAsset)
                .filter(StrategyAsset.symbol == SYMBOL)
                .delete(synchronize_session=False)
            )
            if sa:
                session.commit()
                logger.info(f"[DB] purge_matic_usd: deleted {sa} strategy_asset row(s)")

            # Step 4: Delete all candles
            c = (
                session.query(Candle)
                .filter(Candle.asset == SYMBOL)
                .delete(synchronize_session=False)
            )
            if c:
                session.commit()
                logger.info(f"[DB] purge_matic_usd: deleted {c} candle row(s)")

            # Step 5: Delete cache metadata
            m = (
                session.query(CacheMetadata)
                .filter(CacheMetadata.asset == SYMBOL)
                .delete(synchronize_session=False)
            )
            if m:
                session.commit()
                logger.info(f"[DB] purge_matic_usd: deleted {m} cache metadata row(s)")

            logger.info(
                "[DB] purge_matic_usd: complete — "
                "MATIC/USD fully removed from all tables"
            )
            _invalidate_signal_cache()

        except Exception as e:
            session.rollback()
            logger.error(f"[DB] purge_matic_usd failed: {e}")


def _migrate_add_dji():
    """One-time migration: add DJI to mtf_ema if not already present.
    Safe to call on every startup — no-ops if DJI already exists.
    """
    with _get_session() as session:
        try:
            existing = (
                session.query(StrategyAsset)
                .filter_by(strategy_name="mtf_ema", symbol="DJI")
                .first()
            )
            if existing:
                return
            session.add(
                StrategyAsset(
                    strategy_name="mtf_ema",
                    symbol="DJI",
                    asset_class="forex",
                    sub_category="indices",
                    is_active=1,
                    fcsapi_verified=1,
                    added_by="system_seed",
                )
            )
            session.commit()
            logger.info("[DB] _migrate_add_dji: inserted DJI into mtf_ema")
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] _migrate_add_dji failed: {e}")


def close_stale_manual_signals():
    """One-time cleanup: close OPEN signals not from known strategies that are older than 7 days.

    Idempotent — safe to call on every startup.
    """
    KNOWN_STRATEGIES = {
        "mtf_ema",
        "trend_forex",
        "trend_non_forex",
        "sp500_momentum",
        "highest_lowest_fx",
        "trend_following",
    }
    cutoff = (datetime.utcnow() - timedelta(days=7)).isoformat()

    with _get_session() as session:
        try:
            stale = (
                session.query(Signal)
                .filter(
                    Signal.status == "OPEN",
                    Signal.created_at <= cutoff,
                    Signal.strategy_name.notin_(KNOWN_STRATEGIES),
                )
                .all()
            )

            if not stale:
                logger.info("[DB] close_stale_manual_signals: no stale signals found")
                return

            for sig in stale:
                sig.status = "CLOSED"
                sig.exit_reason = (
                    "Auto-closed: stale manual/AI signal predates strategy engine"
                )
                logger.info(
                    f"[DB] close_stale_manual_signals: closing signal #{sig.id} | "
                    f"asset={sig.asset} | direction={sig.direction} | "
                    f"strategy={sig.strategy_name} | created={sig.created_at}"
                )

            session.commit()
            logger.info(
                f"[DB] close_stale_manual_signals: closed {len(stale)} stale signal(s)"
            )
            _invalidate_signal_cache()

        except Exception as e:
            session.rollback()
            logger.error(f"[DB] close_stale_manual_signals failed: {e}")


def close_specific_stale_signals():
    """Direct close for confirmed stale signals identified in the Mar 17 audit.

    Keyed on (asset, direction, entry_price ±1%) to avoid touching valid signals.
    Idempotent — safe to call on every startup, no-op if already closed.
    """
    STALE_TARGETS = [
        ("XAG/USD", "SELL", 24.75, "AI signal Mar 4 — wrong price level"),
        ("USD/JPY", "SELL", 154.80, "AI signal Feb 22 — stale"),
        (
            "XAU/USD",
            "BUY",
            2340.50,
            "AI signal Feb 22 — stale, lower entry than engine",
        ),
        ("GBP/USD", "SELL", 1.29, "AI signal Mar 4 — stale"),
        ("EUR/USD", "SELL", 1.075, "AI signal Mar 2 — stale"),
        ("BTC/USD", "BUY", 67500.0, "AI signal Feb 22 — stale"),
    ]

    with _get_session() as session:
        try:
            closed = 0
            for asset, direction, entry_px, note in STALE_TARGETS:
                lower = entry_px * 0.99
                upper = entry_px * 1.01
                sigs = (
                    session.query(Signal)
                    .filter(
                        Signal.status == "OPEN",
                        Signal.asset == asset,
                        Signal.direction == direction,
                        Signal.entry_price >= lower,
                        Signal.entry_price <= upper,
                    )
                    .all()
                )
                for sig in sigs:
                    sig.status = "CLOSED"
                    sig.exit_reason = f"Auto-closed: {note}"
                    logger.info(
                        f"[DB] close_specific_stale_signals: closed #{sig.id} | "
                        f"{asset} {direction} @ {sig.entry_price} | {note}"
                    )
                    closed += 1
            session.commit()
            if closed:
                logger.info(
                    f"[DB] close_specific_stale_signals: closed {closed} signal(s)"
                )
                _invalidate_signal_cache()
            else:
                logger.info(
                    "[DB] close_specific_stale_signals: no matching stale signals found "
                    "(already closed or not present)"
                )
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] close_specific_stale_signals failed: {e}")


def close_stale_mtf_ema_longs(h4_ema50_map: Optional[dict] = None) -> None:
    """Close any OPEN MTF EMA BUY positions where the H1 close has already
    crossed below H4 EMA50 (the exit trigger) but check_exits() was never
    called so the position stayed open.

    h4_ema50_map: optional dict of {asset: h4_ema50_value} for assets
    confirmed to have their exit trigger fired.  If None, uses hardcoded
    values from the Mar 19 2026 audit.

    Idempotent — safe to run on every startup, no-op if already closed.
    """
    if h4_ema50_map is None:
        # Confirmed from admin panel data Mar 19 2026
        # H1 close < H4 EMA50 for these assets → exit triggered
        h4_ema50_map = {
            "EUR/USD": 1.15349,  # H1=1.14668 confirmed below
            "GBP/USD": 1.33402,  # H1=1.32636 confirmed below
        }

    STRATEGY = "mtf_ema"

    with _get_session() as session:
        try:
            total_sigs = 0
            total_pos = 0

            for asset, h4_ema50 in h4_ema50_map.items():
                # Close open LONG signals
                sigs = (
                    session.query(Signal)
                    .filter(
                        Signal.asset == asset,
                        Signal.direction == "BUY",
                        Signal.status == "OPEN",
                        Signal.strategy_name == STRATEGY,
                    )
                    .all()
                )

                for sig in sigs:
                    sig.status = "CLOSED"
                    sig.exit_reason = (
                        f"Auto-closed: H1 close crossed below "
                        f"H4 EMA50 ({h4_ema50}) — "
                        f"check_exits() was not running for mtf_ema"
                    )
                    logger.info(
                        f"[DB] close_stale_mtf_ema_longs: closed "
                        f"signal #{sig.id} | {asset} BUY @ "
                        f"{sig.entry_price} | strategy={sig.strategy_name}"
                    )
                    total_sigs += 1

                # Delete open position records
                pos_rows = (
                    session.query(OpenPosition)
                    .filter(
                        OpenPosition.asset == asset,
                        OpenPosition.direction == "BUY",
                        OpenPosition.strategy_name == STRATEGY,
                    )
                    .all()
                )

                for p in pos_rows:
                    logger.info(
                        f"[DB] close_stale_mtf_ema_longs: deleting "
                        f"position #{p.id} | {asset} | "
                        f"strategy={p.strategy_name}"
                    )
                    session.delete(p)
                    total_pos += 1

            session.commit()

            if total_sigs or total_pos:
                logger.info(
                    f"[DB] close_stale_mtf_ema_longs: closed "
                    f"{total_sigs} signal(s) and {total_pos} position(s)"
                )
                _invalidate_signal_cache()
            else:
                logger.info(
                    "[DB] close_stale_mtf_ema_longs: "
                    "no stale positions found (already clean)"
                )

        except Exception as e:
            session.rollback()
            logger.error(f"[DB] close_stale_mtf_ema_longs failed: {e}")


def has_any_open_signal_for_asset(
    asset: str,
    exclude_strategies: Optional[list[str]] = None,
) -> bool:
    """Return True if ANY open signal exists for this asset, regardless of strategy or direction.

    Use as the primary cross-strategy idempotency guard to prevent a second strategy
    from opening a position on an asset that already has one open (e.g. mtf_ema BUY
    while a manual SELL is still OPEN in the DB).

    exclude_strategies: when provided, signals from those strategies are ignored in the
    cross-strategy check, allowing known non-conflicting partner strategies to coexist
    (e.g. mtf_ema and sp500_momentum can both hold open signals on SPX simultaneously).
    """
    with _get_session() as session:
        q = session.query(
            Signal.id,
            Signal.strategy_name,
            Signal.direction,
        ).filter(
            Signal.asset == asset,
            Signal.status == "OPEN",
        )
        if exclude_strategies:
            q = q.filter(Signal.strategy_name.notin_(exclude_strategies))
        row = q.first()
        if row:
            logger.debug(
                f"[DB] has_any_open_signal_for_asset({asset}): "
                f"blocked by signal #{row[0]} | "
                f"strategy={row[1]} | direction={row[2]}"
            )
            return True
        return False


def close_opposite_signal_if_exists(
    strategy_name: str,
    asset: str,
    new_direction: str,
) -> bool:
    """If the strategy has an open signal for this asset in the OPPOSITE direction
    to new_direction, close it and delete the open position so the new signal can
    be inserted cleanly.

    Called by each strategy's evaluate() BEFORE insert_signal() when a new entry
    is about to fire.

    Returns True if an opposite signal was found and closed, False if no opposite
    signal existed (normal case).

    IMPORTANT: only closes signals from THIS strategy. Never touches signals owned
    by other strategies.
    """
    opposite = "SELL" if new_direction == "BUY" else "BUY"

    with _get_session() as session:
        try:
            sig = (
                session.query(Signal)
                .filter(
                    Signal.strategy_name == strategy_name,
                    Signal.asset == asset,
                    Signal.direction == opposite,
                    Signal.status == "OPEN",
                )
                .order_by(Signal.id.desc())
                .first()
            )

            if not sig:
                return False

            sig.status = "CLOSED"
            sig.exit_reason = (
                f"Closed by new {new_direction} signal for same asset "
                f"(direction flip: {opposite} → {new_direction})"
            )
            logger.info(
                f"[DB] close_opposite_signal_if_exists: closed signal #{sig.id} | "
                f"asset={asset} | strategy={strategy_name} | "
                f"old={opposite} → new={new_direction}"
            )

            # Also remove the open position so the new entry can create a fresh one
            pos = (
                session.query(OpenPosition)
                .filter(
                    OpenPosition.asset == asset,
                    OpenPosition.strategy_name == strategy_name,
                )
                .first()
            )
            if pos:
                logger.info(
                    f"[DB] close_opposite_signal_if_exists: "
                    f"deleted position #{pos.id} | "
                    f"asset={asset} | strategy={strategy_name}"
                )
                session.delete(pos)

            session.commit()
            _invalidate_signal_cache()
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"[DB] close_opposite_signal_if_exists failed: {e}")
            return False


def _backfill_asset_class():
    """One-time backfill: set asset_class on existing signals that predate the column.

    Safe to run on every startup — only updates rows where asset_class is NULL or
    'other' and the asset has a known mapping in _ASSET_CLASS_MAP.
    """
    with _get_session() as session:
        try:
            rows = (
                session.query(Signal)
                .filter(
                    Signal.asset_class.in_(
                        [
                            None,
                            "other",
                            "stocks",
                            "commodities",
                            "indices",
                        ]  # old category names
                    )
                )
                .all()
            )
            updated = 0
            for sig in rows:
                mapped = _ASSET_CLASS_MAP.get(sig.asset)
                if mapped and mapped != "other":
                    sig.asset_class = mapped
                    updated += 1
            if updated:
                session.commit()
                logger.info(
                    f"[DB] Backfilled asset_class for {updated} existing signal(s)"
                )
            else:
                logger.info("[DB] asset_class backfill: all signals already classified")
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] asset_class backfill failed: {e}")


def _backfill_full_names():
    """Startup pass: fill full_name where NULL using the static map + NASDAQ names.

    Updates both strategy_assets and signals tables. Safe to run repeatedly —
    only touches rows where full_name IS NULL and the symbol exists in the map.
    Covers:
      1. _ASSET_NAME_MAP  — forex, crypto, ETFs, commodities, indices
      2. _NDX100_COMPANY_NAMES (from nasdaq_sync) — NASDAQ 100 stocks
    """
    # Build combined name map: static map + NASDAQ stock names
    combined_map = dict(_ASSET_NAME_MAP)
    try:
        from trading_engine.utils.nasdaq_sync import _NDX100_COMPANY_NAMES
        combined_map.update(_NDX100_COMPANY_NAMES)
    except Exception:
        pass

    updated_assets = updated_signals = 0
    with engine.connect() as conn:
        for symbol, name in combined_map.items():
            safe_name = name.replace("'", "''")
            safe_sym = symbol.replace("'", "''")
            res = conn.execute(
                text(
                    f"UPDATE strategy_assets SET full_name='{safe_name}' "
                    f"WHERE symbol='{safe_sym}' AND (full_name IS NULL OR full_name='')"
                )
            )
            updated_assets += res.rowcount
            res2 = conn.execute(
                text(
                    f"UPDATE signals SET full_name='{safe_name}' "
                    f"WHERE asset='{safe_sym}' AND (full_name IS NULL OR full_name='')"
                )
            )
            updated_signals += res2.rowcount
        conn.commit()
    if updated_assets or updated_signals:
        logger.info(
            f"[DB] Backfilled full_name: {updated_assets} strategy_assets, "
            f"{updated_signals} signals"
        )


def init_db():
    logger.info("[DB] Initializing database tables via SQLAlchemy...")
    Base.metadata.create_all(engine)
    logger.info("[DB] All tables created/verified")

    _migrate_schema()
    _backfill_full_names()
    _backfill_asset_class()
    seed_strategy_assets()
    purge_matic_usd()
    _migrate_add_dji()

    with _get_session() as session:
        _seed_default_admin(session)

    _purge_unsupported_symbols()
    close_stale_manual_signals()
    close_specific_stale_signals()
    close_stale_mtf_ema_longs()

    health = check_db_health()
    logger.info(f"[DB] Startup health check: {health['status']}")


def upsert_candles(asset: str, timeframe: str, candles: list[dict]):
    with _get_session() as session:
        try:
            for c in candles:
                ts = c.get("timestamp") or c.get("open_time")
                existing = (
                    session.query(Candle)
                    .filter_by(asset=asset, timeframe=timeframe, timestamp=ts)
                    .first()
                )
                if existing:
                    existing.open = c["open"]
                    existing.high = c["high"]
                    existing.low = c["low"]
                    existing.close = c["close"]
                else:
                    session.add(
                        Candle(
                            asset=asset,
                            timeframe=timeframe,
                            timestamp=ts,
                            open=c["open"],
                            high=c["high"],
                            low=c["low"],
                            close=c["close"],
                        )
                    )
            session.commit()
            logger.debug(
                f"[DB] Upserted {len(candles)} candles for {asset}/{timeframe}"
            )
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Upsert candles failed: {e}")
            raise


def get_candles(asset: str, timeframe: str, limit: int = 300) -> list[dict]:
    with _get_session() as session:
        rows = (
            session.query(Candle)
            .filter_by(asset=asset, timeframe=timeframe)
            .order_by(Candle.timestamp.desc())
            .limit(limit)
            .all()
        )
        return [
            {
                "timestamp": r.timestamp,
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
            }
            for r in reversed(rows)
        ]


def update_cache_metadata(
    asset: str, timeframe: str, last_candle_close: Optional[str] = None
):
    now = datetime.utcnow().isoformat()
    with _get_session() as session:
        try:
            existing = (
                session.query(CacheMetadata)
                .filter_by(asset=asset, timeframe=timeframe)
                .first()
            )
            if existing:
                existing.last_fetched = now
                if last_candle_close is not None:
                    existing.last_candle_close = last_candle_close
            else:
                session.add(
                    CacheMetadata(
                        asset=asset,
                        timeframe=timeframe,
                        last_fetched=now,
                        last_candle_close=last_candle_close,
                    )
                )
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Update cache metadata failed: {e}")
            raise


def get_cache_metadata(asset: str, timeframe: str) -> Optional[dict]:
    with _get_session() as session:
        row = (
            session.query(CacheMetadata)
            .filter_by(asset=asset, timeframe=timeframe)
            .first()
        )
        if row:
            return {
                "last_fetched": row.last_fetched,
                "last_candle_close": row.last_candle_close,
            }
        return None


def signal_exists(strategy_name: str, asset: str, signal_timestamp: str) -> bool:
    with _get_session() as session:
        row = (
            session.query(Signal.id)
            .filter_by(
                strategy_name=strategy_name,
                asset=asset,
                signal_timestamp=signal_timestamp,
            )
            .first()
        )
        return row is not None


def has_open_signal(strategy_name: str, asset: str) -> bool:
    with _get_session() as session:
        row = (
            session.query(Signal.id)
            .filter_by(
                strategy_name=strategy_name,
                asset=asset,
                status="OPEN",
            )
            .first()
        )
        return row is not None


def insert_signal(signal: dict) -> Optional[int]:
    with _get_session() as session:
        try:
            existing = (
                session.query(Signal.id)
                .filter_by(
                    strategy_name=signal["strategy_name"],
                    asset=signal["asset"],
                    signal_timestamp=signal["signal_timestamp"],
                )
                .first()
            )
            if existing:
                logger.warning(
                    f"[DB] IDEMPOTENCY | Signal already exists for "
                    f"{signal['strategy_name']}/{signal['asset']}/{signal['signal_timestamp']} "
                    f"(id={existing[0]}) — insert blocked"
                )
                return None

            # Resolve full_name: caller may pass it; fall back to static map
            # then to strategy_assets DB row — all within the same session.
            full_name = signal.get("full_name") or _ASSET_NAME_MAP.get(signal["asset"])
            if not full_name:
                asset_row = (
                    session.query(StrategyAsset.full_name)
                    .filter(
                        StrategyAsset.symbol == signal["asset"],
                        StrategyAsset.full_name.isnot(None),
                    )
                    .first()
                )
                if asset_row and asset_row[0]:
                    full_name = asset_row[0]

            obj = Signal(
                strategy_name=signal["strategy_name"],
                asset=signal["asset"],
                direction=signal["direction"],
                entry_price=signal["entry_price"],
                stop_loss=signal.get("stop_loss"),
                take_profit=signal.get("take_profit"),
                atr_at_entry=signal.get("atr_at_entry"),
                signal_timestamp=signal["signal_timestamp"],
                status="OPEN",
                asset_class=_get_asset_class(signal["asset"]),
                full_name=full_name,
            )
            session.add(obj)
            session.commit()
            logger.info(
                f"[DB] Inserted signal #{obj.id}: {signal['strategy_name']} {signal['direction']} {signal['asset']}"
            )
            _invalidate_signal_cache()
            broadcast_data = {**signal, "id": obj.id, "status": "OPEN"}
            if obj.created_at:
                broadcast_data["created_at"] = obj.created_at.isoformat()
            _ws_broadcast_new(broadcast_data)
            return obj.id
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Insert signal failed: {e}")
            return None


def get_active_signals(
    strategy_name: Optional[str] = None, asset: Optional[str] = None
) -> list[dict]:
    """Return all OPEN signals, deduplicated to max(id) per (strategy_name, asset).

    Deduplication contract: only one OPEN signal per (strategy_name + asset) is
    ever returned. If duplicates exist in the DB (e.g. from a race condition before
    the idempotency guard fires), only the row with the highest id is returned.
    """
    with _get_session() as session:
        from sqlalchemy import func

        latest_open_subq = session.query(func.max(Signal.id).label("max_id")).filter(
            Signal.status == "OPEN"
        )
        if strategy_name:
            latest_open_subq = latest_open_subq.filter(
                Signal.strategy_name == strategy_name
            )
        if asset:
            latest_open_subq = latest_open_subq.filter(Signal.asset == asset)
        latest_open_subq = latest_open_subq.group_by(
            Signal.strategy_name, Signal.asset
        ).subquery()

        q = (
            session.query(Signal)
            .filter(Signal.id.in_(session.query(latest_open_subq.c.max_id)))
            .order_by(Signal.created_at.desc())
        )
        rows = q.all()
        all_sigs = [_signal_to_dict(r) for r in rows]

        # Asset-level dedup: one OPEN signal per asset across ALL strategies (highest id wins)
        seen_assets: dict[str, dict] = {}
        for sig in all_sigs:
            asset_key = sig["asset"]
            if asset_key not in seen_assets:
                seen_assets[asset_key] = sig
            elif sig["id"] > seen_assets[asset_key]["id"]:
                seen_assets[asset_key] = sig

        if len(seen_assets) < len(all_sigs):
            logger.warning(
                f"[DB] get_active_signals: deduplicated {len(all_sigs)} rows → "
                f"{len(seen_assets)} unique assets "
                f"(suppressed {len(all_sigs) - len(seen_assets)} duplicate(s))"
            )

        return list(seen_assets.values())


def close_signal(
    signal_id: int, exit_reason: str = "", exit_price: Optional[float] = None
):
    with _get_session() as session:
        try:
            sig = session.query(Signal).filter_by(id=signal_id).first()
            if sig:
                sig.status = "CLOSED"
                sig.exit_reason = exit_reason or None
                if exit_price is not None:
                    sig.exit_price = exit_price
                session.commit()
                logger.info(
                    f"[DB] Closed signal #{signal_id}: {exit_reason} | exit_price={exit_price}"
                )
                _invalidate_signal_cache()
                _ws_broadcast_closed(signal_id, exit_reason, exit_price)
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Close signal failed: {e}")
            raise


def get_signal_by_id(signal_id: int) -> Optional[dict]:
    with _get_session() as session:
        sig = session.query(Signal).filter_by(id=signal_id).first()
        if sig:
            return _signal_to_dict(sig)
        return None


def update_signal_wp_fields(signal_id: int, fields: dict):
    with _get_session() as session:
        try:
            sig = session.query(Signal).filter_by(id=signal_id).first()
            if not sig:
                logger.warning(
                    f"[DB] update_signal_wp_fields: Signal #{signal_id} not found"
                )
                return
            for key in ("wp_post_id", "publish_status", "wp_last_sync"):
                if key in fields:
                    setattr(sig, key, fields[key])
            session.commit()
            logger.info(f"[DB] Updated WP fields for signal #{signal_id}: {fields}")
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] update_signal_wp_fields failed for #{signal_id}: {e}")


def get_all_signals(
    strategy_name: Optional[str] = None,
    asset: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
    max_age_days: Optional[int] = None,
) -> list[dict]:
    """Return signals with deduplication and optional age filtering.

    Deduplication contract (must never be weakened):
    - OPEN signals: only the row with the highest id per (strategy_name, asset) is returned.
      Rationale: the idempotency guard in evaluate() prevents duplicates, but a DB-level
      guard ensures correctness if a race condition slips through.
    - CLOSED signals: only the row with the highest id per (strategy_name, asset, direction)
      is returned. This suppresses historical re-opens for the same asset+direction pair.
    - Combined (no status filter): OPEN dedup UNION CLOSED dedup, ordered by created_at DESC.

    max_age_days:
    - When set, CLOSED signals with signal_timestamp older than this many days are excluded.
    - OPEN signals are always included regardless of age.
    - Default is None (no age filter) to preserve backward compatibility with existing callers.
    """
    with _get_session() as session:
        from sqlalchemy import func, or_, text as sa_text

        latest_closed_subq = session.query(func.max(Signal.id).label("max_id")).filter(
            Signal.status == "CLOSED"
        )
        if strategy_name:
            latest_closed_subq = latest_closed_subq.filter(
                Signal.strategy_name == strategy_name
            )
        if asset:
            latest_closed_subq = latest_closed_subq.filter(Signal.asset == asset)
        if max_age_days is not None:
            latest_closed_subq = latest_closed_subq.filter(
                Signal.signal_timestamp
                >= sa_text(f"datetime('now', '-{int(max_age_days)} days')")
            )
        latest_closed_subq = latest_closed_subq.group_by(
            Signal.strategy_name, Signal.asset, Signal.direction
        ).subquery()

        latest_open_subq = session.query(func.max(Signal.id).label("max_id")).filter(
            Signal.status == "OPEN"
        )
        if strategy_name:
            latest_open_subq = latest_open_subq.filter(
                Signal.strategy_name == strategy_name
            )
        if asset:
            latest_open_subq = latest_open_subq.filter(Signal.asset == asset)
        latest_open_subq = latest_open_subq.group_by(
            Signal.strategy_name, Signal.asset
        ).subquery()

        q = session.query(Signal)
        if strategy_name:
            q = q.filter(Signal.strategy_name == strategy_name)
        if asset:
            q = q.filter(Signal.asset == asset)

        if status == "CLOSED":
            q = q.filter(Signal.id.in_(session.query(latest_closed_subq.c.max_id)))
        elif status == "OPEN":
            q = q.filter(Signal.id.in_(session.query(latest_open_subq.c.max_id)))
        else:
            q = q.filter(
                or_(
                    Signal.id.in_(session.query(latest_open_subq.c.max_id)),
                    Signal.id.in_(session.query(latest_closed_subq.c.max_id)),
                )
            )

        q = q.order_by(Signal.created_at.desc()).limit(limit)
        rows = q.all()
        raw = [_signal_to_dict(r) for r in rows]

        # OPEN signals: one per asset across all strategies (highest id wins)
        open_seen: dict[str, dict] = {}
        closed_rows = []
        for sig in raw:
            if sig["status"] == "OPEN":
                key = sig["asset"]
                if key not in open_seen:
                    open_seen[key] = sig
                elif sig["id"] > open_seen[key]["id"]:
                    open_seen[key] = sig
            else:
                closed_rows.append(sig)

        result = list(open_seen.values()) + closed_rows
        result.sort(key=lambda s: s.get("created_at") or "", reverse=True)
        return result[:limit]


def purge_old_closed_signals(days: int = 92) -> dict:
    """Hard-delete CLOSED signals older than `days` days and their associated CMS posts.

    Uses signal_timestamp for age comparison (not created_at).
    Returns: {"deleted_signals": N, "deleted_cms_posts": M, "cutoff_date": "YYYY-MM-DD"}
    """
    from datetime import timedelta, date

    cutoff = (date.today() - timedelta(days=days)).isoformat()

    with _get_session() as session:
        try:
            old_signals = (
                session.query(Signal.id)
                .filter(Signal.status == "CLOSED")
                .filter(Signal.signal_timestamp < cutoff)
                .all()
            )
            old_ids = [row[0] for row in old_signals]

            deleted_cms = 0
            if old_ids:
                from trading_engine.models import SignalCmsPost

                deleted_cms = (
                    session.query(SignalCmsPost)
                    .filter(SignalCmsPost.signal_id.in_(old_ids))
                    .delete(synchronize_session=False)
                )
                deleted_sigs = (
                    session.query(Signal)
                    .filter(Signal.id.in_(old_ids))
                    .delete(synchronize_session=False)
                )
            else:
                deleted_sigs = 0

            session.commit()
            result = {
                "deleted_signals": deleted_sigs,
                "deleted_cms_posts": deleted_cms,
                "cutoff_date": cutoff,
            }
            logger.info(
                f"[DB] PURGE: Deleted {deleted_sigs} closed signal(s) and {deleted_cms} CMS post(s) "
                f"older than {days} days (cutoff={cutoff})"
            )
            _invalidate_signal_cache()
            return result
        except Exception as e:
            session.rollback()
            logger.error(
                f"[DB] PURGE: Failed to purge old closed signals: {e}", exc_info=True
            )
            return {
                "deleted_signals": 0,
                "deleted_cms_posts": 0,
                "cutoff_date": cutoff,
                "error": str(e),
            }


def _signal_to_dict(sig: Signal) -> dict:
    return {
        "id": sig.id,
        "asset": sig.asset,
        "full_name": sig.full_name,
        "asset_class": sig.asset_class or _get_asset_class(sig.asset),
        "strategy_name": sig.strategy_name,
        "direction": sig.direction,
        "entry_price": sig.entry_price,
        "stop_loss": sig.stop_loss,
        "take_profit": sig.take_profit,
        "atr_at_entry": sig.atr_at_entry,
        "status": sig.status,
        "exit_price": sig.exit_price,
        "exit_reason": sig.exit_reason,
        "signal_timestamp": sig.signal_timestamp,
        "created_at": sig.created_at,
        "updated_at": sig.updated_at,
        "wp_post_id": sig.wp_post_id,
        "publish_status": sig.publish_status,
        "wp_last_sync": sig.wp_last_sync,
    }


def update_signal_stop_loss(signal_id: int, new_stop_loss: float):
    """Update the stop_loss on an open signal. Used by dynamic trailing stop strategies."""
    with _get_session() as session:
        try:
            sig = session.query(Signal).filter_by(id=signal_id, status="OPEN").first()
            if sig:
                sig.stop_loss = new_stop_loss
                session.commit()
                logger.debug(
                    f"[DB] Updated stop_loss for signal #{signal_id}: {new_stop_loss}"
                )
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] update_signal_stop_loss failed for #{signal_id}: {e}")


def open_position(position: dict) -> Optional[int]:
    atr_value = position.get("atr_at_entry")
    # atr_at_entry is optional — trend_non_forex uses dynamic ATR, other strategies store it.
    # Only reject if explicitly provided but invalid (not if simply absent).
    if atr_value is not None and atr_value <= 0:
        logger.error(
            f"[DB] open_position() called with invalid atr_at_entry={atr_value} "
            f"for {position.get('asset')}/{position.get('strategy_name')} — rejected"
        )
        return None

    with _get_session() as session:
        try:
            existing = (
                session.query(OpenPosition)
                .filter_by(
                    asset=position["asset"],
                    strategy_name=position["strategy_name"],
                )
                .first()
            )
            if existing:
                logger.warning(
                    f"[DB] Open position already exists for {position['asset']}/{position['strategy_name']} "
                    f"(id={existing.id}) - only one allowed per asset+strategy"
                )
                return existing.id

            obj = OpenPosition(
                asset=position["asset"],
                strategy_name=position["strategy_name"],
                direction=position["direction"],
                entry_price=position["entry_price"],
                atr_at_entry=atr_value,
                highest_price_since_entry=position["entry_price"]
                if position["direction"] == "BUY"
                else None,
                lowest_price_since_entry=position["entry_price"]
                if position["direction"] == "SELL"
                else None,
            )
            session.add(obj)
            session.commit()
            logger.info(
                f"[DB] Opened position #{obj.id}: {position['strategy_name']} {position['direction']} {position['asset']}"
            )
            return obj.id
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Open position failed: {e}")
            return None


def get_open_position(strategy_name: str, asset: str) -> Optional[dict]:
    with _get_session() as session:
        pos = (
            session.query(OpenPosition)
            .filter_by(asset=asset, strategy_name=strategy_name)
            .first()
        )
        if pos:
            return _position_to_dict(pos)
        return None


def get_all_open_positions(
    strategy_name: Optional[str] = None, asset: Optional[str] = None
) -> list[dict]:
    with _get_session() as session:
        q = session.query(OpenPosition)
        if strategy_name:
            q = q.filter(OpenPosition.strategy_name == strategy_name)
        if asset:
            q = q.filter(OpenPosition.asset == asset)
        q = q.order_by(OpenPosition.opened_at.desc())
        rows = q.all()
        return [_position_to_dict(r) for r in rows]


def update_position_tracking(
    position_id: int,
    highest_price: Optional[float] = None,
    lowest_price: Optional[float] = None,
):
    with _get_session() as session:
        try:
            pos = session.query(OpenPosition).filter_by(id=position_id).first()
            if not pos:
                return
            if highest_price is not None:
                pos.highest_price_since_entry = max(
                    pos.highest_price_since_entry or 0, highest_price
                )
            if lowest_price is not None:
                pos.lowest_price_since_entry = min(
                    pos.lowest_price_since_entry or 999999, lowest_price
                )
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Update position tracking failed: {e}")
            raise


def close_position(strategy_name: str, asset: str) -> bool:
    with _get_session() as session:
        try:
            pos = (
                session.query(OpenPosition)
                .filter_by(asset=asset, strategy_name=strategy_name)
                .first()
            )
            if pos:
                session.delete(pos)
                session.commit()
                logger.info(f"[DB] Closed position: {strategy_name} {asset}")
                return True
            return False
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Close position failed: {e}")
            return False


def has_open_position(strategy_name: str, asset: str) -> bool:
    with _get_session() as session:
        pos = (
            session.query(OpenPosition.id)
            .filter_by(asset=asset, strategy_name=strategy_name)
            .first()
        )
        return pos is not None


def _position_to_dict(pos: OpenPosition) -> dict:
    return {
        "id": pos.id,
        "asset": pos.asset,
        "strategy_name": pos.strategy_name,
        "direction": pos.direction,
        "entry_price": pos.entry_price,
        "atr_at_entry": pos.atr_at_entry,
        "highest_price_since_entry": pos.highest_price_since_entry,
        "lowest_price_since_entry": pos.lowest_price_since_entry,
        "opened_at": pos.opened_at,
    }


def get_setting(key: str) -> Optional[str]:
    with _get_session() as session:
        row = session.query(AppSetting).filter_by(key=key).first()
        return row.value if row else None


def set_setting(key: str, value: str):
    with _get_session() as session:
        try:
            existing = session.query(AppSetting).filter_by(key=key).first()
            if existing:
                existing.value = value
            else:
                session.add(AppSetting(key=key, value=value))
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Set setting failed: {e}")
            raise


def log_api_usage(endpoint: str, credits_used: int = 1):
    with _get_session() as session:
        try:
            session.add(
                APIUsageLog(
                    endpoint=endpoint,
                    credits_used=credits_used,
                )
            )
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Log API usage failed: {e}")


def get_api_usage_stats() -> dict:
    import calendar

    with _get_session() as session:
        now = datetime.utcnow()
        month_start = now.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        ).isoformat()
        day_ago = (now - timedelta(days=1)).isoformat()

        monthly_total = (
            session.execute(
                text(
                    "SELECT COALESCE(SUM(credits_used), 0) as total FROM api_usage_log WHERE timestamp >= :start"
                ),
                {"start": month_start},
            ).scalar()
            or 0
        )

        daily_total = (
            session.execute(
                text(
                    "SELECT COALESCE(SUM(credits_used), 0) as total FROM api_usage_log WHERE timestamp >= :start"
                ),
                {"start": day_ago},
            ).scalar()
            or 0
        )

        by_endpoint_rows = session.execute(
            text("""
                SELECT endpoint, COUNT(*) as count, SUM(credits_used) as credits
                FROM api_usage_log WHERE timestamp >= :start
                GROUP BY endpoint ORDER BY credits DESC
            """),
            {"start": month_start},
        ).fetchall()
        by_endpoint = [
            {"endpoint": r[0], "count": r[1], "credits": r[2]} for r in by_endpoint_rows
        ]

        thirty_days_ago = (now - timedelta(days=30)).isoformat()
        daily_rows = session.execute(
            text("""
                SELECT date(timestamp) as day, SUM(credits_used) as credits
                FROM api_usage_log WHERE timestamp >= :start
                GROUP BY date(timestamp) ORDER BY day DESC LIMIT 30
            """),
            {"start": thirty_days_ago},
        ).fetchall()
        daily_history = [{"day": r[0], "credits": r[1]} for r in daily_rows]

    monthly_limit = 500000
    usage_pct = (monthly_total / monthly_limit) * 100 if monthly_limit > 0 else 0

    day_of_month = now.day
    total_days = calendar.monthrange(now.year, now.month)[1]
    daily_rate = monthly_total / max(day_of_month, 1)
    projected_eom = daily_rate * total_days
    projected_pct = (projected_eom / monthly_limit) * 100 if monthly_limit > 0 else 0

    alert_level = None
    if usage_pct >= 99:
        alert_level = "kill_switch"
    elif projected_pct >= 90:
        alert_level = "critical"
    elif projected_pct >= 60:
        alert_level = "warning"

    return {
        "monthly_total": monthly_total,
        "monthly_limit": monthly_limit,
        "usage_percentage": round(usage_pct, 2),
        "daily_total": daily_total,
        "daily_rate": round(daily_rate, 2),
        "projected_eom": round(projected_eom, 0),
        "projected_pct": round(projected_pct, 2),
        "days_passed": day_of_month,
        "days_in_month": total_days,
        "alert_level": alert_level,
        "by_endpoint": by_endpoint,
        "daily_history": daily_history,
    }


def get_candle_count(asset: str, timeframe: str) -> int:
    with _get_session() as session:
        return session.query(Candle).filter_by(asset=asset, timeframe=timeframe).count()


def _hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100000).hex()
    return f"{salt}:{h}"


def _verify_password(password: str, stored_hash: str) -> bool:
    try:
        salt, h = stored_hash.split(":")
        computed = hashlib.pbkdf2_hmac(
            "sha256", password.encode(), salt.encode(), 100000
        ).hex()
        return secrets.compare_digest(computed, h)
    except (ValueError, AttributeError):
        return False


def _seed_default_admin(session: Session):
    count = session.query(AdminUser).count()
    if count == 0:
        pw_hash = _hash_password("pass123")
        session.add(AdminUser(username="admin", password_hash=pw_hash, role="ADMIN"))
        session.commit()
        logger.info("[DB] Seeded default admin user")


def authenticate_admin(username: str, password: str) -> Optional[dict]:
    with _get_session() as session:
        user = session.query(AdminUser).filter_by(username=username).first()
        if user and _verify_password(password, user.password_hash):
            return {
                "id": user.id,
                "username": user.username,
                "role": user.role or "CUSTOMER",
                "created_at": user.created_at,
            }
        return None


def create_session(user_id: int) -> str:
    token = secrets.token_urlsafe(32)
    expires = (datetime.utcnow() + timedelta(hours=24)).isoformat()
    with _get_session() as session:
        try:
            session.add(AdminSession(token=token, user_id=user_id, expires_at=expires))
            session.commit()
            logger.info(f"[DB] Created admin session for user_id={user_id}")
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Create session failed: {e}")
            raise
    return token


def validate_session(token: str) -> Optional[dict]:
    if not token:
        return None
    with _get_session() as session:
        now_iso = datetime.utcnow().isoformat()
        row = session.execute(
            text("""
                SELECT s.id, s.token, s.user_id, s.expires_at, s.created_at,
                       u.username, u.role
                FROM admin_sessions s
                JOIN admin_users u ON s.user_id = u.id
                WHERE s.token = :token AND s.expires_at > :now
            """),
            {"token": token, "now": now_iso},
        ).fetchone()
        if row:
            return {
                "id": row[0],
                "token": row[1],
                "user_id": row[2],
                "expires_at": row[3],
                "created_at": row[4],
                "username": row[5],
                "role": row[6] or "CUSTOMER",
            }
        return None


def delete_session(token: str):
    with _get_session() as session:
        try:
            session.execute(
                text("DELETE FROM admin_sessions WHERE token = :token"),
                {"token": token},
            )
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Delete session failed: {e}")


def cleanup_expired_sessions():
    with _get_session() as session:
        try:
            now_iso = datetime.utcnow().isoformat()
            result = session.execute(
                text("DELETE FROM admin_sessions WHERE expires_at <= :now"),
                {"now": now_iso},
            )
            session.commit()
            if result.rowcount > 0:
                logger.info(f"[DB] Cleaned up {result.rowcount} expired sessions")
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Cleanup sessions failed: {e}")


def get_all_admins() -> list[dict]:
    with _get_session() as session:
        rows = session.query(AdminUser).order_by(AdminUser.id).all()
        return [
            {
                "id": r.id,
                "username": r.username,
                "role": r.role or "CUSTOMER",
                "created_at": r.created_at,
            }
            for r in rows
        ]


def get_user_by_username(username: str) -> Optional[dict]:
    with _get_session() as session:
        user = session.query(AdminUser).filter_by(username=username).first()
        if user:
            return {
                "id": user.id,
                "username": user.username,
                "email": user.email,
                "role": user.role or "CUSTOMER",
                "created_at": user.created_at,
            }
        return None


def get_user_by_email(email: str) -> Optional[dict]:
    with _get_session() as session:
        user = session.query(AdminUser).filter(AdminUser.email == email).first()
        if user:
            return {
                "id": user.id,
                "username": user.username,
                "email": user.email,
                "role": user.role or "CUSTOMER",
                "created_at": user.created_at,
            }
        return None


def create_admin(
    username: str,
    password: str,
    email: Optional[str] = None,
    full_name: Optional[str] = None,
    role: str = "CUSTOMER",
) -> Optional[int]:
    pw_hash = _hash_password(password)
    with _get_session() as session:
        try:
            user = AdminUser(
                username=username,
                password_hash=pw_hash,
                email=email,
                full_name=full_name,
                role=role,
            )
            session.add(user)
            session.commit()
            logger.info(f"[DB] Created user: {username} (role={role})")
            return user.id
        except Exception:
            session.rollback()
            return None


def update_admin(
    admin_id: int,
    username: Optional[str] = None,
    password: Optional[str] = None,
    role: Optional[str] = None,
) -> bool:
    with _get_session() as session:
        try:
            user = session.query(AdminUser).filter_by(id=admin_id).first()
            if not user:
                return False
            if username:
                user.username = username
            if password:
                user.password_hash = _hash_password(password)
            if role and role in ("ADMIN", "CUSTOMER"):
                user.role = role
            session.commit()
            return True
        except Exception:
            session.rollback()
            return False


def delete_admin(admin_id: int) -> bool:
    with _get_session() as session:
        try:
            count = session.query(AdminUser).count()
            if count <= 1:
                return False
            session.execute(
                text("DELETE FROM admin_sessions WHERE user_id = :uid"),
                {"uid": admin_id},
            )
            result = session.execute(
                text("DELETE FROM admin_users WHERE id = :uid"), {"uid": admin_id}
            )
            session.commit()
            deleted = result.rowcount > 0
            if deleted:
                logger.info(f"[DB] Deleted admin user id={admin_id}")
            return deleted
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Delete admin failed: {e}")
            return False


def get_admin_by_id(admin_id: int) -> Optional[dict]:
    with _get_session() as session:
        user = session.query(AdminUser).filter_by(id=admin_id).first()
        if user:
            return {
                "id": user.id,
                "username": user.username,
                "role": user.role or "CUSTOMER",
                "created_at": user.created_at,
            }
        return None


def _hash_api_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()


def create_partner_api_key(
    label: str, tier: str = "standard", rate_limit: int = 120, created_by: int = None
) -> dict:
    raw_key = "dfx_" + secrets.token_hex(24)
    key_hash = _hash_api_key(raw_key)
    with _get_session() as session:
        try:
            rec = PartnerApiKey(
                key_hash=key_hash,
                label=label,
                tier=tier,
                rate_limit_per_minute=rate_limit,
                is_active=1,
                created_by=created_by,
            )
            session.add(rec)
            session.commit()
            logger.info(
                f"[DB] Created partner API key id={rec.id} label={label} tier={tier}"
            )
            return {
                "id": rec.id,
                "key": raw_key,
                "label": label,
                "tier": tier,
                "rate_limit_per_minute": rate_limit,
            }
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Create partner API key failed: {e}")
            return {}


def validate_partner_api_key(raw_key: str) -> Optional[dict]:
    key_hash = _hash_api_key(raw_key)
    with _get_session() as session:
        rec = (
            session.query(PartnerApiKey)
            .filter_by(key_hash=key_hash, is_active=1)
            .first()
        )
        if rec:
            rec.last_used_at = datetime.utcnow().isoformat()
            session.commit()
            return {
                "id": rec.id,
                "label": rec.label,
                "tier": rec.tier,
                "rate_limit_per_minute": rec.rate_limit_per_minute,
            }
        return None


def list_partner_api_keys() -> list:
    with _get_session() as session:
        rows = session.query(PartnerApiKey).order_by(PartnerApiKey.id.desc()).all()
        return [
            {
                "id": r.id,
                "label": r.label,
                "tier": r.tier,
                "rate_limit_per_minute": r.rate_limit_per_minute,
                "is_active": bool(r.is_active),
                "last_used_at": r.last_used_at,
                "created_at": r.created_at,
            }
            for r in rows
        ]


def toggle_partner_api_key(key_id: int, active: bool) -> bool:
    with _get_session() as session:
        try:
            rec = session.query(PartnerApiKey).filter_by(id=key_id).first()
            if not rec:
                return False
            rec.is_active = 1 if active else 0
            session.commit()
            logger.info(f"[DB] Partner API key id={key_id} active={active}")
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Toggle partner API key failed: {e}")
            return False


def delete_partner_api_key(key_id: int) -> bool:
    with _get_session() as session:
        try:
            deleted = session.query(PartnerApiKey).filter_by(id=key_id).delete()
            session.commit()
            logger.info(f"[DB] Deleted partner API key id={key_id}")
            return deleted > 0
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Delete partner API key failed: {e}")
            return False


def create_job_log(job_id: str, strategy_name: str) -> int:
    started_at = datetime.utcnow().isoformat()
    with _get_session() as session:
        try:
            log = SchedulerJobLog(
                job_id=job_id,
                strategy_name=strategy_name,
                started_at=started_at,
                status="RUNNING",
            )
            session.add(log)
            session.commit()
            logger.debug(f"[DB] Created job log #{log.id} for {job_id}/{strategy_name}")
            return log.id
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Create job log failed: {e}")
            return -1


def finish_job_log(
    log_id: int,
    status: str,
    assets_evaluated: int = 0,
    signals_generated: int = 0,
    errors: int = 0,
    error_detail: Optional[str] = None,
):
    finished_at = datetime.utcnow().isoformat()
    with _get_session() as session:
        try:
            log = session.query(SchedulerJobLog).filter_by(id=log_id).first()
            if not log:
                return
            log.finished_at = finished_at
            log.status = status
            log.assets_evaluated = assets_evaluated
            log.signals_generated = signals_generated
            log.errors = errors
            if error_detail:
                log.error_detail = error_detail[:2000]
            if log.started_at:
                try:
                    start = datetime.fromisoformat(log.started_at)
                    end = datetime.fromisoformat(finished_at)
                    log.duration_seconds = round((end - start).total_seconds(), 3)
                except Exception:
                    pass
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Finish job log failed: {e}")


def get_recent_job_logs(limit: int = 50) -> list[dict]:
    with _get_session() as session:
        logs = (
            session.query(SchedulerJobLog)
            .order_by(SchedulerJobLog.id.desc())
            .limit(limit)
            .all()
        )
        return [
            {
                "id": l.id,
                "job_id": l.job_id,
                "strategy_name": l.strategy_name,
                "started_at": l.started_at,
                "finished_at": l.finished_at,
                "duration_seconds": l.duration_seconds,
                "status": l.status,
                "assets_evaluated": l.assets_evaluated,
                "signals_generated": l.signals_generated,
                "errors": l.errors,
                "error_detail": l.error_detail,
            }
            for l in logs
        ]


def get_scheduler_health_summary() -> dict:
    with _get_session() as session:
        total = session.query(SchedulerJobLog).count()
        last = (
            session.query(SchedulerJobLog).order_by(SchedulerJobLog.id.desc()).first()
        )
        failed_24h = (
            session.query(SchedulerJobLog)
            .filter(
                SchedulerJobLog.status.in_(["FAILED", "PARTIAL"]),
                SchedulerJobLog.started_at
                >= (datetime.utcnow() - timedelta(hours=24)).isoformat(),
            )
            .count()
        )
        success_24h = (
            session.query(SchedulerJobLog)
            .filter(
                SchedulerJobLog.status == "SUCCESS",
                SchedulerJobLog.started_at
                >= (datetime.utcnow() - timedelta(hours=24)).isoformat(),
            )
            .count()
        )
        return {
            "total_jobs_logged": total,
            "last_job": {
                "job_id": last.job_id if last else None,
                "strategy": last.strategy_name if last else None,
                "started_at": last.started_at if last else None,
                "status": last.status if last else None,
            },
            "last_24h_success": success_24h,
            "last_24h_failures": failed_24h,
        }


def _parse_ts(ts_str):
    if not ts_str:
        return None
    try:
        ts_str = ts_str.replace("Z", "+00:00")
        return datetime.fromisoformat(ts_str).replace(tzinfo=None)
    except (ValueError, TypeError):
        return None


def compute_signal_metrics():
    now_dt = datetime.utcnow()
    now_iso = now_dt.isoformat()
    cutoff_7d = now_dt - timedelta(days=7)
    cutoff_30d = now_dt - timedelta(days=30)

    with _get_session() as session:
        all_signals = session.query(Signal).all()

        if not all_signals:
            logger.info("[METRICS] No signals to compute metrics for")
            return 0

        groups = {}
        for s in all_signals:
            ts_dt = _parse_ts(s.signal_timestamp) or _parse_ts(s.created_at)
            for period, cutoff in [
                ("all_time", None),
                ("7d", cutoff_7d),
                ("30d", cutoff_30d),
            ]:
                if cutoff and (ts_dt is None or ts_dt < cutoff):
                    continue

                for key in [
                    (s.strategy_name, None, period),
                    (s.strategy_name, s.asset, period),
                ]:
                    if key not in groups:
                        groups[key] = []
                    groups[key].append(s)

        count = 0
        for (strategy, asset, period), sigs in groups.items():
            total = len(sigs)
            open_count = sum(1 for s in sigs if s.status == "OPEN")
            closed_count = sum(1 for s in sigs if s.status == "CLOSED")

            gains = []
            losses = []
            durations = []

            for s in sigs:
                if s.status != "CLOSED" or not s.exit_price or not s.entry_price:
                    continue

                if s.direction == "BUY":
                    pct = ((s.exit_price - s.entry_price) / s.entry_price) * 100
                else:
                    pct = ((s.entry_price - s.exit_price) / s.entry_price) * 100

                if pct >= 0:
                    gains.append(pct)
                else:
                    losses.append(pct)

                if s.signal_timestamp and s.updated_at:
                    try:
                        t_open = datetime.fromisoformat(
                            s.signal_timestamp.replace("Z", "")
                        )
                        t_close = datetime.fromisoformat(s.updated_at.replace("Z", ""))
                        dur_hours = (t_close - t_open).total_seconds() / 3600
                        durations.append(dur_hours)
                    except (ValueError, TypeError):
                        pass

            won = len(gains)
            lost = len(losses)
            win_rate = round(won / (won + lost) * 100, 1) if (won + lost) > 0 else 0.0
            avg_gain = round(sum(gains) / len(gains), 4) if gains else 0.0
            avg_loss = round(sum(losses) / len(losses), 4) if losses else 0.0
            best = round(max(gains), 4) if gains else None
            worst = round(min(losses), 4) if losses else None
            avg_dur = round(sum(durations) / len(durations), 2) if durations else None

            last_ts = max((s.signal_timestamp or s.created_at or "") for s in sigs)

            existing = (
                session.query(SignalMetrics)
                .filter_by(strategy_name=strategy, asset=asset, period=period)
                .first()
            )

            if existing:
                existing.total_signals = total
                existing.open_signals = open_count
                existing.closed_signals = closed_count
                existing.won = won
                existing.lost = lost
                existing.win_rate = win_rate
                existing.avg_gain_pct = avg_gain
                existing.avg_loss_pct = avg_loss
                existing.best_gain_pct = best
                existing.worst_loss_pct = worst
                existing.avg_duration_hours = avg_dur
                existing.last_signal_at = last_ts
                existing.computed_at = now_iso
            else:
                session.add(
                    SignalMetrics(
                        strategy_name=strategy,
                        asset=asset,
                        period=period,
                        total_signals=total,
                        open_signals=open_count,
                        closed_signals=closed_count,
                        won=won,
                        lost=lost,
                        win_rate=win_rate,
                        avg_gain_pct=avg_gain,
                        avg_loss_pct=avg_loss,
                        best_gain_pct=best,
                        worst_loss_pct=worst,
                        avg_duration_hours=avg_dur,
                        last_signal_at=last_ts,
                        computed_at=now_iso,
                    )
                )
            count += 1

        session.commit()
        logger.info(
            f"[METRICS] Computed {count} metric rows from {len(all_signals)} signals"
        )
        return count


def get_signal_metrics(
    strategy_name: Optional[str] = None,
    asset: Optional[str] = None,
    period: str = "all_time",
    summary_only: bool = False,
) -> list[dict]:
    with _get_session() as session:
        q = session.query(SignalMetrics).filter_by(period=period)
        if strategy_name:
            q = q.filter_by(strategy_name=strategy_name)
        if asset:
            q = q.filter_by(asset=asset)
        elif summary_only:
            q = q.filter(SignalMetrics.asset.is_(None))

        rows = q.all()
        return [
            {
                "strategy": r.strategy_name,
                "asset": r.asset,
                "period": r.period,
                "total_signals": r.total_signals,
                "open": r.open_signals,
                "closed": r.closed_signals,
                "won": r.won,
                "lost": r.lost,
                "win_rate": r.win_rate,
                "avg_gain_pct": r.avg_gain_pct,
                "avg_loss_pct": r.avg_loss_pct,
                "best_gain_pct": r.best_gain_pct,
                "worst_loss_pct": r.worst_loss_pct,
                "avg_duration_hours": r.avg_duration_hours,
                "last_signal_at": r.last_signal_at,
                "computed_at": r.computed_at,
            }
            for r in rows
        ]


def get_all_signal_metrics() -> list[dict]:
    with _get_session() as session:
        rows = session.query(SignalMetrics).all()
        return [
            {
                "strategy": r.strategy_name,
                "asset": r.asset,
                "period": r.period,
                "total_signals": r.total_signals,
                "open": r.open_signals,
                "closed": r.closed_signals,
                "won": r.won,
                "lost": r.lost,
                "win_rate": r.win_rate,
                "avg_gain_pct": r.avg_gain_pct,
                "avg_loss_pct": r.avg_loss_pct,
                "best_gain_pct": r.best_gain_pct,
                "worst_loss_pct": r.worst_loss_pct,
                "avg_duration_hours": r.avg_duration_hours,
                "last_signal_at": r.last_signal_at,
                "computed_at": r.computed_at,
            }
            for r in rows
        ]


def get_user_cms_configs(user_id: int) -> list[dict]:
    with _get_session() as session:
        rows = (
            session.query(UserCmsConfig)
            .filter_by(user_id=user_id)
            .order_by(UserCmsConfig.id)
            .all()
        )
        return [_user_cms_to_dict(r) for r in rows]


def get_all_user_cms_configs(user_id: Optional[int] = None) -> list[dict]:
    with _get_session() as session:
        q = session.query(UserCmsConfig, AdminUser.username).join(
            AdminUser, UserCmsConfig.user_id == AdminUser.id
        )
        if user_id is not None:
            q = q.filter(UserCmsConfig.user_id == user_id)
        rows = q.order_by(UserCmsConfig.id).all()
        return [{**_user_cms_to_dict(r), "owner": u} for r, u in rows]


def create_user_cms_config(data: dict) -> int:
    from trading_engine.services.encryption import encrypt

    with _get_session() as session:
        try:
            obj = UserCmsConfig(
                user_id=data["user_id"],
                site_url=data["site_url"].rstrip("/"),
                wp_username=data["wp_username"],
                encrypted_app_password=encrypt(data["app_password"]),
                is_active=1 if data.get("is_active", True) else 0,
            )
            session.add(obj)
            session.commit()
            logger.info(
                f"[DB] Created user CMS config #{obj.id} for user_id={data['user_id']}"
            )
            return obj.id
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Create user CMS config failed: {e}")
            raise


def delete_user_cms_config(config_id: int, user_id: Optional[int] = None) -> bool:
    with _get_session() as session:
        try:
            q = session.query(UserCmsConfig).filter_by(id=config_id)
            if user_id is not None:
                q = q.filter_by(user_id=user_id)
            row = q.first()
            if not row:
                return False
            session.delete(row)
            session.commit()
            logger.info(f"[DB] Deleted user CMS config #{config_id}")
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Delete user CMS config failed: {e}")
            return False


def update_user_cms_config(
    config_id: int, data: dict, user_id: Optional[int] = None
) -> bool:
    with _get_session() as session:
        try:
            q = session.query(UserCmsConfig).filter_by(id=config_id)
            if user_id is not None:
                q = q.filter_by(user_id=user_id)
            row = q.first()
            if not row:
                return False
            if "site_url" in data:
                row.site_url = data["site_url"].rstrip("/")
            if "wp_username" in data:
                row.wp_username = data["wp_username"]
            if "app_password" in data and data["app_password"]:
                from trading_engine.services.encryption import encrypt

                row.encrypted_app_password = encrypt(data["app_password"])
            if "is_active" in data:
                row.is_active = 1 if data["is_active"] else 0
            session.commit()
            logger.info(f"[DB] Updated user CMS config #{config_id}")
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Update user CMS config failed: {e}")
            return False


def get_user_cms_config_decrypted(
    config_id: int, user_id: Optional[int] = None
) -> Optional[dict]:
    from trading_engine.services.encryption import decrypt

    with _get_session() as session:
        q = session.query(UserCmsConfig).filter_by(id=config_id)
        if user_id is not None:
            q = q.filter_by(user_id=user_id)
        row = q.first()
        if not row:
            return None
        return {
            "id": row.id,
            "user_id": row.user_id,
            "site_url": row.site_url,
            "wp_username": row.wp_username,
            "app_password": decrypt(row.encrypted_app_password),
            "is_active": bool(row.is_active),
            "created_at": row.created_at,
        }


def get_active_cms_configs_decrypted() -> list[dict]:
    from trading_engine.services.encryption import decrypt

    with _get_session() as session:
        rows = (
            session.query(UserCmsConfig)
            .filter_by(is_active=1)
            .order_by(UserCmsConfig.id)
            .all()
        )
        results = []
        for row in rows:
            try:
                results.append(
                    {
                        "id": row.id,
                        "user_id": row.user_id,
                        "site_url": row.site_url,
                        "wp_username": row.wp_username,
                        "app_password": decrypt(row.encrypted_app_password),
                    }
                )
            except Exception as e:
                logger.error(f"[DB] Failed to decrypt CMS config #{row.id}: {e}")
        return results


def _user_cms_to_dict(row: UserCmsConfig) -> dict:
    return {
        "id": row.id,
        "user_id": row.user_id,
        "site_url": row.site_url,
        "wp_username": row.wp_username,
        "is_active": bool(row.is_active),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def get_signal_cms_post(signal_id: int, cms_config_id: Optional[int]) -> Optional[dict]:
    with _get_session() as session:
        q = session.query(SignalCmsPost).filter_by(
            signal_id=signal_id, cms_config_id=cms_config_id
        )
        row = q.first()
        if not row:
            return None
        return {
            "id": row.id,
            "signal_id": row.signal_id,
            "cms_config_id": row.cms_config_id,
            "wp_post_id": row.wp_post_id,
            "publish_status": row.publish_status,
            "last_sync": row.last_sync,
        }


def upsert_signal_cms_post(signal_id: int, cms_config_id: Optional[int], fields: dict):
    with _get_session() as session:
        try:
            row = (
                session.query(SignalCmsPost)
                .filter_by(signal_id=signal_id, cms_config_id=cms_config_id)
                .first()
            )
            if not row:
                row = SignalCmsPost(signal_id=signal_id, cms_config_id=cms_config_id)
                session.add(row)
            if "wp_post_id" in fields:
                row.wp_post_id = fields["wp_post_id"]
            if "publish_status" in fields:
                row.publish_status = fields["publish_status"]
            if "last_sync" in fields:
                row.last_sync = fields["last_sync"]
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(
                f"[DB] upsert_signal_cms_post failed signal={signal_id} config={cms_config_id}: {e}"
            )


def get_signal_cms_posts_for_signal(signal_id: int) -> list[dict]:
    with _get_session() as session:
        rows = session.query(SignalCmsPost).filter_by(signal_id=signal_id).all()
        return [
            {
                "id": r.id,
                "signal_id": r.signal_id,
                "cms_config_id": r.cms_config_id,
                "wp_post_id": r.wp_post_id,
                "publish_status": r.publish_status,
                "last_sync": r.last_sync,
            }
            for r in rows
        ]


def upsert_strategy_execution_log(strategy_name: str, status: str):
    now_iso = datetime.utcnow().isoformat()
    with _get_session() as session:
        try:
            row = (
                session.query(StrategyExecutionLog)
                .filter_by(strategy_name=strategy_name)
                .order_by(StrategyExecutionLog.id.desc())
                .first()
            )
            if row and row.status == status:
                row.last_run_at = now_iso
            else:
                session.add(
                    StrategyExecutionLog(
                        strategy_name=strategy_name,
                        last_run_at=now_iso,
                        status=status,
                    )
                )
            session.commit()
            logger.debug(
                f"[DB] strategy_execution_log upserted: {strategy_name} = {status}"
            )
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] upsert_strategy_execution_log failed: {e}")


def get_last_successful_execution(strategy_name: str) -> Optional[dict]:
    with _get_session() as session:
        row = (
            session.query(StrategyExecutionLog)
            .filter_by(strategy_name=strategy_name, status="SUCCESS")
            .order_by(StrategyExecutionLog.id.desc())
            .first()
        )
        if not row:
            return None
        return {
            "id": row.id,
            "strategy_name": row.strategy_name,
            "last_run_at": row.last_run_at,
            "status": row.status,
        }


def insert_recovery_notification(
    strategy_name: str,
    missed_window_time: str,
    execution_time: str,
    assets_affected: str,
    status: str,
) -> Optional[int]:
    with _get_session() as session:
        try:
            obj = RecoveryNotification(
                strategy_name=strategy_name,
                missed_window_time=missed_window_time,
                execution_time=execution_time,
                assets_affected=assets_affected,
                status=status,
            )
            session.add(obj)
            session.commit()
            logger.info(
                f"[DB] Inserted recovery_notification #{obj.id}: "
                f"strategy={strategy_name} window={missed_window_time} status={status}"
            )
            return obj.id
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] insert_recovery_notification failed: {e}")
            return None


def get_recovery_notifications(limit: int = 50) -> list[dict]:
    import json as _json

    with _get_session() as session:
        rows = (
            session.query(RecoveryNotification)
            .order_by(RecoveryNotification.id.desc())
            .limit(limit)
            .all()
        )
        results = []
        for r in rows:
            try:
                assets = _json.loads(r.assets_affected)
            except Exception:
                assets = r.assets_affected
            results.append(
                {
                    "id": r.id,
                    "strategy_name": r.strategy_name,
                    "missed_window_time": r.missed_window_time,
                    "execution_time": r.execution_time,
                    "assets_affected": assets,
                    "status": r.status,
                }
            )
        return results


def upsert_daily_close(symbol: str, close_date: str, close_price: float):
    with _get_session() as session:
        existing = (
            session.query(HistoricalDailyClose)
            .filter_by(symbol=symbol, close_date=close_date)
            .first()
        )
        if existing:
            existing.close_price = close_price
        else:
            session.add(
                HistoricalDailyClose(
                    symbol=symbol,
                    close_date=close_date,
                    close_price=close_price,
                )
            )
        session.commit()


def bulk_upsert_daily_closes(rows: list[dict]):
    with _get_session() as session:
        for row in rows:
            existing = (
                session.query(HistoricalDailyClose)
                .filter_by(symbol=row["symbol"], close_date=row["close_date"])
                .first()
            )
            if existing:
                existing.close_price = row["close_price"]
            else:
                session.add(
                    HistoricalDailyClose(
                        symbol=row["symbol"],
                        close_date=row["close_date"],
                        close_price=row["close_price"],
                    )
                )
        session.commit()


def get_recent_daily_closes(symbol: str, n: int = 20) -> list[dict]:
    with _get_session() as session:
        rows = (
            session.query(HistoricalDailyClose)
            .filter_by(symbol=symbol)
            .order_by(HistoricalDailyClose.close_date.desc())
            .limit(n)
            .all()
        )
        return [
            {
                "symbol": r.symbol,
                "close_date": r.close_date,
                "close_price": r.close_price,
            }
            for r in rows
        ]


# ─────────────────────────────────────────────────────────────
# Strategy Asset Management
# ─────────────────────────────────────────────────────────────

_STRATEGY_ASSET_SEEDS = {
    "mtf_ema": [
        ("SPX", "forex", "indices"),
        ("NDX", "forex", "indices"),
        ("RUT", "forex", "indices"),
        ("DJI", "forex", "indices"),
        ("XAU/USD", "forex", "commodities"),
        ("XAG/USD", "forex", "commodities"),
        ("OSX", "forex", "commodities"),
        ("BTC/USD", "crypto", "crypto"),
        ("ETH/USD", "crypto", "crypto"),
        ("GBP/USD", "forex", "forex"),
        ("AUD/USD", "forex", "forex"),
    ],
    "trend_non_forex": [
        # ── Commodity ETFs ──────────────────────────────
        ("CORN", "forex"),
        ("SOYB", "forex"),
        ("WEAT", "forex"),
        ("CANE", "forex"),
        ("WOOD", "forex"),
        ("USO", "forex"),
        ("UNG", "forex"),
        ("UGA", "forex"),
        ("SGOL", "forex"),
        ("SIVR", "forex"),
        ("CPER", "forex"),
        ("PPLT", "forex"),
        ("PALL", "forex"),
        ("DBB", "forex"),
        ("SLX", "forex"),
        # ── Crypto altcoins (LONG_ONLY) ─────────────────
        ("BNB/USD", "crypto"),
        ("XRP/USD", "crypto"),
        ("SOL/USD", "crypto"),
        ("TRX/USD", "crypto"),
        ("DOGE/USD", "crypto"),
        ("ADA/USD", "crypto"),
        ("TON/USD", "crypto"),
        ("SHIB/USD", "crypto"),
        ("AVAX/USD", "crypto"),
        ("LINK/USD", "crypto"),
        ("LTC/USD", "crypto"),
        ("DOT/USD", "crypto"),
        ("BCH/USD", "crypto"),
        ("UNI/USD", "crypto"),
        ("ATOM/USD", "crypto"),
        ("XLM/USD", "crypto"),
        ("HBAR/USD", "crypto"),
        ("ICP/USD", "crypto"),
        ("APT/USD", "crypto"),
        ("NEAR/USD", "crypto"),
        ("ARB/USD", "crypto"),
        ("OP/USD", "crypto"),
        ("SUI/USD", "crypto"),
        ("INJ/USD", "crypto"),
        ("CRO/USD", "crypto"),
    ],
    "trend_forex": [
        ("EUR/USD", "forex"),
        ("USD/JPY", "forex"),
    ],
    "sp500_momentum": [
        ("SPX", "forex"),
    ],
    "highest_lowest_fx": [
        ("EUR/USD", "forex"),
    ],
}


def seed_strategy_assets():
    """One-time bootstrap: seeds strategy_assets only if the table is empty.
    Safe to call on every startup — skips entirely if any rows exist.
    Fully idempotent.
    """
    with _get_session() as session:
        try:
            count = session.query(StrategyAsset).count()
            if count > 0:
                logger.info(
                    f"[DB] seed_strategy_assets: DB already seeded "
                    f"({count} rows) — skipping bootstrap"
                )
                return

            seeded = 0
            for strategy_name, assets in _STRATEGY_ASSET_SEEDS.items():
                for entry in assets:
                    symbol = entry[0]
                    asset_class = entry[1]
                    sub_cat = entry[2] if len(entry) > 2 else None
                    session.add(
                        StrategyAsset(
                            strategy_name=strategy_name,
                            symbol=symbol,
                            asset_class=asset_class,
                            sub_category=sub_cat,
                            is_active=1,
                            fcsapi_verified=1,
                            added_by="system_seed",
                        )
                    )
                    seeded += 1

            session.commit()
            logger.info(
                f"[DB] seed_strategy_assets: bootstrapped {seeded} asset(s)"
            )

        except Exception as e:
            session.rollback()
            logger.error(f"[DB] seed_strategy_assets failed: {e}")


def sync_strategy_assets_dedup() -> dict:
    """Detect and resolve duplicate active asset rows across strategies.
    Deactivates mtf_ema rows that conflict with trend_forex or trend_non_forex.
    Fully idempotent — safe to run multiple times.
    """
    results = {"mtf_vs_forex": 0, "mtf_vs_non_forex": 0, "total": 0}
    with _get_session() as session:
        try:
            # --- mtf_ema vs trend_forex ---
            forex_active = [
                r.symbol
                for r in session.query(StrategyAsset)
                .filter_by(strategy_name="trend_forex", is_active=1)
                .all()
            ]
            if forex_active:
                mtf_forex_conflicts = (
                    session.query(StrategyAsset)
                    .filter(
                        StrategyAsset.strategy_name == "mtf_ema",
                        StrategyAsset.symbol.in_(forex_active),
                        StrategyAsset.is_active == 1,
                    )
                    .all()
                )
                for row in mtf_forex_conflicts:
                    row.is_active = 0
                    results["mtf_vs_forex"] += 1
                    logger.info(
                        f"[DB] sync_dedup: deactivated mtf_ema/{row.symbol} "
                        f"(conflict with trend_forex)"
                    )
                if mtf_forex_conflicts:
                    session.commit()

            # --- mtf_ema vs trend_non_forex ---
            non_forex_active = [
                r.symbol
                for r in session.query(StrategyAsset)
                .filter_by(strategy_name="trend_non_forex", is_active=1)
                .all()
            ]
            if non_forex_active:
                mtf_non_forex_conflicts = (
                    session.query(StrategyAsset)
                    .filter(
                        StrategyAsset.strategy_name == "mtf_ema",
                        StrategyAsset.symbol.in_(non_forex_active),
                        StrategyAsset.is_active == 1,
                    )
                    .all()
                )
                for row in mtf_non_forex_conflicts:
                    row.is_active = 0
                    results["mtf_vs_non_forex"] += 1
                    logger.info(
                        f"[DB] sync_dedup: deactivated mtf_ema/{row.symbol} "
                        f"(conflict with trend_non_forex)"
                    )
                if mtf_non_forex_conflicts:
                    session.commit()

            results["total"] = results["mtf_vs_forex"] + results["mtf_vs_non_forex"]
            if results["total"] > 0:
                logger.info(
                    f"[DB] sync_dedup complete: {results['total']} conflict(s) resolved"
                )
            else:
                logger.info("[DB] sync_dedup complete: no conflicts found")
            return results
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] sync_dedup failed: {e}")
            return results


def _strategy_asset_to_dict(row: StrategyAsset) -> dict:
    return {
        "id": row.id,
        "strategy_name": row.strategy_name,
        "symbol": row.symbol,
        "full_name": row.full_name,
        "asset_class": row.asset_class,
        "sub_category": row.sub_category,
        "is_active": bool(row.is_active),
        "fcsapi_verified": bool(row.fcsapi_verified),
        "added_by": row.added_by,
        "notes": row.notes,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def get_strategy_assets(
    strategy_name: str,
    active_only: bool = True,
) -> list[str]:
    """Return list of symbols for a strategy.
    Returns active symbols only by default.
    Falls back to empty list — callers handle fallback.
    """
    with _get_session() as session:
        q = session.query(StrategyAsset).filter_by(strategy_name=strategy_name)
        if active_only:
            q = q.filter_by(is_active=1)
        rows = q.order_by(StrategyAsset.symbol).all()
        return [r.symbol for r in rows]


def get_all_strategy_assets() -> list[dict]:
    """Return all strategy assets with full metadata for the admin panel."""
    with _get_session() as session:
        rows = (
            session.query(StrategyAsset)
            .order_by(
                StrategyAsset.strategy_name,
                StrategyAsset.asset_class,
                StrategyAsset.symbol,
            )
            .all()
        )
        return [_strategy_asset_to_dict(r) for r in rows]


def get_strategy_assets_full(
    strategy_name: Optional[str] = None,
) -> list[dict]:
    """Return assets with full metadata, optionally filtered by strategy."""
    with _get_session() as session:
        q = session.query(StrategyAsset)
        if strategy_name:
            q = q.filter_by(strategy_name=strategy_name)
        rows = q.order_by(
            StrategyAsset.strategy_name,
            StrategyAsset.asset_class,
            StrategyAsset.symbol,
        ).all()
        return [_strategy_asset_to_dict(r) for r in rows]


def add_strategy_asset(
    strategy_name: str,
    symbol: str,
    asset_class: str,
    sub_category: Optional[str] = None,
    added_by: str = "admin",
    notes: Optional[str] = None,
    fcsapi_verified: bool = False,
    full_name: Optional[str] = None,
) -> Optional[int]:
    """Add a new asset to a strategy.
    Returns the new row id or None if duplicate.
    """
    with _get_session() as session:
        try:
            existing = (
                session.query(StrategyAsset)
                .filter_by(
                    strategy_name=strategy_name,
                    symbol=symbol,
                )
                .first()
            )
            # Resolve full_name: caller may supply it; fall back to static map
            resolved_full_name = full_name or _ASSET_NAME_MAP.get(symbol)
            if existing:
                if not existing.is_active:
                    existing.is_active = 1
                    existing.added_by = added_by
                    existing.notes = notes
                    existing.fcsapi_verified = 1 if fcsapi_verified else 0
                    existing.sub_category = sub_category
                    if resolved_full_name and not existing.full_name:
                        existing.full_name = resolved_full_name
                    session.commit()
                    logger.info(
                        f"[DB] Reactivated strategy asset: {strategy_name}/{symbol}"
                    )
                    return existing.id
                # Asset exists and is active — update fcsapi_verified / full_name if changed
                changed = False
                if fcsapi_verified and not existing.fcsapi_verified:
                    existing.fcsapi_verified = 1
                    changed = True
                if resolved_full_name and not existing.full_name:
                    existing.full_name = resolved_full_name
                    changed = True
                if changed:
                    session.commit()
                    logger.info(
                        f"[DB] Updated metadata for {strategy_name}/{symbol}"
                    )
                else:
                    logger.warning(
                        f"[DB] add_strategy_asset: "
                        f"{strategy_name}/{symbol} already exists"
                    )
                return None
            obj = StrategyAsset(
                strategy_name=strategy_name,
                symbol=symbol,
                asset_class=asset_class,
                sub_category=sub_category,
                is_active=1,
                fcsapi_verified=1 if fcsapi_verified else 0,
                added_by=added_by,
                notes=notes,
                full_name=resolved_full_name,
            )
            session.add(obj)
            session.commit()
            logger.info(
                f"[DB] Added strategy asset: {strategy_name}/{symbol} ({asset_class})"
            )
            return obj.id
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] add_strategy_asset failed: {e}")
            return None


def remove_strategy_asset(
    strategy_name: str,
    symbol: str,
) -> bool:
    """Hard-delete a strategy asset row from the DB.
    Always deletes the row completely regardless of added_by value.
    Returns False if the row was not found.
    """
    with _get_session() as session:
        try:
            row = (
                session.query(StrategyAsset)
                .filter_by(
                    strategy_name=strategy_name,
                    symbol=symbol,
                )
                .first()
            )
            if not row:
                logger.warning(
                    f"[DB] remove_strategy_asset: not found: "
                    f"{strategy_name}/{symbol}"
                )
                return False
            session.delete(row)
            session.commit()
            logger.info(
                f"[DB] Deleted strategy asset: {strategy_name}/{symbol}"
            )
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] remove_strategy_asset failed: {e}")
            return False


def mark_asset_verified(
    strategy_name: str,
    symbol: str,
    verified: bool = True,
    notes: str | None = None,
) -> bool:
    """Mark an asset as FCSAPI-verified or unverified, optionally storing notes."""
    with _get_session() as session:
        try:
            row = (
                session.query(StrategyAsset)
                .filter_by(
                    strategy_name=strategy_name,
                    symbol=symbol,
                )
                .first()
            )
            if not row:
                return False
            row.fcsapi_verified = 1 if verified else 0
            if notes is not None:
                row.notes = notes
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] mark_asset_verified failed: {e}")
            return False


def open_stock_algo2_position(symbol: str, signal_id: int, entry_price: float,
                               stop_loss: float, entry_date: str):
    with _get_session() as session:
        try:
            existing = session.query(StockAlgo2Position).filter_by(symbol=symbol).first()
            if existing:
                logger.warning(f"[DB] stock_algo2_positions: {symbol} already exists (id={existing.id})")
                return existing.id
            obj = StockAlgo2Position(
                symbol=symbol, signal_id=signal_id, entry_price=entry_price,
                stop_loss=stop_loss, entry_date=entry_date, trading_days_held=0,
            )
            session.add(obj)
            session.commit()
            logger.info(f"[DB] Opened stock_algo2 position #{obj.id} for {symbol}")
            return obj.id
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] open_stock_algo2_position failed: {e}")
            return None


def get_all_stock_algo2_positions() -> list:
    with _get_session() as session:
        rows = session.query(StockAlgo2Position).order_by(StockAlgo2Position.id).all()
        return [
            {
                "id": r.id, "symbol": r.symbol, "signal_id": r.signal_id,
                "entry_price": r.entry_price, "stop_loss": r.stop_loss,
                "entry_date": r.entry_date, "trading_days_held": r.trading_days_held,
                "created_at": r.created_at,
            }
            for r in rows
        ]


def increment_stock_algo2_hold_days(position_id: int) -> int:
    with _get_session() as session:
        try:
            row = session.query(StockAlgo2Position).filter_by(id=position_id).first()
            if not row:
                return 0
            row.trading_days_held += 1
            session.commit()
            return row.trading_days_held
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] increment_stock_algo2_hold_days failed: {e}")
            return 0


def close_stock_algo2_position(symbol: str) -> bool:
    with _get_session() as session:
        try:
            row = session.query(StockAlgo2Position).filter_by(symbol=symbol).first()
            if row:
                session.delete(row)
                session.commit()
                return True
            return False
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] close_stock_algo2_position failed: {e}")
            return False


def get_algo1_active_symbols() -> set:
    with _get_session() as session:
        rows = (
            session.query(Signal.asset)
            .filter_by(strategy_name="stocks_algo1", status="OPEN")
            .all()
        )
        return {r[0] for r in rows}


def delete_signal_by_id(signal_id: int) -> dict:
    """Permanently hard-delete a signal and all related records.

    Deletes (in order):
      1. signal_cms_posts rows for this signal
      2. open_positions row matching asset + strategy_name
      3. signals row

    Returns a result dict with deletion details, or
    {"deleted": False, "error": "Signal not found"} when the id is missing.
    """
    with _get_session() as session:
        try:
            sig = session.query(Signal).filter_by(id=signal_id).first()
            if not sig:
                logger.warning(f"[DB-DELETE] Signal #{signal_id} not found — skipping")
                return {"deleted": False, "error": "Signal not found"}

            asset = sig.asset
            strategy_name = sig.strategy_name
            direction = sig.direction
            entry_price = sig.entry_price
            status_was = sig.status

            # 1. Delete CMS post records
            cms_rows = (
                session.query(SignalCmsPost)
                .filter(SignalCmsPost.signal_id == signal_id)
                .all()
            )
            cms_count = len(cms_rows)
            for row in cms_rows:
                session.delete(row)
            logger.info(
                f"[DB-DELETE] Deleted {cms_count} CMS post record(s) for signal #{signal_id}"
            )

            # 2. Delete matching open position
            pos = (
                session.query(OpenPosition)
                .filter_by(asset=asset, strategy_name=strategy_name)
                .first()
            )
            position_deleted = False
            if pos:
                session.delete(pos)
                position_deleted = True
                logger.info(
                    f"[DB-DELETE] Deleted open_position #{pos.id} for "
                    f"{strategy_name}/{asset}"
                )

            # 3. Delete the signal itself
            session.delete(sig)
            session.commit()
            logger.info(
                f"[DB-DELETE] Deleted signal #{signal_id} | {asset} {direction} "
                f"@ {entry_price} | strategy={strategy_name} | status_was={status_was}"
            )

            _invalidate_signal_cache()
            _ws_broadcast_closed(signal_id, "deleted_by_admin")

            return {
                "deleted": True,
                "signal_id": signal_id,
                "asset": asset,
                "strategy_name": strategy_name,
                "direction": direction,
                "entry_price": entry_price,
                "status_was": status_was,
                "cms_posts_deleted": cms_count,
                "position_deleted": position_deleted,
            }
        except Exception as exc:
            session.rollback()
            logger.error(f"[DB-DELETE] Failed to delete signal #{signal_id}: {exc}")
            raise


def bulk_delete_signals(signal_ids: list) -> dict:
    """Delete multiple signals by ID, collecting successes and failures."""
    deleted = []
    failed = []
    for sid in signal_ids:
        try:
            result = delete_signal_by_id(sid)
            if result.get("deleted"):
                deleted.append(sid)
            else:
                failed.append(sid)
        except Exception as exc:
            logger.error(f"[DB-DELETE] bulk_delete_signals: error on id={sid}: {exc}")
            failed.append(sid)
    return {
        "deleted": deleted,
        "failed": failed,
        "total_deleted": len(deleted),
        "total_failed": len(failed),
    }
