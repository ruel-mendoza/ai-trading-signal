import os
import hashlib
import secrets
import logging
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    Float,
    Text,
    CheckConstraint,
    UniqueConstraint,
    Index,
    ForeignKey,
    event,
    text,
    inspect,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    sessionmaker,
    Session,
)
from sqlalchemy.pool import QueuePool

logger = logging.getLogger("trading_engine.database")

DB_PATH = os.path.join(os.path.dirname(__file__), "trading_data.db")
DATABASE_URL = os.environ.get("TRADING_ENGINE_DB_URL", f"sqlite:///{DB_PATH}")

VALID_TIMEFRAMES = ["30m", "1H", "4H", "D"]

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_pre_ping=True,
    echo=False,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)


class Base(DeclarativeBase):
    pass


class OHLCCandle(Base):
    __tablename__ = "ohlc_candles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(Text, nullable=False)
    timeframe = Column(Text, nullable=False)
    open_time = Column(Text, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, default=0)
    is_closed = Column(Integer, default=1)
    created_at = Column(Text, default=lambda: datetime.utcnow().isoformat())

    __table_args__ = (
        UniqueConstraint("symbol", "timeframe", "open_time", name="uq_ohlc_symbol_tf_time"),
        CheckConstraint("timeframe IN ('30m', '1H', '4H', 'D')", name="ck_ohlc_timeframe"),
        Index("idx_ohlc_symbol_tf_time", "symbol", "timeframe", "open_time"),
    )


class CacheMetadata(Base):
    __tablename__ = "cache_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(Text, nullable=False)
    timeframe = Column(Text, nullable=False)
    last_fetched = Column(Text, nullable=False)
    last_candle_close = Column(Text)

    __table_args__ = (
        UniqueConstraint("symbol", "timeframe", name="uq_cache_symbol_tf"),
    )


class StrategySignal(Base):
    __tablename__ = "strategy_signals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy = Column(Text, nullable=False)
    symbol = Column(Text, nullable=False)
    direction = Column(Text, nullable=False)
    entry_price = Column(Float, nullable=False)
    stop_loss = Column(Float)
    take_profit = Column(Float)
    trailing_stop_atr_mult = Column(Float)
    trigger_candle_time = Column(Text, nullable=False)
    trigger_timeframe = Column(Text, nullable=False)
    status = Column(Text, nullable=False, default="active")
    highest_price = Column(Float)
    lowest_price = Column(Float)
    exit_price = Column(Float)
    exit_reason = Column(Text)
    signal_metadata = Column("metadata", Text)
    created_at = Column(Text, default=lambda: datetime.utcnow().isoformat())

    __table_args__ = (
        UniqueConstraint("strategy", "symbol", "trigger_candle_time", "trigger_timeframe", name="uq_signal_lookup"),
        CheckConstraint("direction IN ('long', 'short')", name="ck_signal_direction"),
        CheckConstraint("status IN ('active', 'closed', 'expired')", name="ck_signal_status"),
        Index("idx_strategy_signals_lookup", "strategy", "symbol", "trigger_candle_time", "trigger_timeframe"),
        Index("idx_strategy_signals_active", "strategy", "symbol", "status"),
    )


class APIUsage(Base):
    __tablename__ = "api_usage"

    id = Column(Integer, primary_key=True, autoincrement=True)
    endpoint = Column(Text, nullable=False)
    symbol = Column(Text)
    timeframe = Column(Text)
    credits_used = Column(Integer, default=1)
    created_at = Column(Text, default=lambda: datetime.utcnow().isoformat())

    __table_args__ = (
        Index("idx_api_usage_created", "created_at"),
    )


class AppSetting(Base):
    __tablename__ = "app_settings"

    key = Column(Text, primary_key=True)
    value = Column(Text, nullable=False)
    updated_at = Column(Text, default=lambda: datetime.utcnow().isoformat())


class AdminUser(Base):
    __tablename__ = "admin_users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(Text, nullable=False, unique=True)
    password_hash = Column(Text, nullable=False)
    created_at = Column(Text, default=lambda: datetime.utcnow().isoformat())


class AdminSession(Base):
    __tablename__ = "admin_sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    token = Column(Text, nullable=False, unique=True)
    user_id = Column(Integer, ForeignKey("admin_users.id", ondelete="CASCADE"), nullable=False)
    expires_at = Column(Text, nullable=False)
    created_at = Column(Text, default=lambda: datetime.utcnow().isoformat())

    __table_args__ = (
        Index("idx_admin_sessions_token", "token"),
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
            "database_url": DATABASE_URL.split("///")[-1] if "sqlite" in DATABASE_URL else "(configured)",
            "tables": tables,
            "table_count": len(tables),
            "pool": pool_status,
        }
        logger.info(f"[DB] Health check passed: {len(tables)} tables, pool={pool_status}")
        return status
    except Exception as e:
        logger.error(f"[DB] Health check FAILED: {e}")
        return {"status": "unhealthy", "error": str(e)}


def init_db():
    logger.info("[DB] Initializing database tables via SQLAlchemy...")
    Base.metadata.create_all(engine)
    logger.info("[DB] All tables created/verified")

    with _get_session() as session:
        _seed_default_admin(session)

    health = check_db_health()
    logger.info(f"[DB] Startup health check: {health['status']}")


def upsert_candles(symbol: str, timeframe: str, candles: list[dict]):
    with _get_session() as session:
        try:
            for c in candles:
                existing = session.query(OHLCCandle).filter_by(
                    symbol=symbol, timeframe=timeframe, open_time=c["open_time"]
                ).first()
                if existing:
                    existing.open = c["open"]
                    existing.high = c["high"]
                    existing.low = c["low"]
                    existing.close = c["close"]
                    existing.volume = c.get("volume", 0)
                    existing.is_closed = c.get("is_closed", 1)
                else:
                    session.add(OHLCCandle(
                        symbol=symbol,
                        timeframe=timeframe,
                        open_time=c["open_time"],
                        open=c["open"],
                        high=c["high"],
                        low=c["low"],
                        close=c["close"],
                        volume=c.get("volume", 0),
                        is_closed=c.get("is_closed", 1),
                    ))
            session.commit()
            logger.debug(f"[DB] Upserted {len(candles)} candles for {symbol}/{timeframe}")
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Upsert candles failed: {e}")
            raise


def get_candles(symbol: str, timeframe: str, limit: int = 300) -> list[dict]:
    with _get_session() as session:
        rows = (
            session.query(OHLCCandle)
            .filter_by(symbol=symbol, timeframe=timeframe)
            .order_by(OHLCCandle.open_time.desc())
            .limit(limit)
            .all()
        )
        return [
            {
                "open_time": r.open_time,
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "volume": r.volume,
                "is_closed": r.is_closed,
            }
            for r in reversed(rows)
        ]


def update_cache_metadata(symbol: str, timeframe: str, last_candle_close: Optional[str] = None):
    now = datetime.utcnow().isoformat()
    with _get_session() as session:
        try:
            existing = session.query(CacheMetadata).filter_by(symbol=symbol, timeframe=timeframe).first()
            if existing:
                existing.last_fetched = now
                if last_candle_close is not None:
                    existing.last_candle_close = last_candle_close
            else:
                session.add(CacheMetadata(
                    symbol=symbol,
                    timeframe=timeframe,
                    last_fetched=now,
                    last_candle_close=last_candle_close,
                ))
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Update cache metadata failed: {e}")
            raise


def get_cache_metadata(symbol: str, timeframe: str) -> Optional[dict]:
    with _get_session() as session:
        row = session.query(CacheMetadata).filter_by(symbol=symbol, timeframe=timeframe).first()
        if row:
            return {"last_fetched": row.last_fetched, "last_candle_close": row.last_candle_close}
        return None


def signal_exists(strategy: str, symbol: str, trigger_candle_time: str, trigger_timeframe: str) -> bool:
    with _get_session() as session:
        row = (
            session.query(StrategySignal.id)
            .filter_by(
                strategy=strategy,
                symbol=symbol,
                trigger_candle_time=trigger_candle_time,
                trigger_timeframe=trigger_timeframe,
            )
            .first()
        )
        return row is not None


def insert_signal(signal: dict) -> Optional[int]:
    with _get_session() as session:
        try:
            obj = StrategySignal(
                strategy=signal["strategy"],
                symbol=signal["symbol"],
                direction=signal["direction"],
                entry_price=signal["entry_price"],
                stop_loss=signal.get("stop_loss"),
                take_profit=signal.get("take_profit"),
                trailing_stop_atr_mult=signal.get("trailing_stop_atr_mult"),
                trigger_candle_time=signal["trigger_candle_time"],
                trigger_timeframe=signal["trigger_timeframe"],
                status="active",
                highest_price=signal["entry_price"] if signal["direction"] == "long" else None,
                lowest_price=signal["entry_price"] if signal["direction"] == "short" else None,
                signal_metadata=signal.get("metadata"),
            )
            session.add(obj)
            session.commit()
            logger.info(f"[DB] Inserted signal #{obj.id}: {signal['strategy']} {signal['direction']} {signal['symbol']}")
            return obj.id
        except Exception:
            session.rollback()
            return None


def get_active_signals(strategy: Optional[str] = None, symbol: Optional[str] = None) -> list[dict]:
    with _get_session() as session:
        q = session.query(StrategySignal).filter(StrategySignal.status == "active")
        if strategy:
            q = q.filter(StrategySignal.strategy == strategy)
        if symbol:
            q = q.filter(StrategySignal.symbol == symbol)
        q = q.order_by(StrategySignal.created_at.desc())
        rows = q.all()
        return [_signal_to_dict(r) for r in rows]


def update_signal_tracking(signal_id: int, highest_price: Optional[float] = None, lowest_price: Optional[float] = None):
    with _get_session() as session:
        try:
            sig = session.query(StrategySignal).filter_by(id=signal_id).first()
            if not sig:
                return
            if highest_price is not None:
                sig.highest_price = max(sig.highest_price or 0, highest_price)
            if lowest_price is not None:
                sig.lowest_price = min(sig.lowest_price or 999999, lowest_price)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Update signal tracking failed: {e}")
            raise


def close_signal(signal_id: int, exit_price: float, exit_reason: str):
    with _get_session() as session:
        try:
            sig = session.query(StrategySignal).filter_by(id=signal_id).first()
            if sig:
                sig.status = "closed"
                sig.exit_price = exit_price
                sig.exit_reason = exit_reason
                session.commit()
                logger.info(f"[DB] Closed signal #{signal_id}: {exit_reason}")
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Close signal failed: {e}")
            raise


def get_all_signals(strategy: Optional[str] = None, symbol: Optional[str] = None, status: Optional[str] = None, limit: int = 100) -> list[dict]:
    with _get_session() as session:
        q = session.query(StrategySignal)
        if strategy:
            q = q.filter(StrategySignal.strategy == strategy)
        if symbol:
            q = q.filter(StrategySignal.symbol == symbol)
        if status:
            q = q.filter(StrategySignal.status == status)
        q = q.order_by(StrategySignal.created_at.desc()).limit(limit)
        rows = q.all()
        return [_signal_to_dict(r) for r in rows]


def _signal_to_dict(sig: StrategySignal) -> dict:
    return {
        "id": sig.id,
        "strategy": sig.strategy,
        "symbol": sig.symbol,
        "direction": sig.direction,
        "entry_price": sig.entry_price,
        "stop_loss": sig.stop_loss,
        "take_profit": sig.take_profit,
        "trailing_stop_atr_mult": sig.trailing_stop_atr_mult,
        "trigger_candle_time": sig.trigger_candle_time,
        "trigger_timeframe": sig.trigger_timeframe,
        "status": sig.status,
        "highest_price": sig.highest_price,
        "lowest_price": sig.lowest_price,
        "exit_price": sig.exit_price,
        "exit_reason": sig.exit_reason,
        "metadata": sig.signal_metadata,
        "created_at": sig.created_at,
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
                existing.updated_at = datetime.utcnow().isoformat()
            else:
                session.add(AppSetting(key=key, value=value))
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Set setting failed: {e}")
            raise


def log_api_usage(endpoint: str, symbol: Optional[str] = None, timeframe: Optional[str] = None, credits_used: int = 1):
    with _get_session() as session:
        try:
            session.add(APIUsage(
                endpoint=endpoint,
                symbol=symbol,
                timeframe=timeframe,
                credits_used=credits_used,
            ))
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Log API usage failed: {e}")


def get_api_usage_stats() -> dict:
    with _get_session() as session:
        now = datetime.utcnow()
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
        day_ago = (now - timedelta(days=1)).isoformat()

        monthly_total = session.execute(
            text("SELECT COALESCE(SUM(credits_used), 0) as total FROM api_usage WHERE created_at >= :start"),
            {"start": month_start},
        ).scalar() or 0

        daily_total = session.execute(
            text("SELECT COALESCE(SUM(credits_used), 0) as total FROM api_usage WHERE created_at >= :start"),
            {"start": day_ago},
        ).scalar() or 0

        by_endpoint_rows = session.execute(
            text("""
                SELECT endpoint, COUNT(*) as count, SUM(credits_used) as credits
                FROM api_usage WHERE created_at >= :start
                GROUP BY endpoint ORDER BY credits DESC
            """),
            {"start": month_start},
        ).fetchall()
        by_endpoint = [{"endpoint": r[0], "count": r[1], "credits": r[2]} for r in by_endpoint_rows]

        thirty_days_ago = (now - timedelta(days=30)).isoformat()
        daily_rows = session.execute(
            text("""
                SELECT date(created_at) as day, SUM(credits_used) as credits
                FROM api_usage WHERE created_at >= :start
                GROUP BY date(created_at) ORDER BY day DESC LIMIT 30
            """),
            {"start": thirty_days_ago},
        ).fetchall()
        daily_history = [{"day": r[0], "credits": r[1]} for r in daily_rows]

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
    with _get_session() as session:
        return session.query(OHLCCandle).filter_by(symbol=symbol, timeframe=timeframe).count()


def _hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100000).hex()
    return f"{salt}:{h}"


def _verify_password(password: str, stored_hash: str) -> bool:
    try:
        salt, h = stored_hash.split(":")
        computed = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100000).hex()
        return secrets.compare_digest(computed, h)
    except (ValueError, AttributeError):
        return False


def _seed_default_admin(session: Session):
    count = session.query(AdminUser).count()
    if count == 0:
        pw_hash = _hash_password("pass123")
        session.add(AdminUser(username="admin", password_hash=pw_hash))
        session.commit()
        logger.info("[DB] Seeded default admin user")


def authenticate_admin(username: str, password: str) -> Optional[dict]:
    with _get_session() as session:
        user = session.query(AdminUser).filter_by(username=username).first()
        if user and _verify_password(password, user.password_hash):
            return {"id": user.id, "username": user.username, "created_at": user.created_at}
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
                SELECT s.*, u.username FROM admin_sessions s
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
            }
        return None


def delete_session(token: str):
    with _get_session() as session:
        try:
            session.execute(text("DELETE FROM admin_sessions WHERE token = :token"), {"token": token})
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
        return [{"id": r.id, "username": r.username, "created_at": r.created_at} for r in rows]


def create_admin(username: str, password: str) -> Optional[int]:
    pw_hash = _hash_password(password)
    with _get_session() as session:
        try:
            user = AdminUser(username=username, password_hash=pw_hash)
            session.add(user)
            session.commit()
            logger.info(f"[DB] Created admin user: {username}")
            return user.id
        except Exception:
            session.rollback()
            return None


def update_admin(admin_id: int, username: Optional[str] = None, password: Optional[str] = None) -> bool:
    with _get_session() as session:
        try:
            user = session.query(AdminUser).filter_by(id=admin_id).first()
            if not user:
                return False
            if username:
                user.username = username
            if password:
                user.password_hash = _hash_password(password)
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
            session.execute(text("DELETE FROM admin_sessions WHERE user_id = :uid"), {"uid": admin_id})
            result = session.execute(text("DELETE FROM admin_users WHERE id = :uid"), {"uid": admin_id})
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
            return {"id": user.id, "username": user.username, "created_at": user.created_at}
        return None
