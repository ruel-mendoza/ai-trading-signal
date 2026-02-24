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
    VALID_TIMEFRAMES,
)

logger = logging.getLogger("trading_engine.database")

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
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
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


def upsert_candles(asset: str, timeframe: str, candles: list[dict]):
    with _get_session() as session:
        try:
            for c in candles:
                ts = c.get("timestamp") or c.get("open_time")
                existing = session.query(Candle).filter_by(
                    asset=asset, timeframe=timeframe, timestamp=ts
                ).first()
                if existing:
                    existing.open = c["open"]
                    existing.high = c["high"]
                    existing.low = c["low"]
                    existing.close = c["close"]
                else:
                    session.add(Candle(
                        asset=asset,
                        timeframe=timeframe,
                        timestamp=ts,
                        open=c["open"],
                        high=c["high"],
                        low=c["low"],
                        close=c["close"],
                    ))
            session.commit()
            logger.debug(f"[DB] Upserted {len(candles)} candles for {asset}/{timeframe}")
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


def update_cache_metadata(asset: str, timeframe: str, last_candle_close: Optional[str] = None):
    now = datetime.utcnow().isoformat()
    with _get_session() as session:
        try:
            existing = session.query(CacheMetadata).filter_by(asset=asset, timeframe=timeframe).first()
            if existing:
                existing.last_fetched = now
                if last_candle_close is not None:
                    existing.last_candle_close = last_candle_close
            else:
                session.add(CacheMetadata(
                    asset=asset,
                    timeframe=timeframe,
                    last_fetched=now,
                    last_candle_close=last_candle_close,
                ))
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Update cache metadata failed: {e}")
            raise


def get_cache_metadata(asset: str, timeframe: str) -> Optional[dict]:
    with _get_session() as session:
        row = session.query(CacheMetadata).filter_by(asset=asset, timeframe=timeframe).first()
        if row:
            return {"last_fetched": row.last_fetched, "last_candle_close": row.last_candle_close}
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


def insert_signal(signal: dict) -> Optional[int]:
    with _get_session() as session:
        try:
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
            )
            session.add(obj)
            session.commit()
            logger.info(f"[DB] Inserted signal #{obj.id}: {signal['strategy_name']} {signal['direction']} {signal['asset']}")
            return obj.id
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Insert signal failed: {e}")
            return None


def get_active_signals(strategy_name: Optional[str] = None, asset: Optional[str] = None) -> list[dict]:
    with _get_session() as session:
        q = session.query(Signal).filter(Signal.status == "OPEN")
        if strategy_name:
            q = q.filter(Signal.strategy_name == strategy_name)
        if asset:
            q = q.filter(Signal.asset == asset)
        q = q.order_by(Signal.created_at.desc())
        rows = q.all()
        return [_signal_to_dict(r) for r in rows]


def close_signal(signal_id: int, exit_reason: str = "", exit_price: Optional[float] = None):
    with _get_session() as session:
        try:
            sig = session.query(Signal).filter_by(id=signal_id).first()
            if sig:
                sig.status = "CLOSED"
                sig.exit_reason = exit_reason or None
                if exit_price is not None:
                    sig.exit_price = exit_price
                session.commit()
                logger.info(f"[DB] Closed signal #{signal_id}: {exit_reason} | exit_price={exit_price}")
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Close signal failed: {e}")
            raise


def get_all_signals(strategy_name: Optional[str] = None, asset: Optional[str] = None, status: Optional[str] = None, limit: int = 100) -> list[dict]:
    with _get_session() as session:
        q = session.query(Signal)
        if strategy_name:
            q = q.filter(Signal.strategy_name == strategy_name)
        if asset:
            q = q.filter(Signal.asset == asset)
        if status:
            q = q.filter(Signal.status == status)
        q = q.order_by(Signal.created_at.desc()).limit(limit)
        rows = q.all()
        return [_signal_to_dict(r) for r in rows]


def _signal_to_dict(sig: Signal) -> dict:
    return {
        "id": sig.id,
        "asset": sig.asset,
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
    }


def open_position(position: dict) -> Optional[int]:
    with _get_session() as session:
        try:
            existing = session.query(OpenPosition).filter_by(
                asset=position["asset"],
                strategy_name=position["strategy_name"],
            ).first()
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
                atr_at_entry=position["atr_at_entry"],
                highest_price_since_entry=position["entry_price"] if position["direction"] == "BUY" else None,
                lowest_price_since_entry=position["entry_price"] if position["direction"] == "SELL" else None,
            )
            session.add(obj)
            session.commit()
            logger.info(f"[DB] Opened position #{obj.id}: {position['strategy_name']} {position['direction']} {position['asset']}")
            return obj.id
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Open position failed: {e}")
            return None


def get_open_position(strategy_name: str, asset: str) -> Optional[dict]:
    with _get_session() as session:
        pos = session.query(OpenPosition).filter_by(
            asset=asset, strategy_name=strategy_name
        ).first()
        if pos:
            return _position_to_dict(pos)
        return None


def get_all_open_positions(strategy_name: Optional[str] = None, asset: Optional[str] = None) -> list[dict]:
    with _get_session() as session:
        q = session.query(OpenPosition)
        if strategy_name:
            q = q.filter(OpenPosition.strategy_name == strategy_name)
        if asset:
            q = q.filter(OpenPosition.asset == asset)
        q = q.order_by(OpenPosition.opened_at.desc())
        rows = q.all()
        return [_position_to_dict(r) for r in rows]


def update_position_tracking(position_id: int, highest_price: Optional[float] = None, lowest_price: Optional[float] = None):
    with _get_session() as session:
        try:
            pos = session.query(OpenPosition).filter_by(id=position_id).first()
            if not pos:
                return
            if highest_price is not None:
                pos.highest_price_since_entry = max(pos.highest_price_since_entry or 0, highest_price)
            if lowest_price is not None:
                pos.lowest_price_since_entry = min(pos.lowest_price_since_entry or 999999, lowest_price)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Update position tracking failed: {e}")
            raise


def close_position(strategy_name: str, asset: str) -> bool:
    with _get_session() as session:
        try:
            pos = session.query(OpenPosition).filter_by(
                asset=asset, strategy_name=strategy_name
            ).first()
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
        pos = session.query(OpenPosition.id).filter_by(
            asset=asset, strategy_name=strategy_name
        ).first()
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
            session.add(APIUsageLog(
                endpoint=endpoint,
                credits_used=credits_used,
            ))
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Log API usage failed: {e}")


def get_api_usage_stats() -> dict:
    import calendar

    with _get_session() as session:
        now = datetime.utcnow()
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
        day_ago = (now - timedelta(days=1)).isoformat()

        monthly_total = session.execute(
            text("SELECT COALESCE(SUM(credits_used), 0) as total FROM api_usage_log WHERE timestamp >= :start"),
            {"start": month_start},
        ).scalar() or 0

        daily_total = session.execute(
            text("SELECT COALESCE(SUM(credits_used), 0) as total FROM api_usage_log WHERE timestamp >= :start"),
            {"start": day_ago},
        ).scalar() or 0

        by_endpoint_rows = session.execute(
            text("""
                SELECT endpoint, COUNT(*) as count, SUM(credits_used) as credits
                FROM api_usage_log WHERE timestamp >= :start
                GROUP BY endpoint ORDER BY credits DESC
            """),
            {"start": month_start},
        ).fetchall()
        by_endpoint = [{"endpoint": r[0], "count": r[1], "credits": r[2]} for r in by_endpoint_rows]

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
