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


def _migrate_schema():
    migrations = [
        ("signals", "exit_price", "REAL"),
        ("signals", "exit_reason", "TEXT"),
        ("admin_users", "role", "TEXT DEFAULT 'CUSTOMER'"),
        ("admin_users", "email", "TEXT"),
        ("admin_users", "full_name", "TEXT"),
    ]
    with engine.connect() as conn:
        for table, column, col_type in migrations:
            try:
                conn.execute(text(f"SELECT {column} FROM {table} LIMIT 1"))
            except Exception:
                logger.info(f"[DB] MIGRATE: Adding column {table}.{column} ({col_type})")
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}"))
                conn.commit()

        try:
            result = conn.execute(text(
                "SELECT id FROM admin_users WHERE role IS NULL"
            )).fetchall()
            if result:
                conn.execute(text("UPDATE admin_users SET role = 'ADMIN' WHERE role IS NULL"))
                conn.commit()
                logger.info(f"[DB] MIGRATE: Set {len(result)} existing user(s) to ADMIN role")
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
                p_del = session.query(OpenPosition).filter(OpenPosition.asset == sym).delete()
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


def init_db():
    logger.info("[DB] Initializing database tables via SQLAlchemy...")
    Base.metadata.create_all(engine)
    logger.info("[DB] All tables created/verified")

    _migrate_schema()

    with _get_session() as session:
        _seed_default_admin(session)

    _purge_unsupported_symbols()

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
            existing = session.query(Signal.id).filter_by(
                strategy_name=signal["strategy_name"],
                asset=signal["asset"],
                signal_timestamp=signal["signal_timestamp"],
            ).first()
            if existing:
                logger.warning(
                    f"[DB] IDEMPOTENCY | Signal already exists for "
                    f"{signal['strategy_name']}/{signal['asset']}/{signal['signal_timestamp']} "
                    f"(id={existing[0]}) — insert blocked"
                )
                return None

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
                logger.warning(f"[DB] update_signal_wp_fields: Signal #{signal_id} not found")
                return
            for key in ("wp_post_id", "publish_status", "wp_last_sync"):
                if key in fields:
                    setattr(sig, key, fields[key])
            session.commit()
            logger.info(f"[DB] Updated WP fields for signal #{signal_id}: {fields}")
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] update_signal_wp_fields failed for #{signal_id}: {e}")


def get_all_signals(strategy_name: Optional[str] = None, asset: Optional[str] = None, status: Optional[str] = None, limit: int = 100) -> list[dict]:
    with _get_session() as session:
        from sqlalchemy import func, or_

        latest_closed_subq = session.query(
            func.max(Signal.id).label("max_id")
        ).filter(Signal.status == "CLOSED")
        if strategy_name:
            latest_closed_subq = latest_closed_subq.filter(Signal.strategy_name == strategy_name)
        if asset:
            latest_closed_subq = latest_closed_subq.filter(Signal.asset == asset)
        latest_closed_subq = latest_closed_subq.group_by(
            Signal.strategy_name, Signal.asset, Signal.direction
        ).subquery()

        q = session.query(Signal)
        if strategy_name:
            q = q.filter(Signal.strategy_name == strategy_name)
        if asset:
            q = q.filter(Signal.asset == asset)

        if status == "CLOSED":
            q = q.filter(Signal.id.in_(
                session.query(latest_closed_subq.c.max_id)
            ))
        elif status == "OPEN":
            q = q.filter(Signal.status == "OPEN")
        else:
            q = q.filter(or_(
                Signal.status == "OPEN",
                Signal.id.in_(session.query(latest_closed_subq.c.max_id)),
            ))

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
        "wp_post_id": sig.wp_post_id,
        "publish_status": sig.publish_status,
        "wp_last_sync": sig.wp_last_sync,
    }


def open_position(position: dict) -> Optional[int]:
    atr_value = position.get("atr_at_entry")
    if not atr_value or atr_value <= 0:
        logger.error(
            f"[DB] STATE LOCK VIOLATION: open_position() called without valid atr_at_entry "
            f"for {position.get('asset')}/{position.get('strategy_name')} — "
            f"ATR must be calculated once at entry and stored. Got: {atr_value}"
        )
        return None

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
                atr_at_entry=atr_value,
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
        session.add(AdminUser(username="admin", password_hash=pw_hash, role="ADMIN"))
        session.commit()
        logger.info("[DB] Seeded default admin user")


def authenticate_admin(username: str, password: str) -> Optional[dict]:
    with _get_session() as session:
        user = session.query(AdminUser).filter_by(username=username).first()
        if user and _verify_password(password, user.password_hash):
            return {"id": user.id, "username": user.username, "role": user.role or "CUSTOMER", "created_at": user.created_at}
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
        return [{"id": r.id, "username": r.username, "role": r.role or "CUSTOMER", "created_at": r.created_at} for r in rows]


def get_user_by_username(username: str) -> Optional[dict]:
    with _get_session() as session:
        user = session.query(AdminUser).filter_by(username=username).first()
        if user:
            return {"id": user.id, "username": user.username, "email": user.email, "role": user.role or "CUSTOMER", "created_at": user.created_at}
        return None


def get_user_by_email(email: str) -> Optional[dict]:
    with _get_session() as session:
        user = session.query(AdminUser).filter(AdminUser.email == email).first()
        if user:
            return {"id": user.id, "username": user.username, "email": user.email, "role": user.role or "CUSTOMER", "created_at": user.created_at}
        return None


def create_admin(username: str, password: str, email: Optional[str] = None, full_name: Optional[str] = None, role: str = "CUSTOMER") -> Optional[int]:
    pw_hash = _hash_password(password)
    with _get_session() as session:
        try:
            user = AdminUser(username=username, password_hash=pw_hash, email=email, full_name=full_name, role=role)
            session.add(user)
            session.commit()
            logger.info(f"[DB] Created user: {username} (role={role})")
            return user.id
        except Exception:
            session.rollback()
            return None


def update_admin(admin_id: int, username: Optional[str] = None, password: Optional[str] = None, role: Optional[str] = None) -> bool:
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
            return {"id": user.id, "username": user.username, "role": user.role or "CUSTOMER", "created_at": user.created_at}
        return None


def _hash_api_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()


def create_partner_api_key(label: str, tier: str = "standard", rate_limit: int = 120, created_by: int = None) -> dict:
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
            logger.info(f"[DB] Created partner API key id={rec.id} label={label} tier={tier}")
            return {"id": rec.id, "key": raw_key, "label": label, "tier": tier, "rate_limit_per_minute": rate_limit}
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] Create partner API key failed: {e}")
            return {}


def validate_partner_api_key(raw_key: str) -> Optional[dict]:
    key_hash = _hash_api_key(raw_key)
    with _get_session() as session:
        rec = session.query(PartnerApiKey).filter_by(key_hash=key_hash, is_active=1).first()
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


def finish_job_log(log_id: int, status: str, assets_evaluated: int = 0,
                   signals_generated: int = 0, errors: int = 0,
                   error_detail: Optional[str] = None):
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
            session.query(SchedulerJobLog)
            .order_by(SchedulerJobLog.id.desc())
            .first()
        )
        failed_24h = (
            session.query(SchedulerJobLog)
            .filter(
                SchedulerJobLog.status.in_(["FAILED", "PARTIAL"]),
                SchedulerJobLog.started_at >= (datetime.utcnow() - timedelta(hours=24)).isoformat(),
            )
            .count()
        )
        success_24h = (
            session.query(SchedulerJobLog)
            .filter(
                SchedulerJobLog.status == "SUCCESS",
                SchedulerJobLog.started_at >= (datetime.utcnow() - timedelta(hours=24)).isoformat(),
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
            for period, cutoff in [("all_time", None), ("7d", cutoff_7d), ("30d", cutoff_30d)]:
                if cutoff and (ts_dt is None or ts_dt < cutoff):
                    continue

                for key in [(s.strategy_name, None, period), (s.strategy_name, s.asset, period)]:
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
                        t_open = datetime.fromisoformat(s.signal_timestamp.replace("Z", ""))
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
                session.add(SignalMetrics(
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
                ))
            count += 1

        session.commit()
        logger.info(f"[METRICS] Computed {count} metric rows from {len(all_signals)} signals")
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
        rows = session.query(UserCmsConfig).filter_by(user_id=user_id).order_by(UserCmsConfig.id).all()
        return [_user_cms_to_dict(r) for r in rows]


def get_all_user_cms_configs(user_id: Optional[int] = None) -> list[dict]:
    with _get_session() as session:
        q = (
            session.query(UserCmsConfig, AdminUser.username)
            .join(AdminUser, UserCmsConfig.user_id == AdminUser.id)
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
            logger.info(f"[DB] Created user CMS config #{obj.id} for user_id={data['user_id']}")
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


def update_user_cms_config(config_id: int, data: dict, user_id: Optional[int] = None) -> bool:
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


def get_user_cms_config_decrypted(config_id: int, user_id: Optional[int] = None) -> Optional[dict]:
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
                results.append({
                    "id": row.id,
                    "user_id": row.user_id,
                    "site_url": row.site_url,
                    "wp_username": row.wp_username,
                    "app_password": decrypt(row.encrypted_app_password),
                })
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
        q = session.query(SignalCmsPost).filter_by(signal_id=signal_id, cms_config_id=cms_config_id)
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
            row = session.query(SignalCmsPost).filter_by(
                signal_id=signal_id, cms_config_id=cms_config_id
            ).first()
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
            logger.error(f"[DB] upsert_signal_cms_post failed signal={signal_id} config={cms_config_id}: {e}")


def get_signal_cms_posts_for_signal(signal_id: int) -> list[dict]:
    with _get_session() as session:
        rows = session.query(SignalCmsPost).filter_by(signal_id=signal_id).all()
        return [{
            "id": r.id,
            "signal_id": r.signal_id,
            "cms_config_id": r.cms_config_id,
            "wp_post_id": r.wp_post_id,
            "publish_status": r.publish_status,
            "last_sync": r.last_sync,
        } for r in rows]


def upsert_strategy_execution_log(strategy_name: str, status: str):
    now_iso = datetime.utcnow().isoformat()
    with _get_session() as session:
        try:
            row = session.query(StrategyExecutionLog).filter_by(
                strategy_name=strategy_name
            ).order_by(StrategyExecutionLog.id.desc()).first()
            if row and row.status == status:
                row.last_run_at = now_iso
            else:
                session.add(StrategyExecutionLog(
                    strategy_name=strategy_name,
                    last_run_at=now_iso,
                    status=status,
                ))
            session.commit()
            logger.debug(f"[DB] strategy_execution_log upserted: {strategy_name} = {status}")
        except Exception as e:
            session.rollback()
            logger.error(f"[DB] upsert_strategy_execution_log failed: {e}")


def get_last_successful_execution(strategy_name: str) -> Optional[dict]:
    with _get_session() as session:
        row = session.query(StrategyExecutionLog).filter_by(
            strategy_name=strategy_name, status="SUCCESS"
        ).order_by(StrategyExecutionLog.id.desc()).first()
        if not row:
            return None
        return {
            "id": row.id,
            "strategy_name": row.strategy_name,
            "last_run_at": row.last_run_at,
            "status": row.status,
        }
