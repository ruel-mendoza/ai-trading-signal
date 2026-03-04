import logging
from datetime import datetime, timedelta

from sqlalchemy import text

from trading_engine.database import SessionFactory, engine

logger = logging.getLogger("trading_engine.utils.storage_manager")

SAFE_STATUSES = ("CLOSED", "EXPIRED")


def purge_signals(days_threshold: int) -> dict:
    if days_threshold < 1:
        raise ValueError("days_threshold must be at least 1")

    cutoff = (datetime.utcnow() - timedelta(days=days_threshold)).isoformat()
    logger.info(
        f"[PURGE] Starting signal purge: threshold={days_threshold} days, "
        f"cutoff={cutoff}, safe_statuses={SAFE_STATUSES}"
    )

    session = SessionFactory()
    try:
        eligible_count = session.execute(
            text(
                "SELECT COUNT(*) FROM signals "
                "WHERE status IN ('CLOSED', 'EXPIRED') AND created_at < :cutoff"
            ),
            {"cutoff": cutoff},
        ).scalar() or 0

        if eligible_count == 0:
            logger.info("[PURGE] No eligible signals found for purging")
            return {
                "purged": 0,
                "days_threshold": days_threshold,
                "cutoff_date": cutoff,
                "vacuumed": False,
            }

        result = session.execute(
            text(
                "DELETE FROM signals "
                "WHERE status IN ('CLOSED', 'EXPIRED') AND created_at < :cutoff"
            ),
            {"cutoff": cutoff},
        )
        deleted = result.rowcount
        session.commit()

        logger.info(f"[PURGE] Deleted {deleted} signal(s) older than {days_threshold} days")

    except Exception as e:
        session.rollback()
        logger.error(f"[PURGE] Signal purge FAILED: {e}")
        raise
    finally:
        session.close()

    vacuumed = False
    try:
        with engine.connect() as conn:
            conn.execute(text("VACUUM"))
            conn.execute(text("REINDEX"))
        vacuumed = True
        logger.info("[PURGE] VACUUM and REINDEX completed successfully")
    except Exception as ve:
        logger.warning(f"[PURGE] VACUUM/REINDEX skipped: {ve}")

    return {
        "purged": deleted,
        "days_threshold": days_threshold,
        "cutoff_date": cutoff,
        "vacuumed": vacuumed,
    }
