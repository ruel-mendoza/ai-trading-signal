import os
import shutil
import glob
import logging
from datetime import datetime, timedelta

logger = logging.getLogger("trading_engine.utils.backup_manager")

_UTILS_DIR = os.path.dirname(os.path.abspath(__file__))
_TRADING_ENGINE_DIR = os.path.dirname(_UTILS_DIR)
_PROJECT_ROOT = os.path.dirname(_TRADING_ENGINE_DIR)
DB_PATH = os.path.join(_TRADING_ENGINE_DIR, "trading_data.db")
BACKUP_DIR = os.path.join(_PROJECT_ROOT, "backups")
RETENTION_DAYS = 7


def _ensure_backup_dir():
    os.makedirs(BACKUP_DIR, exist_ok=True)


def backup_database():
    _ensure_backup_dir()

    if not os.path.exists(DB_PATH):
        logger.error(f"[BACKUP] Database file not found: {DB_PATH}")
        return None

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"trading_data_{timestamp}.db"
    backup_path = os.path.join(BACKUP_DIR, backup_filename)

    try:
        shutil.copy2(DB_PATH, backup_path)
        size_mb = os.path.getsize(backup_path) / (1024 * 1024)
        logger.info(
            f"[BACKUP] Database backed up successfully: {backup_filename} "
            f"({size_mb:.2f} MB)"
        )
        return backup_path
    except Exception as e:
        logger.error(f"[BACKUP] Database backup FAILED: {e}")
        return None


def cleanup_old_backups():
    _ensure_backup_dir()

    cutoff = datetime.utcnow() - timedelta(days=RETENTION_DAYS)
    pattern = os.path.join(BACKUP_DIR, "trading_data_*.db")
    backup_files = glob.glob(pattern)

    removed = 0
    for filepath in sorted(backup_files):
        try:
            file_mtime = datetime.utcfromtimestamp(os.path.getmtime(filepath))
            if file_mtime < cutoff:
                os.remove(filepath)
                logger.info(
                    f"[BACKUP] Removed old backup: {os.path.basename(filepath)} "
                    f"(modified {file_mtime.strftime('%Y-%m-%d %H:%M')} UTC)"
                )
                removed += 1
        except Exception as e:
            logger.error(f"[BACKUP] Failed to remove {filepath}: {e}")

    remaining = len(glob.glob(pattern))
    logger.info(
        f"[BACKUP] Cleanup complete: {removed} old backup(s) removed, "
        f"{remaining} backup(s) retained"
    )
    return removed


def run_daily_backup():
    logger.info("[BACKUP] ====== Daily backup job started ======")

    backup_path = backup_database()
    if backup_path:
        removed = cleanup_old_backups()
        logger.info(
            f"[BACKUP] ====== Daily backup job complete | "
            f"backup={os.path.basename(backup_path)} | "
            f"cleaned={removed} old file(s) ======"
        )
    else:
        logger.error("[BACKUP] ====== Daily backup job FAILED ======")
