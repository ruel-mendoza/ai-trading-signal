import os
import glob
import logging

logger = logging.getLogger("trading_engine.utils.system_monitor")

_UTILS_DIR = os.path.dirname(os.path.abspath(__file__))
_TRADING_ENGINE_DIR = os.path.dirname(_UTILS_DIR)
_PROJECT_ROOT = os.path.dirname(_TRADING_ENGINE_DIR)
DB_PATH = os.path.join(_TRADING_ENGINE_DIR, "trading_data.db")
BACKUP_DIR = os.path.join(_PROJECT_ROOT, "backups")

MAX_STORAGE_GB = float(os.environ.get("MAX_STORAGE_GB", "1.0"))


def _get_dir_size_bytes(path: str) -> int:
    total = 0
    if not os.path.isdir(path):
        return 0
    for entry in os.scandir(path):
        if entry.is_file(follow_symlinks=False):
            total += entry.stat().st_size
        elif entry.is_dir(follow_symlinks=False):
            total += _get_dir_size_bytes(entry.path)
    return total


def get_storage_stats() -> dict:
    db_size_bytes = 0
    if os.path.exists(DB_PATH):
        db_size_bytes = os.path.getsize(DB_PATH)
    db_size_mb = round(db_size_bytes / (1024 * 1024), 2)

    backup_size_bytes = _get_dir_size_bytes(BACKUP_DIR)
    backup_size_mb = round(backup_size_bytes / (1024 * 1024), 2)

    backup_count = len(glob.glob(os.path.join(BACKUP_DIR, "trading_data_*.db")))

    total_used_bytes = db_size_bytes + backup_size_bytes
    total_used_mb = round(total_used_bytes / (1024 * 1024), 2)
    max_storage_mb = MAX_STORAGE_GB * 1024
    used_percent = round((total_used_bytes / (MAX_STORAGE_GB * 1024 * 1024 * 1024)) * 100, 1) if MAX_STORAGE_GB > 0 else 0

    return {
        "database": {
            "path": os.path.basename(DB_PATH),
            "size_mb": db_size_mb,
        },
        "backups": {
            "directory": "backups/",
            "size_mb": backup_size_mb,
            "file_count": backup_count,
        },
        "total_used_mb": total_used_mb,
        "max_storage_gb": MAX_STORAGE_GB,
        "max_storage_mb": round(max_storage_mb, 1),
        "used_percent": used_percent,
    }
