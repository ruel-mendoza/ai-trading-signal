"""
staging_sync.py — Copy live SQLite DB into a sanitised staging copy for testing.

Usage:
  python3 trading_engine/utils/staging_sync.py
  python3 trading_engine/utils/staging_sync.py --strategies mtf_ema trend_forex
  python3 trading_engine/utils/staging_sync.py --max-age-days 30 --include-candles
  python3 trading_engine/utils/staging_sync.py --dry-run
"""

import hashlib
import logging
import os
import secrets
import shutil
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import create_engine, text

logger = logging.getLogger("trading_engine.utils.staging_sync")

# ── Path constants (mirrors backup_manager.py pattern) ──────────────────────
_UTILS_DIR = os.path.dirname(os.path.abspath(__file__))
_TRADING_ENGINE_DIR = os.path.dirname(_UTILS_DIR)

LIVE_DB_PATH = os.path.join(_TRADING_ENGINE_DIR, "trading_data.db")
STAGING_DB_PATH = os.path.join(_TRADING_ENGINE_DIR, "trading_data_staging.db")


# ── Standalone password hasher (same algorithm as database._hash_password) ──
def _hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100000).hex()
    return f"{salt}:{h}"


# ────────────────────────────────────────────────────────────────────────────
def sync_staging_from_live(
    strategies: Optional[list[str]] = None,
    max_age_days: int = 92,
    include_candles: bool = False,
    dry_run: bool = False,
) -> dict:
    """
    Copy the live database to a staging path and sanitise sensitive fields.

    Parameters
    ----------
    strategies    : Limit signals/positions to these strategy names. None = all.
    max_age_days  : Delete signals older than this many days from the staging copy.
    include_candles: Keep the candles table in staging. Default False (table is cleared).
    dry_run       : Report what would be done without writing anything.
    """
    logger.info(
        "[STAGING-SYNC] ====== Starting staging sync | "
        f"strategies={strategies} max_age_days={max_age_days} "
        f"include_candles={include_candles} dry_run={dry_run} ======"
    )

    if not os.path.exists(LIVE_DB_PATH):
        raise FileNotFoundError(f"[STAGING-SYNC] Live database not found: {LIVE_DB_PATH}")

    live_size_mb = os.path.getsize(LIVE_DB_PATH) / (1024 * 1024)
    logger.info(f"[STAGING-SYNC] Live DB: {LIVE_DB_PATH} ({live_size_mb:.2f} MB)")

    # ── Dry-run: compute counts without writing ──────────────────────────────
    if dry_run:
        from sqlalchemy import create_engine as _ce
        live_engine = _ce(f"sqlite:///{LIVE_DB_PATH}", connect_args={"check_same_thread": False})
        with live_engine.connect() as conn:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
            sig_q = "SELECT COUNT(*) FROM signals WHERE signal_timestamp >= :cutoff"
            params: dict = {"cutoff": cutoff}
            if strategies:
                placeholders = ",".join(f":s{i}" for i in range(len(strategies)))
                sig_q += f" AND strategy_name IN ({placeholders})"
                for i, s in enumerate(strategies):
                    params[f"s{i}"] = s
            sigs = conn.execute(text(sig_q), params).scalar() or 0

            pos_q = "SELECT COUNT(*) FROM open_positions"
            pos_params: dict = {}
            if strategies:
                placeholders = ",".join(f":ps{i}" for i in range(len(strategies)))
                pos_q += f" WHERE strategy_name IN ({placeholders})"
                for i, s in enumerate(strategies):
                    pos_params[f"ps{i}"] = s
            pos = conn.execute(text(pos_q), pos_params).scalar() or 0

            candles = 0
            if include_candles:
                candles = conn.execute(text("SELECT COUNT(*) FROM candles")).scalar() or 0

        live_engine.dispose()
        summary = {
            "source": os.path.basename(LIVE_DB_PATH),
            "destination": os.path.basename(STAGING_DB_PATH),
            "signals_copied": sigs,
            "positions_copied": pos,
            "candles_copied": candles,
            "rows_sanitised": 0,
            "dry_run": True,
            "completed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        logger.info(f"[STAGING-SYNC] DRY-RUN summary: {summary}")
        return summary

    # ── 1. Copy live DB → staging ────────────────────────────────────────────
    logger.info(f"[STAGING-SYNC] Copying {LIVE_DB_PATH} → {STAGING_DB_PATH}")
    shutil.copy2(LIVE_DB_PATH, STAGING_DB_PATH)
    staging_size_mb = os.path.getsize(STAGING_DB_PATH) / (1024 * 1024)
    logger.info(f"[STAGING-SYNC] Copy complete ({staging_size_mb:.2f} MB)")

    staging_engine = create_engine(
        f"sqlite:///{STAGING_DB_PATH}",
        connect_args={"check_same_thread": False},
    )

    rows_sanitised = 0
    signals_copied = 0
    positions_copied = 0
    candles_copied = 0

    with staging_engine.begin() as conn:

        # ── 2. Apply signal filters ──────────────────────────────────────────
        cutoff = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        # Remove signals older than max_age_days
        old_del = conn.execute(
            text("DELETE FROM signals WHERE signal_timestamp < :cutoff"),
            {"cutoff": cutoff},
        )
        logger.info(
            f"[STAGING-SYNC] Removed {old_del.rowcount} signals older than {max_age_days} days"
        )

        # Remove signals not in requested strategies
        if strategies:
            placeholders = ",".join(f":s{i}" for i in range(len(strategies)))
            params = {f"s{i}": s for i, s in enumerate(strategies)}
            strat_del = conn.execute(
                text(f"DELETE FROM signals WHERE strategy_name NOT IN ({placeholders})"),
                params,
            )
            logger.info(
                f"[STAGING-SYNC] Removed {strat_del.rowcount} signals not in requested strategies"
            )

            pos_params = {f"ps{i}": s for i, s in enumerate(strategies)}
            pos_placeholders = ",".join(f":ps{i}" for i in range(len(strategies)))
            conn.execute(
                text(
                    f"DELETE FROM open_positions WHERE strategy_name NOT IN ({pos_placeholders})"
                ),
                pos_params,
            )

        # ── 3. Remove candles if not requested ──────────────────────────────
        if not include_candles:
            candles_del = conn.execute(text("DELETE FROM candles"))
            logger.info(f"[STAGING-SYNC] Cleared {candles_del.rowcount} candle rows")
        else:
            candles_copied = conn.execute(text("SELECT COUNT(*) FROM candles")).scalar() or 0
            logger.info(f"[STAGING-SYNC] Keeping {candles_copied} candle rows")

        # ── 4. Count remaining signals and positions ─────────────────────────
        signals_copied = conn.execute(text("SELECT COUNT(*) FROM signals")).scalar() or 0
        positions_copied = (
            conn.execute(text("SELECT COUNT(*) FROM open_positions")).scalar() or 0
        )
        logger.info(
            f"[STAGING-SYNC] Remaining: signals={signals_copied} positions={positions_copied}"
        )

        # ── 5. Sanitise sensitive data ───────────────────────────────────────

        # Clear fcsapi_key in app_settings
        r = conn.execute(
            text("UPDATE app_settings SET value = '' WHERE key = 'fcsapi_key'")
        )
        rows_sanitised += r.rowcount
        logger.info(f"[STAGING-SYNC] Cleared fcsapi_key ({r.rowcount} row)")

        # Clear webhook_url in app_settings
        r = conn.execute(
            text("UPDATE app_settings SET value = '' WHERE key = 'webhook_url'")
        )
        rows_sanitised += r.rowcount
        logger.info(f"[STAGING-SYNC] Cleared webhook_url ({r.rowcount} row)")

        # Clear all admin sessions
        r = conn.execute(text("DELETE FROM admin_sessions"))
        rows_sanitised += r.rowcount
        logger.info(f"[STAGING-SYNC] Cleared {r.rowcount} admin_sessions rows")

        # Reset all admin_users passwords to hash of "staging123"
        staging_hash = _hash_password("staging123")
        user_rows = conn.execute(text("SELECT id FROM admin_users")).fetchall()
        for row in user_rows:
            conn.execute(
                text("UPDATE admin_users SET password_hash = :ph WHERE id = :id"),
                {"ph": staging_hash, "id": row[0]},
            )
        rows_sanitised += len(user_rows)
        logger.info(
            f"[STAGING-SYNC] Reset passwords for {len(user_rows)} admin_users → 'staging123'"
        )

        # Clear encrypted_app_password in user_cms_configs
        r = conn.execute(
            text("UPDATE user_cms_configs SET encrypted_app_password = ''")
        )
        rows_sanitised += r.rowcount
        logger.info(
            f"[STAGING-SYNC] Cleared encrypted_app_password in {r.rowcount} user_cms_configs rows"
        )

    staging_engine.dispose()
    logger.info(
        f"[STAGING-SYNC] Staging VACUUM starting"
    )
    # VACUUM outside a transaction (SQLite requirement)
    vac_engine = create_engine(
        f"sqlite:///{STAGING_DB_PATH}",
        connect_args={"check_same_thread": False},
    )
    with vac_engine.connect() as conn:
        conn.execute(text("VACUUM"))
    vac_engine.dispose()
    logger.info("[STAGING-SYNC] VACUUM complete")

    summary = {
        "source": os.path.basename(LIVE_DB_PATH),
        "destination": os.path.basename(STAGING_DB_PATH),
        "signals_copied": signals_copied,
        "positions_copied": positions_copied,
        "candles_copied": candles_copied,
        "rows_sanitised": rows_sanitised,
        "dry_run": False,
        "completed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    logger.info(f"[STAGING-SYNC] ====== Complete | {summary} ======")
    return summary


# ── CLI entry point ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    # Ensure project root is on path so trading_engine imports resolve
    sys.path.insert(0, os.path.dirname(os.path.dirname(_TRADING_ENGINE_DIR)))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Sync live trading DB to a sanitised staging copy.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 trading_engine/utils/staging_sync.py
  python3 trading_engine/utils/staging_sync.py --strategies mtf_ema trend_forex
  python3 trading_engine/utils/staging_sync.py --max-age-days 30 --include-candles
  python3 trading_engine/utils/staging_sync.py --dry-run
        """,
    )
    parser.add_argument(
        "--strategies",
        nargs="+",
        metavar="STRATEGY",
        default=None,
        help="Only sync signals/positions for these strategy names.",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=92,
        metavar="N",
        help="Only keep signals newer than N days (default: 92).",
    )
    parser.add_argument(
        "--include-candles",
        action="store_true",
        default=False,
        help="Include candle data in the staging copy (can be large).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print what would be copied without writing anything.",
    )

    args = parser.parse_args()

    result = sync_staging_from_live(
        strategies=args.strategies,
        max_age_days=args.max_age_days,
        include_candles=args.include_candles,
        dry_run=args.dry_run,
    )

    print("\n── Staging Sync Result ──────────────────────────────")
    for k, v in result.items():
        print(f"  {k:<20} {v}")
    print("─────────────────────────────────────────────────────\n")
