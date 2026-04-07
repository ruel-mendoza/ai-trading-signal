#!/usr/bin/env python3
"""
restore_closed_signals.py — Insert specific closed signals into the staging (or live) database
to match the live site signal history.

Usage:
  python3 restore_closed_signals.py                          # insert into staging DB
  python3 restore_closed_signals.py --dry-run                # preview only
  python3 restore_closed_signals.py --force                  # skip confirmation
  python3 restore_closed_signals.py --db-path trading_engine/trading_data.db  # target live DB
"""

import sys
import os
import argparse
import logging

# Ensure project root is on path so trading_engine imports resolve
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("restore_closed_signals")

# ---------------------------------------------------------------------------
# Signals to restore
# ---------------------------------------------------------------------------
SIGNALS_TO_RESTORE = [
    {
        "strategy_name": "trend_non_forex",
        "asset": "USO",
        "direction": "BUY",
        "entry_price": 139.16000,
        "stop_loss": 128.92046,
        "take_profit": None,
        "atr_at_entry": None,
        "signal_timestamp": "2026-04-06T16:01:00",
        "status": "CLOSED",
        "exit_price": 138.94000,
        "exit_reason": "Manual restore — matched from live site",
        "asset_class": "forex",
    },
    {
        "strategy_name": "trend_non_forex",
        "asset": "UGA",
        "direction": "BUY",
        "entry_price": 106.86000,
        "stop_loss": 99.95472,
        "take_profit": None,
        "atr_at_entry": None,
        "signal_timestamp": "2026-04-06T16:01:00",
        "status": "CLOSED",
        "exit_price": 106.50000,
        "exit_reason": "Manual restore — matched from live site",
        "asset_class": "forex",
    },
]

# Default DB paths
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
_STAGING_DB = os.path.join(_PROJECT_ROOT, "trading_engine", "trading_data_staging.db")
_LIVE_DB = os.path.join(_PROJECT_ROOT, "trading_engine", "trading_data.db")


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------
def print_signal_summary(sig: dict, full_name: str):
    print()
    print("─" * 49)
    print(f"  Asset      : {sig['asset']}")
    print(f"  Full Name  : {full_name}")
    print(f"  Direction  : {sig['direction']}")
    print(f"  Entry      : {sig['entry_price']:.5f}")
    print(f"  Stop Loss  : {sig['stop_loss']:.5f}")
    print(f"  Exit Price : {sig['exit_price']:.5f}")
    print(f"  Status     : {sig['status']}")
    print(f"  Timestamp  : {sig['signal_timestamp']}")
    print("─" * 49)


# ---------------------------------------------------------------------------
# Core restore logic
# ---------------------------------------------------------------------------
def run_restore(db_path: str, dry_run: bool, force: bool) -> int:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker

    if not os.path.exists(db_path):
        log.error("Database not found: %s", db_path)
        return 1

    log.info("=" * 60)
    log.info("AI Signals — Restore Closed Signals")
    log.info("Target DB  : %s", db_path)
    log.info("Dry run    : %s", dry_run)
    log.info("=" * 60)

    # Resolve full names from _ASSET_NAME_MAP
    from trading_engine.database import _ASSET_NAME_MAP

    # Build a dedicated engine/session for the target DB
    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
    )
    Session = sessionmaker(bind=engine, expire_on_commit=False)

    # Import Signal model — it is engine-independent (uses metadata)
    from trading_engine.models import Signal

    inserted = 0
    skipped = 0

    for sig in SIGNALS_TO_RESTORE:
        asset = sig["asset"]
        strategy = sig["strategy_name"]
        ts = sig["signal_timestamp"]
        full_name = _ASSET_NAME_MAP.get(asset, asset)

        print_signal_summary(sig, full_name)

        if dry_run:
            print("  [DRY RUN] Would insert — no changes written.")
            log.info("[DRY RUN] %s / %s @ %s — would insert", strategy, asset, ts)
            continue

        # Check for existing signal against the TARGET db
        with engine.connect() as conn:
            existing = conn.execute(
                text(
                    "SELECT id FROM signals "
                    "WHERE strategy_name = :s AND asset = :a AND signal_timestamp = :t"
                ),
                {"s": strategy, "a": asset, "t": ts},
            ).fetchone()

        if existing:
            log.warning(
                "Signal already exists for %s / %s @ %s (id=%s) — skipping.",
                strategy, asset, ts, existing[0],
            )
            print(f"  ⚠ Skipped — signal id={existing[0]} already exists.")
            skipped += 1
            continue

        # Confirmation prompt (once, before first real write)
        if not force and inserted == 0 and skipped == 0:
            answer = input(
                f"\nRestore {len(SIGNALS_TO_RESTORE)} closed signal(s) into {db_path}? [y/N]: "
            ).strip().lower()
            if answer not in ("y", "yes"):
                print("Restore cancelled.")
                return 0

        # Insert via Signal model
        with Session() as session:
            try:
                row = Signal(
                    asset=asset,
                    strategy_name=strategy,
                    direction=sig["direction"],
                    entry_price=sig["entry_price"],
                    stop_loss=sig["stop_loss"],
                    take_profit=sig.get("take_profit"),
                    atr_at_entry=sig.get("atr_at_entry"),
                    status=sig["status"],
                    exit_price=sig.get("exit_price"),
                    exit_reason=sig.get("exit_reason"),
                    signal_timestamp=ts,
                    asset_class=sig.get("asset_class", "other"),
                    full_name=full_name,
                    publish_status="PENDING",
                )
                session.add(row)
                session.commit()
                session.refresh(row)
                signal_id = row.id
                log.info(
                    "Inserted closed signal id=%s | %s %s %s @ %s",
                    signal_id, strategy, sig["direction"], asset, sig["entry_price"],
                )
                print(f"  ✓ Inserted signal id={signal_id}")
                inserted += 1
            except Exception as exc:
                session.rollback()
                log.error("Failed to insert %s / %s: %s", strategy, asset, exc)
                print(f"  ✗ Insert failed: {exc}")

    engine.dispose()

    # Summary line
    print()
    if dry_run:
        log.info("DRY RUN complete — %d signal(s) would be inserted.", len(SIGNALS_TO_RESTORE))
        print(f"DRY RUN — {len(SIGNALS_TO_RESTORE)} signal(s) would be inserted.")
        return 0

    log.info("Restore complete | inserted=%d skipped=%d", inserted, skipped)
    print(f"Done — inserted={inserted}  skipped={skipped}")

    # Invalidate API TTL cache so signals appear immediately
    if inserted > 0:
        try:
            from trading_engine.api_v1 import invalidate_signal_caches
            count = invalidate_signal_caches()
            cache_msg = f"✓ API cache invalidated ({count} entries cleared)"
            log.info(cache_msg)
            print(cache_msg)
        except Exception as exc:
            log.debug("Cache invalidation skipped (engine not running): %s", exc)
            print("  API cache: not invalidated (engine offline — will refresh on next request)")

    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Insert specific closed signals into the staging or live database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--db-path",
        default=_STAGING_DB,
        metavar="PATH",
        help=(
            f"Path to the target SQLite database "
            f"(default: trading_engine/trading_data_staging.db). "
            f"Pass trading_engine/trading_data.db to target the live DB."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview what would be inserted without writing anything.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Skip the confirmation prompt.",
    )
    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    sys.exit(run_restore(
        db_path=args.db_path,
        dry_run=args.dry_run,
        force=args.force,
    ))
