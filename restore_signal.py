#!/usr/bin/env python3
"""
restore_signal.py — Standalone CLI tool to manually restore a live trade that
exists in the trader's account but is missing from the engine database.

This happens when Replit sleeps mid-transaction, open_positions is cleared on
restart and the orphan handler closes the signal, or the DB is reset between
deployments.

Usage examples:

  # BTC/USD SHORT — restore missing signal
  python3 restore_signal.py \\
    --asset "BTC/USD" \\
    --direction SELL \\
    --entry 70960.65 \\
    --stop-loss 71967.22 \\
    --take-profit 67121.657 \\
    --strategy mtf_ema \\
    --timestamp "2026-03-25T12:00:00"

  # GBP/USD SHORT — already in DB, blocked by safety check
  python3 restore_signal.py \\
    --asset "GBP/USD" \\
    --direction SELL \\
    --entry 1.33707 \\
    --stop-loss 1.34464 \\
    --take-profit 1.32279 \\
    --strategy mtf_ema \\
    --timestamp "2026-03-25T10:00:00"

  # Dry run first to verify before writing
  python3 restore_signal.py \\
    --asset "BTC/USD" \\
    --direction SELL \\
    --entry 70960.65 \\
    --stop-loss 71967.22 \\
    --strategy mtf_ema \\
    --dry-run

  # Force skip confirmation
  python3 restore_signal.py \\
    --asset "BTC/USD" \\
    --direction SELL \\
    --entry 70960.65 \\
    --stop-loss 71967.22 \\
    --strategy mtf_ema \\
    --force
"""

import sys
import os
import argparse
import logging
from datetime import datetime

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
log = logging.getLogger("restore_signal")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
KNOWN_STRATEGIES = [
    "mtf_ema",
    "trend_forex",
    "trend_non_forex",
    "sp500_momentum",
    "highest_lowest_fx",
]

SL_MULT_MAP: dict[str, float] = {
    "mtf_ema":           0.5,
    "trend_forex":       3.0,
    "trend_non_forex":   3.0,
    "sp500_momentum":    2.0,
    "highest_lowest_fx": 2.0,
}

EXIT_RULES: dict[str, str] = {
    "mtf_ema":           "Exit rule: H1 close crosses H1 EMA20, or trailing stop (peak/trough - 2×ATR)",
    "trend_forex":       "Exit rule: Dynamic trailing stop 3×ATR ratcheted at 5:01 PM ET daily",
    "trend_non_forex":   "Exit rule: Dynamic trailing stop 3×ATR ratcheted at 4:01 PM ET daily",
    "sp500_momentum":    "Exit rule: RSI(20) drops below 70 during ARCA session",
    "highest_lowest_fx": "Exit rule: Stop loss 2×ATR or 6-hour time exit",
}


# ---------------------------------------------------------------------------
# ATR derivation
# ---------------------------------------------------------------------------

def derive_atr(entry: float, stop_loss: float, strategy: str) -> float:
    mult = SL_MULT_MAP[strategy]
    sl_distance = abs(entry - stop_loss)
    atr = round(sl_distance / mult, 6)
    log.info(
        "Auto-derived atr_at_entry = %s  (sl_distance=%s / SL_MULT=%s)",
        atr, round(sl_distance, 6), mult,
    )
    return atr


# ---------------------------------------------------------------------------
# Safety checks
# ---------------------------------------------------------------------------

def run_safety_checks(
    asset: str,
    direction: str,
    entry: float,
    stop_loss: float,
    take_profit: float | None,
    strategy: str,
    timestamp: str,
    force: bool,
) -> bool:
    """
    Returns True if safe to proceed, False if should abort.
    Logs all warnings; exits on fatal errors unless --force is set.
    """
    from trading_engine.database import get_active_signals, has_open_position, signal_exists

    ok = True

    # --- Price logic validation ---
    if direction == "BUY":
        if stop_loss >= entry:
            msg = f"BUY signal: stop_loss ({stop_loss}) must be BELOW entry ({entry})"
            if force:
                log.warning("Price logic WARNING (--force): %s", msg)
            else:
                log.error("Price logic FAILED: %s", msg)
                ok = False

        if take_profit is not None and take_profit <= entry:
            msg = f"BUY signal: take_profit ({take_profit}) must be ABOVE entry ({entry})"
            if force:
                log.warning("Price logic WARNING (--force): %s", msg)
            else:
                log.error("Price logic FAILED: %s", msg)
                ok = False

    elif direction == "SELL":
        if stop_loss <= entry:
            msg = f"SELL signal: stop_loss ({stop_loss}) must be ABOVE entry ({entry})"
            if force:
                log.warning("Price logic WARNING (--force): %s", msg)
            else:
                log.error("Price logic FAILED: %s", msg)
                ok = False

        if take_profit is not None and take_profit >= entry:
            msg = f"SELL signal: take_profit ({take_profit}) must be BELOW entry ({entry})"
            if force:
                log.warning("Price logic WARNING (--force): %s", msg)
            else:
                log.error("Price logic FAILED: %s", msg)
                ok = False

    if not ok and not force:
        return False

    # --- Check for existing open signal ---
    existing = get_active_signals(strategy_name=strategy, asset=asset)
    if existing:
        sig = existing[0]
        log.warning(
            "Existing OPEN signal found — no duplicate will be created.\n"
            "  id=%s | direction=%s | entry=%s | timestamp=%s\n"
            "  Close or cancel the existing signal before restoring a new one.",
            sig.get("id"),
            sig.get("direction"),
            sig.get("entry_price"),
            sig.get("signal_timestamp"),
        )
        return False  # Always block — idempotent, not a --force override

    # --- Check for existing open position ---
    if has_open_position(strategy_name=strategy, asset=asset):
        log.warning(
            "An open position record already exists for %s / %s. "
            "Continuing — the position record may exist without a matching signal in edge cases.",
            strategy, asset,
        )

    # --- Check for duplicate timestamp ---
    if signal_exists(strategy_name=strategy, asset=asset, signal_timestamp=timestamp):
        log.error(
            "A signal with this exact timestamp already exists for %s / %s @ %s. "
            "Suggest changing --timestamp by 1 minute (e.g. %s).",
            strategy, asset, timestamp,
            timestamp[:-2] + f"{int(timestamp[-2:]) + 1:02d}" if timestamp else "?",
        )
        return False

    return True


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------

def print_summary(
    strategy: str,
    asset: str,
    direction: str,
    entry: float,
    stop_loss: float,
    take_profit: float | None,
    atr: float,
    timestamp: str,
    atr_derived: bool,
):
    print()
    print("─" * 52)
    print(f"  Strategy   : {strategy}")
    print(f"  Asset      : {asset}")
    print(f"  Direction  : {direction}")
    print(f"  Entry      : {entry}")
    print(f"  Stop Loss  : {stop_loss}")
    print(f"  Take Profit: {take_profit if take_profit is not None else '(none)'}")
    print(f"  ATR        : {atr}{' (auto-derived)' if atr_derived else ''}")
    print(f"  Timestamp  : {timestamp}")
    print("─" * 52)
    print()


# ---------------------------------------------------------------------------
# Main restore logic
# ---------------------------------------------------------------------------

def run_restore(args: argparse.Namespace) -> int:
    from trading_engine.database import (
        init_db,
        insert_signal,
        open_position,
        update_position_tracking,
    )

    log.info("=" * 60)
    log.info("AI Signals — Manual Signal Restore")
    log.info("=" * 60)

    # --- Normalise direction ---
    direction = args.direction.strip().upper()
    if direction not in ("BUY", "SELL"):
        log.error("--direction must be BUY or SELL, got: %s", args.direction)
        return 1

    strategy  = args.strategy
    asset     = args.asset.strip()
    entry     = float(args.entry)
    stop_loss = float(args.stop_loss)
    take_profit = float(args.take_profit) if args.take_profit is not None else None

    # --- Timestamp ---
    if args.timestamp:
        timestamp = args.timestamp.strip()
    else:
        default_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        if args.force or args.dry_run:
            timestamp = default_ts
            log.info("No --timestamp provided; using current UTC time: %s", timestamp)
        else:
            ts_input = input(
                f"Enter signal timestamp [YYYY-MM-DDTHH:MM:SS, default={default_ts}]: "
            ).strip()
            timestamp = ts_input if ts_input else default_ts

    # --- ATR ---
    atr_derived = False
    if args.atr is not None:
        atr_at_entry = round(float(args.atr), 6)
    else:
        atr_at_entry = derive_atr(entry, stop_loss, strategy)
        atr_derived = True

    # --- Init DB ---
    init_db()
    log.info("Database initialised.")

    # --- Safety checks ---
    safe = run_safety_checks(
        asset=asset,
        direction=direction,
        entry=entry,
        stop_loss=stop_loss,
        take_profit=take_profit,
        strategy=strategy,
        timestamp=timestamp,
        force=args.force,
    )
    if not safe:
        return 0  # Blocked — not an error, idempotent

    # --- Print summary ---
    print_summary(strategy, asset, direction, entry, stop_loss, take_profit,
                  atr_at_entry, timestamp, atr_derived)

    # --- Dry run ---
    if args.dry_run:
        print("DRY RUN — no changes written to database.")
        print(EXIT_RULES.get(strategy, ""))
        return 0

    # --- Confirmation prompt ---
    if not args.force:
        answer = input("Restore this signal? [y/N]: ").strip().lower()
        if answer not in ("y", "yes"):
            print("Restore cancelled.")
            return 0

    # --- Insert signal ---
    signal_dict = {
        "strategy_name":    strategy,
        "asset":            asset,
        "direction":        direction,
        "entry_price":      entry,
        "stop_loss":        stop_loss,
        "take_profit":      take_profit,
        "atr_at_entry":     atr_at_entry,
        "signal_timestamp": timestamp,
    }
    log.info("Inserting signal into DB...")
    signal_id = insert_signal(signal_dict)
    if signal_id is None:
        log.error("insert_signal() returned None — DB write failed. No changes made.")
        return 1
    log.info("Signal inserted: id=%s", signal_id)

    # --- Insert open position ---
    position_dict = {
        "asset":          asset,
        "strategy_name":  strategy,
        "direction":      direction,
        "entry_price":    entry,
        "atr_at_entry":   atr_at_entry,
    }
    log.info("Opening position in DB...")
    pos_id = open_position(position_dict)
    if pos_id is None:
        log.error(
            "open_position() returned None — DB write failed. "
            "Signal id=%s was already inserted — check the DB manually.",
            signal_id,
        )
        return 1
    log.info("Open position created: id=%s", pos_id)

    # --- Update tracking extremes explicitly for mtf_ema (also handles all strategies) ---
    if direction == "BUY":
        update_position_tracking(pos_id, highest_price=entry)
    else:
        update_position_tracking(pos_id, lowest_price=entry)

    # --- Invalidate API cache ---
    try:
        from trading_engine.api_v1 import invalidate_signal_caches
        invalidate_signal_caches()
        cache_msg = "✓ API cache invalidated"
    except Exception as exc:
        log.debug("Cache invalidation skipped (engine not running): %s", exc)
        cache_msg = "  API cache: not invalidated (engine offline — will refresh on next request)"

    # --- Final summary ---
    print()
    print(f"✓ Signal restored   | id={signal_id} | {strategy} {direction} {asset} @ {entry} | timestamp={timestamp}")
    print(f"✓ Open position created | id={pos_id}")
    print(cache_msg)
    print("  The engine will track exits for this position on the next scheduler tick.")
    print()
    print(EXIT_RULES.get(strategy, ""))
    print()

    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manually restore a live trade missing from the engine database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    req = parser.add_argument_group("required arguments")
    req.add_argument("--asset",      required=True, help='Asset symbol e.g. "BTC/USD"')
    req.add_argument("--direction",  required=True, help="BUY or SELL")
    req.add_argument("--entry",      required=True, type=float, help="Entry price")
    req.add_argument("--stop-loss",  required=True, type=float, dest="stop_loss", help="Stop loss price")
    req.add_argument(
        "--strategy", required=True,
        choices=KNOWN_STRATEGIES,
        help=f"Strategy name. Choices: {', '.join(KNOWN_STRATEGIES)}",
    )

    opt = parser.add_argument_group("optional arguments")
    opt.add_argument("--take-profit", type=float, dest="take_profit", default=None,
                     help="Take profit price (default: None)")
    opt.add_argument("--atr",         type=float, default=None,
                     help="ATR at entry. Auto-derived from entry/stop-loss if not provided.")
    opt.add_argument("--timestamp",   default=None,
                     help="Signal timestamp YYYY-MM-DDTHH:MM:SS (default: prompt)")
    opt.add_argument("--dry-run",     action="store_true",
                     help="Show what would be inserted without writing to DB")
    opt.add_argument("--force",       action="store_true",
                     help="Skip confirmation prompts and price-logic soft warnings")

    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    sys.exit(run_restore(args))
