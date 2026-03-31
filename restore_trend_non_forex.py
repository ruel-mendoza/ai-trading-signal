#!/usr/bin/env python3
"""
restore_trend_non_forex.py — Restore symbols to the trend_non_forex strategy
in strategy_assets, validating each one against FCSAPI before inserting.

Usage:
  python3 restore_trend_non_forex.py              # verify + insert all symbols
  python3 restore_trend_non_forex.py --dry-run    # verify only, no DB writes
  python3 restore_trend_non_forex.py --force      # skip credit prompt
"""

import sys
import os
import argparse
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Suppress noisy engine logs — only show WARNING+ from the engine
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("restore_trend_non_forex")
logging.getLogger("trading_engine").setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# Symbols to restore
# ---------------------------------------------------------------------------
SYMBOLS = [
    "COTN", "CORN", "WOOD", "SOYB", "CANE", "WEAT",
    "USOIL", "UNG", "UGA", "SGOL", "CPER", "PALL", "PPLT", "SIVR",
    "NICK", "ALUM", "ZINC", "SLX", "TINM",
]

STRATEGY      = "trend_non_forex"
ASSET_CLASS   = "forex"   # Matches existing convention for commodity ETFs
CREDITS_PER_SYMBOL = 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Restore trend_non_forex strategy assets with FCSAPI verification"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run all FCSAPI checks but skip DB writes",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip credit confirmation prompt",
    )
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # Lazy imports (after sys.path is set)
    # ------------------------------------------------------------------
    from trading_engine.database import init_db, add_strategy_asset
    from trading_engine.fcsapi_client import FCSAPIClient

    # ------------------------------------------------------------------
    # Init DB (creates tables / runs migrations if needed)
    # ------------------------------------------------------------------
    init_db()

    api_key = os.environ.get("FCSAPI_KEY", "")
    if not api_key:
        print("ERROR: FCSAPI_KEY environment variable is not set")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Credit budget warning + confirmation
    # ------------------------------------------------------------------
    total_credits = len(SYMBOLS) * CREDITS_PER_SYMBOL
    if total_credits > 20:
        print(f"WARNING: This will use approximately {total_credits} API credits.")

    if not args.force and not args.dry_run:
        try:
            answer = input(
                f"About to verify {len(SYMBOLS)} symbols "
                f"(~{total_credits} credit{'s' if total_credits != 1 else ''}). "
                f"Continue? [y/N] "
            )
        except (EOFError, KeyboardInterrupt):
            print("\nAborted.")
            sys.exit(0)
        if answer.strip().lower() not in ("y", "yes"):
            print("Aborted.")
            sys.exit(0)

    client = FCSAPIClient(api_key)

    # ------------------------------------------------------------------
    # Verify + insert loop
    # ------------------------------------------------------------------
    results: list[dict] = []

    for symbol in SYMBOLS:
        result = client.test_symbol_coverage(symbol)
        api_sym = result.get("api_symbol", symbol)

        if not result["supported"]:
            reason = result.get("reason", "FCSAPI returned no data")
            results.append({
                "symbol":     symbol,
                "api_symbol": api_sym,
                "status":     "FAILED",
                "detail":     reason,
            })
            print(f"  [FAIL]     {symbol:<10}  {reason}")
            continue

        close = result.get("sample_close", "?")

        if args.dry_run:
            results.append({
                "symbol":     symbol,
                "api_symbol": api_sym,
                "status":     "DRY_RUN",
                "detail":     f"close={close}",
            })
            print(f"  [DRY_RUN]  {symbol:<10}  api_sym={api_sym}  close={close}")
            continue

        row_id = add_strategy_asset(
            strategy_name=STRATEGY,
            symbol=symbol,
            asset_class=ASSET_CLASS,
            sub_category=None,
            added_by="restore_script",
            fcsapi_verified=True,
        )

        if row_id is not None:
            status = "ADDED"
            print(f"  [ADDED]    {symbol:<10}  api_sym={api_sym}  close={close}")
        else:
            status = "ALREADY_EXISTS"
            print(f"  [EXISTS]   {symbol:<10}  api_sym={api_sym}  close={close}")

        results.append({
            "symbol":     symbol,
            "api_symbol": api_sym,
            "status":     status,
            "detail":     f"close={close}",
        })

    # ------------------------------------------------------------------
    # Invalidate signal caches (engine may be offline — swallow error)
    # ------------------------------------------------------------------
    if not args.dry_run:
        try:
            from trading_engine.api_v1 import invalidate_signal_caches
            count = invalidate_signal_caches()
            print(f"\n[CACHE] Invalidated {count} signal cache entries")
        except Exception as exc:
            print(f"\n[CACHE] Could not invalidate caches (engine may be offline): {exc}")

    # ------------------------------------------------------------------
    # Summary table
    # ------------------------------------------------------------------
    print("\n" + "=" * 80)
    print(f"{'SYMBOL':<10}  {'API SYMBOL':<18}  {'STATUS':<15}  DETAIL")
    print("-" * 80)
    for r in results:
        print(f"{r['symbol']:<10}  {r['api_symbol']:<18}  {r['status']:<15}  {r['detail']}")
    print("=" * 80)

    added    = sum(1 for r in results if r["status"] == "ADDED")
    existing = sum(1 for r in results if r["status"] == "ALREADY_EXISTS")
    dry      = sum(1 for r in results if r["status"] == "DRY_RUN")
    failed   = sum(1 for r in results if r["status"] == "FAILED")

    if args.dry_run:
        print(f"DRY RUN: {dry} would be inserted | {failed} failed verification")
    else:
        print(f"Added/reactivated: {added} | Already active: {existing} | Failed: {failed}")


if __name__ == "__main__":
    main()
