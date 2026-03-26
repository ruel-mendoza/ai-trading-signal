#!/usr/bin/env python3
"""
backfill.py — Standalone OHLC candle backfill utility for the AI Signals trading engine.

Usage examples:
  python3 backfill.py                          # refresh everything
  python3 backfill.py --strategy mtf_ema       # only MTF EMA assets
  python3 backfill.py --symbol "XAU/USD"       # only XAU/USD all timeframes
  python3 backfill.py --timeframe D1           # only D1 across all strategies
  python3 backfill.py --dry-run                # print plan without API calls
  python3 backfill.py --dry-run --strategy mtf_ema --timeframe D1
  python3 backfill.py --limit 150              # fetch 150 candles instead of 300
  python3 backfill.py --force                  # skip credit confirmation prompt
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
log = logging.getLogger("backfill")

# ---------------------------------------------------------------------------
# Strategy → timeframe mapping
# ---------------------------------------------------------------------------
ASSET_TIMEFRAMES: dict[str, list[str]] = {
    "mtf_ema":          ["D1", "4H", "1H"],
    "trend_forex":      ["D1"],
    "trend_non_forex":  ["D1"],
    "sp500_momentum":   ["30m", "D1"],
    "highest_lowest_fx":["1H", "D1"],
    "stocks_algo1":     ["D1"],
    "stocks_algo2":     ["D1"],
}

KNOWN_STRATEGIES = list(ASSET_TIMEFRAMES.keys())


# ---------------------------------------------------------------------------
# Asset discovery
# ---------------------------------------------------------------------------

def get_all_assets(
    strategy_filter: str | None = None,
    timeframe_filter: str | None = None,
    symbol_filter: str | None = None,
) -> dict[str, set[str]]:
    """
    Returns {timeframe: set_of_symbols} for all active assets, applying any
    CLI filters.  Symbols are deduplicated across strategies via sets.
    """
    from trading_engine.database import get_strategy_assets_full

    strategies = [strategy_filter] if strategy_filter else KNOWN_STRATEGIES
    result: dict[str, set[str]] = {}

    for strategy in strategies:
        timeframes = ASSET_TIMEFRAMES.get(strategy, [])
        if timeframe_filter:
            timeframes = [tf for tf in timeframes if tf == timeframe_filter]
        if not timeframes:
            continue

        assets = get_strategy_assets_full(strategy_name=strategy)
        active = [a for a in assets if a.get("is_active")]

        if symbol_filter:
            active = [a for a in active if a["symbol"] == symbol_filter]

        for tf in timeframes:
            result.setdefault(tf, set())
            for a in active:
                result[tf].add(a["symbol"])

    for tf, symbols in result.items():
        log.info("Timeframe %-4s — %d unique symbol(s)", tf, len(symbols))

    return result


# ---------------------------------------------------------------------------
# Main backfill runner
# ---------------------------------------------------------------------------

def run_backfill(args: argparse.Namespace) -> int:
    """
    Performs the candle backfill.  Returns 0 on full success, 1 if any symbol failed.
    """
    from trading_engine.database import init_db
    from trading_engine.fcsapi_client import FCSAPIClient
    from trading_engine.cache_layer import CacheLayer

    log.info("=" * 60)
    log.info("AI Signals — OHLC Candle Backfill")
    log.info("=" * 60)

    init_db()
    log.info("Database initialised.")

    api_client = FCSAPIClient()
    if not api_client.api_key:
        log.error(
            "FCSAPI key is empty.  Set the FCSAPI_KEY environment variable "
            "or configure it via the admin dashboard before running this script."
        )
        return 1

    cache = CacheLayer(api_client)

    # Build the full refresh plan
    tf_symbols = get_all_assets(
        strategy_filter=args.strategy or None,
        timeframe_filter=args.timeframe or None,
        symbol_filter=args.symbol or None,
    )

    if not tf_symbols:
        log.warning("No assets matched the given filters — nothing to do.")
        return 0

    # Flatten to (timeframe, symbol) pairs for counting
    pairs: list[tuple[str, str]] = []
    for tf, symbols in sorted(tf_symbols.items()):
        for sym in sorted(symbols):
            pairs.append((tf, sym))

    total = len(pairs)
    limit = min(args.limit, 300)

    log.info("─" * 60)
    log.info("Estimated credit cost: %d credits (%d fetch × 1 credit each)", total, total)
    if total > 200:
        log.warning(
            "Large refresh: %d API credits will be consumed.  "
            "Pass --force to skip this prompt.",
            total,
        )
        if not args.force and not args.dry_run:
            answer = input("Continue? [y/N] ").strip().lower()
            if answer not in ("y", "yes"):
                log.info("Aborted by user.")
                return 0
    log.info("─" * 60)

    if args.dry_run:
        log.info("[DRY-RUN] The following fetches would be performed:")
        for tf, sym in pairs:
            log.info("  %-6s  %s", tf, sym)
        log.info("[DRY-RUN] Total: %d fetch(es), %d estimated credit(s)", total, total)
        if total > 100:
            log.warning("[DRY-RUN] Credit warning: >100 API calls planned (%d)", total)
        return 0

    # Execute fetches
    succeeded: list[str] = []
    failed: list[str] = []
    skipped: list[str] = []

    current_tf = None
    for tf, sym in pairs:
        if tf != current_tf:
            current_tf = tf
            log.info("")
            log.info("── Timeframe: %s %s", tf, "─" * (50 - len(tf)))

        label = f"{sym}/{tf}"
        try:
            candles = cache.force_refresh(sym, tf, limit=limit)
            if not candles:
                log.warning("  ✗  %-30s  no candles returned (skipped)", label)
                skipped.append(label)
                continue

            first_ts = candles[0].get("timestamp", "?")
            last_ts  = candles[-1].get("timestamp", "?")
            last_close = candles[-1].get("close", "?")
            count = len(candles)

            def fmt_ts(ts):
                if isinstance(ts, (int, float)):
                    try:
                        return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                    except Exception:
                        return str(ts)
                if isinstance(ts, datetime):
                    return ts.strftime("%Y-%m-%d")
                return str(ts)[:10] if ts else "?"

            log.info(
                "  ✓  %-30s  %3d candles  %s → %s  close=%s",
                label,
                count,
                fmt_ts(first_ts),
                fmt_ts(last_ts),
                f"{last_close:.5f}" if isinstance(last_close, float) else last_close,
            )
            succeeded.append(label)

        except Exception as exc:
            log.error("  ✗  %-30s  ERROR: %s", label, exc)
            failed.append(label)

    # Summary
    log.info("")
    log.info("=" * 60)
    log.info(
        "Backfill complete — %d attempted | %d succeeded | %d failed | %d skipped",
        total,
        len(succeeded),
        len(failed),
        len(skipped),
    )

    if failed:
        log.warning("Failed symbols:")
        for f in failed:
            log.warning("  ✗  %s", f)
        log.warning(
            "Check your FCSAPI key, credit balance, and symbol mapping "
            "for the above assets."
        )

    # Invalidate API cache so fresh data is served immediately
    try:
        from trading_engine.api_v1 import invalidate_signal_caches
        invalidate_signal_caches()
        log.info("API cache invalidated — fresh data will be served on next request.")
    except Exception as exc:
        log.debug("Cache invalidation skipped (engine not running): %s", exc)

    log.info("=" * 60)
    return 1 if failed else 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill OHLC candle data for all AI Signals strategy assets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--strategy",
        choices=KNOWN_STRATEGIES,
        metavar="STRATEGY",
        help=f"Filter to a single strategy. Choices: {', '.join(KNOWN_STRATEGIES)}",
    )
    parser.add_argument(
        "--timeframe",
        metavar="TIMEFRAME",
        help="Filter to a single timeframe (e.g. D1, 4H, 1H, 30m).",
    )
    parser.add_argument(
        "--symbol",
        metavar="SYMBOL",
        help='Filter to a single symbol (e.g. "XAU/USD").',
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be fetched without making any API calls.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=300,
        metavar="N",
        help="Override the candle limit (default 300, max 300).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip the credit confirmation prompt for large refreshes.",
    )
    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    sys.exit(run_backfill(args))
