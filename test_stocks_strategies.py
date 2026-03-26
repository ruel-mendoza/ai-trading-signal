"""
test_stocks_strategies.py — Diagnostic test suite for Stocks Algo 1 & Algo 2.

Usage:
  python3 test_stocks_strategies.py
  python3 test_stocks_strategies.py --test connectivity
  python3 test_stocks_strategies.py --test algo1 --limit 5
  python3 test_stocks_strategies.py --symbol AAPL --verbose
  python3 test_stocks_strategies.py --test all --dry-run
  python3 test_stocks_strategies.py --test algo1 --limit 0
"""

import sys
import os
import argparse
import logging
import time
from datetime import datetime
from typing import Optional

# Re-exec using the Replit-wrapped Python binary which has the correct linker
# environment for numpy/pandas C extensions (libstdc++.so.6 resolution).
_WRAPPED_PYTHON = (
    "/nix/store/flbj8bq2vznkcwss7sm0ky8rd0k6kar7-python-wrapped-0.1.0/bin/python3"
)
_SENTINEL = "__STOCKS_TEST_ENV_SET__"

if _SENTINEL not in os.environ and os.path.exists(_WRAPPED_PYTHON):
    import subprocess
    env = os.environ.copy()
    env[_SENTINEL] = "1"
    result = subprocess.run([_WRAPPED_PYTHON] + sys.argv, env=env)
    sys.exit(result.returncode)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── Stub out strategies that pull in pandas/numpy ────────────────────────────
# base.py and sp500_momentum.py both import pandas, which requires numpy C
# extensions that aren't available in the CLI linker environment.
# We pre-populate sys.modules with lightweight stubs so the import chain
# completes without loading pandas, allowing the test to run from the CLI.
import types as _types
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional as _Opt

def _make_stub(full_name: str, **attrs):
    m = _types.ModuleType(full_name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[full_name] = m
    return m

# ── Stub for trading_engine.strategies.base ──────────────────────────────────
class _Action(str, Enum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    NONE = "NONE"

class _Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"

@dataclass
class _SignalResult:
    action: _Action = _Action.NONE
    direction: _Opt[_Direction] = None
    price: _Opt[float] = None
    stop_loss: _Opt[float] = None
    atr_at_entry: _Opt[float] = None
    metadata: dict = field(default_factory=dict)

    @property
    def is_entry(self) -> bool: return self.action == _Action.ENTRY
    @property
    def is_exit(self) -> bool: return self.action == _Action.EXIT
    @property
    def is_none(self) -> bool: return self.action == _Action.NONE
    def to_dict(self) -> dict:
        return {"action": self.action.value, "direction": self.direction.value if self.direction else None,
                "price": self.price, "stop_loss": self.stop_loss, "atr_at_entry": self.atr_at_entry,
                "metadata": self.metadata}

class _BaseStrategy(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...
    @abstractmethod
    def evaluate(self, asset, timeframe, df, open_position) -> _SignalResult: ...

_make_stub(
    "trading_engine.strategies.base",
    Action=_Action,
    Direction=_Direction,
    SignalResult=_SignalResult,
    BaseStrategy=_BaseStrategy,
)

# ── Stubs for other pandas-heavy strategies ───────────────────────────────────
class _StubStrategy:
    pass

_make_stub("trading_engine.strategies.sp500_momentum", SP500MomentumStrategy=_StubStrategy)
_make_stub("trading_engine.strategies.highest_lowest", HighestLowestFXStrategy=_StubStrategy)
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
)
logger = logging.getLogger("test_stocks")


# ─────────────────────────────────────────────────────────────────────────────
# TestResults tracker
# ─────────────────────────────────────────────────────────────────────────────

class TestResults:
    def __init__(self):
        self.passed   = []
        self.failed   = []
        self.warnings = []
        self.skipped  = []

    def ok(self, name, detail=""):
        self.passed.append((name, detail))
        logger.info("  \u2713 PASS | " + name + (" | " + detail if detail else ""))

    def fail(self, name, detail=""):
        self.failed.append((name, detail))
        logger.error("  \u2717 FAIL | " + name + (" | " + detail if detail else ""))

    def warn(self, name, detail=""):
        self.warnings.append((name, detail))
        logger.warning("  \u26a0 WARN | " + name + (" | " + detail if detail else ""))

    def skip(self, name, detail=""):
        self.skipped.append((name, detail))
        logger.info("  \u25cb SKIP | " + name + (" | " + detail if detail else ""))

    def summary(self) -> bool:
        total = (len(self.passed) + len(self.failed) +
                 len(self.warnings) + len(self.skipped))
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)
        print(f"  \u2713 Passed  : {len(self.passed)}")
        print(f"  \u2717 Failed  : {len(self.failed)}")
        print(f"  \u26a0 Warnings: {len(self.warnings)}")
        print(f"  \u25cb Skipped : {len(self.skipped)}")
        print(f"  Total     : {total}")
        print("=" * 60)
        if self.failed:
            print("\nFAILED TESTS:")
            for name, detail in self.failed:
                print(f"  \u2717 {name}")
                if detail:
                    print(f"    \u2192 {detail}")
        if self.warnings:
            print("\nWARNINGS:")
            for name, detail in self.warnings:
                print(f"  \u26a0 {name}")
                if detail:
                    print(f"    \u2192 {detail}")
        print()
        return len(self.failed) == 0


# ─────────────────────────────────────────────────────────────────────────────
# TEST 1 — FCSAPI Connectivity
# ─────────────────────────────────────────────────────────────────────────────

def test_connectivity(cache, results: TestResults, symbols: list, limit: int, verbose: bool):
    from trading_engine.strategies.stocks_algo1 import _fetch_stock_candles

    print("\n" + "-" * 60)
    print("TEST 1 \u2014 FCSAPI CONNECTIVITY")
    print("-" * 60)

    test_syms = symbols[:limit] if limit > 0 else symbols
    print(f"Testing {len(test_syms)} symbols (limit={'all' if limit == 0 else limit})")
    print(f"Estimated credit cost: {len(test_syms)} credits\n")

    success_count = 0
    failed_syms = []

    for sym in test_syms:
        try:
            candles = _fetch_stock_candles(cache, sym, limit=2)
            if candles:
                close = candles[-1]["close"]
                ts    = candles[-1]["timestamp"]
                results.ok(
                    f"connectivity/{sym}",
                    f"close={close} | ts={ts[:10]} | bars={len(candles)}",
                )
                if verbose:
                    print(f"    {sym}: close={close}, ts={ts[:10]}, bars={len(candles)}")
                success_count += 1
            else:
                results.fail(f"connectivity/{sym}", "No candles returned")
                failed_syms.append(sym)
        except Exception as e:
            results.fail(f"connectivity/{sym}", str(e))
            failed_syms.append(sym)
        time.sleep(0.3)

    print(f"\nConnectivity: {success_count}/{len(test_syms)} passed")
    if failed_syms:
        print(f"Failed symbols: {', '.join(failed_syms)}")


# ─────────────────────────────────────────────────────────────────────────────
# TEST 2 — NDX Market Filter
# ─────────────────────────────────────────────────────────────────────────────

def test_ndx_filter(cache, results: TestResults, verbose: bool):
    from trading_engine.strategies.stocks_algo1 import (
        _fetch_ndx_candles,
        SMA_PERIOD,
    )
    from trading_engine.indicators import IndicatorEngine

    print("\n" + "-" * 60)
    print("TEST 2 \u2014 NDX MARKET FILTER (SMA200)")
    print("-" * 60)

    try:
        ndx_candles = _fetch_ndx_candles(cache, limit=250)
        if len(ndx_candles) < SMA_PERIOD:
            results.fail(
                "ndx/candles",
                f"Only {len(ndx_candles)} bars returned, need {SMA_PERIOD}",
            )
            return
        results.ok("ndx/candles", f"{len(ndx_candles)} bars fetched")

        ndx_closes = [c["close"] for c in ndx_candles]
        sma200_vals = IndicatorEngine.sma(ndx_closes, SMA_PERIOD)
        sma200 = sma200_vals[-1] if sma200_vals else None

        if sma200 is None:
            results.fail("ndx/sma200", "SMA200 returned None")
            return

        latest_close = ndx_closes[-1]
        above_sma = latest_close > sma200
        filter_pass = "PASS (market BULLISH)" if above_sma else "FAIL (market BEARISH — algo1 will skip)"

        results.ok(
            "ndx/sma200",
            f"close={latest_close:.2f} | SMA200={sma200:.2f} | filter={filter_pass}",
        )
        if verbose:
            print(f"    NDX close={latest_close:.2f}, SMA200={sma200:.2f}, above_sma={above_sma}")
        if not above_sma:
            results.warn("ndx/market_filter", "NDX below SMA200 — Algo1 run_monthly will skip this month")

    except Exception as e:
        results.fail("ndx/filter", str(e))


# ─────────────────────────────────────────────────────────────────────────────
# TEST 3 — Stocks Algo 1 Full Cycle
# ─────────────────────────────────────────────────────────────────────────────

def test_algo1(cache, results: TestResults, symbols: list, limit: int, dry_run: bool, verbose: bool):
    from trading_engine.strategies.stocks_algo1 import (
        _fetch_stock_candles,
        _calculate_momentum,
        MOMENTUM_LONG_BARS,
        TOP_N,
        STOP_LOSS_PCT as ALGO1_SL_PCT,
        StocksAlgo1Strategy,
    )

    print("\n" + "-" * 60)
    print("TEST 3 \u2014 STOCKS ALGO 1 (MONTHLY MOMENTUM)")
    print("-" * 60)

    test_syms = symbols[:limit] if limit > 0 else symbols
    print(f"Testing momentum computation for {len(test_syms)} symbols\n")

    momentum_results = []
    fetch_ok = 0
    fetch_fail = 0

    for sym in test_syms:
        try:
            candles = _fetch_stock_candles(cache, sym, limit=MOMENTUM_LONG_BARS + 30)
            if not candles:
                results.fail(f"algo1/fetch/{sym}", "No candles returned")
                fetch_fail += 1
                continue

            fetch_ok += 1
            closes = [c["close"] for c in candles]
            momentum = _calculate_momentum(candles)

            if momentum is None:
                results.warn(
                    f"algo1/momentum/{sym}",
                    f"bars={len(candles)} — insufficient for momentum calc (need {MOMENTUM_LONG_BARS})",
                )
            else:
                momentum_results.append((sym, momentum, closes[-1]))
                results.ok(
                    f"algo1/momentum/{sym}",
                    f"momentum={momentum:.4f} | bars={len(candles)} | close={closes[-1]}",
                )
                if verbose:
                    print(f"    {sym}: momentum={momentum:.4f}, close={closes[-1]}, bars={len(candles)}")

        except Exception as e:
            results.fail(f"algo1/fetch/{sym}", str(e))
            fetch_fail += 1
        time.sleep(0.3)

    print(f"\nCandle fetch: {fetch_ok} ok, {fetch_fail} failed out of {len(test_syms)}")

    if momentum_results:
        sorted_by_mom = sorted(momentum_results, key=lambda x: x[1], reverse=True)
        top = sorted_by_mom[:TOP_N]
        print(f"\nTop {len(top)} by momentum (of {len(sorted_by_mom)} scored):")
        for sym, mom, close in top:
            print(f"    {sym}: momentum={mom:.4f}, close={close}")

    if not dry_run:
        print("\nRunning run_monthly() on live strategy...")
        try:
            strat = StocksAlgo1Strategy(cache)
            summary = strat.run_monthly()
            results.ok(
                "algo1/run_monthly",
                f"opened={summary.get('signals_opened')} | "
                f"closed={summary.get('signals_closed')} | "
                f"skipped={summary.get('skipped')} | "
                f"error={summary.get('error')}",
            )
            if summary.get("error"):
                results.warn("algo1/run_monthly/error", str(summary["error"]))
        except Exception as e:
            results.fail("algo1/run_monthly", str(e))
    else:
        results.skip("algo1/run_monthly", "dry-run mode")


# ─────────────────────────────────────────────────────────────────────────────
# TEST 4 — Stocks Algo 2 Full Cycle
# ─────────────────────────────────────────────────────────────────────────────

def test_algo2(cache, results: TestResults, symbols: list, limit: int, dry_run: bool, verbose: bool):
    from trading_engine.strategies.stocks_algo2 import (
        _fetch_stock_candles as _fetch_stock_candles_2,
        _death_cross_detected,
        STOP_LOSS_PCT as ALGO2_SL_PCT,
        MAX_HOLD_TRADING_DAYS,
        StocksAlgo2Strategy,
    )
    from trading_engine.indicators import IndicatorEngine

    print("\n" + "-" * 60)
    print("TEST 4 \u2014 STOCKS ALGO 2 (DEATH CROSS)")
    print("-" * 60)

    test_syms = symbols[:limit] if limit > 0 else symbols
    print(f"Testing death cross detection for {len(test_syms)} symbols\n")
    print(f"Stop loss: {ALGO2_SL_PCT*100:.0f}% | Max hold: {MAX_HOLD_TRADING_DAYS} trading days\n")

    fetch_ok = 0
    fetch_fail = 0
    cross_detected = []

    for sym in test_syms:
        try:
            candles = _fetch_stock_candles_2(cache, sym, limit=250)
            if not candles:
                results.fail(f"algo2/fetch/{sym}", "No candles returned")
                fetch_fail += 1
                continue

            fetch_ok += 1
            closes = [c["close"] for c in candles]
            death_cross = _death_cross_detected(closes)

            if death_cross:
                cross_detected.append(sym)
                results.warn(
                    f"algo2/death_cross/{sym}",
                    f"DEATH CROSS DETECTED | bars={len(candles)} | close={closes[-1]}",
                )
            else:
                results.ok(
                    f"algo2/candles/{sym}",
                    f"bars={len(candles)} | close={closes[-1]} | no death cross",
                )
            if verbose:
                sma50  = IndicatorEngine.sma(closes, 50)
                sma200 = IndicatorEngine.sma(closes, 200)
                s50  = sma50[-1]  if sma50  else None
                s200 = sma200[-1] if sma200 else None
                print(f"    {sym}: close={closes[-1]}, SMA50={s50}, SMA200={s200}, death_cross={death_cross}")

        except Exception as e:
            results.fail(f"algo2/fetch/{sym}", str(e))
            fetch_fail += 1
        time.sleep(0.3)

    print(f"\nCandle fetch: {fetch_ok} ok, {fetch_fail} failed out of {len(test_syms)}")
    if cross_detected:
        print(f"Death crosses detected: {', '.join(cross_detected)}")
    else:
        print("No death crosses detected in tested universe")

    if not dry_run:
        print("\nRunning run_daily() on live strategy...")
        try:
            strat = StocksAlgo2Strategy(cache)
            summary = strat.run_daily()
            results.ok(
                "algo2/run_daily",
                f"opened={summary.get('signals_opened')} | "
                f"closed={summary.get('signals_closed')} | "
                f"error={summary.get('error')}",
            )
            if summary.get("error"):
                results.warn("algo2/run_daily/error", str(summary["error"]))
        except Exception as e:
            results.fail("algo2/run_daily", str(e))
    else:
        results.skip("algo2/run_daily", "dry-run mode")


# ─────────────────────────────────────────────────────────────────────────────
# TEST 5 — Exit Checks
# ─────────────────────────────────────────────────────────────────────────────

def test_exits(cache, results: TestResults, dry_run: bool):
    from trading_engine.strategies.stocks_algo1 import StocksAlgo1Strategy
    from trading_engine.strategies.stocks_algo2 import StocksAlgo2Strategy

    print("\n" + "-" * 60)
    print("TEST 5 \u2014 EXIT CHECKS")
    print("-" * 60)

    if dry_run:
        results.skip("exits/algo1", "dry-run mode")
        results.skip("exits/algo2", "dry-run mode")
        return

    for name, StratClass in [("algo1", StocksAlgo1Strategy), ("algo2", StocksAlgo2Strategy)]:
        try:
            strat = StratClass(cache)
            closed = strat.check_exits()
            results.ok(
                f"exits/{name}",
                f"check_exits returned {len(closed)} closed position(s)",
            )
            if closed:
                for c in closed:
                    logger.info(f"  Closed: {c}")
        except Exception as e:
            results.fail(f"exits/{name}", str(e))


# ─────────────────────────────────────────────────────────────────────────────
# TEST 6 — NASDAQ 100 Sync
# ─────────────────────────────────────────────────────────────────────────────

def test_sync(results: TestResults, dry_run: bool):
    from trading_engine.utils.nasdaq_sync import sync_nasdaq100_symbols

    print("\n" + "-" * 60)
    print("TEST 6 \u2014 NASDAQ 100 SYNC")
    print("-" * 60)

    if dry_run:
        results.skip("sync/nasdaq100", "dry-run mode")
        return

    try:
        result = sync_nasdaq100_symbols()
        added   = result.get("added", 0)
        removed = result.get("removed", 0)
        total   = result.get("total", 0)
        error   = result.get("error")

        if error:
            results.fail("sync/nasdaq100", str(error))
        else:
            results.ok(
                "sync/nasdaq100",
                f"total={total} | added={added} | removed={removed}",
            )
            print(f"  Sync result: total={total}, added={added}, removed={removed}")
    except Exception as e:
        results.fail("sync/nasdaq100", str(e))


# ─────────────────────────────────────────────────────────────────────────────
# TEST 7 — Single Symbol End-to-End
# ─────────────────────────────────────────────────────────────────────────────

def test_single_symbol(cache, results: TestResults, symbol: str, verbose: bool):
    from trading_engine.strategies.stocks_algo1 import (
        _fetch_stock_candles,
        _calculate_momentum,
        MOMENTUM_LONG_BARS,
        STOP_LOSS_PCT as ALGO1_SL_PCT,
    )
    from trading_engine.strategies.stocks_algo2 import (
        _fetch_stock_candles as _fetch2,
        _death_cross_detected,
        STOP_LOSS_PCT as ALGO2_SL_PCT,
    )
    from trading_engine.fcsapi_client import get_nasdaq_api_symbol
    from trading_engine.indicators import IndicatorEngine

    print("\n" + "-" * 60)
    print(f"SINGLE SYMBOL TEST \u2014 {symbol}")
    print("-" * 60)

    api_sym = get_nasdaq_api_symbol(symbol)
    print(f"  DB symbol : {symbol}")
    print(f"  API symbol: {api_sym}")

    # Algo1 path
    try:
        candles = _fetch_stock_candles(cache, symbol, limit=MOMENTUM_LONG_BARS + 30)
        if candles:
            closes = [c["close"] for c in candles]
            momentum = _calculate_momentum(candles)
            stop = closes[-1] * (1 - ALGO1_SL_PCT)
            results.ok(
                f"single/{symbol}/algo1",
                f"bars={len(candles)} | close={closes[-1]} | momentum={momentum} | stop={stop:.2f}",
            )
            if verbose:
                sma200 = IndicatorEngine.sma(closes, 200)
                print(f"\n  [ALGO1]")
                print(f"    bars       : {len(candles)}")
                print(f"    latest     : open={candles[-1]['open']}, high={candles[-1]['high']}, "
                      f"low={candles[-1]['low']}, close={candles[-1]['close']}")
                print(f"    momentum   : {momentum}")
                print(f"    SMA200     : {sma200[-1] if sma200 else 'N/A'}")
                print(f"    stop loss  : {stop:.2f} ({ALGO1_SL_PCT*100:.0f}% below entry)")
        else:
            results.fail(f"single/{symbol}/algo1", "No candles returned")
    except Exception as e:
        results.fail(f"single/{symbol}/algo1", str(e))

    # Algo2 path
    try:
        candles2 = _fetch2(cache, symbol, limit=250)
        if candles2:
            closes2 = [c["close"] for c in candles2]
            death_cross = _death_cross_detected(closes2)
            sma50  = IndicatorEngine.sma(closes2, 50)
            sma200 = IndicatorEngine.sma(closes2, 200)
            stop2  = closes2[-1] * (1 - ALGO2_SL_PCT)
            results.ok(
                f"single/{symbol}/algo2",
                f"bars={len(candles2)} | death_cross={death_cross} | stop={stop2:.2f}",
            )
            if verbose:
                print(f"\n  [ALGO2]")
                print(f"    bars       : {len(candles2)}")
                print(f"    close      : {closes2[-1]}")
                print(f"    SMA50      : {sma50[-1] if sma50 else 'N/A'}")
                print(f"    SMA200     : {sma200[-1] if sma200 else 'N/A'}")
                print(f"    death_cross: {death_cross}")
                print(f"    stop loss  : {stop2:.2f} ({ALGO2_SL_PCT*100:.0f}% below entry)")
        else:
            results.fail(f"single/{symbol}/algo2", "No candles returned")
    except Exception as e:
        results.fail(f"single/{symbol}/algo2", str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap helpers
# ─────────────────────────────────────────────────────────────────────────────

def _build_cache() -> "CacheLayer":
    """Build a CacheLayer using the FCSAPI key from the environment."""
    from trading_engine.fcsapi_client import FCSAPIClient
    from trading_engine.cache_layer import CacheLayer

    api_key = os.environ.get("FCSAPI_KEY", "")
    if not api_key:
        logger.error("FCSAPI_KEY env var not set — set it before running tests")
        sys.exit(1)

    client = FCSAPIClient(api_key=api_key)
    return CacheLayer(api_client=client)


def _get_strategy_symbols(strategy_name: str) -> list:
    """Return all active symbols for a strategy from the DB."""
    from trading_engine.database import get_strategy_assets

    rows = get_strategy_assets(strategy_name)
    if not rows:
        return []
    # get_strategy_assets returns list[str] (symbol strings)
    if isinstance(rows[0], str):
        return list(rows)
    # Fallback: list of dicts
    return [r["symbol"] for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Diagnostic test suite for Stocks Algo 1 & Algo 2"
    )
    parser.add_argument(
        "--test",
        choices=["connectivity", "ndx", "algo1", "algo2", "exits", "sync", "all"],
        default="all",
        help="Which test to run (default: all)",
    )
    parser.add_argument(
        "--symbol",
        default=None,
        help="Test a single symbol only (e.g. --symbol AAPL)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Max symbols to test per strategy (0 = all, default: 10)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run checks but do not insert signals or modify the DB",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show full candle data and indicator values",
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Attempt to auto-fix issues (re-verify unverified symbols etc.)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("STOCKS STRATEGIES TEST SUITE")
    print("=" * 60)
    print(f"  test    : {args.test}")
    print(f"  symbol  : {args.symbol or 'all'}")
    print(f"  limit   : {'all' if args.limit == 0 else args.limit}")
    print(f"  dry-run : {args.dry_run}")
    print(f"  verbose : {args.verbose}")
    print(f"  fix     : {args.fix}")
    print(f"  started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    from trading_engine.database import init_db
    init_db()

    cache = _build_cache()
    results = TestResults()

    # ── Single symbol mode ──────────────────────────────────────────────────
    if args.symbol:
        sym = args.symbol.upper().strip()
        test_single_symbol(cache, results, sym, args.verbose)
        results.summary()
        sys.exit(0 if not results.failed else 1)

    # ── Determine symbol universe ───────────────────────────────────────────
    algo1_syms = _get_strategy_symbols("stocks_algo1")
    algo2_syms = _get_strategy_symbols("stocks_algo2")

    if not algo1_syms and not algo2_syms:
        print("\nNo strategy assets found in DB. Run the app first to seed symbols.")
        sys.exit(1)

    # Use algo1 symbols as the primary universe for connectivity/ndx tests
    all_syms = list(dict.fromkeys(algo1_syms + algo2_syms))  # deduplicated, ordered

    print(f"\nUniverse: {len(algo1_syms)} algo1 symbols, {len(algo2_syms)} algo2 symbols")
    if args.verbose:
        print(f"  Algo1: {', '.join(algo1_syms[:10])}{'...' if len(algo1_syms) > 10 else ''}")
        print(f"  Algo2: {', '.join(algo2_syms[:10])}{'...' if len(algo2_syms) > 10 else ''}")

    # ── Run tests ───────────────────────────────────────────────────────────
    run_all = args.test == "all"

    if run_all or args.test == "connectivity":
        test_connectivity(cache, results, all_syms, args.limit, args.verbose)

    if run_all or args.test == "ndx":
        test_ndx_filter(cache, results, args.verbose)

    if run_all or args.test == "algo1":
        test_algo1(cache, results, algo1_syms, args.limit, args.dry_run, args.verbose)

    if run_all or args.test == "algo2":
        test_algo2(cache, results, algo2_syms, args.limit, args.dry_run, args.verbose)

    if run_all or args.test == "exits":
        test_exits(cache, results, args.dry_run)

    if run_all or args.test == "sync":
        test_sync(results, args.dry_run)

    # ── Auto-fix ────────────────────────────────────────────────────────────
    if args.fix and results.failed:
        print("\n" + "-" * 60)
        print("AUTO-FIX MODE")
        print("-" * 60)
        from trading_engine.database import mark_asset_verified

        fix_syms = [
            name.split("/")[1]
            for name, _ in results.failed
            if "/fetch/" in name or "/connectivity/" in name
        ]
        for sym in set(fix_syms):
            for strat in ("stocks_algo1", "stocks_algo2"):
                mark_asset_verified(strat, sym, verified=False)
                logger.info(f"  [FIX] Cleared verification flag for {strat}/{sym}")

    ok = results.summary()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
