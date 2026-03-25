"""
stocks_algo1.py — Monthly Momentum Strategy (NASDAQ 100)

Mirrors the QC algorithm "UglyTanDinosaur":
  • Runs on the first trading day of each month
  • Market filter: NDX daily close > NDX SMA(200)  → skip month if FALSE
  • Ranks all NDX100 stocks by 11-month momentum (12m-ago → 1m-ago)
  • Enters LONG on top 20 performers; closes any that fall out of top 20
  • Static 8% initial stop loss; no trailing

FCSAPI mapping
  • NDX index candles  → /indices/history  symbol=NDX  period=1d
  • Stock candles      → /stock/history    symbol=<TICKER>  exchange=NASDAQ  period=1d
"""

import logging
from datetime import datetime, timedelta, date
from typing import Optional

import pytz

from trading_engine.strategies.base import BaseStrategy, SignalResult, Action, Direction
from trading_engine.indicators import IndicatorEngine
from trading_engine.cache_layer import CacheLayer
from trading_engine.database import (
    signal_exists,
    has_open_signal,
    insert_signal,
    close_signal,
    get_active_signals,
    get_all_open_positions,
    open_position as db_open_position,
    close_position,
    get_strategy_assets,
    get_algo1_active_symbols,
)

logger = logging.getLogger("trading_engine.strategy.stocks_algo1")

STRATEGY_NAME = "stocks_algo1"
NDX_SYMBOL = "NDX"
SMA_PERIOD = 200
MOMENTUM_LONG_BARS = 252  # ~12 months of D1 bars
MOMENTUM_SHORT_BARS = 21  # ~1 month  of D1 bars
TOP_N = 20
STOP_LOSS_PCT = 0.08  # 8% below entry

ET_ZONE = pytz.timezone("America/New_York")


def _fetch_stock_candles(
    cache: CacheLayer, symbol: str, limit: int = 300
) -> list[dict]:
    """
    Fetch D1 candles for a NASDAQ-listed equity.

    The FCSAPIClient already routes ETF/stock symbols through
    BASE_URL_V4_STOCK with the correct `type` and `exchange` parameters
    (see get_v4_base_url / get_candles in fcsapi_client.py).  For pure
    stock tickers not in the existing maps we force the stock endpoint
    via a direct call.
    """
    from trading_engine.fcsapi_client import (
        BASE_URL_V4_STOCK,
        TIMEFRAME_MAP,
    )

    api_key = cache.api_client.api_key
    if not api_key:
        logger.warning(f"[ALGO1] No API key — cannot fetch candles for {symbol}")
        return []

    tf_api = TIMEFRAME_MAP.get("D1", "1d")
    params = {
        "symbol": symbol,
        "period": tf_api,
        "length": str(limit),
        "type": "equity",
        "exchange": "NASDAQ",
        "access_key": api_key,
    }
    try:
        import requests as _req
        from trading_engine.database import log_api_usage
        from trading_engine.credit_control import pre_request_check

        pre_request_check()
        url = f"{BASE_URL_V4_STOCK}/history"
        resp = _req.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        log_api_usage(endpoint=f"stock/history/{symbol}")

        if not data.get("status") or not data.get("response"):
            return []

        from trading_engine.fcsapi_client import (
            _parse_response_items,
            _validate_candle_prices,
        )

        candles = _parse_response_items(data["response"])
        candles = _validate_candle_prices(candles, symbol)
        return candles
    except Exception as e:
        logger.warning(f"[ALGO1] {symbol} | candle fetch failed: {e}")
        return []


def _fetch_ndx_candles(cache: CacheLayer, limit: int = 300) -> list[dict]:
    """Fetch D1 candles for NDX via the existing cache layer (routed as forex/index)."""
    try:
        candles = cache.get_candles(NDX_SYMBOL, "D1", limit)
        return candles
    except Exception as e:
        logger.error(f"[ALGO1] NDX candle fetch failed: {e}")
        return []


def _is_first_trading_day_of_month() -> bool:
    """
    True if today is the first trading weekday of the current calendar month
    OR if the last run was in a previous month (catch-up guard).
    We keep it simple: true if today is a weekday and the calendar day is ≤ 3
    (covers Mon when 1st/2nd fell on weekend).
    """
    today = datetime.now(ET_ZONE).date()
    if today.weekday() >= 5:  # weekend
        return False
    # First 3 calendar days of month cover Mon–Fri first-business-day scenarios
    return today.day <= 3


def _calculate_momentum(candles: list[dict]) -> Optional[float]:
    """
    11-month momentum = (price_1m_ago - price_12m_ago) / price_12m_ago

    Uses the most recent 252 D1 candles.  Returns None if insufficient data.
    """
    if len(candles) < MOMENTUM_LONG_BARS:
        return None

    closes = [c["close"] for c in candles]
    price_12m_ago = closes[-MOMENTUM_LONG_BARS]  # oldest bar in window
    price_1m_ago = (
        closes[-MOMENTUM_SHORT_BARS]
        if len(closes) >= MOMENTUM_SHORT_BARS
        else closes[-1]
    )

    if price_12m_ago <= 0:
        return None
    return (price_1m_ago - price_12m_ago) / price_12m_ago


class StocksAlgo1Strategy(BaseStrategy):
    """Monthly Momentum — NASDAQ 100."""

    def __init__(self, cache: CacheLayer):
        self.cache = cache

    @property
    def name(self) -> str:
        return STRATEGY_NAME

    # evaluate() is a no-op for this strategy — all logic lives in run_monthly().
    def evaluate(self, asset, timeframe, df, open_position) -> SignalResult:
        return SignalResult()

    # ─────────────────────────────────────────────────────────────────────────
    # Public entry point called by the scheduler
    # ─────────────────────────────────────────────────────────────────────────
    def run_monthly(self) -> dict:
        """
        Full monthly cycle.  Returns summary dict for job logging.
        """
        now_et = datetime.now(ET_ZONE)
        logger.info(
            f"[ALGO1] ====== Monthly cycle start | {now_et.strftime('%Y-%m-%d %H:%M')} ET ======"
        )

        result = {
            "signals_opened": 0,
            "signals_closed": 0,
            "skipped": False,
            "error": None,
        }

        # ── 1. Market filter ────────────────────────────────────────────────
        ndx_candles = _fetch_ndx_candles(self.cache, limit=250)
        if len(ndx_candles) < SMA_PERIOD:
            logger.warning(
                f"[ALGO1] Insufficient NDX candles: {len(ndx_candles)} (need {SMA_PERIOD})"
            )
            result["skipped"] = True
            return result

        ndx_closes = [c["close"] for c in ndx_candles]
        sma200_vals = IndicatorEngine.sma(ndx_closes, SMA_PERIOD)
        sma200 = (
            sma200_vals[-1] if sma200_vals and sma200_vals[-1] is not None else None
        )

        if sma200 is None:
            logger.warning("[ALGO1] NDX SMA200 returned None — skipping month")
            result["skipped"] = True
            return result

        ndx_latest_close = ndx_closes[-1]
        if ndx_latest_close < sma200:
            logger.info(
                f"[ALGO1] MARKET FILTER FAIL — NDX {ndx_latest_close:.2f} < SMA200 {sma200:.2f} | Skipping month"
            )
            result["skipped"] = True
            return result

        logger.info(
            f"[ALGO1] MARKET FILTER PASS — NDX {ndx_latest_close:.2f} > SMA200 {sma200:.2f}"
        )

        # ── 2. Score all NDX100 symbols ─────────────────────────────────────
        universe = get_strategy_assets(STRATEGY_NAME, active_only=True)
        logger.info(f"[ALGO1] Scoring {len(universe)} symbols")

        scores: dict[str, float] = {}
        for sym in universe:
            candles = _fetch_stock_candles(
                self.cache, sym, limit=MOMENTUM_LONG_BARS + 5
            )
            if not candles:
                logger.debug(f"[ALGO1] {sym} | no candles — skipping")
                continue
            mom = _calculate_momentum(candles)
            if mom is not None:
                scores[sym] = mom
                logger.debug(f"[ALGO1] {sym} | momentum={mom:.4f}")

        if not scores:
            logger.warning("[ALGO1] No momentum scores — aborting")
            result["error"] = "No momentum scores computed"
            return result

        # ── 3. Select top 20 ────────────────────────────────────────────────
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top20 = {sym for sym, _ in ranked[:TOP_N]}
        logger.info(f"[ALGO1] Top {len(top20)} symbols selected")

        # ── 4. Close signals that fell out of top 20 ────────────────────────
        currently_active = get_algo1_active_symbols()
        to_close = currently_active - top20
        for sym in to_close:
            active_sigs = get_active_signals(strategy_name=STRATEGY_NAME, asset=sym)
            for sig in active_sigs:
                close_signal(
                    sig["id"],
                    "Monthly rebalance: symbol dropped from top 20",
                    exit_price=None,
                )
            close_position(STRATEGY_NAME, sym)
            result["signals_closed"] += 1
            logger.info(f"[ALGO1] Closed {sym} — dropped from top 20")

        # ── 5. Open new signals for new top-20 entrants ─────────────────────
        signal_timestamp = now_et.strftime("%Y-%m-%dT%H:%M:%S")
        for sym in top20:
            if sym in currently_active:
                logger.debug(f"[ALGO1] {sym} already in top 20 — holding")
                continue

            if has_open_signal(STRATEGY_NAME, sym):
                logger.debug(
                    f"[ALGO1] {sym} already has open signal — idempotency skip"
                )
                continue

            if signal_exists(STRATEGY_NAME, sym, signal_timestamp):
                logger.debug(
                    f"[ALGO1] {sym} signal already exists for {signal_timestamp}"
                )
                continue

            # Use latest available candle close as entry proxy
            sym_candles = _fetch_stock_candles(self.cache, sym, limit=5)
            if not sym_candles:
                logger.warning(f"[ALGO1] {sym} | no price data for entry — skipping")
                continue

            entry_price = float(sym_candles[-1]["close"])
            stop_loss = round(entry_price * (1 - STOP_LOSS_PCT), 6)

            signal = {
                "strategy_name": STRATEGY_NAME,
                "asset": sym,
                "direction": "BUY",
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "take_profit": None,
                "atr_at_entry": None,  # static % stop — no ATR
                "signal_timestamp": signal_timestamp,
            }
            sig_id = insert_signal(signal)
            if sig_id:
                db_open_position(
                    {
                        "asset": sym,
                        "strategy_name": STRATEGY_NAME,
                        "direction": "BUY",
                        "entry_price": entry_price,
                        "atr_at_entry": None,
                    }
                )
                result["signals_opened"] += 1
                logger.info(
                    f"[ALGO1] LONG {sym} @ {entry_price:.4f} | SL={stop_loss:.4f} (8%) | id={sig_id}"
                )

        logger.info(
            f"[ALGO1] ====== Monthly cycle complete | "
            f"opened={result['signals_opened']} closed={result['signals_closed']} ======"
        )
        return result

    # ─────────────────────────────────────────────────────────────────────────
    # check_exits — called daily by the scheduler (stop loss monitoring)
    # ─────────────────────────────────────────────────────────────────────────
    def check_exits(self) -> list[dict]:
        """
        Check static 8% stop loss for all open Algo1 positions.
        Does NOT evaluate monthly rebalancing — that is handled by run_monthly().
        """
        closed: list[dict] = []
        positions = get_all_open_positions(strategy_name=STRATEGY_NAME)

        if not positions:
            return closed

        logger.info(f"[ALGO1-EXIT] Checking {len(positions)} position(s)")

        for pos in positions:
            sym = pos["asset"]
            entry = pos["entry_price"]
            stop = entry * (1 - STOP_LOSS_PCT)  # always re-derive from entry (static %)

            # Use latest cached D1 candle for price
            try:
                candles = _fetch_stock_candles(self.cache, sym, limit=5)
                if not candles:
                    continue
                current = float(candles[-1]["close"])
            except Exception as e:
                logger.warning(f"[ALGO1-EXIT] {sym} | price fetch failed: {e}")
                continue

            hit = current <= stop
            logger.debug(
                f"[ALGO1-EXIT] {sym} | current={current:.4f} entry={entry:.4f} "
                f"stop={stop:.4f} hit={hit}"
            )

            if hit:
                exit_reason = (
                    f"Stop loss hit (8%) | close={current:.4f} <= stop={stop:.4f}"
                )
                active_sigs = get_active_signals(strategy_name=STRATEGY_NAME, asset=sym)
                for sig in active_sigs:
                    close_signal(sig["id"], exit_reason, exit_price=current)
                close_position(STRATEGY_NAME, sym)
                closed.append(
                    {**pos, "exit_price": current, "exit_reason": "stop_loss_8pct"}
                )
                logger.info(
                    f"[ALGO1-EXIT] {sym} | EXIT — stop loss hit @ {current:.4f}"
                )

        return closed
