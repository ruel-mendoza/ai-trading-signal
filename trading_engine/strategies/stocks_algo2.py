"""
stocks_algo2.py — Mean Reversion / Death Cross Strategy (NASDAQ 100)

Mirrors the QC algorithm "SwimmingYellowGreenDinosaur":
  • Runs daily after market close
  • Entry: stock SMA50 crosses below SMA200 (death cross) AND
           NDX yesterday close was below NDX SMA200
  • Stop Loss: 4% from entry (static)
  • Exit: stop loss hit OR 5 trading days elapsed — whichever comes first
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

import pytz

from trading_engine.strategies.base import BaseStrategy, SignalResult, Action, Direction
from trading_engine.indicators import IndicatorEngine
from trading_engine.cache_layer import CacheLayer
from trading_engine.database import (
    signal_exists,
    has_open_signal,
    has_any_open_signal_for_asset,
    insert_signal,
    close_signal,
    get_active_signals,
    get_strategy_assets,
)

# New DB helpers defined in the models additions file
from trading_engine.database import (
    open_stock_algo2_position,
    get_all_stock_algo2_positions,
    close_stock_algo2_position,
)

logger = logging.getLogger("trading_engine.strategy.stocks_algo2")

STRATEGY_NAME = "stocks_algo2"
NDX_SYMBOL = "NDX"
SMA_FAST = 50
SMA_SLOW = 200
NDX_SMA_PERIOD = 200
STOP_LOSS_PCT = 0.04  # 4% below entry
MAX_HOLD_TRADING_DAYS = 5

ET_ZONE = pytz.timezone("America/New_York")


def _trading_days_since(entry_date_str: str) -> int:
    """Count actual NYSE trading days (Mon–Fri) from entry_date to today (ET)."""
    try:
        entry = datetime.strptime(str(entry_date_str)[:10], "%Y-%m-%d").date()
        today = datetime.now(ET_ZONE).date()
        count = 0
        d = entry
        while d < today:
            d += timedelta(days=1)
            if d.weekday() < 5:  # Mon–Fri only
                count += 1
        return count
    except Exception:
        return 0


def _fetch_stock_candles(
    cache: CacheLayer, symbol: str, limit: int = 300
) -> list[dict]:
    """Fetch D1 candles for a NASDAQ equity using NASDAQ:SYMBOL prefix format."""
    from trading_engine.fcsapi_client import (
        BASE_URL_V4_STOCK,
        TIMEFRAME_MAP,
        get_nasdaq_api_symbol,
        _parse_response_items,
        _validate_candle_prices,
    )
    from trading_engine.credit_control import pre_request_check
    from trading_engine.database import log_api_usage
    import requests as _req

    api_key = cache.api_client.api_key
    if not api_key:
        return []

    api_symbol = get_nasdaq_api_symbol(symbol)
    params = {
        "symbol": api_symbol,
        "period": TIMEFRAME_MAP.get("D1", "1d"),
        "length": str(limit),
        "access_key": api_key,
    }
    logger.debug(f"[ALGO2] {symbol} | Fetching as {api_symbol}")
    try:
        pre_request_check()
        url = f"{BASE_URL_V4_STOCK}/history"
        resp = _req.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        log_api_usage(endpoint=f"stock/history/{symbol}")
        if not data.get("status") or not data.get("response"):
            return []
        candles = _parse_response_items(data["response"])
        return _validate_candle_prices(candles, symbol)
    except Exception as e:
        logger.warning(f"[ALGO2] {symbol} | candle fetch failed: {e}")
        return []


def _fetch_ndx_candles(cache: CacheLayer, limit: int = 250) -> list[dict]:
    try:
        return cache.get_candles(NDX_SYMBOL, "D1", limit)
    except Exception as e:
        logger.error(f"[ALGO2] NDX candle fetch failed: {e}")
        return []


def _death_cross_detected(closes: list[float]) -> bool:
    """
    True if SMA50 just crossed BELOW SMA200 between the previous bar and
    the current (latest) bar.
    Requires at least SMA_SLOW + 1 bars.
    """
    if len(closes) < SMA_SLOW + 1:
        return False

    sma50 = IndicatorEngine.sma(closes, SMA_FAST)
    sma200 = IndicatorEngine.sma(closes, SMA_SLOW)

    if sma50 is None or sma200 is None or len(sma50) < 2 or len(sma200) < 2:
        return False

    prev_50 = sma50[-2]
    curr_50 = sma50[-1]
    prev_200 = sma200[-2]
    curr_200 = sma200[-1]

    if any(v is None for v in [prev_50, curr_50, prev_200, curr_200]):
        return False

    # Death cross: was above yesterday, now below today
    was_above = prev_50 > prev_200
    is_below = curr_50 < curr_200
    return was_above and is_below


class StocksAlgo2Strategy(BaseStrategy):
    """Mean Reversion / Death Cross — NASDAQ 100."""

    def __init__(self, cache: CacheLayer):
        self.cache = cache

    @property
    def name(self) -> str:
        return STRATEGY_NAME

    def evaluate(self, asset, timeframe, df, open_position) -> SignalResult:
        return SignalResult()

    # ─────────────────────────────────────────────────────────────────────────
    # Public entry point called by the scheduler (daily after close)
    # ─────────────────────────────────────────────────────────────────────────
    def run_daily(self) -> dict:
        now_et = datetime.now(ET_ZONE)
        logger.info(
            f"[ALGO2] ====== Daily cycle start | {now_et.strftime('%Y-%m-%d %H:%M')} ET ======"
        )

        result = {
            "signals_opened": 0,
            "signals_closed": 0,
            "ndx_filter_pass": False,
            "error": None,
        }

        # ── 1. NDX market-regime filter ─────────────────────────────────────
        # Entry condition: NDX *yesterday* was below its 200-day SMA.
        ndx_candles = _fetch_ndx_candles(self.cache, limit=250)
        if len(ndx_candles) < NDX_SMA_PERIOD + 1:
            logger.warning(f"[ALGO2] Insufficient NDX candles: {len(ndx_candles)}")
            result["error"] = "insufficient_ndx_candles"
            return result

        ndx_closes = [c["close"] for c in ndx_candles]
        ndx_sma200_vals = IndicatorEngine.sma(ndx_closes, NDX_SMA_PERIOD)
        ndx_sma200 = (
            ndx_sma200_vals[-1]
            if ndx_sma200_vals and ndx_sma200_vals[-1] is not None
            else None
        )

        if ndx_sma200 is None:
            logger.warning("[ALGO2] NDX SMA200 returned None")
            result["error"] = "ndx_sma200_none"
            return result

        # "yesterday" = second-to-last close
        ndx_yesterday = ndx_closes[-2] if len(ndx_closes) >= 2 else ndx_closes[-1]
        ndx_below_sma = ndx_yesterday < ndx_sma200

        logger.info(
            f"[ALGO2] NDX yesterday close={ndx_yesterday:.2f} | "
            f"SMA200={ndx_sma200:.2f} | below_sma={ndx_below_sma}"
        )

        if not ndx_below_sma:
            logger.info("[ALGO2] NDX market filter NOT met — no new entries today")
            # Still run exits even when market filter fails
            self.check_exits()
            result["signals_closed"] = 0  # updated inside check_exits
            return result

        result["ndx_filter_pass"] = True

        # ── 2. Scan universe for death crosses ───────────────────────────────
        universe = get_strategy_assets(STRATEGY_NAME, active_only=True)
        signal_timestamp = now_et.strftime("%Y-%m-%dT%H:%M:%S")

        for sym in universe:
            if has_open_signal(STRATEGY_NAME, sym):
                logger.debug(f"[ALGO2] {sym} already has open signal — skip")
                continue

            if has_any_open_signal_for_asset(sym, exclude_strategies=[STRATEGY_NAME]):
                logger.debug(f"[ALGO2] {sym} blocked by cross-strategy idempotency")
                continue

            candles = _fetch_stock_candles(self.cache, sym, limit=SMA_SLOW + 10)
            if not candles or len(candles) < SMA_SLOW + 1:
                logger.debug(
                    f"[ALGO2] {sym} | insufficient candles ({len(candles) if candles else 0})"
                )
                continue

            closes = [float(c["close"]) for c in candles]
            death_cross = _death_cross_detected(closes)

            logger.debug(f"[ALGO2] {sym} | death_cross={death_cross}")

            if not death_cross:
                continue

            if signal_exists(STRATEGY_NAME, sym, signal_timestamp):
                logger.debug(
                    f"[ALGO2] {sym} | signal already exists for {signal_timestamp}"
                )
                continue

            entry_price = float(candles[-1]["close"])
            stop_loss = round(entry_price * (1 - STOP_LOSS_PCT), 6)

            signal = {
                "strategy_name": STRATEGY_NAME,
                "asset": sym,
                "direction": "BUY",
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "take_profit": None,
                "atr_at_entry": None,
                "signal_timestamp": signal_timestamp,
            }
            sig_id = insert_signal(signal)
            if sig_id:
                pos_id = open_stock_algo2_position(
                    symbol=sym,
                    signal_id=sig_id,
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    entry_date=signal_timestamp,
                )
                if pos_id is None:
                    logger.error(
                        f"[ALGO2] CRITICAL: signal #{sig_id} inserted for {sym} but "
                        f"open_stock_algo2_position() returned None — "
                        f"position row missing, attempting direct insert"
                    )
                    from trading_engine.database import SessionFactory
                    from trading_engine.models import StockAlgo2Position
                    with SessionFactory() as session:
                        try:
                            session.add(StockAlgo2Position(
                                symbol=sym,
                                signal_id=sig_id,
                                entry_price=entry_price,
                                stop_loss=stop_loss,
                                entry_date=signal_timestamp,
                                trading_days_held=0,
                            ))
                            session.commit()
                            logger.info(f"[ALGO2] Recovery insert succeeded for {sym} signal #{sig_id}")
                        except Exception as e:
                            session.rollback()
                            logger.error(f"[ALGO2] Recovery insert also failed for {sym}: {e}")
                else:
                    logger.info(
                        f"[ALGO2] LONG {sym} @ {entry_price:.4f} | "
                        f"SL={stop_loss:.4f} (4%) | hold_days=5 | "
                        f"signal_id={sig_id} | pos_id={pos_id}"
                    )
                result["signals_opened"] += 1

        # ── 3. Run exit checks ───────────────────────────────────────────────
        closed = self.check_exits()
        result["signals_closed"] = len(closed)

        logger.info(
            f"[ALGO2] ====== Daily cycle complete | "
            f"opened={result['signals_opened']} closed={result['signals_closed']} ======"
        )
        return result

    # ─────────────────────────────────────────────────────────────────────────
    # check_exits — stop loss (4%) and 5-trading-day hold period
    # ─────────────────────────────────────────────────────────────────────────
    def check_exits(self) -> list[dict]:
        closed: list[dict] = []

        # ── Orphan detection: find OPEN signals with no matching position row ──
        from trading_engine.database import get_active_signals, SessionFactory
        from trading_engine.models import StockAlgo2Position

        active_sigs = get_active_signals(strategy_name=STRATEGY_NAME)
        positions = get_all_stock_algo2_positions()
        position_signal_ids = {p["signal_id"] for p in positions}

        for sig in active_sigs:
            if sig["id"] not in position_signal_ids:
                logger.warning(
                    f"[ALGO2-EXIT] ORPHAN SIGNAL detected | id={sig['id']} | "
                    f"asset={sig['asset']} | no stock_algo2_positions row — inserting now"
                )
                with SessionFactory() as session:
                    try:
                        existing = session.query(StockAlgo2Position).filter_by(
                            symbol=sig["asset"]
                        ).first()
                        if not existing:
                            from datetime import datetime
                            import pytz
                            _et = pytz.timezone("America/New_York")
                            _now = datetime.now(pytz.utc).astimezone(_et)
                            _ts = sig.get("signal_timestamp", "")
                            _days = 0
                            try:
                                _entry_dt = datetime.strptime(str(_ts)[:19], "%Y-%m-%dT%H:%M:%S")
                                _entry_dt = _et.localize(_entry_dt)
                                _days = max(0, (_now.date() - _entry_dt.date()).days)
                            except Exception:
                                pass
                            session.add(StockAlgo2Position(
                                symbol=sig["asset"],
                                signal_id=sig["id"],
                                entry_price=sig["entry_price"],
                                stop_loss=sig["stop_loss"],
                                entry_date=_ts,
                                trading_days_held=_days,
                            ))
                            session.commit()
                            logger.info(
                                f"[ALGO2-EXIT] Orphan position row inserted for "
                                f"{sig['asset']} signal #{sig['id']}"
                            )
                        else:
                            logger.info(
                                f"[ALGO2-EXIT] Position row already exists for "
                                f"{sig['asset']} — skipping orphan insert"
                            )
                    except Exception as e:
                        session.rollback()
                        logger.error(
                            f"[ALGO2-EXIT] Failed to insert orphan position for "
                            f"{sig['asset']}: {e}"
                        )

        # Re-fetch positions after orphan fix
        positions = get_all_stock_algo2_positions()

        if not positions:
            return closed

        logger.info(f"[ALGO2-EXIT] Checking {len(positions)} position(s)")

        for pos in positions:
            sym = pos["symbol"]
            entry = pos["entry_price"]
            stop = pos["stop_loss"]
            # Compute live from entry_date — matches QC algo intent exactly
            days_held = _trading_days_since(pos["entry_date"])

            # Get current price
            try:
                candles = _fetch_stock_candles(self.cache, sym, limit=5)
                current = float(candles[-1]["close"]) if candles else entry
            except Exception as e:
                logger.warning(f"[ALGO2-EXIT] {sym} | price fetch failed: {e}")
                continue

            stop_hit = current <= stop
            hold_expired = days_held >= MAX_HOLD_TRADING_DAYS

            logger.debug(
                f"[ALGO2-EXIT] {sym} | current={current:.4f} stop={stop:.4f} "
                f"stop_hit={stop_hit} | days={days_held}/{MAX_HOLD_TRADING_DAYS} expired={hold_expired}"
            )

            if stop_hit:
                reason = f"Stop loss (4%) | close={current:.4f} <= stop={stop:.4f}"
                self._close(sym, pos["signal_id"], current, reason)
                closed.append(
                    {**pos, "exit_price": current, "exit_reason": "stop_loss_4pct"}
                )
                logger.info(f"[ALGO2-EXIT] {sym} | EXIT stop loss @ {current:.4f}")
                continue

            if hold_expired:
                reason = f"5-trading-day hold expired | days={days_held} | close={current:.4f}"
                self._close(sym, pos["signal_id"], current, reason)
                closed.append(
                    {**pos, "exit_price": current, "exit_reason": "hold_period_expired"}
                )
                logger.info(
                    f"[ALGO2-EXIT] {sym} | EXIT hold period expired ({days_held}d)"
                )
                continue

            logger.debug(f"[ALGO2-EXIT] {sym} | Holding | days_held={days_held}/{MAX_HOLD_TRADING_DAYS}")

        return closed

    def _close(self, symbol: str, signal_id: int, exit_price: float, reason: str):
        """Close signal and remove tracking row."""
        close_signal(signal_id, reason, exit_price=exit_price)
        close_stock_algo2_position(symbol)
        logger.info(f"[ALGO2] Closed {symbol} | reason={reason[:80]}")
