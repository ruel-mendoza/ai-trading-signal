import logging
import threading
import time
import functools
import math
from typing import Optional
from datetime import datetime

from fastapi import APIRouter, Query, HTTPException, Request
from cachetools import TTLCache

from trading_engine.database import (
    get_all_signals,
    get_active_signals,
    get_candles,
    get_candle_count,
    get_cache_metadata,
    get_all_open_positions,
    get_api_usage_stats,
    get_recent_job_logs,
    get_scheduler_health_summary,
)

logger = logging.getLogger("trading_engine.api_v1")


class CachePool:
    DEFAULT_TTL = 60
    DEFAULT_MAX_SIZE = 256
    POOL_SIZE = 4

    def __init__(self, pool_size: int = POOL_SIZE, max_size: int = DEFAULT_MAX_SIZE, default_ttl: int = DEFAULT_TTL):
        self._pool_size = pool_size
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._pools: dict[int, list[TTLCache]] = {}
        self._pool_locks: dict[int, list[threading.Lock]] = {}
        self._global_lock = threading.Lock()
        self._stats = {"hits": 0, "misses": 0, "sets": 0}
        self._stats_lock = threading.Lock()
        self._ensure_pool(default_ttl)

    def _ensure_pool(self, ttl: int):
        if ttl not in self._pools:
            with self._global_lock:
                if ttl not in self._pools:
                    self._pools[ttl] = [TTLCache(maxsize=self._max_size, ttl=ttl) for _ in range(self._pool_size)]
                    self._pool_locks[ttl] = [threading.Lock() for _ in range(self._pool_size)]

    def _shard_index(self, key: str) -> int:
        h = 0
        for c in key:
            h = (h * 31 + ord(c)) & 0xFFFFFFFF
        return h % self._pool_size

    def get(self, key: str, ttl: Optional[int] = None):
        pool_ttl = ttl or self._default_ttl
        self._ensure_pool(pool_ttl)
        idx = self._shard_index(key)
        with self._pool_locks[pool_ttl][idx]:
            val = self._pools[pool_ttl][idx].get(key)
        with self._stats_lock:
            if val is not None:
                self._stats["hits"] += 1
            else:
                self._stats["misses"] += 1
        return val

    def set(self, key: str, value, ttl: Optional[int] = None):
        pool_ttl = ttl or self._default_ttl
        self._ensure_pool(pool_ttl)
        idx = self._shard_index(key)
        with self._pool_locks[pool_ttl][idx]:
            self._pools[pool_ttl][idx][key] = value
        with self._stats_lock:
            self._stats["sets"] += 1

    def invalidate(self, key: str, ttl: Optional[int] = None) -> bool:
        pool_ttl = ttl or self._default_ttl
        if pool_ttl not in self._pools:
            return False
        idx = self._shard_index(key)
        with self._pool_locks[pool_ttl][idx]:
            try:
                del self._pools[pool_ttl][idx][key]
                return True
            except KeyError:
                return False

    def flush(self):
        with self._global_lock:
            for ttl_val in list(self._pools.keys()):
                for i in range(self._pool_size):
                    with self._pool_locks[ttl_val][i]:
                        self._pools[ttl_val][i].clear()

    def get_stats(self) -> dict:
        total_size = 0
        ttl_pools = []
        with self._global_lock:
            pool_keys = list(self._pools.keys())
        for ttl_val in pool_keys:
            pool_size = 0
            for i in range(self._pool_size):
                with self._pool_locks[ttl_val][i]:
                    pool_size += len(self._pools[ttl_val][i])
            total_size += pool_size
            ttl_pools.append({"ttl": ttl_val, "entries": pool_size})

        with self._stats_lock:
            total = self._stats["hits"] + self._stats["misses"]
            return {
                "current_size": total_size,
                "max_size_per_shard": self._max_size,
                "shards_per_pool": self._pool_size,
                "pools": ttl_pools,
                "total_requests": total,
                "hits": self._stats["hits"],
                "misses": self._stats["misses"],
                "sets": self._stats["sets"],
                "hit_rate_percent": round(self._stats["hits"] / total * 100, 1) if total > 0 else 0.0,
            }


cache_pool = CachePool()


def _build_cache_key(prefix: str, params: dict) -> str:
    parts = [prefix]
    for k, v in sorted(params.items()):
        parts.append(f"{k}={v}")
    return ":".join(parts)


def cache_response(ttl: int = CachePool.DEFAULT_TTL, prefix: Optional[str] = None):
    def decorator(fn):
        import inspect
        sig = inspect.signature(fn)

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            all_params = dict(bound.arguments)

            key_prefix = prefix or fn.__name__
            cache_key = _build_cache_key(key_prefix, all_params)

            cached = cache_pool.get(cache_key, ttl=ttl)
            if cached is not None:
                out = dict(cached)
                out["cache"] = "hit"
                return out

            t0 = time.monotonic()
            result = fn(*args, **kwargs)
            elapsed_ms = round((time.monotonic() - t0) * 1000, 1)

            cache_pool.set(cache_key, result, ttl=ttl)

            out = dict(result)
            out["cache"] = "miss"
            out["response_time_ms"] = elapsed_ms
            return out

        return wrapper
    return decorator


router = APIRouter(prefix="/v1", tags=["Public API v1"])


CATEGORY_MAP = {
    "EUR/USD": "forex", "GBP/USD": "forex", "USD/JPY": "forex",
    "USD/CAD": "forex", "AUD/USD": "forex", "NZD/USD": "forex",
    "USD/CHF": "forex", "EUR/GBP": "forex",
    "BTC/USD": "crypto", "ETH/USD": "crypto",
    "XAU/USD": "commodities", "XAG/USD": "commodities", "OSX": "commodities",
    "SPX": "indices", "NDX": "indices", "RUT": "indices",
}

STRATEGY_LABELS = {
    "mtf_ema": "MTF EMA",
    "trend_non_forex": "Trend Non-Forex",
    "trend_forex": "Trend Forex",
    "sp500_momentum": "SP500 Momentum",
    "highest_lowest_fx": "Highest/Lowest FX",
}

VALID_TIMEFRAMES = ["30m", "1H", "4H", "D1"]


def _format_signal(s: dict) -> dict:
    return {
        "id": s["id"],
        "asset": s["asset"],
        "category": CATEGORY_MAP.get(s["asset"], "other"),
        "strategy": s["strategy_name"],
        "strategy_label": STRATEGY_LABELS.get(s["strategy_name"], s["strategy_name"]),
        "direction": s["direction"],
        "entry_price": s["entry_price"],
        "stop_loss": s["stop_loss"],
        "take_profit": s["take_profit"],
        "trailing_stop": s["take_profit"] is None,
        "status": s["status"],
        "exit_price": s.get("exit_price"),
        "exit_reason": s.get("exit_reason"),
        "opened_at": s.get("signal_timestamp") or s.get("created_at"),
        "updated_at": s.get("updated_at"),
    }


def _filter_by_category(signals: list, category: Optional[str]) -> list:
    if not category:
        return signals
    return [s for s in signals if s["category"] == category]


def _filter_by_asset_class(signals: list, asset_class: Optional[str]) -> list:
    if not asset_class:
        return signals
    return [s for s in signals if s["category"] == asset_class]


@router.get("/signals/latest")
@cache_response(ttl=60, prefix="signals_latest")
def get_signals_latest(
    asset: Optional[str] = Query(None, description="Filter by asset symbol (e.g. EUR/USD, SPX)"),
    strategy: Optional[str] = Query(None, description="Filter by strategy name (e.g. mtf_ema, trend_forex)"),
    asset_class: Optional[str] = Query(None, description="Filter by asset class: forex, crypto, commodities, indices"),
):
    raw = get_active_signals(strategy_name=strategy, asset=asset)
    formatted = [_format_signal(s) for s in raw]
    formatted = _filter_by_asset_class(formatted, asset_class)
    return {"signals": formatted, "count": len(formatted)}


@router.get("/signals/history")
@cache_response(ttl=60, prefix="signals_history")
def get_signals_history(
    asset: Optional[str] = Query(None, description="Filter by asset symbol"),
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    status: Optional[str] = Query(None, description="Filter by status: OPEN or CLOSED"),
    asset_class: Optional[str] = Query(None, description="Filter by asset class: forex, crypto, commodities, indices"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    size: int = Query(20, ge=1, le=100, description="Items per page (max 100)"),
):
    all_raw = get_all_signals(strategy_name=strategy, asset=asset, status=status, limit=500)
    all_formatted = [_format_signal(s) for s in all_raw]
    all_formatted = _filter_by_asset_class(all_formatted, asset_class)

    total_count = len(all_formatted)
    total_pages = max(1, math.ceil(total_count / size))
    offset = (page - 1) * size
    page_items = all_formatted[offset:offset + size]

    return {
        "signals": page_items,
        "total_count": total_count,
        "page": page,
        "size": size,
        "total_pages": total_pages,
    }


@router.get("/signals/active")
@cache_response(ttl=60, prefix="signals_active")
def get_signals_active(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    asset: Optional[str] = Query(None, description="Filter by asset symbol"),
    category: Optional[str] = Query(None, description="Filter by category: forex, crypto, commodities, indices"),
):
    raw = get_active_signals(strategy_name=strategy, asset=asset)
    formatted = [_format_signal(s) for s in raw]
    formatted = _filter_by_category(formatted, category)
    return {"signals": formatted, "count": len(formatted)}


@router.get("/signals/{signal_id}")
@cache_response(ttl=60, prefix="signal_detail")
def get_signal_by_id(signal_id: int):
    all_sigs = get_all_signals(limit=500)
    match = next((s for s in all_sigs if s["id"] == signal_id), None)
    if not match:
        raise HTTPException(status_code=404, detail="Signal not found")
    return {"signal": _format_signal(match)}


@router.get("/signals")
@cache_response(ttl=60, prefix="signals_all")
def get_signals(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    asset: Optional[str] = Query(None, description="Filter by asset symbol"),
    status: Optional[str] = Query(None, description="Filter by status: OPEN or CLOSED"),
    category: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(50, ge=1, le=200, description="Max results (default 50, max 200)"),
):
    raw = get_all_signals(strategy_name=strategy, asset=asset, status=status, limit=limit)
    formatted = [_format_signal(s) for s in raw]
    formatted = _filter_by_category(formatted, category)
    return {"signals": formatted, "count": len(formatted)}


@router.get("/strategies")
@cache_response(ttl=60, prefix="strategies")
def list_strategies():
    all_sigs = get_all_signals(limit=500)
    strategy_stats = {}
    for s in all_sigs:
        name = s["strategy_name"]
        if name not in strategy_stats:
            strategy_stats[name] = {
                "name": name,
                "label": STRATEGY_LABELS.get(name, name),
                "total": 0,
                "open": 0,
                "closed": 0,
            }
        strategy_stats[name]["total"] += 1
        if s["status"] == "OPEN":
            strategy_stats[name]["open"] += 1
        else:
            strategy_stats[name]["closed"] += 1

    return {"strategies": list(strategy_stats.values()), "count": len(strategy_stats)}


@router.get("/market/candles")
@cache_response(ttl=60, prefix="market_candles")
def get_market_candles(
    asset: str = Query(..., description="Asset symbol (e.g. EUR/USD, SPX, BTC/USD)"),
    timeframe: str = Query("D1", description="Timeframe: 30m, 1H, 4H, D1"),
    limit: int = Query(100, ge=1, le=300, description="Max candles (default 100, max 300)"),
):
    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(status_code=400, detail=f"Invalid timeframe. Use: {', '.join(VALID_TIMEFRAMES)}")

    candles = get_candles(asset, timeframe, limit=limit)
    total_stored = get_candle_count(asset, timeframe)
    meta = get_cache_metadata(asset, timeframe)

    return {
        "asset": asset,
        "timeframe": timeframe,
        "candles": candles,
        "count": len(candles),
        "total_stored": total_stored,
        "last_fetched": meta.get("last_fetched") if meta else None,
    }


@router.get("/market/indicators")
@cache_response(ttl=60, prefix="market_indicators")
def get_market_indicators(
    asset: str = Query(..., description="Asset symbol"),
    timeframe: str = Query("D1", description="Timeframe: 30m, 1H, 4H, D1"),
):
    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(status_code=400, detail=f"Invalid timeframe. Use: {', '.join(VALID_TIMEFRAMES)}")

    candles = get_candles(asset, timeframe, limit=300)
    if not candles:
        raise HTTPException(status_code=404, detail=f"No candle data for {asset}/{timeframe}")

    from trading_engine.indicators import IndicatorEngine
    indicator_engine = IndicatorEngine()

    closes = [float(c["close"]) for c in candles]
    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]

    if not closes:
        raise HTTPException(status_code=404, detail="Insufficient data for indicators")

    latest = {}
    for period in [20, 50, 100, 200]:
        sma = indicator_engine.sma(closes, period)
        if sma and sma[-1] is not None:
            latest[f"sma_{period}"] = round(sma[-1], 6)

        ema = indicator_engine.ema(closes, period)
        if ema and ema[-1] is not None:
            latest[f"ema_{period}"] = round(ema[-1], 6)

    for period in [14, 20]:
        rsi = indicator_engine.rsi(closes, period)
        if rsi and rsi[-1] is not None:
            latest[f"rsi_{period}"] = round(rsi[-1], 4)

    for period in [14, 100]:
        atr = indicator_engine.atr(highs, lows, closes, period)
        if atr and atr[-1] is not None:
            latest[f"atr_{period}"] = round(atr[-1], 6)

    return {
        "asset": asset,
        "timeframe": timeframe,
        "latest": latest,
        "candle_count": len(candles),
        "last_close": closes[-1] if closes else None,
    }


@router.get("/positions")
@cache_response(ttl=60, prefix="positions")
def get_positions(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    asset: Optional[str] = Query(None, description="Filter by asset symbol"),
):
    positions = get_all_open_positions(strategy_name=strategy, asset=asset)

    formatted = []
    for p in positions:
        formatted.append({
            "id": p.get("id"),
            "asset": p.get("asset"),
            "category": CATEGORY_MAP.get(p.get("asset", ""), "other"),
            "strategy": p.get("strategy_name"),
            "strategy_label": STRATEGY_LABELS.get(p.get("strategy_name", ""), p.get("strategy_name", "")),
            "direction": p.get("direction"),
            "entry_price": p.get("entry_price"),
            "atr_at_entry": p.get("atr_at_entry"),
            "highest_price_since_entry": p.get("highest_price_since_entry"),
            "lowest_price_since_entry": p.get("lowest_price_since_entry"),
            "opened_at": p.get("created_at"),
        })

    return {"positions": formatted, "count": len(formatted)}


@router.get("/scheduler/status")
@cache_response(ttl=30, prefix="scheduler_status")
def get_scheduler_status():
    summary = get_scheduler_health_summary()
    return {
        "last_24h": {
            "success": summary.get("success_24h", 0),
            "failures": summary.get("failure_24h", 0),
        },
        "last_job": summary.get("last_job"),
    }


@router.get("/scheduler/jobs")
@cache_response(ttl=30, prefix="scheduler_jobs")
def get_scheduler_jobs(
    limit: int = Query(20, ge=1, le=100, description="Max job logs (default 20, max 100)"),
):
    logs = get_recent_job_logs(limit=limit)
    return {"jobs": logs, "count": len(logs)}


@router.get("/health")
def api_health():
    stats = cache_pool.get_stats()
    return {
        "status": "ok",
        "version": "v1",
        "cache": stats,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


@router.get("/health/public")
def api_health_public():
    return {
        "status": "UP",
        "version": "v1",
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


@router.post("/cache/flush")
def flush_cache():
    cache_pool.flush()
    return {"status": "flushed", "message": "All cache shards cleared"}
