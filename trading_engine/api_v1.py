import logging
import hashlib
import json
import time
from typing import Optional

from fastapi import APIRouter, Query, HTTPException
from cachetools import TTLCache

from trading_engine.database import get_all_signals, get_active_signals

logger = logging.getLogger("trading_engine.api_v1")

CACHE_TTL = 60
CACHE_MAX_SIZE = 256

_cache = TTLCache(maxsize=CACHE_MAX_SIZE, ttl=CACHE_TTL)


def _cache_key(prefix: str, **kwargs) -> str:
    raw = f"{prefix}:" + ":".join(f"{k}={v}" for k, v in sorted(kwargs.items()) if v is not None)
    return hashlib.md5(raw.encode()).hexdigest()


def _cache_get(key: str):
    val = _cache.get(key)
    if val is not None:
        logger.debug(f"[CACHE] HIT  key={key[:12]}…")
    else:
        logger.debug(f"[CACHE] MISS key={key[:12]}…")
    return val


def _cache_set(key: str, value):
    _cache[key] = value


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


@router.get("/signals")
def get_signals(
    strategy: Optional[str] = Query(None, description="Filter by strategy name (e.g. mtf_ema, trend_forex)"),
    asset: Optional[str] = Query(None, description="Filter by asset symbol (e.g. EUR/USD, SPX)"),
    status: Optional[str] = Query(None, description="Filter by status: OPEN or CLOSED"),
    category: Optional[str] = Query(None, description="Filter by category: forex, crypto, commodities, indices"),
    limit: int = Query(50, ge=1, le=200, description="Max results (default 50, max 200)"),
):
    cache_key = _cache_key("signals", strategy=strategy, asset=asset, status=status, category=category, limit=limit)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    raw = get_all_signals(strategy_name=strategy, asset=asset, status=status, limit=limit)
    formatted = [_format_signal(s) for s in raw]

    if category:
        formatted = [s for s in formatted if s["category"] == category]

    result = {
        "signals": formatted,
        "count": len(formatted),
        "cache": "miss",
    }
    _cache_set(cache_key, result)
    result_copy = dict(result)
    result_copy["cache"] = "miss"
    return result


@router.get("/signals/active")
def get_signals_active(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    asset: Optional[str] = Query(None, description="Filter by asset symbol"),
    category: Optional[str] = Query(None, description="Filter by category: forex, crypto, commodities, indices"),
):
    cache_key = _cache_key("active", strategy=strategy, asset=asset, category=category)
    cached = _cache_get(cache_key)
    if cached is not None:
        cached_copy = dict(cached)
        cached_copy["cache"] = "hit"
        return cached_copy

    raw = get_active_signals(strategy_name=strategy, asset=asset)
    formatted = [_format_signal(s) for s in raw]

    if category:
        formatted = [s for s in formatted if s["category"] == category]

    result = {
        "signals": formatted,
        "count": len(formatted),
        "cache": "miss",
    }
    _cache_set(cache_key, result)
    return result


@router.get("/signals/{signal_id}")
def get_signal_by_id(signal_id: int):
    cache_key = _cache_key("signal_detail", id=signal_id)
    cached = _cache_get(cache_key)
    if cached is not None:
        cached_copy = dict(cached)
        cached_copy["cache"] = "hit"
        return cached_copy

    all_sigs = get_all_signals(limit=500)
    match = next((s for s in all_sigs if s["id"] == signal_id), None)
    if not match:
        raise HTTPException(status_code=404, detail="Signal not found")

    result = {
        "signal": _format_signal(match),
        "cache": "miss",
    }
    _cache_set(cache_key, result)
    return result


@router.get("/strategies")
def list_strategies():
    cache_key = _cache_key("strategies")
    cached = _cache_get(cache_key)
    if cached is not None:
        cached_copy = dict(cached)
        cached_copy["cache"] = "hit"
        return cached_copy

    all_sigs = get_all_signals(limit=500)
    strategy_stats = {}
    for s in all_sigs:
        name = s["strategy_name"]
        if name not in strategy_stats:
            strategy_stats[name] = {"name": name, "label": STRATEGY_LABELS.get(name, name), "total": 0, "open": 0, "closed": 0}
        strategy_stats[name]["total"] += 1
        if s["status"] == "OPEN":
            strategy_stats[name]["open"] += 1
        else:
            strategy_stats[name]["closed"] += 1

    result = {
        "strategies": list(strategy_stats.values()),
        "count": len(strategy_stats),
        "cache": "miss",
    }
    _cache_set(cache_key, result)
    return result


@router.get("/health")
def api_health():
    return {
        "status": "ok",
        "version": "v1",
        "cache_size": len(_cache),
        "cache_max": CACHE_MAX_SIZE,
        "cache_ttl_seconds": CACHE_TTL,
    }
