import logging
import hashlib
import math
from typing import Optional
from datetime import datetime

from fastapi import APIRouter, Query, HTTPException
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

CACHE_TTL = 60
CACHE_MAX_SIZE = 256

_cache = TTLCache(maxsize=CACHE_MAX_SIZE, ttl=CACHE_TTL)
_stats = {"hits": 0, "misses": 0}


def _cache_key(prefix: str, **kwargs) -> str:
    raw = f"{prefix}:" + ":".join(f"{k}={v}" for k, v in sorted(kwargs.items()) if v is not None)
    return hashlib.md5(raw.encode()).hexdigest()


def _cache_get(key: str):
    val = _cache.get(key)
    if val is not None:
        _stats["hits"] += 1
        return val
    _stats["misses"] += 1
    return None


def _cache_set(key: str, value):
    _cache[key] = value


def _with_cache(result: dict, hit: bool) -> dict:
    out = dict(result)
    out["cache"] = "hit" if hit else "miss"
    return out


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
def get_signals_latest(
    asset: Optional[str] = Query(None, description="Filter by asset symbol (e.g. EUR/USD, SPX)"),
    strategy: Optional[str] = Query(None, description="Filter by strategy name (e.g. mtf_ema, trend_forex)"),
    asset_class: Optional[str] = Query(None, description="Filter by asset class: forex, crypto, commodities, indices"),
):
    cache_key = _cache_key("latest", asset=asset, strategy=strategy, asset_class=asset_class)
    cached = _cache_get(cache_key)
    if cached is not None:
        return _with_cache(cached, True)

    raw = get_active_signals(strategy_name=strategy, asset=asset)
    formatted = [_format_signal(s) for s in raw]
    formatted = _filter_by_asset_class(formatted, asset_class)

    result = {"signals": formatted, "count": len(formatted)}
    _cache_set(cache_key, result)
    return _with_cache(result, False)


@router.get("/signals/history")
def get_signals_history(
    asset: Optional[str] = Query(None, description="Filter by asset symbol"),
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    status: Optional[str] = Query(None, description="Filter by status: OPEN or CLOSED"),
    asset_class: Optional[str] = Query(None, description="Filter by asset class: forex, crypto, commodities, indices"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    size: int = Query(20, ge=1, le=100, description="Items per page (max 100)"),
):
    cache_key = _cache_key("history", asset=asset, strategy=strategy, status=status, asset_class=asset_class, page=page, size=size)
    cached = _cache_get(cache_key)
    if cached is not None:
        return _with_cache(cached, True)

    all_raw = get_all_signals(strategy_name=strategy, asset=asset, status=status, limit=500)
    all_formatted = [_format_signal(s) for s in all_raw]
    all_formatted = _filter_by_asset_class(all_formatted, asset_class)

    total_count = len(all_formatted)
    total_pages = max(1, math.ceil(total_count / size))
    offset = (page - 1) * size
    page_items = all_formatted[offset:offset + size]

    result = {
        "signals": page_items,
        "total_count": total_count,
        "page": page,
        "size": size,
        "total_pages": total_pages,
    }
    _cache_set(cache_key, result)
    return _with_cache(result, False)


@router.get("/signals/active")
def get_signals_active(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    asset: Optional[str] = Query(None, description="Filter by asset symbol"),
    category: Optional[str] = Query(None, description="Filter by category: forex, crypto, commodities, indices"),
):
    cache_key = _cache_key("active", strategy=strategy, asset=asset, category=category)
    cached = _cache_get(cache_key)
    if cached is not None:
        return _with_cache(cached, True)

    raw = get_active_signals(strategy_name=strategy, asset=asset)
    formatted = [_format_signal(s) for s in raw]
    formatted = _filter_by_category(formatted, category)

    result = {"signals": formatted, "count": len(formatted)}
    _cache_set(cache_key, result)
    return _with_cache(result, False)


@router.get("/signals/{signal_id}")
def get_signal_by_id(signal_id: int):
    cache_key = _cache_key("signal_detail", id=signal_id)
    cached = _cache_get(cache_key)
    if cached is not None:
        return _with_cache(cached, True)

    all_sigs = get_all_signals(limit=500)
    match = next((s for s in all_sigs if s["id"] == signal_id), None)
    if not match:
        raise HTTPException(status_code=404, detail="Signal not found")

    result = {"signal": _format_signal(match)}
    _cache_set(cache_key, result)
    return _with_cache(result, False)


@router.get("/signals")
def get_signals(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    asset: Optional[str] = Query(None, description="Filter by asset symbol"),
    status: Optional[str] = Query(None, description="Filter by status: OPEN or CLOSED"),
    category: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(50, ge=1, le=200, description="Max results (default 50, max 200)"),
):
    cache_key = _cache_key("signals", strategy=strategy, asset=asset, status=status, category=category, limit=limit)
    cached = _cache_get(cache_key)
    if cached is not None:
        return _with_cache(cached, True)

    raw = get_all_signals(strategy_name=strategy, asset=asset, status=status, limit=limit)
    formatted = [_format_signal(s) for s in raw]
    formatted = _filter_by_category(formatted, category)

    result = {"signals": formatted, "count": len(formatted)}
    _cache_set(cache_key, result)
    return _with_cache(result, False)


@router.get("/strategies")
def list_strategies():
    cache_key = _cache_key("strategies")
    cached = _cache_get(cache_key)
    if cached is not None:
        return _with_cache(cached, True)

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

    result = {"strategies": list(strategy_stats.values()), "count": len(strategy_stats)}
    _cache_set(cache_key, result)
    return _with_cache(result, False)


@router.get("/market/candles")
def get_market_candles(
    asset: str = Query(..., description="Asset symbol (e.g. EUR/USD, SPX, BTC/USD)"),
    timeframe: str = Query("D1", description="Timeframe: 30m, 1H, 4H, D1"),
    limit: int = Query(100, ge=1, le=300, description="Max candles (default 100, max 300)"),
):
    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(status_code=400, detail=f"Invalid timeframe. Use: {', '.join(VALID_TIMEFRAMES)}")

    cache_key = _cache_key("candles", asset=asset, timeframe=timeframe, limit=limit)
    cached = _cache_get(cache_key)
    if cached is not None:
        return _with_cache(cached, True)

    candles = get_candles(asset, timeframe, limit=limit)
    total_stored = get_candle_count(asset, timeframe)
    meta = get_cache_metadata(asset, timeframe)

    result = {
        "asset": asset,
        "timeframe": timeframe,
        "candles": candles,
        "count": len(candles),
        "total_stored": total_stored,
        "last_fetched": meta.get("last_fetched") if meta else None,
    }
    _cache_set(cache_key, result)
    return _with_cache(result, False)


@router.get("/market/indicators")
def get_market_indicators(
    asset: str = Query(..., description="Asset symbol"),
    timeframe: str = Query("D1", description="Timeframe: 30m, 1H, 4H, D1"),
):
    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(status_code=400, detail=f"Invalid timeframe. Use: {', '.join(VALID_TIMEFRAMES)}")

    cache_key = _cache_key("indicators", asset=asset, timeframe=timeframe)
    cached = _cache_get(cache_key)
    if cached is not None:
        return _with_cache(cached, True)

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

    result = {
        "asset": asset,
        "timeframe": timeframe,
        "latest": latest,
        "candle_count": len(candles),
        "last_close": closes[-1] if closes else None,
    }
    _cache_set(cache_key, result)
    return _with_cache(result, False)


@router.get("/positions")
def get_positions(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    asset: Optional[str] = Query(None, description="Filter by asset symbol"),
):
    cache_key = _cache_key("positions", strategy=strategy, asset=asset)
    cached = _cache_get(cache_key)
    if cached is not None:
        return _with_cache(cached, True)

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

    result = {"positions": formatted, "count": len(formatted)}
    _cache_set(cache_key, result)
    return _with_cache(result, False)


@router.get("/scheduler/status")
def get_scheduler_status():
    cache_key = _cache_key("scheduler_status")
    cached = _cache_get(cache_key)
    if cached is not None:
        return _with_cache(cached, True)

    summary = get_scheduler_health_summary()

    result = {
        "last_24h": {
            "success": summary.get("success_24h", 0),
            "failures": summary.get("failure_24h", 0),
        },
        "last_job": summary.get("last_job"),
    }
    _cache_set(cache_key, result)
    return _with_cache(result, False)


@router.get("/scheduler/jobs")
def get_scheduler_jobs(
    limit: int = Query(20, ge=1, le=100, description="Max job logs (default 20, max 100)"),
):
    cache_key = _cache_key("scheduler_jobs", limit=limit)
    cached = _cache_get(cache_key)
    if cached is not None:
        return _with_cache(cached, True)

    logs = get_recent_job_logs(limit=limit)

    result = {"jobs": logs, "count": len(logs)}
    _cache_set(cache_key, result)
    return _with_cache(result, False)


@router.get("/health")
def api_health():
    total_requests = _stats["hits"] + _stats["misses"]
    hit_rate = round(_stats["hits"] / total_requests * 100, 1) if total_requests > 0 else 0.0

    return {
        "status": "ok",
        "version": "v1",
        "cache": {
            "current_size": len(_cache),
            "max_size": CACHE_MAX_SIZE,
            "ttl_seconds": CACHE_TTL,
            "total_requests": total_requests,
            "hits": _stats["hits"],
            "misses": _stats["misses"],
            "hit_rate_percent": hit_rate,
        },
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
