import logging
import threading
import time
import functools
import math
from typing import Optional, Any
from datetime import datetime

from fastapi import APIRouter, Query, HTTPException, Request
from pydantic import BaseModel, Field
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
    get_signal_metrics,
    get_all_signal_metrics,
    get_full_name_for_asset as _get_name,
)

logger = logging.getLogger("trading_engine.api_v1")

# ── Exit instructions per strategy / direction ────────────────────────────────
_EXIT_INSTRUCTIONS: dict[str, dict[str, str]] = {
    "mtf_ema": {
        "BUY":  "Close the trade at the next hourly candlestick which closes below the EMA (20) on the H1 price chart.",
        "SELL": "Close the trade at the next hourly candlestick which closes above the EMA (20) on the H1 price chart.",
    },
    "trend_forex": {
        "BUY":  "Close the trade at the first daily (New York) close which is more than 3 × the ATR (100) indicator (daily chart) below the highest daily close of the trade.",
        "SELL": "Close the trade at the first daily (New York) close which is more than 3 × the ATR (100) indicator (daily chart) above the lowest daily close of the trade.",
    },
    "trend_non_forex": {
        "BUY":      "Close the trade at the first daily (New York) close which is more than 3 × the ATR (100) indicator (daily chart) below the highest daily close of the trade.",
        "_default": "Close the trade at the first daily (New York) close which is more than 3 × the ATR (100) indicator (daily chart) below the highest daily close of the trade.",
    },
    "highest_lowest_fx": {
        "BUY":      "Close the trade 6 hours after entering it.",
        "SELL":     "Close the trade 6 hours after entering it.",
        "_default": "Close the trade 6 hours after entering it.",
    },
    "sp500_momentum": {
        "_default": "Close the trade at the next 30m candlestick which closes with the RSI (20) below 70.",
    },
    "stocks_algo1": {
        "_default": "Close the trade at the first New York open of the next calendar month if the stock is not still displayed as a signal.",
    },
    "stocks_algo2": {
        "_default": "Close the trade 5 market days after you open it.",
    },
}

EXIT_RULES_PUBLIC: dict[str, str] = {
    "mtf_ema":           "LONG: Close the trade at the next hourly candlestick which closes below the EMA (20) on the H1 price chart. SHORT: Close the trade at the next hourly candlestick which closes above the EMA (20) on the H1 price chart.",
    "trend_forex":       "LONG: Close the trade at the first daily (New York) close which is more than 3 × the ATR (100) indicator (daily chart) below the highest daily close of the trade. SHORT: Close the trade at the first daily (New York) close which is more than 3 × the ATR (100) indicator (daily chart) above the lowest daily close of the trade.",
    "trend_non_forex":   "Close the trade at the first daily (New York) close which is more than 3 × the ATR (100) indicator (daily chart) below the highest daily close of the trade.",
    "sp500_momentum":    "Close the trade at the next 30m candlestick which closes with the RSI (20) below 70.",
    "highest_lowest_fx": "Close the trade 6 hours after entering it.",
    "stocks_algo1":      "Close the trade at the first New York open of the next calendar month if the stock is not still displayed as a signal.",
    "stocks_algo2":      "Close the trade 5 market days after you open it.",
}


def _get_exit_instruction(strategy: str, direction: str) -> Optional[str]:
    """Return the exit instruction string for a strategy + direction combo."""
    bucket = _EXIT_INSTRUCTIONS.get(strategy)
    if not bucket:
        return None
    return bucket.get(direction) or bucket.get("_default")


class SignalPublic(BaseModel):
    asset: str = Field(..., example="EUR/USD")
    full_name: Optional[str] = Field(None, example="Euro / US Dollar")
    asset_class: str = Field(
        ...,
        example="forex",
        description=(
            "Asset class: forex (includes forex pairs, commodities "
            "and indices) | crypto | stocks (reserved). "
            "Set automatically at signal creation from the asset symbol."
        ),
    )
    direction: str = Field(..., example="LONG")
    entry: float = Field(..., example=1.0845)
    stop_loss: float = Field(..., example=1.0790)
    strategy: str = Field(..., example="mtf_ema")
    published_at: str = Field(..., example="2026-03-02T12:00:00Z")
    exit_instructions: Optional[str] = Field(None, description="Human-readable exit rule for this strategy and direction.")
    meta: Optional[dict] = None

class SignalsLatestResponse(BaseModel):
    count: int
    data: list[SignalPublic]
    cache: Optional[str] = None
    response_time_ms: Optional[float] = None

class SignalLegacy(BaseModel):
    id: int
    asset: str
    full_name: Optional[str] = None
    category: str
    strategy: str
    strategy_label: str
    direction: str
    entry_price: float
    stop_loss: float
    exit_instructions: Optional[str] = None
    status: str
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    opened_at: Optional[str] = None
    updated_at: Optional[str] = None

class SignalsListResponse(BaseModel):
    signals: list[SignalLegacy]
    count: int
    cache: Optional[str] = None
    response_time_ms: Optional[float] = None

class SignalsHistoryResponse(BaseModel):
    signals: list[SignalLegacy]
    total_count: int
    page: int
    size: int
    total_pages: int
    cache: Optional[str] = None
    response_time_ms: Optional[float] = None

class SignalDetailResponse(BaseModel):
    signal: SignalLegacy
    cache: Optional[str] = None
    response_time_ms: Optional[float] = None

class StrategySummary(BaseModel):
    name: str
    label: str
    total: int
    open: int
    closed: int
    exit_rules: Optional[str] = None

class StrategiesResponse(BaseModel):
    strategies: list[StrategySummary]
    count: int
    cache: Optional[str] = None
    response_time_ms: Optional[float] = None

class CandleItem(BaseModel):
    timestamp: str
    open: float
    high: float
    low: float
    close: float

class CandlesResponse(BaseModel):
    asset: str
    timeframe: str
    candles: list[dict]
    count: int
    total_stored: int
    last_fetched: Optional[str] = None
    cache: Optional[str] = None
    response_time_ms: Optional[float] = None

class IndicatorsResponse(BaseModel):
    asset: str
    timeframe: str
    latest: dict
    candle_count: int
    last_close: Optional[float] = None
    cache: Optional[str] = None
    response_time_ms: Optional[float] = None

class PositionItem(BaseModel):
    id: Optional[int] = None
    asset: Optional[str] = None
    full_name: Optional[str] = None
    category: str
    strategy: Optional[str] = None
    strategy_label: str
    direction: Optional[str] = None
    entry_price: Optional[float] = None
    atr_at_entry: Optional[float] = None
    highest_price_since_entry: Optional[float] = None
    lowest_price_since_entry: Optional[float] = None
    opened_at: Optional[str] = None

class PositionsResponse(BaseModel):
    positions: list[PositionItem]
    count: int
    cache: Optional[str] = None
    response_time_ms: Optional[float] = None

class MetricItem(BaseModel):
    strategy: str
    asset: Optional[str] = None
    full_name: Optional[str] = None
    period: str
    total_signals: int
    open: int
    closed: int
    won: int
    lost: int
    win_rate: float
    avg_gain_pct: Optional[float] = None
    avg_loss_pct: Optional[float] = None
    best_gain_pct: Optional[float] = None
    worst_loss_pct: Optional[float] = None
    avg_duration_hours: Optional[float] = None
    last_signal_at: Optional[str] = None
    computed_at: Optional[str] = None

class MetricsResponse(BaseModel):
    metrics: list[MetricItem]
    count: int
    period: str
    cache: Optional[str] = None
    response_time_ms: Optional[float] = None

class MetricsSummaryResponse(BaseModel):
    total_signals: int
    total_won: int
    total_lost: int
    overall_win_rate: float
    strategies: list[dict]
    last_computed: Optional[str] = None
    cache: Optional[str] = None
    response_time_ms: Optional[float] = None

class SchedulerStatusResponse(BaseModel):
    last_24h: dict
    last_job: Optional[dict] = None
    cache: Optional[str] = None
    response_time_ms: Optional[float] = None

class SchedulerJobsResponse(BaseModel):
    jobs: list[dict]
    count: int
    cache: Optional[str] = None
    response_time_ms: Optional[float] = None

class HealthResponse(BaseModel):
    status: str
    version: str
    cache: dict
    timestamp: str

class SecurityStatusResponse(BaseModel):
    total_blocked_requests_24h: int = Field(..., example=0)
    current_active_ip_bans: int = Field(..., example=0)


class HealthPublicResponse(BaseModel):
    status: str = Field(..., example="UP")
    version: str = Field(..., example="v1")
    timestamp: str
    security_status: SecurityStatusResponse

class CacheFlushResponse(BaseModel):
    status: str = Field(..., example="flushed")
    message: str


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

    def flush_prefix(self, prefix: str):
        count = 0
        with self._global_lock:
            pool_keys = list(self._pools.keys())
        for ttl_val in pool_keys:
            for i in range(self._pool_size):
                with self._pool_locks[ttl_val][i]:
                    to_delete = [k for k in self._pools[ttl_val][i] if k.startswith(prefix)]
                    for k in to_delete:
                        del self._pools[ttl_val][i][k]
                        count += 1
        return count

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


SIGNAL_CACHE_PREFIXES = [
    "signals_latest",
    "signals_history",
    "signals_active",
    "signals_all",
    "signal_detail",
    "public_signals",
    "public_signals_active",
    "public_signal_detail",
    "public_assets",
    "strategies",
    "positions",
]


def invalidate_signal_caches():
    total = 0
    for prefix in SIGNAL_CACHE_PREFIXES:
        total += cache_pool.flush_prefix(prefix)
    if total > 0:
        logger.info(f"[CACHE] Invalidated {total} signal cache entries")
    return total


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


router = APIRouter(prefix="/api/v1", tags=["Public API v1"])


CATEGORY_MAP: dict[str, str] = {
    # ── Forex (pairs) ──────────────────────────────────
    "EUR/USD": "forex",  "GBP/USD": "forex",  "USD/JPY": "forex",
    "USD/CAD": "forex",  "AUD/USD": "forex",  "NZD/USD": "forex",
    "USD/CHF": "forex",  "EUR/GBP": "forex",
    # ── Forex (spot commodities) ───────────────────────
    "XAU/USD": "forex",  "XAG/USD": "forex",  "XPT/USD": "forex",
    "XPD/USD": "forex",  "XCU/USD": "forex",  "OSX":     "forex",
    "NATGAS/USD": "forex",
    "CORN/USD": "forex", "SOYBEAN/USD": "forex",
    "WHEAT/USD": "forex","SUGAR/USD":   "forex",
    # ── Forex (commodity ETFs — active/working symbols only) ───────────
    "UNG":  "forex",
    "DBB":  "forex", "SLX":  "forex",
    "CPER": "forex",
    "CORN": "forex", "SOYB": "forex",
    "CANE": "forex",
    "COTN": "forex", "NICK": "forex",
    "WEAT": "forex", "USO": "forex", "UGA": "forex",
    "SGOL": "forex", "SIVR": "forex", "PPLT": "forex", "PALL": "forex",
    "ALUM": "forex", "ZINC": "forex", "TINM": "forex",
    "WOOD": "forex",
    # ── Forex (indices) ────────────────────────────────
    "SPX": "forex", "NDX": "forex", "RUT": "forex",
    "DJI": "forex",
    # ── Crypto ─────────────────────────────────────────
    "BTC/USD": "crypto", "ETH/USD": "crypto",
    "LTC/USD": "crypto", "XRP/USD": "crypto",
    "BNB/USD": "crypto",
    # Altcoins — Tier 1
    "SOL/USD":  "crypto", "DOGE/USD": "crypto",
    "ADA/USD":  "crypto", "AVAX/USD": "crypto",
    "LINK/USD": "crypto",
    "DOT/USD":  "crypto", "BCH/USD":  "crypto",
    "XLM/USD":  "crypto", "ATOM/USD": "crypto",
    "UNI/USD":  "crypto",
    # Altcoins — Tier 2
    "TON/USD":  "crypto", "SHIB/USD": "crypto",
    "HBAR/USD": "crypto", "NEAR/USD": "crypto",
    "ICP/USD":  "crypto", "CRO/USD":  "crypto",
    # Altcoins — Tier 3
    "APT/USD":  "crypto", "ARB/USD":  "crypto",
    "OP/USD":   "crypto", "SUI/USD":  "crypto",
    "INJ/USD":  "crypto", "TRX/USD":  "crypto",
    # ── Stocks (NASDAQ 100) ──────────────────────────────────────────────
    **{sym: "stocks" for sym in [
        "AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","TSLA","AVGO","COST",
        "NFLX","AMD","ADBE","QCOM","PEP","CSCO","TMUS","INTC","INTU","AMGN",
        "TXN","HON","AMAT","SBUX","BKNG","ISRG","VRTX","REGN","GILD","ADI",
        "LRCX","MU","PANW","KLAC","SNPS","CDNS","MELI","ADP","MDLZ","PYPL",
        "CRWD","CTAS","ORLY","WDAY","MNST","MRVL","PCAR","FTNT","CEG","ODFL",
        "ROST","CPRT","DXCM","BIIB","KDP","FANG","PAYX","IDXX","EXC","MRNA",
        "FAST","CTSH","VRSK","ON","GEHC","EA","KHC","XEL","DLTR","CDW",
        "WBD","DDOG","ZS","CCEP","ANSS","BKR","TTWO","ILMN","WBA","MTCH",
        "SIRI","OKTA","ZM","ALGN","ENPH","LCID","RIVN","NXPI","MCHP","LULU",
        "TEAM","DOCU","EBAY","ASML","ABNB","DASH","APP","HOOD","RBLX","COIN",
    ]},
}

# Aliases for legacy and shorthand asset_class filter values.
# Allows ?asset_class=commodities and ?asset_class=indices to
# keep working after the three-category remapping.
ASSET_CLASS_ALIASES: dict[str, str] = {
    "commodities": "forex",
    "indices":     "forex",
    "fx":          "forex",
    "index":       "forex",
    "commodity":   "forex",
    "stock":     "stocks",
    "equity":    "stocks",
    "nasdaq":    "stocks",
}


def _resolve_asset_class(value: Optional[str]) -> Optional[str]:
    """Resolve an asset_class filter value to its canonical form.
    Aliases (commodities, indices, fx) transparently map to forex.
    Returns None unchanged so no-filter behaviour is preserved.
    """
    if not value:
        return value
    normalised = value.lower().strip()
    return ASSET_CLASS_ALIASES.get(normalised, normalised)


STRATEGY_LABELS = {
    "mtf_ema": "MTF EMA",
    "trend_non_forex": "Trend Non-Forex",
    "trend_forex": "Trend Forex",
    "sp500_momentum": "SP500 Momentum",
    "highest_lowest_fx": "Highest/Lowest FX",
    "stocks_algo1": "Stocks Algo 1 — Monthly Momentum",
    "stocks_algo2": "Stocks Algo 2 — Mean Reversion",
}

VALID_TIMEFRAMES = ["30m", "1H", "4H", "D1"]


DIRECTION_MAP = {"BUY": "LONG", "SELL": "SHORT"}


def _format_signal_public(s: dict, position: Optional[dict] = None) -> dict:
    ts = s.get("signal_timestamp") or s.get("created_at") or ""
    if ts and not ts.endswith("Z"):
        ts = ts.replace(" ", "T")
        if "T" in ts:
            ts = ts + "Z"

    meta = {}
    atr = s.get("atr_at_entry")
    if atr is not None:
        meta["atr_entry"] = atr
    if position:
        if position.get("highest_price_since_entry") is not None:
            meta["highest_close"] = position["highest_price_since_entry"]
        if position.get("lowest_price_since_entry") is not None:
            meta["lowest_close"] = position["lowest_price_since_entry"]

    result = {
        "asset": s["asset"],
        "full_name": s.get("full_name") or _get_name(s["asset"]),
        "asset_class": s.get("asset_class") or CATEGORY_MAP.get(s["asset"], "other"),
        "direction": DIRECTION_MAP.get(s["direction"], s["direction"]),
        "entry": s["entry_price"],
        "stop_loss": s["stop_loss"],
        "strategy": s["strategy_name"],
        "published_at": ts,
        "exit_instructions": _get_exit_instruction(s.get("strategy_name", ""), s.get("direction", "")),
    }
    if meta:
        result["meta"] = meta
    return result


def _format_signal(s: dict) -> dict:
    return {
        "id": s["id"],
        "asset": s["asset"],
        "full_name": s.get("full_name") or _get_name(s["asset"]),
        "category": s.get("asset_class") or CATEGORY_MAP.get(s["asset"], "other"),
        "strategy": s["strategy_name"],
        "strategy_label": STRATEGY_LABELS.get(s["strategy_name"], s["strategy_name"]),
        "direction": s["direction"],
        "entry_price": s["entry_price"],
        "stop_loss": s["stop_loss"],
        "exit_instructions": _get_exit_instruction(s.get("strategy_name", ""), s.get("direction", "")),
        "status": s["status"],
        "exit_price": s.get("exit_price"),
        "exit_reason": s.get("exit_reason"),
        "opened_at": s.get("signal_timestamp") or s.get("created_at"),
        "updated_at": s.get("updated_at"),
    }


def _filter_by_category(signals: list, category: Optional[str]) -> list:
    # category has already been resolved via _resolve_asset_class()
    # at the endpoint level — no alias handling needed here.
    if not category:
        return signals
    return [s for s in signals if s["category"] == category]


def _filter_by_asset_class(signals: list, asset_class: Optional[str]) -> list:
    # asset_class has already been resolved via _resolve_asset_class()
    # at the endpoint level — no alias handling needed here.
    if not asset_class:
        return signals
    return [s for s in signals if s["category"] == asset_class]


def _deduplicate_signals(signals: list[dict]) -> list[dict]:
    """Python-side safety-net deduplication applied after DB fetch.

    - OPEN signals: keep only the entry with the highest id per ASSET (across all strategies).
      This prevents manual/AI signals and strategy signals from both appearing active.
    - CLOSED signals: keep only the entry with the highest id per (asset, strategy_name, direction).
    List is assumed to be already sorted by id/created_at DESC from the DB, so the
    first occurrence of each key is always the one we want to keep.
    """
    seen_open: dict = {}
    seen_closed: dict = {}
    for s in signals:
        sid = s.get("id", 0)
        st = s.get("status", "")
        if st == "OPEN":
            # One OPEN signal per ASSET across all strategies
            key = s.get("asset")
            if key not in seen_open or (sid or 0) > (seen_open[key].get("id") or 0):
                seen_open[key] = s
        else:
            key = (s.get("asset"), s.get("strategy_name"), s.get("direction"))
            if key not in seen_closed or (sid or 0) > (seen_closed[key].get("id") or 0):
                seen_closed[key] = s

    result = []
    seen_keys_open: set = set()
    seen_keys_closed: set = set()
    for s in signals:
        st = s.get("status", "")
        if st == "OPEN":
            key = s.get("asset")
            if key not in seen_keys_open and seen_open.get(key) is s:
                seen_keys_open.add(key)
                result.append(s)
        else:
            key = (s.get("asset"), s.get("strategy_name"), s.get("direction"))
            if key not in seen_keys_closed and seen_closed.get(key) is s:
                seen_keys_closed.add(key)
                result.append(s)
    return result


@router.get("/signals/latest", response_model=SignalsLatestResponse, tags=["Signals"])
@cache_response(ttl=60, prefix="signals_latest")
def get_signals_latest(
    asset: Optional[str] = Query(None, description="Filter by asset symbol (e.g. EUR/USD, SPX)"),
    strategy: Optional[str] = Query(None, description="Filter by strategy name (e.g. mtf_ema, trend_forex)"),
    asset_class: Optional[str] = Query(
        None,
        description=(
            "Filter by asset class: forex | crypto | stocks. "
            "Legacy values 'commodities' and 'indices' are accepted "
            "as aliases for 'forex'."
        ),
    ),
):
    """
    Fetch the latest active (OPEN) signals in the public format.

    This is the primary hot-path endpoint for the DailyForex frontend.
    Reads from the local SQLite database with a 60-second TTLCache.
    Direction is normalized to LONG/SHORT. Each signal is enriched with
    trailing-stop position metadata (highest_close, lowest_close) when available.
    """
    asset_class = _resolve_asset_class(asset_class)
    raw = get_active_signals(strategy_name=strategy, asset=asset)
    raw = _deduplicate_signals(raw)

    positions = get_all_open_positions()
    pos_map = {}
    for p in positions:
        key = (p.get("asset"), p.get("strategy_name"))
        pos_map[key] = p

    formatted = []
    for s in raw:
        pos = pos_map.get((s["asset"], s["strategy_name"]))
        f = _format_signal_public(s, position=pos)
        f["_category"] = CATEGORY_MAP.get(s["asset"], "other")
        formatted.append(f)

    if asset_class:
        formatted = [f for f in formatted if f.get("_category") == asset_class]

    for f in formatted:
        f.pop("_category", None)

    # Final safety net: one OPEN signal per asset regardless of direction or strategy
    KNOWN_STRATEGIES = {"mtf_ema", "trend_forex", "trend_non_forex",
                        "sp500_momentum", "highest_lowest_fx", "trend_following"}
    final: dict[str, dict] = {}
    for f in formatted:
        key = f["asset"]
        if key not in final:
            final[key] = f
        else:
            existing_known = final[key].get("strategy") in KNOWN_STRATEGIES
            new_known = f.get("strategy") in KNOWN_STRATEGIES
            if new_known and not existing_known:
                final[key] = f
            elif not existing_known and not new_known:
                if f.get("id", 0) > final[key].get("id", 0):
                    final[key] = f
    formatted = list(final.values())

    return {"count": len(formatted), "data": formatted}


@router.get("/signals/history", response_model=SignalsHistoryResponse, tags=["Signals"])
@cache_response(ttl=60, prefix="signals_history")
def get_signals_history(
    asset: Optional[str] = Query(None, description="Filter by asset symbol"),
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    status: Optional[str] = Query(None, description="Filter by status: OPEN or CLOSED"),
    asset_class: Optional[str] = Query(
        None,
        description=(
            "Filter by asset class: forex | crypto | stocks. "
            "Legacy values 'commodities' and 'indices' are accepted "
            "as aliases for 'forex'."
        ),
    ),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    size: int = Query(20, ge=1, le=50, description="Items per page (max 50)"),
):
    """
    Paginated signal history with full filtering.

    Returns up to 500 signals from the local database, paginated by page/size
    (max 50 per page). Uses the legacy format (BUY/SELL direction, entry_price field).
    Cached for 60 seconds per unique filter combination.

    Retention window: CLOSED signals older than 92 days are excluded. OPEN signals are
    always returned regardless of age.
    """
    asset_class = _resolve_asset_class(asset_class)
    all_raw = get_all_signals(strategy_name=strategy, asset=asset, status=status, limit=500, max_age_days=92)
    all_raw = _deduplicate_signals(all_raw)
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


@router.get("/signals/active", response_model=SignalsListResponse, tags=["Signals"])
@cache_response(ttl=60, prefix="signals_active")
def get_signals_active(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    asset: Optional[str] = Query(None, description="Filter by asset symbol"),
    category: Optional[str] = Query(
        None,
        description=(
            "Filter by asset class: forex | crypto | stocks. "
            "Legacy values 'commodities' and 'indices' are accepted "
            "as aliases for 'forex'."
        ),
    ),
):
    """
    Fetch currently open signals in the legacy format.

    Returns only signals with status=OPEN. Uses the internal format
    with BUY/SELL direction and entry_price. Cached for 60 seconds.
    """
    category = _resolve_asset_class(category)
    raw = get_active_signals(strategy_name=strategy, asset=asset)
    raw = _deduplicate_signals(raw)
    formatted = [_format_signal(s) for s in raw]
    formatted = _filter_by_category(formatted, category)
    return {"signals": formatted, "count": len(formatted)}


@router.get("/signals/{signal_id}", response_model=SignalDetailResponse, tags=["Signals"])
@cache_response(ttl=60, prefix="signal_detail")
def get_signal_by_id(signal_id: int):
    """
    Retrieve a single signal by its database ID.

    Returns the full signal record in legacy format. Returns 404 if the
    signal ID does not exist. Cached for 60 seconds.
    """
    all_sigs = get_all_signals(limit=500)
    match = next((s for s in all_sigs if s["id"] == signal_id), None)
    if not match:
        raise HTTPException(status_code=404, detail="Signal not found")
    return {"signal": _format_signal(match)}


@router.get("/signals", response_model=SignalsListResponse, tags=["Signals"])
@cache_response(ttl=60, prefix="signals_all")
def get_signals(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    asset: Optional[str] = Query(None, description="Filter by asset symbol"),
    status: Optional[str] = Query(None, description="Filter by status: OPEN or CLOSED"),
    category: Optional[str] = Query(
        None,
        description=(
            "Filter by asset class: forex | crypto | stocks. "
            "Legacy values 'commodities' and 'indices' are accepted "
            "as aliases for 'forex'."
        ),
    ),
    limit: int = Query(50, ge=1, le=200, description="Max results (default 50, max 200)"),
):
    """
    Fetch all signals with optional filters.

    Returns both OPEN and CLOSED signals in legacy format.
    Supports filtering by strategy, asset, status, and category.
    Results capped at 200 per request. Cached for 60 seconds.
    """
    category = _resolve_asset_class(category)
    raw = get_all_signals(strategy_name=strategy, asset=asset, status=status, limit=limit)
    raw = _deduplicate_signals(raw)
    formatted = [_format_signal(s) for s in raw]
    formatted = _filter_by_category(formatted, category)
    return {"signals": formatted, "count": len(formatted)}


@router.get("/strategies", response_model=StrategiesResponse, tags=["Strategies"])
@cache_response(ttl=60, prefix="strategies")
def list_strategies():
    """
    List all registered trading strategies with signal counts.

    Returns each strategy name, its human-readable label, and the count
    of open vs closed signals. Cached for 60 seconds.
    """
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
                "exit_rules": EXIT_RULES_PUBLIC.get(name),
            }
        strategy_stats[name]["total"] += 1
        if s["status"] == "OPEN":
            strategy_stats[name]["open"] += 1
        else:
            strategy_stats[name]["closed"] += 1

    return {"strategies": list(strategy_stats.values()), "count": len(strategy_stats)}


@router.get("/market/candles", response_model=CandlesResponse, tags=["Market Data"])
@cache_response(ttl=60, prefix="market_candles")
def get_market_candles(
    asset: str = Query(..., description="Asset symbol (e.g. EUR/USD, SPX, BTC/USD)"),
    timeframe: str = Query("D1", description="Timeframe: 30m, 1H, 4H, D1"),
    limit: int = Query(100, ge=1, le=300, description="Max candles (default 100, max 300)"),
):
    """
    Retrieve OHLC candle data from the local database.

    Returns historical candles for the specified asset and timeframe.
    No external API calls — all data is pre-fetched by the background scheduler.
    Includes total stored count and last fetch timestamp. Cached for 60 seconds.
    """
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


@router.get("/market/indicators", response_model=IndicatorsResponse, tags=["Market Data"])
@cache_response(ttl=60, prefix="market_indicators")
def get_market_indicators(
    asset: str = Query(..., description="Asset symbol"),
    timeframe: str = Query("D1", description="Timeframe: 30m, 1H, 4H, D1"),
):
    """
    Compute technical indicators for a given asset and timeframe.

    Calculates SMA, EMA (20/50/100/200), RSI (14/20), and ATR (14/100)
    from locally stored candle data. Returns only the latest value for each
    indicator. Requires at least one candle in the database. Cached for 60 seconds.
    """
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


@router.get("/positions", response_model=PositionsResponse, tags=["Positions"])
@cache_response(ttl=60, prefix="positions")
def get_positions(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    asset: Optional[str] = Query(None, description="Filter by asset symbol"),
):
    """
    List all open positions with trailing-stop metadata.

    Each position includes the entry price, ATR at entry, and the
    highest/lowest price observed since entry (used for trailing stop
    calculations). Cached for 60 seconds.
    """
    positions = get_all_open_positions(strategy_name=strategy, asset=asset)

    formatted = []
    for p in positions:
        sym = p.get("asset", "")
        formatted.append({
            "id": p.get("id"),
            "asset": sym,
            "full_name": p.get("full_name") or _get_name(sym),
            "category": CATEGORY_MAP.get(sym, "other"),
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


@router.get("/metrics", response_model=MetricsResponse, tags=["Metrics"])
@cache_response(ttl=60, prefix="signal_metrics")
def get_metrics(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    asset: Optional[str] = Query(None, description="Filter by specific asset"),
    period: str = Query("all_time", description="Period: all_time, 7d, 30d"),
    summary_only: bool = Query(False, description="If true, return only strategy-level summaries (no per-asset rows)"),
):
    """
    Signal performance metrics with flexible filtering.

    Returns win rate, average gain/loss, best/worst trades, and duration
    statistics. By default returns both per-asset and strategy-level aggregate
    rows. Set summary_only=true for aggregates only. Metrics are recomputed
    hourly by a background worker (+ full recap at 5:15 PM ET). Cached for 60 seconds.
    """
    if period not in ("all_time", "7d", "30d"):
        raise HTTPException(status_code=400, detail="Invalid period. Use: all_time, 7d, 30d")

    metrics = get_signal_metrics(
        strategy_name=strategy,
        asset=asset,
        period=period,
        summary_only=summary_only if not asset else False,
    )

    for m in metrics:
        sym = m.get("asset")
        m["full_name"] = _get_name(sym) if sym else None

    return {"metrics": metrics, "count": len(metrics), "period": period}


@router.get("/metrics/summary", response_model=MetricsSummaryResponse, tags=["Metrics"])
@cache_response(ttl=60, prefix="metrics_summary")
def get_metrics_summary():
    """
    Overall platform performance summary.

    Aggregates win rate, total won/lost counts across all strategies.
    Returns per-strategy breakdown as well. Uses all_time period only.
    Cached for 60 seconds.
    """
    all_metrics = get_all_signal_metrics()

    summary_rows = [m for m in all_metrics if m["asset"] is None and m["period"] == "all_time"]

    total_signals = sum(m["total_signals"] for m in summary_rows)
    total_won = sum(m["won"] for m in summary_rows)
    total_lost = sum(m["lost"] for m in summary_rows)
    total_closed = total_won + total_lost
    overall_win_rate = round(total_won / total_closed * 100, 1) if total_closed > 0 else 0.0

    computed = max((m["computed_at"] for m in all_metrics), default=None) if all_metrics else None

    for row in summary_rows:
        sym = row.get("asset")
        row["full_name"] = _get_name(sym) if sym else None

    return {
        "total_signals": total_signals,
        "total_won": total_won,
        "total_lost": total_lost,
        "overall_win_rate": overall_win_rate,
        "strategies": summary_rows,
        "last_computed": computed,
    }


@router.get("/scheduler/status", response_model=SchedulerStatusResponse, tags=["Scheduler"])
@cache_response(ttl=30, prefix="scheduler_status")
def get_scheduler_status():
    """
    Scheduler health overview for the last 24 hours.

    Returns success/failure counts and the most recent job execution record.
    Cached for 30 seconds (shorter TTL for near-real-time monitoring).
    """
    summary = get_scheduler_health_summary()
    return {
        "last_24h": {
            "success": summary.get("success_24h", 0),
            "failures": summary.get("failure_24h", 0),
        },
        "last_job": summary.get("last_job"),
    }


@router.get("/scheduler/jobs", response_model=SchedulerJobsResponse, tags=["Scheduler"])
@cache_response(ttl=30, prefix="scheduler_jobs")
def get_scheduler_jobs(
    limit: int = Query(20, ge=1, le=100, description="Max job logs (default 20, max 100)"),
):
    """
    Recent scheduler job execution logs.

    Returns the most recent job runs with strategy name, status
    (SUCCESS/PARTIAL/FAILED), duration, asset counts, and error details.
    Cached for 30 seconds.
    """
    logs = get_recent_job_logs(limit=limit)
    return {"jobs": logs, "count": len(logs)}


@router.get("/health", response_model=HealthResponse, tags=["Health"])
def api_health():
    """
    API health check with cache statistics.

    Returns API status, version, and detailed cache pool stats including
    shard count, hit/miss/set counts, and overall hit rate. Not cached.
    """
    stats = cache_pool.get_stats()
    return {
        "status": "ok",
        "version": "v1",
        "cache": stats,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


@router.get("/health/public", response_model=HealthPublicResponse, tags=["Health"])
def api_health_public():
    """
    Public liveness check — safe for external monitoring.

    Returns status (UP/DOWN), version, timestamp, and non-sensitive
    security telemetry (blocked request count, active IP bans).
    """
    from trading_engine.security_middleware import get_public_security_status
    return {
        "status": "UP",
        "version": "v1",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "security_status": get_public_security_status(),
    }


@router.post("/cache/flush", response_model=CacheFlushResponse, tags=["State Management"])
def flush_cache():
    """
    Flush all cache shards to force fresh data on subsequent requests.

    Clears all 4 TTLCache shards across every TTL tier.
    Subsequent requests will re-query the database (cache miss).
    """
    cache_pool.flush()
    return {"status": "flushed", "message": "All cache shards cleared"}


class WpTestRequest(BaseModel):
    site_url: str = Field(..., description="WordPress site URL (e.g. https://example.com)")
    wp_username: str = Field(..., description="WordPress username")
    app_password: str = Field(..., description="WordPress Application Password")


class WpTestResponse(BaseModel):
    status: str = Field(..., description="'ok' on success, 'error' on failure")
    site_title: Optional[str] = Field(None, description="WordPress site title (on success)")
    wp_version: Optional[str] = Field(None, description="WordPress version (on success)")
    error: Optional[str] = Field(None, description="Specific error message (on failure)")


@router.post(
    "/user/integrations/test",
    response_model=WpTestResponse,
    tags=["User Integrations"],
    summary="Test WordPress credentials",
)
def test_wp_integration(body: WpTestRequest):
    """
    Validate WordPress credentials by attempting to authenticate and
    retrieve site info. Returns the site title and WordPress version
    on success, or a specific error on failure.
    """
    import httpx

    site_url = body.site_url.rstrip("/")
    timeout = 15.0

    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            auth_resp = client.get(
                f"{site_url}/wp-json/wp/v2/users/me",
                auth=(body.wp_username, body.app_password),
            )

            if auth_resp.status_code == 401:
                return WpTestResponse(
                    status="error",
                    error="Invalid Application Password",
                )
            if auth_resp.status_code == 403:
                return WpTestResponse(
                    status="error",
                    error="Access forbidden — Application Password lacks required permissions",
                )
            if auth_resp.status_code == 404:
                return WpTestResponse(
                    status="error",
                    error="REST API Disabled — /wp-json/wp/v2 not found at this URL",
                )
            if auth_resp.status_code != 200:
                return WpTestResponse(
                    status="error",
                    error=f"Unexpected response (HTTP {auth_resp.status_code})",
                )

            site_title = None
            wp_version = None

            try:
                info_resp = client.get(f"{site_url}/wp-json", timeout=10)
                if info_resp.status_code == 200:
                    info = info_resp.json()
                    site_title = info.get("name")
                    namespaces = info.get("namespaces", [])
                    if "wp/v2" in namespaces:
                        wp_version = "wp/v2"
                        for ns in namespaces:
                            if ns.startswith("wp/v"):
                                wp_version = ns
            except Exception:
                pass

            try:
                settings_resp = client.get(
                    f"{site_url}/wp-json/wp/v2/settings",
                    auth=(body.wp_username, body.app_password),
                    timeout=10,
                )
                if settings_resp.status_code == 200:
                    settings = settings_resp.json()
                    if not site_title:
                        site_title = settings.get("title")
            except Exception:
                pass

            if not site_title:
                try:
                    user_data = auth_resp.json()
                    site_title = user_data.get("name", body.wp_username)
                except Exception:
                    site_title = body.wp_username

            return WpTestResponse(
                status="ok",
                site_title=site_title,
                wp_version=wp_version,
            )

    except httpx.ConnectError:
        return WpTestResponse(
            status="error",
            error=f"Connection failed — could not reach {site_url}",
        )
    except httpx.TimeoutException:
        return WpTestResponse(
            status="error",
            error="Connection timed out after 15 seconds",
        )
    except Exception as e:
        return WpTestResponse(
            status="error",
            error=f"Unexpected error: {str(e)}",
        )
