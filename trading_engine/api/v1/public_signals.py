import logging
from typing import Optional
from datetime import datetime

from fastapi import APIRouter, Query, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from trading_engine.database import (
    get_all_signals,
    get_active_signals,
    get_all_open_positions,
)
from trading_engine.api_v1 import (
    cache_pool,
    cache_response,
    CATEGORY_MAP,
    STRATEGY_LABELS,
)

logger = logging.getLogger("trading_engine.api_v1.public_signals")


class SignalRead(BaseModel):
    id: int = Field(..., description="Unique signal identifier")
    asset: str = Field(..., example="EUR/USD", description="Trading pair / asset symbol")
    asset_class: str = Field(..., example="forex", description="Asset class: forex, crypto, commodities, indices")
    strategy: str = Field(..., example="mtf_ema", description="Strategy that generated this signal")
    strategy_label: str = Field(..., example="MTF EMA", description="Human-readable strategy name")
    direction: str = Field(..., example="LONG", description="Signal direction: LONG or SHORT")
    entry: float = Field(..., example=1.0845, description="Suggested entry price")
    stop_loss: float = Field(..., example=1.0790, description="Stop-loss price level")
    take_profit: Optional[float] = Field(None, description="Take-profit price level (null for trailing-stop strategies)")
    trailing_stop: bool = Field(..., description="Whether trailing stop is used instead of fixed TP")
    status: str = Field(..., example="OPEN", description="Signal status: OPEN or CLOSED")
    published_at: str = Field(..., description="ISO 8601 timestamp when signal was generated")
    closed_at: Optional[str] = Field(None, description="ISO 8601 timestamp when signal was closed (null if OPEN)")
    exit_price: Optional[float] = Field(None, description="Price at which position was exited (null if OPEN)")
    exit_reason: Optional[str] = Field(None, description="Reason for exit: stop_loss, take_profit, trailing_stop, manual")


class AssetRead(BaseModel):
    symbol: str = Field(..., example="EUR/USD", description="Asset symbol")
    asset_class: str = Field(..., example="forex", description="Asset class: forex, crypto, commodities, indices")
    active_signals: int = Field(..., description="Number of currently OPEN signals for this asset")
    strategies: list[str] = Field(..., description="Strategies that have generated signals for this asset")


class SignalListResponse(BaseModel):
    signals: list[SignalRead]
    count: int
    cache: Optional[str] = None
    response_time_ms: Optional[float] = None


class SignalDetailResponse(BaseModel):
    signal: SignalRead
    cache: Optional[str] = None
    response_time_ms: Optional[float] = None


class AssetListResponse(BaseModel):
    assets: list[AssetRead]
    count: int
    cache: Optional[str] = None
    response_time_ms: Optional[float] = None


BLOCKED_METHODS = {"POST", "PUT", "DELETE", "PATCH"}


router = APIRouter(prefix="/api/v1/public", tags=["Public Signals"])


@router.api_route(
    "/{path:path}",
    methods=["POST", "PUT", "DELETE", "PATCH"],
    include_in_schema=False,
)
async def block_write_methods(path: str):
    raise HTTPException(
        status_code=405,
        detail="This API is read-only. Only GET requests are allowed.",
    )


def _g(s, key, default=None):
    if isinstance(s, dict):
        return s.get(key, default)
    return getattr(s, key, default)


def _to_signal_read(s) -> dict:
    asset = _g(s, "asset", "")
    direction_raw = (_g(s, "direction", "") or "").upper()
    direction = "LONG" if direction_raw == "BUY" else "SHORT" if direction_raw == "SELL" else direction_raw
    tp = _g(s, "take_profit")
    has_tp = tp is not None and tp > 0
    status = _g(s, "status", "OPEN")
    strategy_name = _g(s, "strategy_name", "")
    return SignalRead(
        id=_g(s, "id", 0),
        asset=asset,
        asset_class=CATEGORY_MAP.get(asset, "unknown"),
        strategy=strategy_name,
        strategy_label=STRATEGY_LABELS.get(strategy_name, strategy_name),
        direction=direction,
        entry=_g(s, "entry_price", 0),
        stop_loss=_g(s, "stop_loss", 0),
        take_profit=tp if has_tp else None,
        trailing_stop=not has_tp,
        status=status,
        published_at=_g(s, "signal_timestamp") or _g(s, "created_at", ""),
        closed_at=_g(s, "updated_at") if status == "CLOSED" else None,
        exit_price=_g(s, "exit_price") if status == "CLOSED" else None,
        exit_reason=_g(s, "exit_reason") if status == "CLOSED" else None,
    ).model_dump()


@router.get(
    "/signals",
    response_model=SignalListResponse,
    tags=["Public Signals"],
    summary="List trading signals",
)
def list_signals(
    strategy: Optional[str] = Query(None, description="Filter by strategy name (e.g. mtf_ema)"),
    asset: Optional[str] = Query(None, description="Filter by asset symbol (e.g. EUR/USD)"),
    asset_class: Optional[str] = Query(None, description="Filter by asset class: forex, crypto, commodities, indices"),
    status: Optional[str] = Query(None, description="Filter by status: OPEN or CLOSED"),
    limit: int = Query(50, ge=1, le=200, description="Max results (1-200)"),
):
    """List trading signals with optional filters.

    Returns sanitized signal data through the SignalRead schema.
    Internal fields like atr_at_entry and raw database metadata are excluded.
    Cached for 60 seconds.
    """
    @cache_response(ttl=60, prefix="public_signals")
    def _inner(strategy=strategy, asset=asset, asset_class=asset_class, status=status, limit=limit):
        all_sigs = get_all_signals()
        result = []
        for s in all_sigs:
            if strategy and _g(s, "strategy_name") != strategy:
                continue
            if asset and _g(s, "asset") != asset:
                continue
            if status and _g(s, "status") != status.upper():
                continue
            sig_class = CATEGORY_MAP.get(_g(s, "asset", ""), "unknown")
            if asset_class and sig_class != asset_class.lower():
                continue
            result.append(_to_signal_read(s))
            if len(result) >= limit:
                break

        return {"signals": result, "count": len(result)}

    return _inner(strategy=strategy, asset=asset, asset_class=asset_class, status=status, limit=limit)


@router.get(
    "/signals/active",
    response_model=SignalListResponse,
    tags=["Public Signals"],
    summary="List active (OPEN) signals",
)
def list_active_signals(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    asset: Optional[str] = Query(None, description="Filter by asset symbol"),
    asset_class: Optional[str] = Query(None, description="Filter by asset class"),
):
    """Fetch currently active (OPEN) signals only.

    Returns only signals with status=OPEN. Ideal for frontend widgets
    showing current trading opportunities. Cached for 60 seconds.
    """
    @cache_response(ttl=60, prefix="public_signals_active")
    def _inner(strategy=strategy, asset=asset, asset_class=asset_class):
        active = get_active_signals()
        result = []
        for s in active:
            if strategy and _g(s, "strategy_name") != strategy:
                continue
            if asset and _g(s, "asset") != asset:
                continue
            sig_class = CATEGORY_MAP.get(_g(s, "asset", ""), "unknown")
            if asset_class and sig_class != asset_class.lower():
                continue
            result.append(_to_signal_read(s))
        return {"signals": result, "count": len(result)}

    return _inner(strategy=strategy, asset=asset, asset_class=asset_class)


@router.get(
    "/signals/{signal_id}",
    response_model=SignalDetailResponse,
    tags=["Public Signals"],
    summary="Get a single signal by ID",
)
def get_signal(signal_id: int):
    """Retrieve a single signal by database ID.

    Returns 404 if the signal does not exist. Cached for 60 seconds.
    """
    @cache_response(ttl=60, prefix="public_signal_detail")
    def _inner(signal_id=signal_id):
        all_sigs = get_all_signals()
        for s in all_sigs:
            if _g(s, "id") == signal_id:
                return {"signal": _to_signal_read(s)}
        raise HTTPException(status_code=404, detail=f"Signal {signal_id} not found")

    return _inner(signal_id=signal_id)


@router.get(
    "/assets",
    response_model=AssetListResponse,
    tags=["Public Signals"],
    summary="List assets with signal activity",
)
def list_assets(
    asset_class: Optional[str] = Query(None, description="Filter by asset class"),
):
    """List all assets that have at least one signal, with open signal counts.

    Returns the AssetRead schema which includes the asset symbol, class,
    number of active signals, and which strategies have produced signals for it.
    Cached for 60 seconds.
    """
    @cache_response(ttl=60, prefix="public_assets")
    def _inner(asset_class=asset_class):
        all_sigs = get_all_signals()
        asset_map: dict[str, dict] = {}
        for s in all_sigs:
            sym = _g(s, "asset", "")
            cls = CATEGORY_MAP.get(sym, "unknown")
            if asset_class and cls != asset_class.lower():
                continue
            if sym not in asset_map:
                asset_map[sym] = {"symbol": sym, "asset_class": cls, "active_signals": 0, "strategies": set()}
            if _g(s, "status") == "OPEN":
                asset_map[sym]["active_signals"] += 1
            sn = _g(s, "strategy_name")
            if sn:
                asset_map[sym]["strategies"].add(sn)

        assets = []
        for v in sorted(asset_map.values(), key=lambda x: x["symbol"]):
            assets.append(AssetRead(
                symbol=v["symbol"],
                asset_class=v["asset_class"],
                active_signals=v["active_signals"],
                strategies=sorted(v["strategies"]),
            ).model_dump())

        return {"assets": assets, "count": len(assets)}

    return _inner(asset_class=asset_class)
