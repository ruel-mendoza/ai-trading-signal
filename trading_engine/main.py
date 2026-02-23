import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

from trading_engine.database import init_db, get_candles, get_candle_count, get_all_signals, get_active_signals, VALID_TIMEFRAMES
from trading_engine.fcsapi_client import FCSAPIClient
from trading_engine.cache_layer import CacheLayer
from trading_engine.indicators import IndicatorEngine
from trading_engine.strategy_engine import StrategyEngine
from trading_engine.admin import router as admin_router

app = FastAPI(
    title="Trading Signal Engine",
    description="Python-based trading signal engine with OHLC data, caching, and technical indicators",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

init_db()

api_client = FCSAPIClient()
cache = CacheLayer(api_client)
strategy_engine = StrategyEngine(cache)

app.include_router(admin_router)


class CandleResponse(BaseModel):
    symbol: str
    timeframe: str
    candle_count: int
    candles: list[dict]


class IndicatorResponse(BaseModel):
    symbol: str
    timeframe: str
    latest: dict
    series: Optional[dict] = None


class StatusResponse(BaseModel):
    status: str
    api_key_configured: bool
    database: str
    timeframes: list[str]


class RefreshResponse(BaseModel):
    symbol: str
    timeframe: str
    candles_stored: int
    message: str


@app.get("/", response_model=StatusResponse)
def health_check():
    return StatusResponse(
        status="running",
        api_key_configured=bool(api_client.api_key),
        database="SQLite",
        timeframes=VALID_TIMEFRAMES,
    )


@app.get("/api/candles", response_model=CandleResponse)
def get_candle_data(
    symbol: str = Query(..., description="Trading pair symbol, e.g. EUR/USD"),
    timeframe: str = Query(..., description="Timeframe: 30m, 1H, 4H, or D"),
    limit: int = Query(300, ge=1, le=1000, description="Number of candles to return"),
):
    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Must be one of: {VALID_TIMEFRAMES}",
        )

    candles = cache.get_candles(symbol, timeframe, limit)

    return CandleResponse(
        symbol=symbol,
        timeframe=timeframe,
        candle_count=len(candles),
        candles=candles,
    )


@app.get("/api/indicators", response_model=IndicatorResponse)
def get_indicators(
    symbol: str = Query(..., description="Trading pair symbol, e.g. EUR/USD"),
    timeframe: str = Query(..., description="Timeframe: 30m, 1H, 4H, or D"),
    include_series: bool = Query(False, description="Include full indicator series"),
    limit: int = Query(300, ge=1, le=1000, description="Number of candles to calculate from"),
):
    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Must be one of: {VALID_TIMEFRAMES}",
        )

    candles = cache.get_candles(symbol, timeframe, limit)

    if not candles:
        raise HTTPException(
            status_code=404,
            detail=f"No candle data available for {symbol} on {timeframe}",
        )

    latest = IndicatorEngine.get_latest(candles)

    series = None
    if include_series:
        series = IndicatorEngine.calculate_all(candles)

    return IndicatorResponse(
        symbol=symbol,
        timeframe=timeframe,
        latest=latest,
        series=series,
    )


@app.post("/api/candles/refresh", response_model=RefreshResponse)
def refresh_candles(
    symbol: str = Query(..., description="Trading pair symbol, e.g. EUR/USD"),
    timeframe: str = Query(..., description="Timeframe: 30m, 1H, 4H, or D"),
    limit: int = Query(300, ge=1, le=1000),
):
    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Must be one of: {VALID_TIMEFRAMES}",
        )

    cache.force_refresh(symbol, timeframe, limit)
    count = get_candle_count(symbol, timeframe)

    return RefreshResponse(
        symbol=symbol,
        timeframe=timeframe,
        candles_stored=count,
        message=f"Successfully refreshed {count} candles for {symbol} on {timeframe}",
    )


@app.get("/api/symbols")
def list_symbols():
    try:
        symbols = api_client.get_available_symbols()
        return {"symbols": symbols, "count": len(symbols)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch symbols: {str(e)}")


@app.get("/api/cache/status")
def cache_status(
    symbol: str = Query(..., description="Trading pair symbol"),
    timeframe: str = Query(..., description="Timeframe: 30m, 1H, 4H, or D"),
):
    from trading_engine.database import get_cache_metadata

    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Must be one of: {VALID_TIMEFRAMES}",
        )

    meta = get_cache_metadata(symbol, timeframe)
    count = get_candle_count(symbol, timeframe)

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "candles_stored": count,
        "cache_metadata": meta,
        "should_fetch": cache._should_fetch(symbol, timeframe),
    }


@app.post("/api/strategies/evaluate")
def evaluate_strategies(
    symbols: Optional[str] = Query(None, description="Comma-separated list of symbols to evaluate"),
):
    symbol_list = [s.strip() for s in symbols.split(",")] if symbols else None
    new_signals = strategy_engine.evaluate_all(symbols=symbol_list)
    return {
        "new_signals": new_signals,
        "count": len(new_signals),
        "message": f"Evaluated all strategies, generated {len(new_signals)} new signal(s)",
    }


@app.post("/api/strategies/evaluate/{strategy_name}")
def evaluate_single_strategy(
    strategy_name: str,
    symbol: str = Query(..., description="Symbol to evaluate"),
):
    result = None
    if strategy_name == "mtf_ema":
        result = strategy_engine.evaluate_mtf_ema(symbol)
    elif strategy_name == "trend_following":
        result = strategy_engine.evaluate_trend_following(symbol)
    elif strategy_name == "sp500_momentum":
        result = strategy_engine.evaluate_sp500_momentum(symbol)
    elif strategy_name == "highest_lowest_fx":
        result = strategy_engine.evaluate_highest_lowest_fx(symbol)
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown strategy '{strategy_name}'. Available: mtf_ema, trend_following, sp500_momentum, highest_lowest_fx",
        )

    return {
        "strategy": strategy_name,
        "symbol": symbol,
        "signal": result,
        "triggered": result is not None,
    }


@app.post("/api/strategies/check-exits")
def check_exit_conditions():
    closed = strategy_engine.check_exit_conditions()
    return {
        "closed_signals": closed,
        "count": len(closed),
    }


@app.get("/api/strategy-signals")
def list_strategy_signals(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    status: Optional[str] = Query(None, description="Filter by status: active, closed, expired"),
    limit: int = Query(100, ge=1, le=500),
):
    signals = get_all_signals(strategy=strategy, symbol=symbol, status=status, limit=limit)
    return {
        "signals": signals,
        "count": len(signals),
    }


@app.get("/api/strategy-signals/active")
def list_active_signals(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
):
    signals = get_active_signals(strategy=strategy, symbol=symbol)
    return {
        "signals": signals,
        "count": len(signals),
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PYTHON_ENGINE_PORT", "5001"))
    uvicorn.run(app, host="0.0.0.0", port=port)
