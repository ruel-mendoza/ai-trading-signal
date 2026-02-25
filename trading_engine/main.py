import os
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from contextlib import asynccontextmanager
import pytz

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

ET_ZONE = pytz.timezone("America/New_York")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.INFO)
logger = logging.getLogger("trading_engine")

from trading_engine.database import init_db, get_candles, get_candle_count, get_all_signals, get_active_signals
from trading_engine.models import VALID_TIMEFRAMES
from trading_engine.fcsapi_client import FCSAPIClient
from trading_engine.cache_layer import CacheLayer
from trading_engine.indicators import IndicatorEngine
from trading_engine.strategy_engine import StrategyEngine
from trading_engine.admin import router as admin_router
from trading_engine.strategies.trend_forex import TARGET_SYMBOLS as TREND_FOREX_SYMBOLS
from trading_engine.strategies.trend_non_forex import TARGET_SYMBOLS as TREND_NON_FOREX_SYMBOLS

scheduler = BackgroundScheduler()

init_db()

api_client = FCSAPIClient()
cache = CacheLayer(api_client)
strategy_engine = StrategyEngine(cache)


def _scheduled_trend_forex_evaluate():
    logger.info("[SCHEDULER] ====== Triggered trend_forex daily evaluation at 5:00 PM ET ======")
    for asset in TREND_FOREX_SYMBOLS:
        try:
            result = strategy_engine.trend_forex_strategy.evaluate(asset)
            if result:
                logger.info(f"[SCHEDULER] trend_forex | {asset} | NEW SIGNAL generated: {result.get('direction', '')} id={result.get('id')}")
            else:
                logger.info(f"[SCHEDULER] trend_forex | {asset} | No signal triggered")
        except Exception as e:
            logger.error(f"[SCHEDULER] trend_forex | {asset} | Exception: {e}")
    try:
        exits = strategy_engine.trend_forex_strategy.check_exits()
        if exits:
            logger.info(f"[SCHEDULER] trend_forex | {len(exits)} exit(s) triggered")
        else:
            logger.info("[SCHEDULER] trend_forex | No exits triggered")
    except Exception as e:
        logger.error(f"[SCHEDULER] trend_forex | Exit check exception: {e}")
    logger.info("[SCHEDULER] ====== trend_forex daily evaluation complete ======")


def _scheduled_trend_non_forex_evaluate():
    import pandas as pd
    from trading_engine.database import get_open_position
    from trading_engine.strategies.trend_non_forex import TIMEFRAME as TNF_TIMEFRAME
    now_et = datetime.now(pytz.utc).astimezone(ET_ZONE)
    is_dst = bool(now_et.dst() and now_et.dst().total_seconds() > 0)
    tz_label = "EDT" if is_dst else "EST"
    logger.info(
        f"[SCHEDULER] ====== Triggered trend_non_forex daily evaluation at 4:00 PM ET | "
        f"system_time={now_et.strftime('%Y-%m-%d %H:%M:%S')} {tz_label} | "
        f"DST={'active' if is_dst else 'inactive'} ======"
    )
    for asset in TREND_NON_FOREX_SYMBOLS:
        try:
            candles = cache.get_candles(asset, TNF_TIMEFRAME, 300)
            if not candles:
                logger.warning(f"[SCHEDULER] trend_non_forex | {asset} | No candles available")
                continue
            df = pd.DataFrame(candles)
            open_pos = get_open_position("trend_non_forex", asset)
            result = strategy_engine.trend_non_forex_strategy.evaluate(asset, TNF_TIMEFRAME, df, open_pos)
            if result.is_entry:
                signal = result.metadata.get("signal", {})
                logger.info(f"[SCHEDULER] trend_non_forex | {asset} | NEW SIGNAL generated: {signal.get('direction', '')} id={signal.get('id')}")
            else:
                logger.info(f"[SCHEDULER] trend_non_forex | {asset} | No signal triggered")
        except Exception as e:
            logger.error(f"[SCHEDULER] trend_non_forex | {asset} | Exception: {e}")
    try:
        exits = strategy_engine.trend_non_forex_strategy.check_exits()
        if exits:
            logger.info(f"[SCHEDULER] trend_non_forex | {len(exits)} exit(s) triggered")
        else:
            logger.info("[SCHEDULER] trend_non_forex | No exits triggered")
    except Exception as e:
        logger.error(f"[SCHEDULER] trend_non_forex | Exit check exception: {e}")
    logger.info("[SCHEDULER] ====== trend_non_forex daily evaluation complete ======")


def _scheduled_sp500_momentum_30m():
    import pandas as pd
    from trading_engine.database import get_open_position
    now_et = datetime.now(pytz.utc).astimezone(ET_ZONE)
    is_dst = bool(now_et.dst() and now_et.dst().total_seconds() > 0)
    tz_label = "EDT" if is_dst else "EST"
    et_minutes = now_et.hour * 60 + now_et.minute
    arca_start = 9 * 60 + 30
    arca_end = 15 * 60 + 30

    logger.info(
        f"[SCHEDULER] ====== SP500 Momentum 30m tick | "
        f"time={now_et.strftime('%Y-%m-%d %H:%M:%S')} {tz_label} | "
        f"DST={'active' if is_dst else 'inactive'} ======"
    )

    if et_minutes < arca_start or et_minutes > arca_end:
        logger.info(
            f"[SCHEDULER] sp500_momentum | Outside ARCA session "
            f"({now_et.strftime('%H:%M')} {tz_label} not in 09:30-15:30 ET) — skipping"
        )
        return

    logger.info(
        f"[SCHEDULER] sp500_momentum | Inside ARCA session "
        f"({now_et.strftime('%H:%M')} {tz_label}) — running intraday cycle"
    )

    try:
        result = strategy_engine.run_sp500_intraday_cycle("SPX")
        entry = result.get("entry")
        exits = result.get("exits", [])
        state_updated = result.get("state_updated", False)

        if entry:
            logger.info(
                f"[SCHEDULER] sp500_momentum | NEW SIGNAL: BUY @ {entry.get('entry_price', 0):.2f} | "
                f"atr_at_entry={entry.get('atr_at_entry', 0):.6f} | "
                f"stop={entry.get('stop_loss', 0):.2f} | id={entry.get('id')}"
            )
        if exits:
            for ex in exits:
                logger.info(
                    f"[SCHEDULER] sp500_momentum | EXIT: {ex.get('exit_reason')} @ {ex.get('exit_price', 0):.2f}"
                )
        if state_updated:
            logger.info("[SCHEDULER] sp500_momentum | Position state updated (peak tracking)")
        if not entry and not exits and not state_updated:
            logger.info("[SCHEDULER] sp500_momentum | No action taken this tick")
    except Exception as e:
        logger.error(f"[SCHEDULER] sp500_momentum | Exception: {e}", exc_info=True)

    logger.info("[SCHEDULER] ====== SP500 Momentum 30m tick complete ======")


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(
        _scheduled_trend_forex_evaluate,
        trigger=CronTrigger(hour=17, minute=0, timezone=ET_ZONE),
        id="trend_forex_daily",
        name="Forex Trend Daily Evaluation (5:00 PM ET)",
        replace_existing=True,
    )
    scheduler.add_job(
        _scheduled_trend_non_forex_evaluate,
        trigger=CronTrigger(hour=16, minute=0, timezone=ET_ZONE),
        id="trend_non_forex_daily",
        name="Non-Forex Trend Daily Evaluation (4:00 PM ET)",
        replace_existing=True,
    )
    scheduler.add_job(
        _scheduled_sp500_momentum_30m,
        trigger=CronTrigger(minute="0,30", timezone=ET_ZONE),
        id="sp500_momentum_30m",
        name="SP500 Momentum 30m Evaluation (:00 and :30 ET)",
        replace_existing=True,
    )
    scheduler.start()
    now_et = datetime.now(pytz.utc).astimezone(ET_ZONE)
    is_dst = bool(now_et.dst() and now_et.dst().total_seconds() > 0)
    tz_label = "EDT" if is_dst else "EST"
    logger.info(
        f"[SCHEDULER] APScheduler started | "
        f"sp500_momentum every 30m (:00/:30), "
        f"trend_non_forex at 16:00, trend_forex at 17:00 "
        f"America/New_York (currently {tz_label}, DST={'active' if is_dst else 'inactive'})"
    )
    yield
    scheduler.shutdown(wait=False)
    logger.info("[SCHEDULER] APScheduler shut down")


app = FastAPI(
    title="Trading Signal Engine",
    description="Python-based trading signal engine with OHLC data, caching, and technical indicators",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(admin_router)


class CandleResponse(BaseModel):
    asset: str
    timeframe: str
    candle_count: int
    candles: list[dict]


class IndicatorResponse(BaseModel):
    asset: str
    timeframe: str
    latest: dict
    series: Optional[dict] = None


class StatusResponse(BaseModel):
    status: str
    api_key_configured: bool
    database: str
    timeframes: list[str]


class RefreshResponse(BaseModel):
    asset: str
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
    timeframe: str = Query(..., description="Timeframe: 30m, 1H, 4H, or D1"),
    limit: int = Query(300, ge=1, le=1000, description="Number of candles to return"),
):
    if timeframe not in VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Must be one of: {VALID_TIMEFRAMES}",
        )

    candles = cache.get_candles(symbol, timeframe, limit)

    return CandleResponse(
        asset=symbol,
        timeframe=timeframe,
        candle_count=len(candles),
        candles=candles,
    )


@app.get("/api/indicators", response_model=IndicatorResponse)
def get_indicators(
    symbol: str = Query(..., description="Trading pair symbol, e.g. EUR/USD"),
    timeframe: str = Query(..., description="Timeframe: 30m, 1H, 4H, or D1"),
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
        asset=symbol,
        timeframe=timeframe,
        latest=latest,
        series=series,
    )


@app.post("/api/candles/refresh", response_model=RefreshResponse)
def refresh_candles(
    symbol: str = Query(..., description="Trading pair symbol, e.g. EUR/USD"),
    timeframe: str = Query(..., description="Timeframe: 30m, 1H, 4H, or D1"),
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
        asset=symbol,
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
    timeframe: str = Query(..., description="Timeframe: 30m, 1H, 4H, or D1"),
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


@app.get("/api/credit-control/status")
def credit_control_status():
    from trading_engine.credit_control import check_credit_thresholds, is_api_blocked
    projection = check_credit_thresholds()
    projection["api_blocked"] = is_api_blocked()
    return projection


@app.post("/api/credit-control/reset-kill-switch")
def reset_credit_kill_switch():
    from trading_engine.credit_control import reset_kill_switch, is_api_blocked
    reset_kill_switch()
    return {"success": True, "api_blocked": is_api_blocked()}


VALID_QUOTE_PERIODS = {"30m", "1h", "4h", "1d", "1H", "4H", "D1"}


@app.get("/api/quotes")
def get_quotes(
    symbols: str = Query(..., description="Comma-separated list of symbols, e.g. EUR/USD,SPX,BTC/USD"),
    period: str = Query("1h", description="Timeframe period: 30m, 1h, 4h, 1d"),
    merge: str = Query("latest,profile", description="Data to merge: latest, profile, tech, perf"),
):
    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        raise HTTPException(status_code=400, detail="At least one symbol is required")
    if len(symbol_list) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 symbols per request")
    if period not in VALID_QUOTE_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period '{period}'. Must be one of: 30m, 1h, 4h, 1d",
        )

    try:
        quotes = api_client.get_advance_data(symbol_list, period=period, merge=merge)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch quotes: {str(e)}")

    return {
        "quotes": quotes,
        "count": len(quotes),
        "requested": symbol_list,
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
    elif strategy_name == "trend_forex":
        result = strategy_engine.trend_forex_strategy.evaluate(symbol)
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown strategy '{strategy_name}'. Available: mtf_ema, trend_following, sp500_momentum, highest_lowest_fx, trend_forex",
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
    status: Optional[str] = Query(None, description="Filter by status: OPEN, CLOSED"),
    limit: int = Query(100, ge=1, le=500),
):
    signals = get_all_signals(strategy_name=strategy, asset=symbol, status=status, limit=limit)
    return {
        "signals": signals,
        "count": len(signals),
    }


@app.get("/api/strategy-signals/active")
def list_active_signals(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
):
    signals = get_active_signals(strategy_name=strategy, asset=symbol)
    return {
        "signals": signals,
        "count": len(signals),
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PYTHON_ENGINE_PORT", "5001"))
    uvicorn.run(app, host="0.0.0.0", port=port)
