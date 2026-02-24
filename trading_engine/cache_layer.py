import logging
from datetime import datetime, timedelta
from typing import Optional
from trading_engine.database import (
    get_cache_metadata,
    update_cache_metadata,
    upsert_candles,
    get_candles,
    get_candle_count,
)
from trading_engine.fcsapi_client import FCSAPIClient, TIMEFRAME_DURATION_MINUTES

logger = logging.getLogger("trading_engine.cache")


class CacheLayer:
    def __init__(self, api_client: FCSAPIClient):
        self.api_client = api_client

    def _get_last_closed_candle_time(self, timeframe: str) -> datetime:
        now = datetime.utcnow()
        minutes = TIMEFRAME_DURATION_MINUTES[timeframe]

        if timeframe == "D1":
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            if now >= today_start:
                return today_start - timedelta(days=1)
            return today_start - timedelta(days=1)

        total_minutes = now.hour * 60 + now.minute
        current_candle_start = (total_minutes // minutes) * minutes
        previous_candle_close = current_candle_start
        close_time = now.replace(
            hour=previous_candle_close // 60,
            minute=previous_candle_close % 60,
            second=0,
            microsecond=0,
        )
        if close_time >= now:
            close_time -= timedelta(minutes=minutes)
        return close_time

    def _candle_close_to_iso(self, timestamp: str, timeframe: str) -> str:
        minutes = TIMEFRAME_DURATION_MINUTES[timeframe]
        try:
            dt = datetime.fromisoformat(timestamp)
            close_dt = dt + timedelta(minutes=minutes)
            return close_dt.isoformat()
        except (ValueError, TypeError):
            return timestamp

    def _should_fetch(self, asset: str, timeframe: str) -> bool:
        meta = get_cache_metadata(asset, timeframe)
        if meta is None:
            return True

        candle_count = get_candle_count(asset, timeframe)
        if candle_count == 0:
            return True

        last_fetched_str = meta["last_fetched"]
        try:
            last_fetched = datetime.fromisoformat(last_fetched_str)
        except (ValueError, TypeError):
            return True

        last_closed = self._get_last_closed_candle_time(timeframe)

        last_candle_close_str = meta.get("last_candle_close")
        if last_candle_close_str:
            try:
                last_stored_close = datetime.fromisoformat(last_candle_close_str)
                if last_closed > last_stored_close:
                    return True
                return False
            except (ValueError, TypeError):
                return True

        minutes = TIMEFRAME_DURATION_MINUTES[timeframe]
        if datetime.utcnow() - last_fetched > timedelta(minutes=minutes):
            return True

        return False

    def get_candles(self, asset: str, timeframe: str, limit: int = 300) -> list[dict]:
        should_fetch = self._should_fetch(asset, timeframe)
        logger.info(f"[CACHE] get_candles({asset}, {timeframe}, limit={limit}) | should_fetch={should_fetch}")
        if should_fetch:
            self._fetch_and_store(asset, timeframe, limit)

        candles = get_candles(asset, timeframe, limit)
        logger.info(f"[CACHE] {asset}/{timeframe} | Returned {len(candles)} candles from DB")
        return candles

    def _fetch_and_store(self, asset: str, timeframe: str, limit: int = 300):
        candle_count = get_candle_count(asset, timeframe)
        logger.info(f"[CACHE-FETCH] {asset}/{timeframe} | existing_candles={candle_count} | {'full history' if candle_count == 0 else 'latest only'}")

        if candle_count == 0:
            candles = self.api_client.fetch_history(asset, timeframe, period=limit)
        else:
            candles = self.api_client.fetch_latest(asset, timeframe)

        if candles:
            upsert_candles(asset, timeframe, candles)
            last_ts = candles[-1]["timestamp"]
            last_close = self._candle_close_to_iso(last_ts, timeframe)
            update_cache_metadata(asset, timeframe, last_close)
            logger.info(f"[CACHE-FETCH] {asset}/{timeframe} | Stored {len(candles)} candles | last_close={last_close}")
        else:
            logger.warning(f"[CACHE-FETCH] {asset}/{timeframe} | API returned 0 candles")

    def force_refresh(self, asset: str, timeframe: str, limit: int = 300) -> list[dict]:
        candles = self.api_client.fetch_history(asset, timeframe, period=limit)
        if candles:
            upsert_candles(asset, timeframe, candles)
            last_ts = candles[-1]["timestamp"]
            last_close = self._candle_close_to_iso(last_ts, timeframe)
            update_cache_metadata(asset, timeframe, last_close)
        return get_candles(asset, timeframe, limit)
