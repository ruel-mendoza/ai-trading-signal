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


class CacheLayer:
    def __init__(self, api_client: FCSAPIClient):
        self.api_client = api_client

    def _get_last_closed_candle_time(self, timeframe: str) -> datetime:
        now = datetime.utcnow()
        minutes = TIMEFRAME_DURATION_MINUTES[timeframe]

        if timeframe == "D":
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

    def _candle_close_to_iso(self, open_time: str, timeframe: str) -> str:
        minutes = TIMEFRAME_DURATION_MINUTES[timeframe]
        try:
            dt = datetime.fromisoformat(open_time)
            close_dt = dt + timedelta(minutes=minutes)
            return close_dt.isoformat()
        except (ValueError, TypeError):
            return open_time

    def _should_fetch(self, symbol: str, timeframe: str) -> bool:
        meta = get_cache_metadata(symbol, timeframe)
        if meta is None:
            return True

        candle_count = get_candle_count(symbol, timeframe)
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

    def get_candles(self, symbol: str, timeframe: str, limit: int = 300) -> list[dict]:
        if self._should_fetch(symbol, timeframe):
            self._fetch_and_store(symbol, timeframe, limit)

        return get_candles(symbol, timeframe, limit)

    def _fetch_and_store(self, symbol: str, timeframe: str, limit: int = 300):
        candle_count = get_candle_count(symbol, timeframe)

        if candle_count == 0:
            candles = self.api_client.fetch_history(symbol, timeframe, period=limit)
        else:
            candles = self.api_client.fetch_latest(symbol, timeframe)

        if candles:
            upsert_candles(symbol, timeframe, candles)
            last_open = candles[-1]["open_time"]
            last_close = self._candle_close_to_iso(last_open, timeframe)
            update_cache_metadata(symbol, timeframe, last_close)

    def force_refresh(self, symbol: str, timeframe: str, limit: int = 300) -> list[dict]:
        candles = self.api_client.fetch_history(symbol, timeframe, period=limit)
        if candles:
            upsert_candles(symbol, timeframe, candles)
            last_open = candles[-1]["open_time"]
            last_close = self._candle_close_to_iso(last_open, timeframe)
            update_cache_metadata(symbol, timeframe, last_close)
        return get_candles(symbol, timeframe, limit)
