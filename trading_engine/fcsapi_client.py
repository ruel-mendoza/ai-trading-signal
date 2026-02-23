import httpx
import os
from datetime import datetime, timedelta
from typing import Optional

BASE_URL = "https://fcsapi.com/api-v3/forex"

TIMEFRAME_MAP = {
    "30m": "30m",
    "1H": "1h",
    "4H": "4h",
    "D": "1d",
}

TIMEFRAME_DURATION_MINUTES = {
    "30m": 30,
    "1H": 60,
    "4H": 240,
    "D": 1440,
}


class FCSAPIClient:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("FCSAPI_KEY", "")
        self.client = httpx.Client(timeout=30)

    def _get(self, endpoint: str, params: dict) -> dict:
        params["access_key"] = self.api_key
        url = f"{BASE_URL}/{endpoint}"
        response = self.client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def fetch_history(self, symbol: str, timeframe: str, period: int = 300) -> list[dict]:
        tf = TIMEFRAME_MAP.get(timeframe, "1h")
        params = {
            "symbol": symbol,
            "period": str(period),
            "time": tf,
        }
        data = self._get("history", params)

        if data.get("status") is False or not data.get("response"):
            return []

        candles = []
        for item in data["response"]:
            candles.append({
                "open_time": item.get("tm", item.get("o_time", "")),
                "open": float(item.get("o", 0)),
                "high": float(item.get("h", 0)),
                "low": float(item.get("l", 0)),
                "close": float(item.get("c", 0)),
                "volume": float(item.get("v", 0)) if item.get("v") else 0,
                "is_closed": 1,
            })
        return candles

    def fetch_latest(self, symbol: str, timeframe: str) -> list[dict]:
        tf = TIMEFRAME_MAP.get(timeframe, "1h")
        params = {
            "symbol": symbol,
            "period": "5",
            "time": tf,
        }
        data = self._get("history", params)

        if data.get("status") is False or not data.get("response"):
            return []

        candles = []
        for item in data["response"]:
            candles.append({
                "open_time": item.get("tm", item.get("o_time", "")),
                "open": float(item.get("o", 0)),
                "high": float(item.get("h", 0)),
                "low": float(item.get("l", 0)),
                "close": float(item.get("c", 0)),
                "volume": float(item.get("v", 0)) if item.get("v") else 0,
                "is_closed": 1,
            })
        return candles

    def get_available_symbols(self) -> list[str]:
        params = {"type": "forex"}
        data = self._get("list", params)
        if data.get("status") is False or not data.get("response"):
            return []
        symbols = []
        for item in data["response"]:
            symbols.append(item.get("symbol", ""))
        return [s for s in symbols if s]

    def close(self):
        self.client.close()
