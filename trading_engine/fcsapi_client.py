import httpx
import os
import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("trading_engine.fcsapi")

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


def _parse_response_items(response) -> list[dict]:
    items = []
    if isinstance(response, dict):
        items = list(response.values())
    elif isinstance(response, list):
        items = response
    else:
        return []

    candles = []
    for item in items:
        if not isinstance(item, dict):
            continue
        vol_str = item.get("v", "")
        volume = 0.0
        if vol_str and vol_str.strip():
            try:
                volume = float(vol_str)
            except (ValueError, TypeError):
                volume = 0.0

        candles.append({
            "open_time": item.get("tm", item.get("o_time", "")),
            "open": float(item.get("o", 0)),
            "high": float(item.get("h", 0)),
            "low": float(item.get("l", 0)),
            "close": float(item.get("c", 0)),
            "volume": volume,
            "is_closed": 1,
        })

    candles.sort(key=lambda x: x["open_time"])
    return candles


class FCSAPIClient:
    def __init__(self, api_key: Optional[str] = None):
        self._static_key = api_key
        self.client = httpx.Client(timeout=30)

    @property
    def api_key(self) -> str:
        from trading_engine.database import get_setting
        db_key = get_setting("fcsapi_key")
        if db_key:
            logger.info("[API-KEY] Source: database (local storage)")
            return db_key
        if self._static_key:
            logger.info("[API-KEY] Source: constructor parameter")
            return self._static_key
        env_key = os.environ.get("FCSAPI_KEY", "")
        if env_key:
            logger.info("[API-KEY] Source: FCSAPI_KEY environment variable")
        else:
            logger.warning("[API-KEY] No API key found in database, constructor, or environment")
        return env_key

    def _get(self, endpoint: str, params: dict) -> dict:
        from trading_engine.database import log_api_usage
        params["access_key"] = self.api_key
        url = f"{BASE_URL}/{endpoint}"
        logger.info(f"[FCSAPI-REQUEST] {endpoint} | symbol={params.get('symbol')} | timeframe={params.get('time')} | period={params.get('period')}")
        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error(f"[FCSAPI-HTTP-ERROR] {endpoint} | status={e.response.status_code} | body={e.response.text[:500]}")
            raise
        except Exception as e:
            logger.error(f"[FCSAPI-ERROR] {endpoint} | error={str(e)}")
            raise
        data = response.json()
        status = data.get("status")
        code = data.get("code")
        msg = data.get("msg", "")
        info = data.get("info", {})
        credit_count = info.get("credit_count", "N/A")
        logger.info(f"[FCSAPI-RESPONSE] {endpoint} | status={status} | code={code} | msg={msg} | credits_remaining={credit_count}")
        if status is False or code == 101:
            logger.error(f"[FCSAPI-API-ERROR] {endpoint} | API returned error: {msg}")
        if "out of credit" in msg.lower() or "credit" in msg.lower():
            logger.critical(f"[FCSAPI-CREDITS] OUT OF CREDITS detected: {msg}")
        response_data = data.get("response")
        if response_data is None:
            logger.warning(f"[FCSAPI-RESPONSE] {endpoint} | response field is None/missing")
        elif isinstance(response_data, list):
            logger.info(f"[FCSAPI-RESPONSE] {endpoint} | response contains {len(response_data)} items")
        elif isinstance(response_data, dict):
            logger.info(f"[FCSAPI-RESPONSE] {endpoint} | response contains {len(response_data)} keys")
        log_api_usage(
            endpoint=endpoint,
            symbol=params.get("symbol"),
            timeframe=params.get("time"),
        )
        return data

    def test_connection(self) -> dict:
        from trading_engine.database import log_api_usage, get_api_usage_stats
        key = self.api_key
        if not key:
            return {"success": False, "error": "No API key configured"}
        try:
            url = f"{BASE_URL}/profile"
            response = self.client.get(url, params={"access_key": key, "symbol": "EUR"})
            response.raise_for_status()
            data = response.json()
            if data.get("status") is False or data.get("code") == 101:
                return {"success": False, "error": data.get("msg", "Invalid API key")}
            log_api_usage(endpoint="profile/test")
            info = data.get("info", {})
            usage_stats = get_api_usage_stats()
            total_credits = 500000
            used_credits = usage_stats.get("monthly_total", 0)
            remaining = total_credits - used_credits
            return {
                "success": True,
                "plan_type": "FCSAPI Active",
                "remaining_credits": remaining,
                "total_credits": total_credits,
                "used_credits": used_credits,
                "server_time": info.get("server_time", ""),
                "credit_count": info.get("credit_count", 0),
            }
        except httpx.HTTPStatusError as e:
            return {"success": False, "error": f"HTTP error: {e.response.status_code}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def fetch_history(self, symbol: str, timeframe: str, period: int = 300) -> list[dict]:
        tf = TIMEFRAME_MAP.get(timeframe, "1h")
        params = {
            "symbol": symbol,
            "period": str(period),
            "time": tf,
        }
        logger.info(f"[FETCH-HISTORY] {symbol} | tf={timeframe}({tf}) | period={period}")
        data = self._get("history", params)

        if data.get("status") is False or not data.get("response"):
            logger.warning(f"[FETCH-HISTORY] {symbol} | No data returned (status={data.get('status')}, response={'empty' if not data.get('response') else 'present'})")
            return []

        candles = _parse_response_items(data["response"])
        logger.info(f"[FETCH-HISTORY] {symbol} | Parsed {len(candles)} candles | first={candles[0]['open_time'] if candles else 'N/A'} | last={candles[-1]['open_time'] if candles else 'N/A'}")
        return candles

    def fetch_latest(self, symbol: str, timeframe: str) -> list[dict]:
        tf = TIMEFRAME_MAP.get(timeframe, "1h")
        params = {
            "symbol": symbol,
            "period": "5",
            "time": tf,
        }
        logger.info(f"[FETCH-LATEST] {symbol} | tf={timeframe}({tf})")
        data = self._get("history", params)

        if data.get("status") is False or not data.get("response"):
            logger.warning(f"[FETCH-LATEST] {symbol} | No data returned")
            return []

        candles = _parse_response_items(data["response"])
        logger.info(f"[FETCH-LATEST] {symbol} | Parsed {len(candles)} candles")
        return candles

    def get_available_symbols(self) -> list[str]:
        params = {"type": "forex"}
        data = self._get("list", params)
        if data.get("status") is False or not data.get("response"):
            return []
        symbols = []
        response = data["response"]
        if isinstance(response, dict):
            items = list(response.values())
        elif isinstance(response, list):
            items = response
        else:
            return []
        for item in items:
            if isinstance(item, dict):
                symbols.append(item.get("symbol", ""))
        return [s for s in symbols if s]

    def close(self):
        self.client.close()
