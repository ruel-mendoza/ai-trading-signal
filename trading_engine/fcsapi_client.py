import requests
import os
import logging
from typing import Optional

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    retry_if_exception,
    before_sleep_log,
)
from requests.exceptions import ConnectionError, Timeout

logger = logging.getLogger("trading_engine.fcsapi")

BASE_URL = "https://fcsapi.com/api-v3/forex"

TIMEFRAME_MAP = {
    "30m": "30m",
    "1H": "1h",
    "4H": "4h",
    "D1": "1d",
}

TIMEFRAME_DURATION_MINUTES = {
    "30m": 30,
    "1H": 60,
    "4H": 240,
    "D1": 1440,
}


def _is_server_error(exc: BaseException) -> bool:
    if isinstance(exc, requests.exceptions.HTTPError):
        return exc.response is not None and exc.response.status_code >= 500
    return False


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

        candles.append({
            "timestamp": item.get("tm", item.get("o_time", "")),
            "open": float(item.get("o", 0)),
            "high": float(item.get("h", 0)),
            "low": float(item.get("l", 0)),
            "close": float(item.get("c", 0)),
        })

    candles.sort(key=lambda x: x["timestamp"])
    return candles


class FCSAPIClient:
    def __init__(self, api_key: Optional[str] = None):
        self._static_key = api_key
        self.session = requests.Session()
        self._timeout = 30

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
        env_key = os.environ.get("FCS_API_KEY") or os.environ.get("FCSAPI_KEY", "")
        if env_key:
            logger.info("[API-KEY] Source: environment variable")
        else:
            logger.warning("[API-KEY] No API key found in database, constructor, or environment")
        return env_key

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=(
            retry_if_exception_type((ConnectionError, Timeout))
            | retry_if_exception(_is_server_error)
        ),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _get(self, endpoint: str, params: dict) -> dict:
        from trading_engine.database import log_api_usage
        from trading_engine.credit_control import pre_request_check, check_credit_thresholds

        pre_request_check()

        params["access_key"] = self.api_key
        url = f"{BASE_URL}/{endpoint}"
        logger.info(f"[FCSAPI-REQUEST] {endpoint} | symbol={params.get('symbol')} | timeframe={params.get('time')} | period={params.get('period')}")
        try:
            response = self.session.get(url, params=params, timeout=self._timeout)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else 0
            body = e.response.text[:500] if e.response is not None else "N/A"
            logger.error(f"[FCSAPI-HTTP-ERROR] {endpoint} | status={status_code} | body={body}")
            raise
        except (ConnectionError, Timeout) as e:
            logger.error(f"[FCSAPI-CONN-ERROR] {endpoint} | error={str(e)}")
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

        if msg:
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

        log_api_usage(endpoint=endpoint)
        check_credit_thresholds()
        return data

    def test_connection(self) -> dict:
        from trading_engine.database import log_api_usage, get_api_usage_stats
        key = self.api_key
        if not key:
            return {"success": False, "error": "No API key configured"}
        try:
            url = f"{BASE_URL}/profile"
            response = self.session.get(url, params={"access_key": key, "symbol": "EUR"}, timeout=self._timeout)
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
        except requests.exceptions.HTTPError as e:
            return {"success": False, "error": f"HTTP error: {e.response.status_code if e.response else 'unknown'}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_candles(self, symbol: str, period: str = "1h", from_timestamp: Optional[str] = None, limit: int = 300) -> list[dict]:
        tf_api = TIMEFRAME_MAP.get(period, period)
        params = {
            "symbol": symbol,
            "period": str(limit),
            "time": tf_api,
        }
        if from_timestamp:
            params["from"] = from_timestamp
        logger.info(f"[GET-CANDLES] {symbol} | period={period}({tf_api}) | limit={limit} | from={from_timestamp or 'latest'}")
        data = self._get("history", params)

        if data.get("status") is False or not data.get("response"):
            msg = data.get("msg", "")
            if msg:
                logger.error(f"[GET-CANDLES] {symbol} | API error msg: {msg}")
            logger.warning(f"[GET-CANDLES] {symbol} | No data returned (status={data.get('status')}, response={'empty' if not data.get('response') else 'present'})")
            return []

        candles = _parse_response_items(data["response"])
        logger.info(f"[GET-CANDLES] {symbol} | Parsed {len(candles)} candles | first={candles[0]['timestamp'] if candles else 'N/A'} | last={candles[-1]['timestamp'] if candles else 'N/A'}")
        return candles

    def fetch_history(self, symbol: str, timeframe: str, period: int = 300) -> list[dict]:
        return self.get_candles(symbol=symbol, period=timeframe, limit=period)

    def fetch_latest(self, symbol: str, timeframe: str) -> list[dict]:
        return self.get_candles(symbol=symbol, period=timeframe, limit=5)

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
        self.session.close()
