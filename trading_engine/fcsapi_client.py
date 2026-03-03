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

BASE_URL_V4_FOREX = "https://api-v4.fcsapi.com/forex"
BASE_URL_V4_CRYPTO = "https://api-v4.fcsapi.com/crypto"
BASE_URL_V4_STOCK = "https://api-v4.fcsapi.com/stock"

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

CRYPTO_SYMBOLS = {"BTC/USD", "ETH/USD", "LTC/USD", "XRP/USD", "BNB/USD"}

CRYPTO_SYMBOL_MAP = {
    "BTC/USD": "BTCUSD",
    "ETH/USD": "ETHUSDT",
    "LTC/USD": "LTCUSDT",
    "XRP/USD": "XRPUSDT",
    "BNB/USD": "BNBUSDT",
}

STOCK_INDEX_SYMBOLS = {"SPX", "NDX", "DJI", "RUT"}

STOCK_SYMBOL_MAP = {
    "SPX": "SPX",
    "NDX": "NDX",
    "DJI": "DJI",
    "RUT": "RUT",
}

COMMODITY_SYMBOLS = {"XAU/USD", "XAG/USD", "XPT/USD", "XPD/USD", "XCU/USD", "NATGAS/USD", "CORN/USD", "SOYBEAN/USD", "WHEAT/USD", "SUGAR/USD", "OSX"}

COMMODITY_SYMBOL_MAP = {
    "XAU/USD": "XAUUSD",
    "XAG/USD": "XAGUSD",
    "XPT/USD": "XPTUSD",
    "XPD/USD": "XPDUSD",
    "XCU/USD": "XCUUSD",
    "NATGAS/USD": "NATGASUSD",
    "CORN/USD": "CORNUSD",
    "SOYBEAN/USD": "SOYBNUSD",
    "WHEAT/USD": "WHEATUSD",
    "SUGAR/USD": "SUGARUSD",
    "OSX": "OSX",
}

UNSUPPORTED_SYMBOLS: set[str] = {"WTI/USD", "BRENT/USD"}

ADVANCE_SYMBOL_MAP = {
    "EUR/USD": "EURUSD",
    "GBP/USD": "GBPUSD",
    "USD/JPY": "USDJPY",
    "USD/CAD": "USDCAD",
    "AUD/USD": "AUDUSD",
    "NZD/USD": "NZDUSD",
    "USD/CHF": "USDCHF",
    "EUR/GBP": "EURGBP",
    "XAU/USD": "XAUUSD",
    "XAG/USD": "XAGUSD",
    "XPT/USD": "XPTUSD",
    "XPD/USD": "XPDUSD",
    "XCU/USD": "XCUUSD",
    "NATGAS/USD": "NATGASUSD",
    "CORN/USD": "CORNUSD",
    "SOYBEAN/USD": "SOYBNUSD",
    "WHEAT/USD": "WHEATUSD",
    "SUGAR/USD": "SUGARUSD",
    "BTC/USD": "BTCUSD",
    "ETH/USD": "ETHUSDT",
    "LTC/USD": "LTCUSDT",
    "XRP/USD": "XRPUSDT",
    "BNB/USD": "BNBUSDT",
    "SPX": "SPX",
    "NDX": "NDX",
    "DJI": "DJI",
    "RUT": "RUT",
    "OSX": "OSX",
}


def get_advance_symbol(symbol: str) -> str:
    mapped = ADVANCE_SYMBOL_MAP.get(symbol)
    if mapped is None:
        logger.warning(f"[ADVANCE] No v4 symbol mapping for '{symbol}', using as-is")
        return symbol.replace("/", "")
    return mapped


def get_asset_class(symbol: str) -> str:
    if symbol in CRYPTO_SYMBOLS:
        return "crypto"
    if symbol in STOCK_INDEX_SYMBOLS:
        return "stock"
    if symbol in COMMODITY_SYMBOLS:
        return "commodity"
    return "forex"


def get_v4_base_url(symbol: str) -> str:
    asset_class = get_asset_class(symbol)
    if asset_class == "crypto":
        return BASE_URL_V4_CRYPTO
    if asset_class == "stock":
        return BASE_URL_V4_STOCK
    return BASE_URL_V4_FOREX


def get_v4_history_symbol(symbol: str) -> str:
    if symbol in CRYPTO_SYMBOL_MAP:
        return CRYPTO_SYMBOL_MAP[symbol]
    if symbol in STOCK_SYMBOL_MAP:
        return STOCK_SYMBOL_MAP[symbol]
    if symbol in COMMODITY_SYMBOL_MAP:
        return COMMODITY_SYMBOL_MAP[symbol]
    return symbol.replace("/", "")


def is_symbol_supported(symbol: str) -> bool:
    if symbol in UNSUPPORTED_SYMBOLS:
        return False
    return True


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _validate_candle_prices(candles: list[dict], symbol: str) -> list[dict]:
    valid = []
    for c in candles:
        o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
        if o <= 0 or h <= 0 or l <= 0 or cl <= 0:
            logger.warning(f"[VALIDATE] {symbol} | Dropping candle {c['timestamp']} with non-positive price")
            continue
        if h < max(o, cl) or l > min(o, cl):
            logger.warning(f"[VALIDATE] {symbol} | Candle {c['timestamp']} has invalid OHLC (h={h}, l={l}, o={o}, c={cl})")
        valid.append(c)
    if len(valid) < len(candles):
        logger.warning(f"[VALIDATE] {symbol} | Dropped {len(candles) - len(valid)}/{len(candles)} invalid candles")
    return valid


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
    def _get(self, endpoint: str, params: dict, base_url: str = None) -> dict:
        from trading_engine.database import log_api_usage
        from trading_engine.credit_control import pre_request_check, check_credit_thresholds

        pre_request_check()

        params["access_key"] = self.api_key
        effective_base = base_url or BASE_URL_V4_FOREX
        url = f"{effective_base}/{endpoint}"
        logger.info(f"[FCSAPI-REQUEST] {url} | params={{{', '.join(f'{k}={v}' for k, v in params.items() if k != 'access_key')}}}")
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
            url = f"{BASE_URL_V4_FOREX}/list"
            response = self.session.get(url, params={"access_key": key, "type": "forex", "per_page": "1"}, timeout=self._timeout)
            response.raise_for_status()
            data = response.json()
            if data.get("status") is False or data.get("code") == 101:
                return {"success": False, "error": data.get("msg", "Invalid API key")}
            log_api_usage(endpoint="list/test")
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
        if not is_symbol_supported(symbol):
            logger.warning(f"[GET-CANDLES] {symbol} | Symbol is marked as unsupported, skipping API call")
            return []

        tf_api = TIMEFRAME_MAP.get(period, period)
        api_symbol = get_v4_history_symbol(symbol)
        base_url = get_v4_base_url(symbol)
        asset_class = get_asset_class(symbol)

        params = {
            "symbol": api_symbol,
            "period": tf_api,
            "length": str(limit),
        }

        if asset_class == "stock":
            params["type"] = "index"
        elif asset_class == "commodity":
            params["type"] = "commodity"

        if from_timestamp:
            params["from"] = from_timestamp

        logger.info(f"[GET-CANDLES] {symbol} (api={api_symbol}, class={asset_class}, url={base_url}) | period={period}({tf_api}) | length={limit} | from={from_timestamp or 'latest'}")
        data = self._get("history", params, base_url=base_url)

        if data.get("status") is False or not data.get("response"):
            msg = data.get("msg", "")
            if msg:
                logger.error(f"[GET-CANDLES] {symbol} | API error msg: {msg}")
            logger.warning(f"[GET-CANDLES] {symbol} | No data returned (status={data.get('status')}, response={'empty' if not data.get('response') else 'present'})")
            return []

        candles = _parse_response_items(data["response"])
        candles = _validate_candle_prices(candles, symbol)
        logger.info(f"[GET-CANDLES] {symbol} | Parsed {len(candles)} candles | first={candles[0]['timestamp'] if candles else 'N/A'} | last={candles[-1]['timestamp'] if candles else 'N/A'}")
        return candles

    def fetch_history(self, symbol: str, timeframe: str, period: int = 300) -> list[dict]:
        return self.get_candles(symbol=symbol, period=timeframe, limit=period)

    def fetch_latest(self, symbol: str, timeframe: str) -> list[dict]:
        return self.get_candles(symbol=symbol, period=timeframe, limit=5)

    def get_available_symbols(self) -> list[str]:
        params = {"type": "forex"}
        data = self._get("list", params, base_url=BASE_URL_V4_FOREX)
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

    def get_advance_data(self, symbols: list[str], period: str = "1h", merge: str = "latest,profile") -> list[dict]:
        grouped: dict[str, list[tuple[str, str]]] = {"forex": [], "crypto": [], "stock": [], "commodity": []}
        for sym in symbols:
            if not is_symbol_supported(sym):
                continue
            asset_class = get_asset_class(sym)
            adv_sym = get_advance_symbol(sym)
            grouped[asset_class].append((sym, adv_sym))

        results = []
        for asset_class, sym_pairs in grouped.items():
            if not sym_pairs:
                continue
            api_symbols = ",".join(adv_sym for _, adv_sym in sym_pairs)
            symbol_to_original = {adv_sym: orig for orig, adv_sym in sym_pairs}

            base_url = get_v4_base_url(sym_pairs[0][0])

            tf_api = TIMEFRAME_MAP.get(period, period)
            params = {
                "symbol": api_symbols,
                "merge": merge,
                "period": tf_api,
            }
            if asset_class == "stock":
                params["type"] = "index"
            elif asset_class == "commodity":
                params["type"] = "commodity"

            logger.info(f"[ADVANCE] Fetching {asset_class} quotes: {api_symbols} | period={tf_api} | merge={merge}")
            try:
                data = self._get("advance", params, base_url=base_url)
            except Exception as e:
                logger.error(f"[ADVANCE] {asset_class} request failed: {e}")
                continue

            if data.get("status") is False or not data.get("response"):
                logger.warning(f"[ADVANCE] {asset_class} | No data returned: {data.get('msg', '')}")
                continue

            seen_symbols: set[str] = set()
            for item in data["response"]:
                ticker = item.get("ticker", "")
                profile = item.get("profile", {})
                api_sym = profile.get("symbol", ticker.split(":")[-1] if ":" in ticker else ticker)
                original_symbol = symbol_to_original.get(api_sym, api_sym)

                if original_symbol in seen_symbols:
                    continue
                seen_symbols.add(original_symbol)

                active = item.get("active", {})
                previous = item.get("previous", {})

                quote = {
                    "symbol": original_symbol,
                    "ticker": ticker,
                    "asset_class": asset_class,
                    "current": {
                        "open": _safe_float(active.get("o")),
                        "high": _safe_float(active.get("h")),
                        "low": _safe_float(active.get("l")),
                        "close": _safe_float(active.get("c")),
                        "ask": _safe_float(active.get("a")),
                        "bid": _safe_float(active.get("b")),
                        "volume": active.get("v"),
                        "vwap": _safe_float(active.get("vw")),
                        "change": _safe_float(active.get("ch")),
                        "change_pct": _safe_float(active.get("chp")),
                        "timestamp": active.get("tm", ""),
                    },
                    "previous": {
                        "open": _safe_float(previous.get("o")),
                        "high": _safe_float(previous.get("h")),
                        "low": _safe_float(previous.get("l")),
                        "close": _safe_float(previous.get("c")),
                        "volume": previous.get("v"),
                        "timestamp": previous.get("tm", ""),
                    },
                    "profile": {
                        "name": profile.get("name", ""),
                        "exchange": profile.get("exchange", ""),
                        "type": profile.get("type", ""),
                        "currency": profile.get("currency", ""),
                    },
                    "update_time": item.get("updateTime", ""),
                }
                results.append(quote)

        return results

    def close(self):
        self.session.close()
