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

_STALENESS_THRESHOLDS: dict[str, int] = {
    "D1": 1440,
    "4H": 300,
    "1H": 120,
    "30m": 60,
}


class CacheLayer:
    def __init__(self, api_client: FCSAPIClient):
        self.api_client = api_client

    def _get_last_closed_candle_time(self, timeframe: str) -> datetime:
        now = datetime.utcnow()
        minutes = TIMEFRAME_DURATION_MINUTES[timeframe]

        if timeframe == "D1":
            # Return now minus 24h so any stored last_candle_close
            # within the last 24h is considered fresh.
            # Handles assets with non-midnight D1 closes
            # (e.g. XAU/USD at 22:00 UTC, SPX at 13:30 UTC).
            return now - timedelta(hours=24)

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

    # ── Layer 1: Pre-evaluation candle freshness ───────────────────────────────

    def _ensure_fresh_candles(
        self,
        asset: str,
        timeframe: str,
        max_staleness_minutes: Optional[int] = None,
    ) -> dict:
        """
        Verify the latest cached candle is within the acceptable staleness
        window for the given timeframe. Force-refreshes if stale.

        Staleness thresholds (if max_staleness_minutes not specified):
          D1  → 1440 minutes (24 hours)
          4H  → 300  minutes (5 hours)
          1H  → 120  minutes (2 hours)
          30m → 60   minutes (1 hour)
        """
        threshold = max_staleness_minutes or _STALENESS_THRESHOLDS.get(timeframe, 1440)
        result: dict = {
            "asset": asset,
            "timeframe": timeframe,
            "was_stale": False,
            "refreshed": False,
            "candle_count": 0,
            "latest_candle_ts": None,
            "staleness_minutes": None,
        }

        meta = get_cache_metadata(asset, timeframe)
        now_utc = datetime.utcnow()

        if meta is None or not meta.get("last_candle_close"):
            staleness_minutes = threshold + 1
        else:
            try:
                last_close_dt = datetime.fromisoformat(meta["last_candle_close"])
                staleness_minutes = int((now_utc - last_close_dt).total_seconds() / 60)
            except (ValueError, TypeError):
                staleness_minutes = threshold + 1

        result["staleness_minutes"] = staleness_minutes
        is_stale = staleness_minutes > threshold

        if is_stale:
            logger.info(
                f"[CACHE-FRESH] {asset}/{timeframe} | stale=True | "
                f"staleness={staleness_minutes}min | threshold={threshold}min | refreshing..."
            )
            result["was_stale"] = True
            try:
                candles = self.force_refresh(asset, timeframe)
                result["refreshed"] = True
                result["candle_count"] = len(candles)
                if candles:
                    result["latest_candle_ts"] = candles[-1].get("timestamp")
            except Exception as e:
                logger.error(f"[CACHE-FRESH] {asset}/{timeframe} | force_refresh failed: {e}")
        else:
            logger.info(
                f"[CACHE-FRESH] {asset}/{timeframe} | fresh | "
                f"staleness={staleness_minutes}min | threshold={threshold}min | no refresh needed"
            )
            result["candle_count"] = get_candle_count(asset, timeframe)
            if meta and meta.get("last_candle_close"):
                result["latest_candle_ts"] = meta["last_candle_close"]

        return result

    def ensure_fresh_candles_batch(
        self,
        assets_timeframes: list[tuple[str, str]],
        max_staleness_minutes: Optional[int] = None,
    ) -> dict:
        """
        Run _ensure_fresh_candles sequentially for a list of (asset, timeframe) tuples.

        Returns:
          {
              "total": 15,
              "refreshed": 3,
              "already_fresh": 12,
              "failed": 0,
              "details": [...per-asset results...]
          }
        """
        summary: dict = {
            "total": len(assets_timeframes),
            "refreshed": 0,
            "already_fresh": 0,
            "failed": 0,
            "details": [],
        }

        for asset, timeframe in assets_timeframes:
            try:
                res = self._ensure_fresh_candles(asset, timeframe, max_staleness_minutes)
                if res["refreshed"]:
                    summary["refreshed"] += 1
                else:
                    summary["already_fresh"] += 1
                summary["details"].append(res)
            except Exception as e:
                logger.error(f"[CACHE-FRESH-BATCH] {asset}/{timeframe} | error: {e}")
                summary["failed"] += 1
                summary["details"].append(
                    {
                        "asset": asset,
                        "timeframe": timeframe,
                        "was_stale": None,
                        "refreshed": False,
                        "error": str(e),
                    }
                )

        return summary

    # ── Layer 2: Pre-signal entry price validation ─────────────────────────────

    def _validate_entry_price(
        self,
        asset: str,
        cached_price: float,
        timeframe: str = "D1",
        tolerance_pct: float = 0.5,
    ) -> dict:
        """
        Cross-check the cached candle close price against a fresh FCSAPI
        advance endpoint call before allowing a signal to be inserted.

        If the live price differs from the cached price by more than
        tolerance_pct, the entry is flagged as invalid and a candle
        refresh is triggered automatically.

        tolerance_pct auto-selected by asset class if not overridden:
          crypto pairs  → 2.0%
          ETFs/stocks   → 1.0%
          forex pairs   → 0.5%
        """
        from trading_engine.fcsapi_client import get_asset_class

        asset_class = get_asset_class(asset)
        auto_tolerance: float
        if asset_class == "crypto":
            auto_tolerance = 2.0
        elif asset_class in ("etf", "stock"):
            auto_tolerance = 1.0
        else:
            auto_tolerance = 0.5
        effective_tolerance = tolerance_pct if tolerance_pct != 0.5 else auto_tolerance

        result: dict = {
            "valid": True,
            "cached_price": cached_price,
            "live_price": None,
            "diff_pct": None,
            "tolerance_pct": effective_tolerance,
            "refreshed": False,
            "reason": "pending",
        }

        try:
            advance_data = self.api_client.get_advance_data([asset], period="1d", merge="latest")
        except Exception as e:
            logger.warning(f"[PRICE-VALID] {asset}/{timeframe} | advance API error: {e} — skipping validation")
            result["valid"] = True
            result["reason"] = "advance_unavailable_skipped"
            return result

        if not advance_data:
            logger.warning(f"[PRICE-VALID] {asset}/{timeframe} | advance returned no data — skipping validation")
            result["valid"] = True
            result["reason"] = "advance_unavailable_skipped"
            return result

        live_row = advance_data[0] if advance_data else None
        live_price_raw = live_row.get("close") if live_row else None

        if live_price_raw is None:
            logger.warning(f"[PRICE-VALID] {asset}/{timeframe} | advance data has no close price — skipping")
            result["valid"] = True
            result["reason"] = "advance_unavailable_skipped"
            return result

        try:
            live_price = float(live_price_raw)
        except (TypeError, ValueError):
            result["valid"] = True
            result["reason"] = "advance_unavailable_skipped"
            return result

        result["live_price"] = live_price

        if cached_price == 0:
            result["valid"] = True
            result["reason"] = "cached_price_zero_skipped"
            return result

        diff_pct = abs(live_price - cached_price) / cached_price * 100
        result["diff_pct"] = round(diff_pct, 4)

        if diff_pct > effective_tolerance:
            logger.warning(
                f"[PRICE-VALID] {asset}/{timeframe} | "
                f"cached={cached_price:.5f} live={live_price:.5f} "
                f"diff={diff_pct:.2f}% > {effective_tolerance:.1f}% | INVALID — refreshing cache"
            )
            try:
                self.force_refresh(asset, timeframe)
                result["refreshed"] = True
            except Exception as e:
                logger.error(f"[PRICE-VALID] {asset}/{timeframe} | force_refresh after discrepancy failed: {e}")
            result["valid"] = False
            result["reason"] = "price_discrepancy"
        else:
            logger.info(
                f"[PRICE-VALID] {asset}/{timeframe} | "
                f"cached={cached_price:.5f} live={live_price:.5f} "
                f"diff={diff_pct:.2f}% <= {effective_tolerance:.1f}% | VALID"
            )
            result["valid"] = True
            result["reason"] = "within_tolerance"

        return result
