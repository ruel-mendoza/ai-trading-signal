import asyncio
import logging
from datetime import datetime, timezone, timedelta

from trading_engine.fcsapi_client import FCSAPIClient
from trading_engine.database import SessionFactory, get_setting, set_setting
from trading_engine.models import RecoveryNotification
from trading_engine.credit_control import is_api_blocked
from trading_engine.utils.quota_manager import is_watchdog_disabled_by_quota

logger = logging.getLogger("trading_engine.engine.watchdog")

WATCHLIST_SYMBOLS = ["EUR/USD", "AUD/USD", "GBP/USD"]

TARGET_LOW = 1.15845
PROXIMITY_PIPS = 0.00030
SUPPRESSION_MINUTES = 30
CHECK_INTERVAL_SECONDS = 60

ADMIN_WATCHDOG_DISABLED_KEY = "admin_watchdog_disabled"

_last_alert_times: dict[str, datetime] = {}


def is_watchdog_manually_disabled() -> bool:
    return get_setting(ADMIN_WATCHDOG_DISABLED_KEY) == "true"


def set_watchdog_manual_override(disabled: bool):
    set_setting(ADMIN_WATCHDOG_DISABLED_KEY, "true" if disabled else "false")
    logger.info(f"[WATCHDOG] Manual override set: {'DISABLED' if disabled else 'ENABLED'}")


def _is_suppressed(symbol: str, now: datetime) -> bool:
    last = _last_alert_times.get(symbol)
    if last and (now - last) < timedelta(minutes=SUPPRESSION_MINUTES):
        return True
    return False


def check_proximity():
    now = datetime.now(timezone.utc)

    if is_watchdog_manually_disabled():
        logger.info("[WATCHDOG] Disabled by admin toggle (manual override)")
        return

    if is_api_blocked():
        logger.debug("[WATCHDOG] API blocked by credit kill-switch, skipping price check")
        return

    if is_watchdog_disabled_by_quota():
        logger.info("[WATCHDOG] Disabled by quota manager (>95% credits used), preserving credits for signals")
        return

    try:
        client = FCSAPIClient()
        prices = client.get_v3_latest_prices(WATCHLIST_SYMBOLS)
    except Exception as e:
        logger.error(f"[WATCHDOG] Failed to batch-fetch prices: {e}")
        return

    if not prices:
        logger.warning("[WATCHDOG] No price data returned from v3 batch request")
        return

    for symbol in WATCHLIST_SYMBOLS:
        price = prices.get(symbol)
        if price is None:
            logger.warning(f"[WATCHDOG] No price returned for {symbol}")
            continue

        distance = price - TARGET_LOW
        logger.info(
            f"[WATCHDOG] {symbol} price={price:.5f} | target_low={TARGET_LOW:.5f} | "
            f"distance={distance:.5f} ({distance / 0.0001:.1f} pips)"
        )

        if distance <= PROXIMITY_PIPS and not _is_suppressed(symbol, now):
            _last_alert_times[symbol] = now
            msg = (
                f"\u26a0\ufe0f {symbol} is within 3 pips of 50-Day Low ({TARGET_LOW}). "
                f"Breakout imminent."
            )
            logger.warning(f"[WATCHDOG] PROXIMITY_ALERT: {msg}")

            session = SessionFactory()
            try:
                record = RecoveryNotification(
                    strategy_name="PROXIMITY_ALERT",
                    missed_window_time=now.strftime("%Y-%m-%d %H:%M:%S"),
                    execution_time=now.strftime("%Y-%m-%d %H:%M:%S"),
                    assets_affected=symbol,
                    status=msg,
                )
                session.add(record)
                session.commit()
                logger.info(f"[WATCHDOG] Proximity alert for {symbol} saved to recovery_notifications")
            except Exception as e:
                session.rollback()
                logger.error(f"[WATCHDOG] Failed to save alert for {symbol}: {e}")
            finally:
                session.close()


async def start_price_watchdog():
    symbols_str = ",".join(WATCHLIST_SYMBOLS)
    logger.info(
        f"[WATCHDOG] Multi-symbol proximity watchdog started "
        f"(symbols={symbols_str}, interval={CHECK_INTERVAL_SECONDS}s, "
        f"target={TARGET_LOW}, threshold={PROXIMITY_PIPS / 0.0001:.0f} pips, "
        f"v3 batch=1 credit per check)"
    )
    while True:
        try:
            await asyncio.get_event_loop().run_in_executor(None, check_proximity)
        except Exception as e:
            logger.error(f"[WATCHDOG] Unexpected error in price watchdog loop: {e}", exc_info=True)
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)
