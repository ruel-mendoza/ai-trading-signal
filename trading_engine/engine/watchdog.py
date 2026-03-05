import logging
from datetime import datetime, timezone, timedelta

from trading_engine.fcsapi_client import FCSAPIClient
from trading_engine.database import SessionFactory
from trading_engine.models import RecoveryNotification
from trading_engine.credit_control import is_api_blocked

logger = logging.getLogger("trading_engine.engine.watchdog")

TARGET_LOW = 1.15845
PROXIMITY_PIPS = 0.00030
SUPPRESSION_MINUTES = 30

_last_alert_time: datetime | None = None


def check_eurusd_proximity():
    global _last_alert_time

    now = datetime.now(timezone.utc)
    if _last_alert_time and (now - _last_alert_time) < timedelta(minutes=SUPPRESSION_MINUTES):
        return

    if is_api_blocked():
        logger.debug("[WATCHDOG] API blocked by credit kill-switch, skipping price check")
        return

    try:
        client = FCSAPIClient()
        data = client.get_advance_data(["EUR/USD"], period="1h", merge="latest")
    except Exception as e:
        logger.error(f"[WATCHDOG] Failed to fetch EUR/USD quote: {e}")
        return

    if not data:
        logger.warning("[WATCHDOG] No quote data returned for EUR/USD")
        return

    quote = data[0]
    current = quote.get("current", {})
    price = current.get("close") or current.get("bid")
    if price is None:
        logger.warning(f"[WATCHDOG] Could not extract price from quote keys: {list(current.keys())}")
        return

    try:
        price = float(price)
    except (ValueError, TypeError):
        logger.error(f"[WATCHDOG] Invalid price value: {price}")
        return

    distance = price - TARGET_LOW
    logger.info(f"[WATCHDOG] EUR/USD price={price:.5f} | target_low={TARGET_LOW:.5f} | distance={distance:.5f} ({distance / 0.0001:.1f} pips)")

    if distance <= PROXIMITY_PIPS:
        _last_alert_time = now
        msg = (
            "\u26a0\ufe0f EUR/USD is within 3 pips of 50-Day Low. "
            "Breakout imminent for 9 AM Window."
        )
        logger.warning(f"[WATCHDOG] PROXIMITY_ALERT: {msg}")

        session = SessionFactory()
        try:
            record = RecoveryNotification(
                strategy_name="PROXIMITY_ALERT",
                missed_window_time=now.strftime("%Y-%m-%d %H:%M:%S"),
                execution_time=now.strftime("%Y-%m-%d %H:%M:%S"),
                assets_affected="EUR/USD",
                status=msg,
            )
            session.add(record)
            session.commit()
            logger.info("[WATCHDOG] Proximity alert saved to recovery_notifications")
        except Exception as e:
            session.rollback()
            logger.error(f"[WATCHDOG] Failed to save alert: {e}")
        finally:
            session.close()
