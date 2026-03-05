import logging
import json
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("trading_engine.utils.quota_manager")

QUOTA_CREDIT_LIMIT_KEY = "fcsapi_credit_limit"
QUOTA_REMAINING_KEY = "fcsapi_remaining_credits"
QUOTA_LAST_UPDATED_KEY = "fcsapi_quota_last_updated"
QUOTA_WATCHDOG_DISABLED_KEY = "quota_watchdog_disabled"

WARNING_THRESHOLD_PCT = 80
CRITICAL_THRESHOLD_PCT = 95

_ALERT_SUPPRESSION_MINUTES = 60
_last_warning_time: datetime | None = None
_last_critical_time: datetime | None = None


def update_quota(response_data: dict) -> dict | None:
    if not isinstance(response_data, dict):
        return None

    info = response_data.get("info", {})
    if not info or not isinstance(info, dict):
        return None

    credit_count = info.get("credit_count")
    if credit_count is None:
        return None

    try:
        remaining = int(credit_count)
    except (ValueError, TypeError):
        logger.warning(f"[QUOTA] Could not parse credit_count: {credit_count}")
        return None

    from trading_engine.database import set_setting, get_setting

    existing_limit = get_setting(QUOTA_CREDIT_LIMIT_KEY)
    if existing_limit:
        try:
            credit_limit = int(existing_limit)
        except (ValueError, TypeError):
            credit_limit = 500_000
    else:
        credit_limit = 500_000

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    set_setting(QUOTA_REMAINING_KEY, str(remaining))
    set_setting(QUOTA_LAST_UPDATED_KEY, now_str)

    if not existing_limit:
        set_setting(QUOTA_CREDIT_LIMIT_KEY, str(credit_limit))

    result = {
        "credit_limit": credit_limit,
        "remaining_credits": remaining,
        "updated_at": now_str,
    }

    logger.debug(f"[QUOTA] Updated: remaining={remaining}, limit={credit_limit}")
    return result


def check_budget_health() -> dict:
    global _last_warning_time, _last_critical_time

    from trading_engine.database import get_setting

    limit_str = get_setting(QUOTA_CREDIT_LIMIT_KEY)
    remaining_str = get_setting(QUOTA_REMAINING_KEY)

    if not limit_str or not remaining_str:
        return {
            "status": "unknown",
            "message": "No quota data available yet",
            "usage_pct": 0,
            "credit_limit": 0,
            "remaining_credits": 0,
            "watchdog_disabled": False,
        }

    try:
        credit_limit = int(limit_str)
        remaining = int(remaining_str)
    except (ValueError, TypeError):
        return {
            "status": "error",
            "message": "Invalid quota data in settings",
            "usage_pct": 0,
            "credit_limit": 0,
            "remaining_credits": 0,
            "watchdog_disabled": False,
        }

    used = credit_limit - remaining
    usage_pct = (used / credit_limit * 100) if credit_limit > 0 else 0

    watchdog_disabled = get_setting(QUOTA_WATCHDOG_DISABLED_KEY) == "true"
    now = datetime.now(timezone.utc)
    status = "healthy"
    message = f"Credit usage at {usage_pct:.1f}%"

    if usage_pct >= CRITICAL_THRESHOLD_PCT:
        status = "critical"
        message = f"CRITICAL: {usage_pct:.1f}% of API credits used ({used:,}/{credit_limit:,}). Only {remaining:,} credits remaining."

        should_alert = (
            _last_critical_time is None
            or (now - _last_critical_time) >= timedelta(minutes=_ALERT_SUPPRESSION_MINUTES)
        )

        if should_alert:
            _last_critical_time = now
            _insert_quota_alert(
                "CRITICAL",
                f"⚠️ API quota CRITICAL: {usage_pct:.1f}% used. {remaining:,} credits remaining. Watchdog auto-disabled to preserve credits for signals.",
            )
            logger.critical(f"[QUOTA] {message}")

        if not watchdog_disabled:
            _disable_watchdog()

    elif usage_pct >= WARNING_THRESHOLD_PCT:
        status = "warning"
        message = f"WARNING: {usage_pct:.1f}% of API credits used ({used:,}/{credit_limit:,}). {remaining:,} credits remaining."

        should_alert = (
            _last_warning_time is None
            or (now - _last_warning_time) >= timedelta(minutes=_ALERT_SUPPRESSION_MINUTES)
        )

        if should_alert:
            _last_warning_time = now
            _insert_quota_alert(
                "WARNING",
                f"⚠️ API quota warning: {usage_pct:.1f}% used. {remaining:,} credits remaining.",
            )
            logger.warning(f"[QUOTA] {message}")
    else:
        if watchdog_disabled:
            _enable_watchdog()

    return {
        "status": status,
        "message": message,
        "usage_pct": round(usage_pct, 2),
        "credit_limit": credit_limit,
        "remaining_credits": remaining,
        "used_credits": used,
        "watchdog_disabled": watchdog_disabled,
    }


def _insert_quota_alert(level: str, message: str):
    from trading_engine.database import SessionFactory
    from trading_engine.models import RecoveryNotification

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    session = SessionFactory()
    try:
        record = RecoveryNotification(
            strategy_name="QUOTA_ALERT",
            missed_window_time=now_str,
            execution_time=now_str,
            assets_affected=level,
            status=message,
        )
        session.add(record)
        session.commit()
        logger.info(f"[QUOTA] {level} alert saved to recovery_notifications")
    except Exception as e:
        session.rollback()
        logger.error(f"[QUOTA] Failed to save alert: {e}")
    finally:
        session.close()


def _disable_watchdog():
    from trading_engine.database import set_setting

    set_setting(QUOTA_WATCHDOG_DISABLED_KEY, "true")
    logger.warning("[QUOTA] Watchdog DISABLED to preserve remaining API credits for signal generation")


def _enable_watchdog():
    from trading_engine.database import set_setting

    set_setting(QUOTA_WATCHDOG_DISABLED_KEY, "false")
    logger.info("[QUOTA] Watchdog re-enabled — quota usage back below critical threshold")


def is_watchdog_disabled_by_quota() -> bool:
    from trading_engine.database import get_setting

    return get_setting(QUOTA_WATCHDOG_DISABLED_KEY) == "true"


def set_credit_limit(limit: int):
    from trading_engine.database import set_setting

    set_setting(QUOTA_CREDIT_LIMIT_KEY, str(limit))
    logger.info(f"[QUOTA] Credit limit updated to {limit:,}")


def get_quota_status() -> dict:
    from trading_engine.database import get_setting

    return {
        "credit_limit": get_setting(QUOTA_CREDIT_LIMIT_KEY),
        "remaining_credits": get_setting(QUOTA_REMAINING_KEY),
        "last_updated": get_setting(QUOTA_LAST_UPDATED_KEY),
        "watchdog_disabled": get_setting(QUOTA_WATCHDOG_DISABLED_KEY) == "true",
    }
