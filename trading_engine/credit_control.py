import logging
import threading
import calendar
from datetime import datetime

logger = logging.getLogger("trading_engine.credit_control")

MONTHLY_LIMIT = 500_000
KILL_SWITCH_THRESHOLD = 495_000
WARNING_PROJECTION = 300_000
CRITICAL_PROJECTION = 450_000

_KILL_SWITCH_SETTING_KEY = "credit_kill_switch"
_KILL_SWITCH_MONTH_KEY = "credit_kill_switch_month"
_lock = threading.Lock()


class CreditLimitReached(Exception):
    pass


def _current_month_str() -> str:
    now = datetime.utcnow()
    return f"{now.year}-{now.month:02d}"


def _get_kill_switch_from_db() -> bool:
    from trading_engine.database import get_setting
    flag = get_setting(_KILL_SWITCH_SETTING_KEY)
    if flag != "true":
        return False
    stored_month = get_setting(_KILL_SWITCH_MONTH_KEY)
    if stored_month != _current_month_str():
        _clear_kill_switch_in_db()
        logger.info("[CREDIT-CONTROL] Kill switch auto-reset — new billing cycle detected.")
        return False
    return True


def _set_kill_switch_in_db():
    from trading_engine.database import set_setting
    set_setting(_KILL_SWITCH_SETTING_KEY, "true")
    set_setting(_KILL_SWITCH_MONTH_KEY, _current_month_str())


def _clear_kill_switch_in_db():
    from trading_engine.database import set_setting
    set_setting(_KILL_SWITCH_SETTING_KEY, "false")
    set_setting(_KILL_SWITCH_MONTH_KEY, "")


def get_monthly_usage() -> int:
    from trading_engine.database import get_api_usage_stats
    stats = get_api_usage_stats()
    return stats.get("monthly_total", 0)


def get_monthly_projection() -> dict:
    now = datetime.utcnow()
    day_of_month = now.day
    total_days = calendar.monthrange(now.year, now.month)[1]
    current_usage = get_monthly_usage()

    if day_of_month == 0:
        projected = current_usage
    else:
        daily_rate = current_usage / day_of_month
        projected = daily_rate * total_days

    return {
        "current_usage": current_usage,
        "day_of_month": day_of_month,
        "total_days_in_month": total_days,
        "daily_rate": round(current_usage / max(day_of_month, 1), 2),
        "projected_eom": round(projected, 0),
        "monthly_limit": MONTHLY_LIMIT,
        "projected_pct": round((projected / MONTHLY_LIMIT) * 100, 2) if MONTHLY_LIMIT > 0 else 0,
        "kill_switch_active": is_api_blocked(),
    }


def check_credit_thresholds() -> dict:
    projection = get_monthly_projection()
    current_usage = projection["current_usage"]
    projected_eom = projection["projected_eom"]
    alert_level = None

    with _lock:
        if current_usage >= KILL_SWITCH_THRESHOLD:
            if not _get_kill_switch_from_db():
                _set_kill_switch_in_db()
                logger.critical(
                    f"[CREDIT-CONTROL] KILL SWITCH ACTIVATED — actual usage {current_usage:,} "
                    f"has hit the {KILL_SWITCH_THRESHOLD:,} hard limit. "
                    f"All outbound API requests are now BLOCKED until next billing cycle."
                )
            alert_level = "kill_switch"
        elif projected_eom >= CRITICAL_PROJECTION:
            alert_level = "critical"
            logger.critical(
                f"[CREDIT-CONTROL] CRITICAL — projected EOM usage {projected_eom:,.0f} "
                f"exceeds 90% threshold ({CRITICAL_PROJECTION:,}). "
                f"Current: {current_usage:,} | Daily rate: {projection['daily_rate']:,.0f}"
            )
        elif projected_eom >= WARNING_PROJECTION:
            alert_level = "warning"
            logger.warning(
                f"[CREDIT-CONTROL] WARNING — projected EOM usage {projected_eom:,.0f} "
                f"exceeds 60% threshold ({WARNING_PROJECTION:,}). "
                f"Current: {current_usage:,} | Daily rate: {projection['daily_rate']:,.0f}"
            )

    projection["alert_level"] = alert_level
    return projection


def is_api_blocked() -> bool:
    return _get_kill_switch_from_db()


def reset_kill_switch():
    with _lock:
        _clear_kill_switch_in_db()
        logger.info("[CREDIT-CONTROL] Kill switch has been manually reset.")


def pre_request_check():
    if _get_kill_switch_from_db():
        raise CreditLimitReached(
            f"API requests blocked: monthly usage has reached {KILL_SWITCH_THRESHOLD:,} "
            f"of {MONTHLY_LIMIT:,} limit. Requests paused until next billing cycle."
        )

    current_usage = get_monthly_usage()
    if current_usage >= KILL_SWITCH_THRESHOLD:
        with _lock:
            _set_kill_switch_in_db()
        logger.critical(
            f"[CREDIT-CONTROL] KILL SWITCH ACTIVATED during pre-request check — "
            f"usage {current_usage:,} >= {KILL_SWITCH_THRESHOLD:,}"
        )
        raise CreditLimitReached(
            f"API requests blocked: monthly usage {current_usage:,} has reached the "
            f"{KILL_SWITCH_THRESHOLD:,} hard limit."
        )
