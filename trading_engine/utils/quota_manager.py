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

# Re-entrance guard: prevents update_quota → sync_quota_from_api → update_quota loops
_sync_in_progress: bool = False


# ---------------------------------------------------------------------------
# Billing-month helper
# ---------------------------------------------------------------------------

def _is_new_billing_month() -> bool:
    """
    Returns True if the stored credit_kill_switch_month is from a previous
    calendar month, indicating the billing cycle has reset.
    """
    from trading_engine.database import get_setting
    stored = get_setting("credit_kill_switch_month")
    if not stored:
        return False
    now = datetime.utcnow()
    current_month = f"{now.year}-{now.month:02d}"
    return stored != current_month


def _delete_stale_usage_logs() -> int:
    """Delete api_usage_log rows from previous billing months. Returns row count deleted."""
    try:
        from trading_engine.database import SessionFactory
        from sqlalchemy import text
        with SessionFactory() as session:
            result = session.execute(
                text("DELETE FROM api_usage_log WHERE timestamp < date('now', 'start of month')")
            )
            session.commit()
            deleted = result.rowcount
            logger.info(f"[QUOTA] Deleted {deleted} stale api_usage_log rows from previous billing months")
            return deleted
    except Exception as e:
        logger.warning(f"[QUOTA] Could not delete stale usage logs: {e}")
        return 0


# ---------------------------------------------------------------------------
# Public sync function
# ---------------------------------------------------------------------------

def sync_quota_from_api() -> dict:
    """
    Force-sync the local credit counter by making a lightweight FCSAPI
    test call and reading the credit_count from the response info block.

    Called:
      - On app startup (in main.py lifespan)
      - When admin clicks "Sync Credits" button
      - Automatically at the start of each new billing month (via update_quota)

    Returns a dict with the sync result.
    """
    global _sync_in_progress

    if _sync_in_progress:
        logger.debug("[QUOTA] sync_quota_from_api skipped — sync already in progress")
        return {"success": False, "error": "sync already in progress"}

    _sync_in_progress = True
    try:
        return _do_sync()
    finally:
        _sync_in_progress = False


def _do_sync() -> dict:
    """Inner implementation of the quota sync (called with _sync_in_progress=True)."""
    import requests as _requests
    from trading_engine.fcsapi_client import FCSAPIClient, BASE_URL_V4_FOREX
    from trading_engine.database import set_setting, get_setting
    from trading_engine.credit_control import _clear_kill_switch_in_db

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # --- Lightweight API call: /forex/list with per_page=1 (1 credit) ---
    client = FCSAPIClient()
    api_key = client.api_key
    if not api_key:
        logger.warning("[QUOTA] Cannot sync — no FCSAPI key configured")
        return {"success": False, "error": "no api key", "synced_at": now_str}

    url = f"{BASE_URL_V4_FOREX}/list"
    params = {"access_key": api_key, "per_page": 1}

    try:
        logger.info("[QUOTA] Syncing quota from FCSAPI /forex/list...")
        resp = _requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning(f"[QUOTA] API call for quota sync failed: {exc}")
        return {"success": False, "error": str(exc), "synced_at": now_str}

    # --- Extract credit_count from response ---
    info = data.get("info", {}) if isinstance(data, dict) else {}
    credit_count = info.get("credit_count")
    if credit_count is None:
        logger.warning("[QUOTA] credit_count missing from sync response")
        return {"success": False, "error": "credit_count missing from response", "synced_at": now_str}

    try:
        real_remaining = int(credit_count)
    except (ValueError, TypeError):
        logger.warning(f"[QUOTA] Could not parse credit_count: {credit_count!r}")
        return {"success": False, "error": f"invalid credit_count: {credit_count}", "synced_at": now_str}

    # Resolve credit limit
    limit_str = get_setting(QUOTA_CREDIT_LIMIT_KEY)
    try:
        real_limit = int(limit_str) if limit_str else 500_000
    except (ValueError, TypeError):
        real_limit = 500_000

    used = max(real_limit - real_remaining, 0)
    usage_pct = round(used / real_limit * 100, 2) if real_limit > 0 else 0.0
    critical_threshold_remaining = real_limit * (1 - CRITICAL_THRESHOLD_PCT / 100)

    # --- Update local settings ---
    set_setting(QUOTA_REMAINING_KEY, str(real_remaining))
    set_setting(QUOTA_LAST_UPDATED_KEY, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))

    logger.info(
        f"[QUOTA] Synced from FCSAPI: remaining={real_remaining:,} / {real_limit:,} "
        f"({usage_pct:.1f}% used)"
    )

    # --- Log credit usage for this sync call (1 credit) ---
    try:
        from trading_engine.database import log_api_usage
        log_api_usage(endpoint="/forex/list (quota-sync)", credits_used=1)
    except Exception:
        pass

    # --- Auto-clear kill switch if credits are above critical threshold ---
    kill_switch_cleared = False
    if real_remaining > critical_threshold_remaining:
        _clear_kill_switch_in_db()
        set_setting(QUOTA_WATCHDOG_DISABLED_KEY, "false")
        kill_switch_cleared = True
        logger.info(
            f"[QUOTA] Kill switch cleared and watchdog re-enabled — "
            f"real_remaining={real_remaining:,} is above critical threshold ({critical_threshold_remaining:,.0f})"
        )
    else:
        logger.warning(
            f"[QUOTA] Kill switch NOT cleared — remaining={real_remaining:,} is still "
            f"below critical threshold ({critical_threshold_remaining:,.0f})"
        )

    # --- Delete stale api_usage_log rows from previous billing months ---
    stale_deleted = _delete_stale_usage_logs()

    return {
        "success": True,
        "real_remaining": real_remaining,
        "real_limit": real_limit,
        "usage_pct": usage_pct,
        "kill_switch_cleared": kill_switch_cleared,
        "stale_logs_deleted": stale_deleted,
        "synced_at": now_str,
    }


# ---------------------------------------------------------------------------
# Core quota tracking (called on every FCSAPI response)
# ---------------------------------------------------------------------------

def update_quota(response_data: dict) -> dict | None:
    global _sync_in_progress

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

    # --- Billing-month detection: auto-sync if the billing cycle has reset ---
    if _is_new_billing_month() and not _sync_in_progress:
        logger.info(
            "[QUOTA] New billing month detected — auto-syncing quota from FCSAPI to clear kill switch"
        )
        try:
            sync_quota_from_api()
        except Exception as e:
            logger.warning(f"[QUOTA] Auto-sync on new billing month failed: {e}")

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


# ---------------------------------------------------------------------------
# Budget health check (triggers watchdog disable on critical usage)
# ---------------------------------------------------------------------------

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
