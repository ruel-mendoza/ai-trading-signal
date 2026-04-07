import logging
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

# Re-entrance guard (kept as a safeguard for future changes)
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


# ---------------------------------------------------------------------------
# Public sync function
# ---------------------------------------------------------------------------

def sync_quota_from_api() -> dict:
    """
    Sync the local credit counter by recalculating from the api_usage_log table.

    The FCSAPI info.credit_count field only reflects credits consumed by the
    *current* request (always 1-2), not the account balance.  The api_usage_log
    table is the authoritative source for cumulative monthly usage.

    Steps:
      1. Delete stale api_usage_log rows from previous billing months.
      2. Sum credits_used for the current billing month from api_usage_log.
      3. Compute real_remaining = credit_limit - monthly_used.
      4. Persist to app_settings (fcsapi_remaining_credits, fcsapi_credit_limit).
      5. Clear kill switch + watchdog flag if usage_pct < 95 %.

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
    """Inner sync implementation (executes with _sync_in_progress=True)."""
    from trading_engine.database import get_setting, set_setting, SessionFactory
    from sqlalchemy import text

    now = datetime.now(timezone.utc)
    synced_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    now_str = now.strftime("%Y-%m-%d %H:%M:%S UTC")

    # Resolve credit limit from settings (default 500,000)
    limit_str = get_setting(QUOTA_CREDIT_LIMIT_KEY)
    try:
        credit_limit = int(limit_str) if limit_str else 500_000
    except (ValueError, TypeError):
        credit_limit = 500_000

    # Billing-month boundary (ISO string, e.g. "2026-04-01T00:00:00")
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()

    # Step 1: Delete stale api_usage_log rows from previous billing months
    stale_deleted = 0
    try:
        with SessionFactory() as session:
            result = session.execute(
                text("DELETE FROM api_usage_log WHERE timestamp < :start"),
                {"start": month_start},
            )
            session.commit()
            stale_deleted = result.rowcount
        if stale_deleted:
            logger.info(f"[QUOTA] Cleared {stale_deleted} stale api_usage_log rows from previous billing period")
    except Exception as e:
        logger.warning(f"[QUOTA] Could not delete stale usage logs: {e}")

    # Step 2: Sum credits used this billing month
    monthly_used = 0
    try:
        with SessionFactory() as session:
            monthly_used = (
                session.execute(
                    text(
                        "SELECT COALESCE(SUM(credits_used), 0) FROM api_usage_log "
                        "WHERE timestamp >= :start"
                    ),
                    {"start": month_start},
                ).scalar()
                or 0
            )
    except Exception as e:
        logger.warning(f"[QUOTA] Could not query monthly usage: {e}")

    # Step 3: Compute remaining and usage %
    real_remaining = max(0, credit_limit - int(monthly_used))
    usage_pct = round((int(monthly_used) / credit_limit) * 100, 2) if credit_limit > 0 else 0.0

    logger.info(
        f"[QUOTA] Sync result: monthly_used={int(monthly_used):,}, "
        f"remaining={real_remaining:,} / {credit_limit:,} ({usage_pct:.2f}% used)"
    )

    # Step 4: Persist to app_settings
    set_setting(QUOTA_REMAINING_KEY, str(real_remaining))
    set_setting(QUOTA_CREDIT_LIMIT_KEY, str(credit_limit))
    set_setting(QUOTA_LAST_UPDATED_KEY, now_str)

    # Step 5: Clear kill switch if usage is below critical threshold
    kill_switch_cleared = False
    if usage_pct < float(CRITICAL_THRESHOLD_PCT):
        set_setting("credit_kill_switch", "false")
        set_setting(QUOTA_WATCHDOG_DISABLED_KEY, "false")
        set_setting("credit_kill_switch_month", "")
        kill_switch_cleared = True
        logger.info(
            f"[QUOTA] Kill switch cleared and watchdog re-enabled "
            f"(usage {usage_pct:.2f}% < critical threshold {CRITICAL_THRESHOLD_PCT}%)"
        )
    else:
        logger.warning(
            f"[QUOTA] Kill switch NOT cleared — usage {usage_pct:.2f}% "
            f">= critical threshold {CRITICAL_THRESHOLD_PCT}%"
        )

    return {
        "success": True,
        "real_remaining": real_remaining,
        "real_limit": credit_limit,
        "monthly_used": int(monthly_used),
        "usage_pct": usage_pct,
        "kill_switch_cleared": kill_switch_cleared,
        "stale_logs_deleted": stale_deleted,
        "synced_at": synced_at,
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

    # Billing-month detection: auto-sync if the billing cycle has reset
    if _is_new_billing_month() and not _sync_in_progress:
        logger.info(
            "[QUOTA] New billing month detected — auto-syncing quota from usage logs "
            "to clear kill switch"
        )
        try:
            sync_quota_from_api()
        except Exception as e:
            logger.warning(f"[QUOTA] Auto-sync on new billing month failed: {e}")

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    # NOTE: remaining here is the credits consumed by *this single request* (info.credit_count),
    # not the account balance.  We do NOT persist it to fcsapi_remaining_credits; the authoritative
    # balance is managed by sync_quota_from_api() via api_usage_log.
    if not existing_limit:
        set_setting(QUOTA_CREDIT_LIMIT_KEY, str(credit_limit))

    result = {
        "credit_limit": credit_limit,
        "credits_this_request": remaining,
        "updated_at": now_str,
    }

    logger.debug(f"[QUOTA] Request consumed {remaining} credit(s); limit={credit_limit}")
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
        message = (
            f"CRITICAL: {usage_pct:.1f}% of API credits used "
            f"({used:,}/{credit_limit:,}). Only {remaining:,} credits remaining."
        )

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
        message = (
            f"WARNING: {usage_pct:.1f}% of API credits used "
            f"({used:,}/{credit_limit:,}). {remaining:,} credits remaining."
        )

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
