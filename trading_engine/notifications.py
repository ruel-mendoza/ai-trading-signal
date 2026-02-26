import logging
import threading
import json
from datetime import datetime
from typing import Optional
import httpx

logger = logging.getLogger("trading_engine.notifications")

ALERT_LEVELS = ("info", "warning", "critical", "kill_switch")

_webhook_url: Optional[str] = None
_webhook_lock = threading.Lock()


def configure_webhook(url: Optional[str]):
    global _webhook_url
    with _webhook_lock:
        _webhook_url = url
        if url:
            logger.info(f"[NOTIFY] Webhook configured: {url[:40]}...")
        else:
            logger.info("[NOTIFY] Webhook cleared")


def get_webhook_url() -> Optional[str]:
    return _webhook_url


def _format_discord(title: str, message: str, level: str, fields: Optional[dict] = None) -> dict:
    color_map = {
        "info": 0x3498DB,
        "warning": 0xF39C12,
        "critical": 0xE74C3C,
        "kill_switch": 0x8B0000,
    }
    embed = {
        "title": f"{'🔴' if level in ('critical', 'kill_switch') else '🟡' if level == 'warning' else 'ℹ️'} {title}",
        "description": message,
        "color": color_map.get(level, 0x95A5A6),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "footer": {"text": "AI Signals Trading Engine"},
    }
    if fields:
        embed["fields"] = [
            {"name": k, "value": str(v), "inline": True}
            for k, v in fields.items()
        ]
    return {"embeds": [embed]}


def _format_slack(title: str, message: str, level: str, fields: Optional[dict] = None) -> dict:
    emoji_map = {
        "info": ":information_source:",
        "warning": ":warning:",
        "critical": ":rotating_light:",
        "kill_switch": ":no_entry:",
    }
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{emoji_map.get(level, '')} {title}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": message},
        },
    ]
    if fields:
        field_blocks = []
        for k, v in fields.items():
            field_blocks.append({"type": "mrkdwn", "text": f"*{k}:* {v}"})
        blocks.append({"type": "section", "fields": field_blocks})

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": f"AI Signals Engine | {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"}],
    })
    return {"blocks": blocks}


def _format_generic(title: str, message: str, level: str, fields: Optional[dict] = None) -> dict:
    payload = {
        "event": title,
        "message": message,
        "level": level,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "trading_engine",
    }
    if fields:
        payload["fields"] = fields
    return payload


def _detect_webhook_type(url: str) -> str:
    if "discord.com/api/webhooks" in url or "discordapp.com/api/webhooks" in url:
        return "discord"
    if "hooks.slack.com" in url:
        return "slack"
    return "generic"


def _send_webhook(title: str, message: str, level: str, fields: Optional[dict] = None):
    url = _webhook_url
    if not url:
        return

    webhook_type = _detect_webhook_type(url)

    if webhook_type == "discord":
        payload = _format_discord(title, message, level, fields)
    elif webhook_type == "slack":
        payload = _format_slack(title, message, level, fields)
    else:
        payload = _format_generic(title, message, level, fields)

    try:
        with httpx.Client(timeout=10) as client:
            resp = client.post(url, json=payload)
            if resp.status_code >= 400:
                logger.warning(
                    f"[NOTIFY] Webhook returned {resp.status_code}: {resp.text[:200]}"
                )
            else:
                logger.debug(f"[NOTIFY] Webhook sent ({webhook_type}): {title}")
    except Exception as e:
        logger.error(f"[NOTIFY] Webhook delivery failed: {e}")


def send_alert(title: str, message: str, level: str = "info", fields: Optional[dict] = None):
    if level not in ALERT_LEVELS:
        level = "info"

    logger.log(
        logging.CRITICAL if level in ("critical", "kill_switch") else
        logging.WARNING if level == "warning" else logging.INFO,
        f"[ALERT:{level.upper()}] {title} — {message}"
    )

    thread = threading.Thread(
        target=_send_webhook,
        args=(title, message, level, fields),
        daemon=True,
    )
    thread.start()


def notify_kill_switch_activated(current_usage: int, threshold: int, monthly_limit: int):
    send_alert(
        "Credit Kill Switch ACTIVATED",
        f"API requests are now BLOCKED. Monthly usage ({current_usage:,}) has reached "
        f"the hard limit ({threshold:,}/{monthly_limit:,}). "
        f"All outbound API calls are suspended until the next billing cycle.",
        level="kill_switch",
        fields={
            "Current Usage": f"{current_usage:,}",
            "Threshold": f"{threshold:,}",
            "Monthly Limit": f"{monthly_limit:,}",
        },
    )


def notify_credit_warning(alert_level: str, current_usage: int, projected_eom: float, daily_rate: float):
    send_alert(
        f"Credit Usage {alert_level.upper()}",
        f"Projected end-of-month usage: {projected_eom:,.0f} credits. "
        f"Current: {current_usage:,} | Daily rate: {daily_rate:,.0f}",
        level=alert_level,
        fields={
            "Current Usage": f"{current_usage:,}",
            "Projected EOM": f"{projected_eom:,.0f}",
            "Daily Rate": f"{daily_rate:,.0f}",
        },
    )


def notify_strategy_failure(strategy_name: str, error_count: int, total_assets: int, error_detail: str = ""):
    send_alert(
        f"Strategy Run Failed: {strategy_name}",
        f"{error_count}/{total_assets} assets failed during scheduled evaluation. "
        f"{'Details: ' + error_detail[:500] if error_detail else 'Check scheduler job logs.'}",
        level="critical" if error_count == total_assets else "warning",
        fields={
            "Strategy": strategy_name,
            "Failed Assets": f"{error_count}/{total_assets}",
        },
    )


def notify_scheduler_down(restart_attempted: bool, restart_success: bool):
    if restart_success:
        send_alert(
            "Scheduler Auto-Restarted",
            "The APScheduler was found stopped and has been successfully restarted by the watchdog.",
            level="warning",
        )
    else:
        send_alert(
            "Scheduler DOWN — Restart Failed",
            "The APScheduler has stopped and could NOT be restarted. "
            "Manual intervention required. Strategies will not execute until the scheduler is restored.",
            level="critical",
        )


def notify_new_signal(strategy_name: str, asset: str, direction: str, entry_price: float, signal_id: int = 0):
    send_alert(
        f"New Signal: {direction} {asset}",
        f"Strategy '{strategy_name}' generated a {direction} signal for {asset} at {entry_price}.",
        level="info",
        fields={
            "Strategy": strategy_name,
            "Asset": asset,
            "Direction": direction,
            "Entry Price": f"{entry_price}",
            "Signal ID": str(signal_id),
        },
    )
