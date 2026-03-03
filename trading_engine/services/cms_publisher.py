import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception,
    before_sleep_log,
)

logger = logging.getLogger("trading_engine.cms_publisher")

_DIRECTION_LABEL = {"BUY": "LONG", "SELL": "SHORT"}


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, requests.exceptions.ConnectionError):
        return True
    if isinstance(exc, requests.exceptions.Timeout):
        return True
    if isinstance(exc, requests.exceptions.HTTPError):
        resp = exc.response
        if resp is not None and resp.status_code >= 500:
            return True
    return False


class CmsPublisher:
    def __init__(self):
        self.wp_url = (os.environ.get("WP_URL") or "").rstrip("/")
        self.wp_username = os.environ.get("WP_USERNAME") or ""
        self.wp_password = os.environ.get("WP_APP_PASSWORD") or ""
        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        if self.wp_username and self.wp_password:
            self._session.auth = (self.wp_username, self.wp_password)

    @property
    def is_configured(self) -> bool:
        return bool(self.wp_url and self.wp_username and self.wp_password)

    def _api_url(self, path: str) -> str:
        return f"{self.wp_url}/wp-json/wp/v2/{path.lstrip('/')}"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception(_is_retryable),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _wp_post(self, url: str, payload: dict) -> dict:
        resp = self._session.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def publish_signal(self, signal_id: int) -> dict:
        from trading_engine.database import get_signal_by_id, update_signal_wp_fields

        if not self.is_configured:
            logger.error("[CMS] WordPress credentials not configured")
            return {"status": "error", "message": "WordPress credentials not configured"}

        signal = get_signal_by_id(signal_id)
        if not signal:
            return {"status": "error", "message": f"Signal #{signal_id} not found"}

        if signal.get("wp_post_id"):
            logger.info(f"[CMS] Signal #{signal_id} already published (wp_post_id={signal['wp_post_id']})")
            return {
                "status": "skipped",
                "message": "Already published",
                "wp_post_id": signal["wp_post_id"],
            }

        html = self._format_signal_html(signal)
        title = self._format_title(signal)

        payload = {
            "title": title,
            "content": html,
            "status": "publish",
        }

        try:
            result = self._wp_post(self._api_url("posts"), payload)
            wp_post_id = result.get("id")
            now_iso = datetime.now(timezone.utc).isoformat()
            update_signal_wp_fields(signal_id, {
                "wp_post_id": wp_post_id,
                "publish_status": "PUBLISHED",
                "wp_last_sync": now_iso,
            })
            logger.info(f"[CMS] Published signal #{signal_id} → WP post #{wp_post_id}")
            return {
                "status": "ok",
                "message": "Published successfully",
                "wp_post_id": wp_post_id,
            }
        except Exception as e:
            now_iso = datetime.now(timezone.utc).isoformat()
            update_signal_wp_fields(signal_id, {
                "publish_status": "FAILED",
                "wp_last_sync": now_iso,
            })
            error_detail = {
                "signal_id": signal_id,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "timestamp": now_iso,
                "retries_exhausted": True,
            }
            logger.error(f"[CMS] Publish failed after retries: {json.dumps(error_detail)}")
            return {"status": "error", "message": str(e), "detail": error_detail}

    def update_closed_signal(self, signal_id: int) -> dict:
        from trading_engine.database import get_signal_by_id, update_signal_wp_fields

        if not self.is_configured:
            return {"status": "error", "message": "WordPress credentials not configured"}

        signal = get_signal_by_id(signal_id)
        if not signal:
            return {"status": "error", "message": f"Signal #{signal_id} not found"}

        wp_post_id = signal.get("wp_post_id")
        if not wp_post_id:
            return {"status": "error", "message": f"Signal #{signal_id} has no WP post to update"}

        if signal.get("status") != "CLOSED":
            return {"status": "error", "message": f"Signal #{signal_id} is not CLOSED"}

        closing_html = self._format_closing_html(signal)
        original_html = self._fetch_existing_content(wp_post_id)
        updated_html = original_html + closing_html

        payload = {"content": updated_html}

        try:
            self._wp_post(self._api_url(f"posts/{wp_post_id}"), payload)
            now_iso = datetime.now(timezone.utc).isoformat()
            update_signal_wp_fields(signal_id, {"wp_last_sync": now_iso})
            logger.info(f"[CMS] Updated WP post #{wp_post_id} with close data for signal #{signal_id}")
            return {"status": "ok", "message": "Post updated with closing data"}
        except Exception as e:
            error_detail = {
                "signal_id": signal_id,
                "wp_post_id": wp_post_id,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            logger.error(f"[CMS] Update closed signal failed: {json.dumps(error_detail)}")
            return {"status": "error", "message": str(e), "detail": error_detail}

    def _fetch_existing_content(self, wp_post_id: int) -> str:
        try:
            resp = self._session.get(self._api_url(f"posts/{wp_post_id}"), timeout=15)
            resp.raise_for_status()
            data = resp.json()
            return data.get("content", {}).get("rendered", "")
        except Exception as e:
            logger.warning(f"[CMS] Could not fetch existing content for WP post #{wp_post_id}: {e}")
            return ""

    def _format_title(self, signal: dict) -> str:
        direction = _DIRECTION_LABEL.get(signal.get("direction", ""), signal.get("direction", ""))
        asset = signal.get("asset", "Unknown")
        return f"Trading Signal: {direction} {asset}"

    def _format_signal_html(self, signal: dict) -> str:
        direction = _DIRECTION_LABEL.get(signal.get("direction", ""), signal.get("direction", ""))
        asset = signal.get("asset", "Unknown")
        strategy = signal.get("strategy_name", "")
        entry = signal.get("entry_price", "N/A")
        sl = signal.get("stop_loss", "N/A")
        tp = signal.get("take_profit", "N/A")
        timestamp = signal.get("signal_timestamp", signal.get("created_at", ""))

        return f"""<!-- AI Signals – Signal #{signal.get('id', '')} -->
<div class="trading-signal">
<h2>{direction} {asset}</h2>
<table style="border-collapse:collapse;width:100%;max-width:500px;">
<tr><td style="padding:8px;font-weight:bold;">Direction</td><td style="padding:8px;">{direction}</td></tr>
<tr><td style="padding:8px;font-weight:bold;">Asset</td><td style="padding:8px;">{asset}</td></tr>
<tr><td style="padding:8px;font-weight:bold;">Strategy</td><td style="padding:8px;">{strategy}</td></tr>
<tr><td style="padding:8px;font-weight:bold;">Entry Price</td><td style="padding:8px;">{entry}</td></tr>
<tr><td style="padding:8px;font-weight:bold;">Stop Loss</td><td style="padding:8px;">{sl}</td></tr>
<tr><td style="padding:8px;font-weight:bold;">Take Profit</td><td style="padding:8px;">{tp}</td></tr>
<tr><td style="padding:8px;font-weight:bold;">Published</td><td style="padding:8px;">{timestamp}</td></tr>
</table>
<p><em>Signal generated by AI Signals Trading Engine</em></p>
</div>"""

    def _format_closing_html(self, signal: dict) -> str:
        exit_price = signal.get("exit_price", "N/A")
        exit_reason = signal.get("exit_reason", "N/A")
        entry_price = signal.get("entry_price", 0)
        direction = signal.get("direction", "")

        outcome = "N/A"
        if exit_price != "N/A" and entry_price:
            try:
                ep = float(exit_price)
                en = float(entry_price)
                if direction == "BUY":
                    pips = ep - en
                else:
                    pips = en - ep
                outcome = f"{'Profit' if pips > 0 else 'Loss'} ({pips:+.5f})"
            except (ValueError, TypeError):
                pass

        return f"""
<hr style="margin:20px 0;">
<div class="signal-closed">
<h3>Signal Closed</h3>
<table style="border-collapse:collapse;width:100%;max-width:500px;">
<tr><td style="padding:8px;font-weight:bold;">Closing Price</td><td style="padding:8px;">{exit_price}</td></tr>
<tr><td style="padding:8px;font-weight:bold;">Exit Reason</td><td style="padding:8px;">{exit_reason}</td></tr>
<tr><td style="padding:8px;font-weight:bold;">Outcome</td><td style="padding:8px;">{outcome}</td></tr>
</table>
</div>"""


_publisher: Optional[CmsPublisher] = None


def get_publisher() -> CmsPublisher:
    global _publisher
    if _publisher is None:
        _publisher = CmsPublisher()
    return _publisher


def reset_publisher():
    global _publisher
    _publisher = None
