import json
import logging
import os
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
    def __init__(self, wp_url: str = "", wp_username: str = "", wp_password: str = "", config_id: Optional[int] = None):
        self.wp_url = wp_url.rstrip("/")
        self.wp_username = wp_username
        self.wp_password = wp_password
        self.config_id = config_id
        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "DailyForex-SignalEngine/1.0",
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
        from trading_engine.database import get_signal_by_id, get_signal_cms_post, upsert_signal_cms_post

        if not self.is_configured:
            logger.error("[CMS] WordPress credentials not configured")
            return {"status": "error", "message": "WordPress credentials not configured"}

        signal = get_signal_by_id(signal_id)
        if not signal:
            return {"status": "error", "message": f"Signal #{signal_id} not found"}

        existing = get_signal_cms_post(signal_id, self.config_id)
        if existing and existing.get("wp_post_id"):
            logger.info(f"[CMS] Signal #{signal_id} already published to config={self.config_id} (wp_post_id={existing['wp_post_id']})")
            return {
                "status": "skipped",
                "message": "Already published to this site",
                "wp_post_id": existing["wp_post_id"],
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
            upsert_signal_cms_post(signal_id, self.config_id, {
                "wp_post_id": wp_post_id,
                "publish_status": "PUBLISHED",
                "last_sync": now_iso,
            })
            self._backfill_signal_wp_fields(signal_id, wp_post_id, now_iso)
            logger.info(f"[CMS] Published signal #{signal_id} → WP post #{wp_post_id} (config={self.config_id})")
            return {
                "status": "ok",
                "message": "Published successfully",
                "wp_post_id": wp_post_id,
            }
        except Exception as e:
            now_iso = datetime.now(timezone.utc).isoformat()
            upsert_signal_cms_post(signal_id, self.config_id, {
                "publish_status": "FAILED",
                "last_sync": now_iso,
            })
            error_detail = {
                "signal_id": signal_id,
                "config_id": self.config_id,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "timestamp": now_iso,
                "retries_exhausted": True,
            }
            logger.error(f"[CMS] Publish failed after retries: {json.dumps(error_detail)}")
            return {"status": "error", "message": str(e), "detail": error_detail}

    def update_closed_signal(self, signal_id: int) -> dict:
        from trading_engine.database import get_signal_by_id, get_signal_cms_post, upsert_signal_cms_post

        if not self.is_configured:
            return {"status": "error", "message": "WordPress credentials not configured"}

        signal = get_signal_by_id(signal_id)
        if not signal:
            return {"status": "error", "message": f"Signal #{signal_id} not found"}

        if signal.get("status") != "CLOSED":
            return {"status": "error", "message": f"Signal #{signal_id} is not CLOSED"}

        existing = get_signal_cms_post(signal_id, self.config_id)
        if not existing or not existing.get("wp_post_id"):
            return {"status": "error", "message": f"Signal #{signal_id} has no WP post for config={self.config_id}"}

        wp_post_id = existing["wp_post_id"]
        closing_html = self._format_closing_html(signal)
        original_html = self._fetch_existing_content(wp_post_id)
        updated_html = original_html + closing_html

        payload = {"content": updated_html}

        try:
            self._wp_post(self._api_url(f"posts/{wp_post_id}"), payload)
            now_iso = datetime.now(timezone.utc).isoformat()
            upsert_signal_cms_post(signal_id, self.config_id, {"last_sync": now_iso})
            logger.info(f"[CMS] Updated WP post #{wp_post_id} with close data for signal #{signal_id} (config={self.config_id})")
            return {"status": "ok", "message": "Post updated with closing data"}
        except Exception as e:
            error_detail = {
                "signal_id": signal_id,
                "wp_post_id": wp_post_id,
                "config_id": self.config_id,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            logger.error(f"[CMS] Update closed signal failed: {json.dumps(error_detail)}")
            return {"status": "error", "message": str(e), "detail": error_detail}

    def _backfill_signal_wp_fields(self, signal_id: int, wp_post_id: int, now_iso: str):
        from trading_engine.database import update_signal_wp_fields
        try:
            update_signal_wp_fields(signal_id, {
                "wp_post_id": wp_post_id,
                "publish_status": "PUBLISHED",
                "wp_last_sync": now_iso,
            })
        except Exception:
            pass

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


def _get_publishers() -> list[CmsPublisher]:
    from trading_engine.database import get_active_cms_configs_decrypted
    publishers = []

    configs = get_active_cms_configs_decrypted()
    for cfg in configs:
        pub = CmsPublisher(
            wp_url=cfg["site_url"],
            wp_username=cfg["wp_username"],
            wp_password=cfg["app_password"],
            config_id=cfg["id"],
        )
        publishers.append(pub)

    if not publishers:
        env_url = (os.environ.get("WP_URL") or "").rstrip("/")
        env_user = os.environ.get("WP_USERNAME") or ""
        env_pass = os.environ.get("WP_APP_PASSWORD") or ""
        if env_url and env_user and env_pass:
            publishers.append(CmsPublisher(
                wp_url=env_url,
                wp_username=env_user,
                wp_password=env_pass,
                config_id=None,
            ))

    return publishers


def publish_signal_to_all(signal_id: int) -> list[dict]:
    publishers = _get_publishers()
    if not publishers:
        logger.info(f"[CMS] No WordPress configs active — skipping publish for signal #{signal_id}")
        return []

    results = []
    for pub in publishers:
        result = pub.publish_signal(signal_id)
        result["config_id"] = pub.config_id
        results.append(result)
        logger.info(f"[CMS] publish_signal({signal_id}) config={pub.config_id} → {result.get('status')}")
    return results


def update_closed_signal_on_all(signal_id: int) -> list[dict]:
    from trading_engine.database import get_signal_cms_posts_for_signal
    posts = get_signal_cms_posts_for_signal(signal_id)

    if not posts:
        logger.info(f"[CMS] No CMS posts found for signal #{signal_id} — skipping update")
        return []

    results = []
    for post in posts:
        if not post.get("wp_post_id"):
            continue
        config_id = post["cms_config_id"]
        publishers = _get_publishers()
        pub = None
        for p in publishers:
            if p.config_id == config_id:
                pub = p
                break

        if not pub:
            if config_id is not None:
                from trading_engine.database import get_user_cms_config_decrypted
                cfg = get_user_cms_config_decrypted(config_id)
                if cfg:
                    pub = CmsPublisher(
                        wp_url=cfg["site_url"],
                        wp_username=cfg["wp_username"],
                        wp_password=cfg["app_password"],
                        config_id=cfg["id"],
                    )
            else:
                env_url = (os.environ.get("WP_URL") or "").rstrip("/")
                env_user = os.environ.get("WP_USERNAME") or ""
                env_pass = os.environ.get("WP_APP_PASSWORD") or ""
                if env_url and env_user and env_pass:
                    pub = CmsPublisher(wp_url=env_url, wp_username=env_user, wp_password=env_pass, config_id=None)

        if pub:
            result = pub.update_closed_signal(signal_id)
            result["config_id"] = pub.config_id
            results.append(result)
            logger.info(f"[CMS] update_closed_signal({signal_id}) config={pub.config_id} → {result.get('status')}")
    return results


def get_publisher(config_id: Optional[int] = None) -> CmsPublisher:
    if config_id is not None:
        from trading_engine.database import get_user_cms_config_decrypted
        cfg = get_user_cms_config_decrypted(config_id)
        if cfg:
            return CmsPublisher(
                wp_url=cfg["site_url"],
                wp_username=cfg["wp_username"],
                wp_password=cfg["app_password"],
                config_id=cfg["id"],
            )

    env_url = (os.environ.get("WP_URL") or "").rstrip("/")
    env_user = os.environ.get("WP_USERNAME") or ""
    env_pass = os.environ.get("WP_APP_PASSWORD") or ""
    return CmsPublisher(wp_url=env_url, wp_username=env_user, wp_password=env_pass)


def reset_publisher():
    pass
