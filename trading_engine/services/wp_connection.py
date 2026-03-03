import logging
from typing import Optional

import httpx

logger = logging.getLogger("trading_engine.wp_connection")


_WP_HEADERS = {
    "User-Agent": "DailyForex-SignalEngine/1.0",
    "Accept": "application/json",
}


def verify_wp_connection(
    url: str,
    username: str,
    password: str,
    timeout: float = 15.0,
) -> tuple[bool, str, Optional[str]]:
    url = url.rstrip("/")
    try:
        with httpx.Client(timeout=timeout, headers=_WP_HEADERS) as client:
            resp = client.get(
                f"{url}/wp-json/wp/v2/users/me",
                auth=(username, password),
            )
            if resp.status_code == 200:
                user_data = resp.json()
                site_name = ""
                try:
                    site_resp = client.get(f"{url}/wp-json", timeout=10)
                    if site_resp.status_code == 200:
                        site_name = site_resp.json().get("name", "")
                except Exception:
                    pass
                return True, f"Authenticated as {user_data.get('name', username)}", site_name
            elif resp.status_code == 401:
                return False, "Invalid credentials (HTTP 401)", None
            elif resp.status_code == 403:
                return False, "Access forbidden — check Application Password permissions (HTTP 403)", None
            elif resp.status_code == 404:
                return False, "REST API not found at this URL — is the WordPress REST API enabled? (HTTP 404)", None
            else:
                return False, f"Unexpected response (HTTP {resp.status_code}): {resp.text[:200]}", None
    except httpx.ConnectError as e:
        return False, f"Connection failed: {e}", None
    except httpx.TimeoutException:
        return False, "Connection timed out", None
    except Exception as e:
        return False, f"Error: {e}", None
