import json as _json
import os
import time
import logging
from collections import defaultdict, deque
from datetime import datetime, timezone
from threading import Lock
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger("trading_engine.security")

_BURST_WINDOW = 2
_BURST_LIMIT = 20
_BURST_COOLDOWN = 300

_MINUTE_WINDOW = 60
_MINUTE_LIMIT = 60

_HOUR_WINDOW = 3600
_HOUR_LIMIT = 1000

_ENUM_WINDOW = 60
_ENUM_LIMIT = 5
_ENUM_BLOCK_DURATION = 86400

_EXEMPT_PATHS = frozenset({"/health", "/ws/signals"})
_EXEMPT_PREFIXES = ("/admin/",)

REQUIRE_API_KEY = os.environ.get("REQUIRE_API_KEY", "").lower() in ("true", "1", "yes")

_API_KEY_PATHS = ("/api/v1/",)


class _LeakyBucket:
    __slots__ = ("capacity", "leak_rate", "tokens", "last_leak")

    def __init__(self, capacity: int, window: float):
        self.capacity = capacity
        self.leak_rate = capacity / window
        self.tokens = 0.0
        self.last_leak = time.monotonic()

    def allow(self) -> bool:
        now = time.monotonic()
        elapsed = now - self.last_leak
        self.tokens = max(0.0, self.tokens - elapsed * self.leak_rate)
        self.last_leak = now
        if self.tokens + 1.0 > self.capacity:
            return False
        self.tokens += 1.0
        return True


_PARTNER_TIERS = {
    "standard": {"burst": 40, "minute": 120, "hour": 5000},
    "premium": {"burst": 100, "minute": 300, "hour": 20000},
    "unlimited": {"burst": 10000, "minute": 100000, "hour": 1000000},
}


class _IPState:
    __slots__ = (
        "burst_bucket", "minute_bucket", "hour_bucket",
        "not_found_timestamps", "blocked_until", "cooldown_until",
    )

    def __init__(self, burst_limit=None, minute_limit=None, hour_limit=None):
        self.burst_bucket = _LeakyBucket(burst_limit or _BURST_LIMIT, _BURST_WINDOW)
        self.minute_bucket = _LeakyBucket(minute_limit or _MINUTE_LIMIT, _MINUTE_WINDOW)
        self.hour_bucket = _LeakyBucket(hour_limit or _HOUR_LIMIT, _HOUR_WINDOW)
        self.not_found_timestamps: list[float] = []
        self.blocked_until: float = 0.0
        self.cooldown_until: float = 0.0


_states: dict[str, _IPState] = defaultdict(_IPState)
_apikey_states: dict[int, _IPState] = {}
_lock = Lock()

_EVENT_WINDOW = 86400
_blocked_events: deque = deque(maxlen=10000)


def _mask_ip(ip: str) -> str:
    parts = ip.split(".")
    if len(parts) == 4:
        return f"{parts[0]}.{parts[1]}.xxx.xxx"
    if ":" in ip:
        segments = ip.split(":")
        return ":".join(segments[:3]) + "::xxx"
    return "xxx.xxx.xxx.xxx"


def _log_security_event(ip: str, reason: str, detail: str = ""):
    now_ts = time.time()
    now_iso = datetime.now(timezone.utc).isoformat()
    masked = _mask_ip(ip)
    event = {
        "timestamp": now_iso,
        "mono": now_ts,
        "ip_masked": masked,
        "reason": reason,
        "detail": detail,
    }
    _blocked_events.append(event)
    logger.warning(_json.dumps({
        "event": "SECURITY_BLOCK",
        "timestamp": now_iso,
        "ip": masked,
        "reason": reason,
        "detail": detail,
    }))


def get_public_security_status() -> dict:
    cutoff = time.time() - _EVENT_WINDOW
    now = time.monotonic()
    with _lock:
        recent_count = sum(1 for e in _blocked_events if e["mono"] > cutoff)
        active_bans = sum(1 for s in _states.values() if s.blocked_until > now)
    return {
        "total_blocked_requests_24h": recent_count,
        "current_active_ip_bans": active_bans,
    }


_CLEANUP_INTERVAL = 600
_last_cleanup: float = time.monotonic()


def _cleanup_states():
    global _last_cleanup
    now = time.monotonic()
    if now - _last_cleanup < _CLEANUP_INTERVAL:
        return
    _last_cleanup = now
    stale = [
        ip for ip, s in _states.items()
        if s.blocked_until < now and s.cooldown_until < now
        and s.burst_bucket.tokens < 0.5
        and s.minute_bucket.tokens < 0.5
        and s.hour_bucket.tokens < 0.5
    ]
    for ip in stale:
        del _states[ip]
    if stale:
        logger.debug(f"[SECURITY] Cleaned up {len(stale)} stale IP states")


def _get_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


def _get_apikey_state(key_id: int, tier: str, rate_limit: int) -> _IPState:
    if key_id not in _apikey_states:
        tier_cfg = _PARTNER_TIERS.get(tier, _PARTNER_TIERS["standard"])
        _apikey_states[key_id] = _IPState(
            burst_limit=tier_cfg["burst"],
            minute_limit=rate_limit if rate_limit else tier_cfg["minute"],
            hour_limit=tier_cfg["hour"],
        )
    return _apikey_states[key_id]


class SecurityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path in _EXEMPT_PATHS or any(path.startswith(p) for p in _EXEMPT_PREFIXES):
            return await call_next(request)

        api_key_header = request.headers.get("x-api-key", "").strip()
        is_api_path = any(path.startswith(p) for p in _API_KEY_PATHS)

        ip = _get_client_ip(request)

        if REQUIRE_API_KEY and is_api_path and not api_key_header:
            if path in ("/api/v1/health/public", "/api/v1/auth/login", "/api/v1/auth/register"):
                pass
            else:
                _log_security_event(ip, "MISSING_KEY", f"path={path}")
                return JSONResponse(
                    status_code=401,
                    content={"error": "API key required", "detail": "Provide a valid X-API-KEY header"},
                )

        partner_info = None
        if api_key_header and is_api_path:
            from trading_engine.database import validate_partner_api_key
            partner_info = validate_partner_api_key(api_key_header)
            if not partner_info:
                _log_security_event(ip, "INVALID_KEY", f"path={path}")
                return JSONResponse(
                    status_code=401,
                    content={"error": "Invalid API key", "detail": "The provided X-API-KEY is not valid or has been revoked"},
                )

        now = time.monotonic()

        with _lock:
            _cleanup_states()

            if partner_info:
                state = _get_apikey_state(
                    partner_info["id"],
                    partner_info["tier"],
                    partner_info["rate_limit_per_minute"],
                )
                request.state.partner = partner_info
            else:
                state = _states[ip]

            if state.blocked_until > now:
                remaining = int(state.blocked_until - now)
                _log_security_event(ip, "SCANNING", f"blocked {remaining}s remaining")
                return JSONResponse(
                    status_code=403,
                    content={"error": "Temporarily blocked", "reason": "Suspicious activity detected"},
                    headers={"Retry-After": str(remaining)},
                )

            if state.cooldown_until > now:
                remaining = int(state.cooldown_until - now)
                _log_security_event(ip, "BURST", f"cooldown {remaining}s remaining")
                return JSONResponse(
                    status_code=429,
                    content={"error": "Too Many Requests", "reason": "Burst rate limit exceeded", "retry_after": remaining},
                    headers={"Retry-After": str(remaining)},
                )

            if not state.burst_bucket.allow():
                state.cooldown_until = now + _BURST_COOLDOWN
                _log_security_event(ip, "BURST", f"triggered cooldown {_BURST_COOLDOWN}s")
                return JSONResponse(
                    status_code=429,
                    content={"error": "Too Many Requests", "reason": "Burst rate limit exceeded", "retry_after": _BURST_COOLDOWN},
                    headers={"Retry-After": str(_BURST_COOLDOWN)},
                )

            if not state.minute_bucket.allow():
                _log_security_event(ip, "RATE_LIMIT", "per-minute limit exceeded")
                return JSONResponse(
                    status_code=429,
                    content={"error": "Too Many Requests", "reason": "Rate limit exceeded (per-minute)"},
                    headers={"Retry-After": "60"},
                )

            if not state.hour_bucket.allow():
                _log_security_event(ip, "RATE_LIMIT", "per-hour limit exceeded")
                return JSONResponse(
                    status_code=429,
                    content={"error": "Too Many Requests", "reason": "Rate limit exceeded (per-hour)"},
                    headers={"Retry-After": "3600"},
                )

        response = await call_next(request)

        if response.status_code == 404 and not partner_info:
            with _lock:
                state = _states[ip]
                cutoff = time.monotonic() - _ENUM_WINDOW
                state.not_found_timestamps = [
                    t for t in state.not_found_timestamps if t > cutoff
                ]
                state.not_found_timestamps.append(time.monotonic())
                if len(state.not_found_timestamps) > _ENUM_LIMIT:
                    state.blocked_until = time.monotonic() + _ENUM_BLOCK_DURATION
                    state.not_found_timestamps.clear()
                    _log_security_event(ip, "SCANNING", f"blocked {_ENUM_BLOCK_DURATION}s — {_ENUM_LIMIT}+ 404s in {_ENUM_WINDOW}s")

        return response


def get_security_stats() -> dict:
    now = time.monotonic()
    with _lock:
        total_tracked = len(_states)
        blocked_ips = [
            ip for ip, s in _states.items()
            if s.blocked_until > now
        ]
        cooled_ips = [
            ip for ip, s in _states.items()
            if s.cooldown_until > now and s.blocked_until <= now
        ]
        active_api_keys = len(_apikey_states)
    return {
        "tracked_ips": total_tracked,
        "blocked_ips": len(blocked_ips),
        "blocked_ip_list": blocked_ips[:20],
        "cooled_down_ips": len(cooled_ips),
        "active_api_key_sessions": active_api_keys,
        "require_api_key": REQUIRE_API_KEY,
        "limits": {
            "burst": f"{_BURST_LIMIT} requests / {_BURST_WINDOW}s (cooldown: {_BURST_COOLDOWN}s)",
            "per_minute": f"{_MINUTE_LIMIT} requests / minute",
            "per_hour": f"{_HOUR_LIMIT} requests / hour",
            "enum_guard": f"{_ENUM_LIMIT}+ 404s in {_ENUM_WINDOW}s → {_ENUM_BLOCK_DURATION}s block",
        },
    }


def unblock_ip(ip: str) -> bool:
    with _lock:
        if ip in _states:
            _states[ip].blocked_until = 0.0
            _states[ip].cooldown_until = 0.0
            _states[ip].not_found_timestamps.clear()
            logger.info(f"[SECURITY] Manually unblocked IP {ip}")
            return True
        return False


_SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Content-Security-Policy": "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src 'self' ws: wss:",
    "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
    "X-XSS-Protection": "1; mode=block",
}


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        for header, value in _SECURITY_HEADERS.items():
            response.headers[header] = value
        return response


MAX_REQUEST_BODY_BYTES = 1 * 1024 * 1024

_PAYLOAD_LIMIT_PATHS = ("/api/v1/",)
_PAYLOAD_EXEMPT_PATHS = frozenset({"/api/v1/health/public"})


class PayloadLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if request.method in ("POST", "PUT", "PATCH") and any(path.startswith(p) for p in _PAYLOAD_LIMIT_PATHS) and path not in _PAYLOAD_EXEMPT_PATHS:
            content_length = request.headers.get("content-length")
            if content_length and int(content_length) > MAX_REQUEST_BODY_BYTES:
                logger.warning(f"[SECURITY] Rejected oversized payload from {_get_client_ip(request)}: {content_length} bytes on {path}")
                return JSONResponse(
                    status_code=413,
                    content={"error": "Payload too large", "detail": f"Maximum request body size is {MAX_REQUEST_BODY_BYTES // 1024}KB"},
                )
        return await call_next(request)
