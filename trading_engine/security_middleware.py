import time
import logging
from collections import defaultdict
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


class _IPState:
    __slots__ = (
        "burst_bucket", "minute_bucket", "hour_bucket",
        "not_found_timestamps", "blocked_until", "cooldown_until",
    )

    def __init__(self):
        self.burst_bucket = _LeakyBucket(_BURST_LIMIT, _BURST_WINDOW)
        self.minute_bucket = _LeakyBucket(_MINUTE_LIMIT, _MINUTE_WINDOW)
        self.hour_bucket = _LeakyBucket(_HOUR_LIMIT, _HOUR_WINDOW)
        self.not_found_timestamps: list[float] = []
        self.blocked_until: float = 0.0
        self.cooldown_until: float = 0.0


_states: dict[str, _IPState] = defaultdict(_IPState)
_lock = Lock()

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


class SecurityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path in _EXEMPT_PATHS or any(path.startswith(p) for p in _EXEMPT_PREFIXES):
            return await call_next(request)

        ip = _get_client_ip(request)
        now = time.monotonic()

        with _lock:
            _cleanup_states()
            state = _states[ip]

            if state.blocked_until > now:
                remaining = int(state.blocked_until - now)
                logger.warning(f"[SECURITY] Blocked IP {ip} — {remaining}s remaining (endpoint enumeration)")
                return JSONResponse(
                    status_code=403,
                    content={"error": "Temporarily blocked", "reason": "Suspicious activity detected"},
                    headers={"Retry-After": str(remaining)},
                )

            if state.cooldown_until > now:
                remaining = int(state.cooldown_until - now)
                return JSONResponse(
                    status_code=429,
                    content={"error": "Too Many Requests", "reason": "Burst rate limit exceeded", "retry_after": remaining},
                    headers={"Retry-After": str(remaining)},
                )

            if not state.burst_bucket.allow():
                state.cooldown_until = now + _BURST_COOLDOWN
                logger.warning(f"[SECURITY] Burst detected from {ip} — cooldown {_BURST_COOLDOWN}s")
                return JSONResponse(
                    status_code=429,
                    content={"error": "Too Many Requests", "reason": "Burst rate limit exceeded", "retry_after": _BURST_COOLDOWN},
                    headers={"Retry-After": str(_BURST_COOLDOWN)},
                )

            if not state.minute_bucket.allow():
                logger.info(f"[SECURITY] Minute limit hit for {ip}")
                return JSONResponse(
                    status_code=429,
                    content={"error": "Too Many Requests", "reason": "Rate limit exceeded (per-minute)"},
                    headers={"Retry-After": "60"},
                )

            if not state.hour_bucket.allow():
                logger.info(f"[SECURITY] Hour limit hit for {ip}")
                return JSONResponse(
                    status_code=429,
                    content={"error": "Too Many Requests", "reason": "Rate limit exceeded (per-hour)"},
                    headers={"Retry-After": "3600"},
                )

        response = await call_next(request)

        if response.status_code == 404:
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
                    logger.warning(
                        f"[SECURITY] IP {ip} blocked for {_ENUM_BLOCK_DURATION}s — "
                        f"endpoint enumeration detected ({_ENUM_LIMIT}+ 404s in {_ENUM_WINDOW}s)"
                    )

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
    return {
        "tracked_ips": total_tracked,
        "blocked_ips": len(blocked_ips),
        "blocked_ip_list": blocked_ips[:20],
        "cooled_down_ips": len(cooled_ips),
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
