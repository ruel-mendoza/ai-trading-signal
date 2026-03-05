# AI Signals - AI-Powered Trading Signals Platform

## Overview
AI Signals is an AI-powered trading signals platform that generates forex, crypto, and commodity trading signals. It integrates a Python-based Trading Signal Engine with FastAPI for data management, technical analysis, and automated strategy execution. The platform aims to provide users with timely and accurate trading insights across various asset classes, leveraging advanced AI and quantitative strategies.

## User Preferences
I want iterative development. I prefer detailed explanations and for the agent to ask before making major changes.

## System Architecture
The platform features a React, TypeScript, Vite, TailwindCSS, and Shadcn UI frontend, connected to an Express.js (TypeScript) backend acting as an API gateway. The core is a Python FastAPI Trading Signal Engine that handles OHLC data, caching, technical indicators (EMA, SMA, ATR, RSI), and orchestrates six automated trading strategies: MTF EMA, Trend Following (Forex & Non-Forex), S&P 500 Momentum, and Highest/Lowest Close FX. AI signal generation is powered by OpenAI through Replit AI Integrations.

Key architectural decisions include:
- **Modular Design:** Clear separation between frontend, Node.js backend, and Python trading engine.
- **Data Caching:** Optimized caching in the Python engine for OHLC data.
- **Idempotent Strategy Execution:** Strategies are designed for consistent signal and position management, including ATR State Lock.
- **Admin Interface:** A comprehensive dashboard for monitoring signals, managing API keys, tracking credit usage, and configuring user settings, including detailed "Signal Analysis," "Scheduler Health," "System Status," "WordPress" integration, and individual strategy dashboards. Role-based access control (ADMIN/CUSTOMER) enforces endpoint-level authorization: Scheduler Health, System Status, and User Management endpoints require ADMIN role; CMS configs are scoped to the logged-in user for CUSTOMER role.
- **Robust Scheduling:** APScheduler manages background tasks for strategy evaluations and data refreshes with timezone awareness and misfire recovery. A watchdog thread monitors and auto-restarts the scheduler. EUR/USD Proximity Watchdog (`trading_engine/engine/watchdog.py`) runs every 60s, fetching the current price via FCSAPI advance endpoint and checking if within 3 pips of the 50-Day Low target (1.15845). Alerts are logged to `recovery_notifications` table as `PROXIMITY_ALERT` with 30-minute suppression.
- **Scalable Data Handling:** Local storage of OHLC candle data across multiple timeframes for rapid indicator calculations.
- **Production Hardening:**
  - **Webhook Notifications:** Configurable external alerting for critical events.
  - **Security Middleware (`security_middleware.py`):** Multi-layer leaky bucket rate limiting with: burst protection (20 req/2s → 5 min cooldown), standard limits (60/min, 1000/hr per IP), endpoint enumeration guard (5+ 404s in 60s → 24h IP block). Admin paths (`/admin/`) and health endpoints are exempt. Admin API provides `/admin/api/security/stats` and `/admin/api/security/unblock` for monitoring and manual IP management.
  - **Partner API Key System:** SHA-256 hashed keys in `partner_api_keys` table with tiered rate limits (standard: 120/min, premium: 300/min, unlimited). Keys are validated via `X-API-KEY` header and bypass IP-based rate limiting. `REQUIRE_API_KEY=true` env flag rejects all `/api/v1/` requests without a valid key (auth/health endpoints exempt). Admin dashboard tab for key management (create/revoke/delete/activate).
  - **Rate Limiting:** SlowAPI middleware as a secondary layer enforcing 60/min default and 1000/hr application limits.
  - **CORS:** Locked down to `https://*.dailyforex.com`, Replit deployment URLs, and localhost (dev). Wildcard `*` is explicitly blocked. `X-API-KEY` header is whitelisted.
  - **Global Error Handler:** Centralized logging and structured JSON error responses.
  - **Security Headers:** Express uses `helmet` middleware. FastAPI `SecurityHeadersMiddleware` injects X-Content-Type-Options (nosniff), X-Frame-Options (DENY), Content-Security-Policy (default-src 'self'), Strict-Transport-Security (HSTS 1yr), Referrer-Policy, Permissions-Policy, and X-XSS-Protection on every response.
  - **Payload Limits:** `PayloadLimitMiddleware` enforces 1MB max request body on public API (`/api/v1/`) POST/PUT/PATCH to prevent memory exhaustion from oversized payloads.
  - **Health Endpoints:** Internal (`/health`) and public (`/api/v1/health/public`) endpoints for system status monitoring. Public health includes `security_status` with `total_blocked_requests_24h` and `current_active_ip_bans` telemetry. All security blocks emit structured JSON logs with masked IPs, reason codes (BURST, SCANNING, INVALID_KEY, RATE_LIMIT, MISSING_KEY), and timestamps.
  - **WebSocket Signal Stream:** Real-time signal push via `ws://host/ws/signals`.
- **Public API v1:** Read-only API with cached responses (`cache_response(ttl)` decorator) for various data points including signals, strategies, market data, positions, and metrics.
- **Public Signals API (Hardened):** A separate, isolated router (`/api/v1/public`) with strict Pydantic schemas to prevent internal field leaks and enforce read-only access.
- **User Registration:** Public registration endpoint at `POST /api/v1/auth/register` with server-side form validation (username uniqueness, email uniqueness, password confirmation). New users are assigned the CUSTOMER role. The login page links to registration, and successful registration redirects to login with a success message.
- **WordPress CMS Publisher:** Multi-tenant CMS publishing via `CmsPublisher` class. Per-user WordPress credentials stored in `UserCmsConfig` (Fernet-encrypted). On signal creation, `publish_signal_to_all()` iterates all active configs and creates a `SignalCmsPost` record per config (tracking per-site `wp_post_id`). On signal close, `update_closed_signal_on_all()` updates each site's post. Env vars (`WP_URL`, `WP_USERNAME`, `WP_APP_PASSWORD`) serve as fallback when no DB configs exist. Retry logic via `tenacity` (3 attempts, exponential backoff 2s→30s). Admin endpoints support manual retry-publish and update-wp operations.

## External Dependencies
- **OpenAI:** Used for AI-powered signal generation.
- **FCSAPI v4:** Provides OHLC data and real-time quotes for forex, crypto, commodities, and stock indices. Crypto symbols (except BTC) require USDT pairing for history endpoint (e.g., ETH/USD → ETHUSDT, XRP/USD → XRPUSDT).
- **WordPress REST API:** Utilized for signal publishing and updates.
- **PostgreSQL:** Primary database for trading signals and user data.
- **SQLite:** Used for OHLC candle data, strategy-specific signals, API usage statistics, and internal engine data.
- **`holidays` Python package:** For detecting US and Japanese public holidays.
- **`tenacity` Python package:** For retry logic with exponential backoff in WordPress API calls.