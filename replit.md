# AI Signals - AI-Powered Trading Signals Platform

## Overview
AI Signals is an AI-powered trading signals platform designed to generate forex, crypto, and commodity trading signals. It integrates a Python-based Trading Signal Engine with FastAPI for robust data management, technical analysis, and automated strategy execution. The platform aims to provide users with timely and accurate trading insights across various asset classes, leveraging advanced AI and quantitative strategies.

## User Preferences
I want iterative development. I prefer detailed explanations and for the agent to ask before making major changes.

## System Architecture
The platform utilizes a modern stack for both frontend and backend development. The frontend is built with React, TypeScript, Vite, TailwindCSS, and Shadcn UI, focusing on a responsive and intuitive user experience. The backend is an Express.js server in TypeScript, acting as the main API gateway and proxying requests to the Python FastAPI Trading Signal Engine.

The Trading Signal Engine handles OHLC data management, caching, technical indicator calculations (EMA, SMA, ATR, RSI), and orchestrates six automated trading strategies: MTF EMA (Multi-Timeframe EMA Trend-Pullback), Trend Following (Forex & Non-Forex), S&P 500 Momentum, and Highest/Lowest Close FX. The MTF EMA strategy (`strategies/multi_timeframe.py`) follows the BaseStrategy interface and covers indices (SPX, NDX, RUT), commodities (XAU/USD, XAG/USD, OSX), crypto (BTC/USD, ETH/USD), and forex (EUR/USD, USD/JPY, GBP/USD, AUD/USD) using D1+H4+H1 timeframe sync with EMA 20/50/200 and ATR 100 indicators. Data persistence is managed with PostgreSQL using Drizzle ORM for signals and SQLite for OHLC candle data, strategy signals, and API usage tracking. AI signal generation is powered by OpenAI through Replit AI Integrations.

Key architectural decisions include:
- **Modular Design:** Separation of concerns between frontend, Node.js backend, and Python trading engine.
- **Data Caching:** Smart caching mechanisms in the Python engine to optimize external API calls for OHLC data.
- **Idempotent Strategy Execution:** Strategies are designed to prevent duplicate signals and manage positions effectively, including ATR State Lock for consistent trailing stop calculations.
- **Admin Interface:** A dedicated admin dashboard for monitoring signals, managing API keys, tracking credit usage, and configuring user settings. Includes "Signal Analysis" tab (real-time evaluation of all strategy entry conditions across every asset, with condition badges, percentage-from-breakout indicators, SMA bias, MTF alignment arrows, position status, and per-strategy rule summaries), "Scheduler Health" tab (job logs, 24h stats, watchdog heartbeat, running status), "System Status" tab (all production hardening features: rate limiting, security headers, error handler, database, watchdog, API key, kill switch, webhooks, auto-restart, misfire recovery with live status badges), and individual strategy dashboards for MTF EMA, Trend Following, SPX 500 Momentum, Forex Trend, and Highest/Lowest Close FX (each with symbol data, indicator values, active trades, signal history, and strategy rules).
- **Robust Scheduling:** APScheduler handles background tasks for strategy evaluations and data refreshes, with timezone awareness (America/New_York). All 5 scheduled jobs have `misfire_grace_time=120s` for recovery if a job misses its window. A background watchdog thread monitors scheduler health every 300s and auto-restarts if the scheduler stops. Each job execution is logged to the `scheduler_job_log` SQLite table with status (RUNNING/SUCCESS/PARTIAL/FAILED), timing, asset counts, and error details. Per-asset retry logic (2 attempts with 5s delay) prevents transient failures from failing an entire strategy run.
- **Scheduler Jobs:** MTF EMA (hourly :00), SP500 Momentum (every 30m :00/:30 filtered to ARCA 09:30-15:30), Highest/Lowest FX (09:00 & 10:00 filtered for holidays), Trend Non-Forex (16:00), Trend Forex (17:00), Signal Metrics Worker (every 5m). All times America/New_York with automatic DST handling.
- **Scalable Data Handling:** OHLC candle data is stored locally for various timeframes (30m, 1H, 4H, Daily) to support rapid indicator calculations.
- **Production Hardening:**
  - **Webhook Notifications** (`trading_engine/notifications.py`): Configurable external alerting via Discord, Slack, or generic webhooks. Auto-detects webhook type from URL. Sends alerts for: kill switch activation, credit warnings, strategy failures, scheduler down, new signals. Admin API endpoints for webhook config (`/admin/api/webhook`), test (`/admin/api/webhook/test`). Full admin dashboard "Notifications" tab with master on/off toggle, webhook URL configuration (save/clear/test), and per-category toggles (new_signals, strategy_failures, credit_warnings, scheduler_alerts). All preferences persisted in `app_settings` SQLite table and loaded on startup.
  - **Rate Limiting**: slowapi middleware on FastAPI — 120 requests/minute per IP (default), 100 requests/minute application-wide limit, fixed-window strategy.
  - **CORS**: Strict origin whitelist auto-configured from Replit environment variables (`REPL_SLUG`, `REPL_OWNER`). Override via `CORS_ALLOWED_ORIGINS` env var (comma-separated). Only allows `GET`, `POST`, `OPTIONS` methods. Exposes rate-limit headers (`X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`).
  - **Global Error Handler**: Catch-all exception handler logs unhandled errors with full traceback and returns structured JSON error responses.
  - **Security Headers**: Express uses `helmet` middleware (HSTS, X-Content-Type-Options, X-Frame-Options, etc.) with CSP disabled for SPA compatibility.
  - **Health Endpoint**: `GET /health` returns scheduler status, database connectivity, 24h job success/failure counts, watchdog heartbeat, and API key status. Returns `"healthy"` or `"degraded"` with specific failed checks.
  - **Public Health**: `GET /api/v1/health/public` returns only `status: "UP"/"DOWN"` and `version` — no internal metadata exposed. Safe for external monitoring.

## Public API v1
Read-only API for the DailyForex frontend (`trading_engine/api_v1.py`), mounted at `/api/v1` on the Python engine (proxied through Express at `/api/v1`).

- **Architecture**: Read-only, local DB only (no external API calls). `cache_response(ttl)` decorator with `CachePool` — 4-shard thread-safe `cachetools` TTLCache (60s default TTL, 256 max entries per shard). Human-readable cache keys (`signals_latest:asset=BTC/USD:strategy=mtf`). Every response includes `cache` field (`hit`/`miss`) and `response_time_ms` on misses. `POST /v1/cache/flush` to clear all shards. Health endpoint reports shard count, hit/miss/set counts, and hit rate.
- **Endpoints**:
  - `GET /api/v1/signals` — All signals with filters: `strategy`, `asset`, `status`, `category`, `limit`
  - `GET /api/v1/signals/latest` — Active signals (hot path), filters: `asset`, `strategy`, `asset_class`
  - `GET /api/v1/signals/history` — Paginated history, filters: `asset`, `strategy`, `status`, `asset_class`, `page`, `size`
  - `GET /api/v1/signals/active` — Open signals only, filters: `strategy`, `asset`, `category`
  - `GET /api/v1/signals/{id}` — Single signal by ID
  - `GET /api/v1/strategies` — Strategy summary with open/closed counts
  - `GET /api/v1/market/candles` — OHLC candle data, params: `asset`, `timeframe`, `limit`
  - `GET /api/v1/market/indicators` — Technical indicators (SMA, EMA, RSI, ATR at multiple periods), params: `asset`, `timeframe`
  - `GET /api/v1/positions` — Open positions with trailing stop data, filters: `strategy`, `asset`
  - `GET /api/v1/metrics` — Signal performance metrics, filters: `strategy`, `asset`, `period` (all_time/7d/30d), `summary_only` (bool). Returns both per-asset and aggregate rows by default
  - `GET /api/v1/metrics/summary` — Overall platform win rate, total signals, per-strategy summary
  - `GET /api/v1/scheduler/status` — 24h success/failure counts and last job info
  - `GET /api/v1/scheduler/jobs` — Recent job execution logs, param: `limit`
  - `GET /api/v1/health` — API health + cache stats (size, hit rate, TTL)
  - `GET /api/v1/health/public` — Liveness check, returns only `status: "UP"/"DOWN"` and `version`
  - `POST /api/v1/cache/flush` — Clear all cache shards
- **Response format**: `/v1/signals/latest` returns `{count, data: [{asset, direction (LONG/SHORT), entry, stop_loss, strategy, published_at, meta: {atr_entry, highest_close, lowest_close}}]}`. Internal endpoints (`/signals/history`, `/signals/active`, `/signals`) use legacy format with `signals` array, `entry_price`, `BUY/SELL` direction, `opened_at`

## Public Signals API (Hardened)
Separate isolated router at `/api/v1/public` (`trading_engine/api/v1/public_signals.py`) with strict Pydantic schemas (`SignalRead`, `AssetRead`) that prevent internal field leaks (no `atr_at_entry`, `raw_json`, `fcsapi_id`). POST/PUT/DELETE/PATCH are blocked at the router level (405). Uses dict-safe `_g()` accessor since database functions return dicts.
- **Endpoints**:
  - `GET /api/v1/public/signals` — Filtered signals with `SignalRead` schema (LONG/SHORT direction, `trailing_stop` flag)
  - `GET /api/v1/public/signals/active` — Open signals only
  - `GET /api/v1/public/signals/{id}` — Single signal by ID
  - `GET /api/v1/public/assets` — Asset list with active signal counts and strategy coverage

## External Dependencies
- **OpenAI:** Used for AI-powered signal generation.
- **FCSAPI v4:** Provides OHLC (Open-High-Low-Close) data and real-time quotes for forex, crypto, commodities (with `type=commodity`), and stock indices (with `type=index`). Commodity symbols (XAU/USD, XAG/USD, XPT/USD, XPD/USD, XCU/USD, NATGAS/USD, CORN/USD, SOYBEAN/USD, WHEAT/USD, SUGAR/USD) use the forex endpoint with `type=commodity`. Stock indices (SPX, NDX, DJI, RUT) use the stock endpoint with `type=index` and plain symbols (no exchange prefix). WTI/USD and BRENT/USD are not available on FCSAPI and are marked unsupported.
- **PostgreSQL:** Primary database for storing trading signals and user-related data.
- **SQLite:** Used for storing OHLC candle data, strategy-specific signals, API usage statistics, and internal engine data.
- **`holidays` Python package:** Utilized for detecting US and Japanese public holidays to inform strategy execution.