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
- **Robust Scheduling:** APScheduler manages background tasks for strategy evaluations and data refreshes with timezone awareness and misfire recovery. A watchdog thread monitors and auto-restarts the scheduler.
- **Scalable Data Handling:** Local storage of OHLC candle data across multiple timeframes for rapid indicator calculations.
- **Production Hardening:**
  - **Webhook Notifications:** Configurable external alerting for critical events.
  - **Rate Limiting:** Implemented via `slowapi` middleware on FastAPI.
  - **CORS:** Strict origin whitelist auto-configured.
  - **Global Error Handler:** Centralized logging and structured JSON error responses.
  - **Security Headers:** Express uses `helmet` middleware.
  - **Health Endpoints:** Internal (`/health`) and public (`/api/v1/health/public`) endpoints for system status monitoring.
  - **WebSocket Signal Stream:** Real-time signal push via `ws://host/ws/signals`.
- **Public API v1:** Read-only API with cached responses (`cache_response(ttl)` decorator) for various data points including signals, strategies, market data, positions, and metrics.
- **Public Signals API (Hardened):** A separate, isolated router (`/api/v1/public`) with strict Pydantic schemas to prevent internal field leaks and enforce read-only access.
- **WordPress Integrations:** Frontend interface (`/wordpress`) for managing WordPress site credentials, with async validation and connection testing.
- **WordPress CMS Publisher:** A service (`CmsPublisher`) for publishing signals to WordPress via REST API, including retry logic and graceful failure handling.

## External Dependencies
- **OpenAI:** Used for AI-powered signal generation.
- **FCSAPI v4:** Provides OHLC data and real-time quotes for forex, crypto, commodities, and stock indices.
- **WordPress REST API:** Utilized for signal publishing and updates.
- **PostgreSQL:** Primary database for trading signals and user data.
- **SQLite:** Used for OHLC candle data, strategy-specific signals, API usage statistics, and internal engine data.
- **`holidays` Python package:** For detecting US and Japanese public holidays.
- **`tenacity` Python package:** For retry logic with exponential backoff in WordPress API calls.