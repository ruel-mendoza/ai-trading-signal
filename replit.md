# AI Signals - AI-Powered Trading Signals Platform

## Overview
AI Signals is an AI-powered trading signals platform designed to generate forex, crypto, and commodity trading signals. It integrates a Python-based Trading Signal Engine with FastAPI for robust data management, technical analysis, and automated strategy execution. The platform aims to provide users with timely and accurate trading insights across various asset classes, leveraging advanced AI and quantitative strategies.

## User Preferences
I want iterative development. I prefer detailed explanations and for the agent to ask before making major changes.

## System Architecture
The platform utilizes a modern stack for both frontend and backend development. The frontend is built with React, TypeScript, Vite, TailwindCSS, and Shadcn UI, focusing on a responsive and intuitive user experience. The backend is an Express.js server in TypeScript, acting as the main API gateway and proxying requests to the Python FastAPI Trading Signal Engine.

The Trading Signal Engine handles OHLC data management, caching, technical indicator calculations (EMA, SMA, ATR, RSI), and orchestrates six automated trading strategies: MTF EMA (Multi-Timeframe EMA Trend-Pullback), Trend Following (Forex & Non-Forex), S&P 500 Momentum, and Highest/Lowest Close FX. The MTF EMA strategy (`strategies/multi_timeframe.py`) follows the BaseStrategy interface and covers indices (SPX, NDX, RUT), commodities (XAU/USD, XAG/USD, WTI/USD), crypto (BTC/USD, ETH/USD), and forex (EUR/USD, USD/JPY, GBP/USD, AUD/USD) using D1+H4+H1 timeframe sync with EMA 20/50/200 and ATR 100 indicators. Data persistence is managed with PostgreSQL using Drizzle ORM for signals and SQLite for OHLC candle data, strategy signals, and API usage tracking. AI signal generation is powered by OpenAI through Replit AI Integrations.

Key architectural decisions include:
- **Modular Design:** Separation of concerns between frontend, Node.js backend, and Python trading engine.
- **Data Caching:** Smart caching mechanisms in the Python engine to optimize external API calls for OHLC data.
- **Idempotent Strategy Execution:** Strategies are designed to prevent duplicate signals and manage positions effectively, including ATR State Lock for consistent trailing stop calculations.
- **Admin Interface:** A dedicated admin dashboard for monitoring signals, managing API keys, tracking credit usage, and configuring user settings.
- **Robust Scheduling:** APScheduler handles background tasks for strategy evaluations and data refreshes, with timezone awareness.
- **Scalable Data Handling:** OHLC candle data is stored locally for various timeframes (30m, 1H, 4H, Daily) to support rapid indicator calculations.

## External Dependencies
- **OpenAI:** Used for AI-powered signal generation.
- **FCSAPI v4:** Provides OHLC (Open-High-Low-Close) data and real-time quotes for forex, crypto, commodities, and stock indices.
- **PostgreSQL:** Primary database for storing trading signals and user-related data.
- **SQLite:** Used for storing OHLC candle data, strategy-specific signals, API usage statistics, and internal engine data.
- **`holidays` Python package:** Utilized for detecting US and Japanese public holidays to inform strategy execution.