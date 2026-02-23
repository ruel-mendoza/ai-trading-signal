# AI Signals - AI-Powered Trading Signals Platform

## Overview
An AI-powered trading signals platform that generates forex, crypto, and commodity trading signals using OpenAI. Now includes a Python-based Trading Signal Engine with FastAPI for OHLC data management, caching, and technical indicator calculations.

## Tech Stack
- **Frontend**: React + TypeScript + Vite + TailwindCSS + Shadcn UI
- **Backend**: Express.js + TypeScript (main server, port 5000)
- **Trading Engine**: Python FastAPI (port 5001, proxied via Express at /api/engine/)
- **Database**: PostgreSQL with Drizzle ORM (signals), SQLite (OHLC candle data)
- **AI**: OpenAI via Replit AI Integrations (gpt-5-mini for signal generation)
- **Routing**: Wouter
- **State**: TanStack React Query

## Project Structure
```
client/src/
  App.tsx          - Main router (/, /signal/:id)
  pages/
    home.tsx       - Signal listing with filters & stats
    signal-detail.tsx - Detailed signal view with analysis
  components/
    signal-card.tsx        - Signal card component
    category-filter.tsx    - Category filter buttons
    generate-signal-dialog.tsx - AI signal generation dialog
server/
  index.ts    - Express server entry (spawns Python engine, proxies /api/engine/)
  routes.ts   - API routes (/api/signals, /api/pairs, /api/signals/generate)
  storage.ts  - Database storage layer
  db.ts       - Database connection
  seed.ts     - Seed data
shared/
  schema.ts   - Drizzle schema (users, signals tables)
trading_engine/          - Python FastAPI trading engine
  main.py               - FastAPI app with routes
  database.py           - SQLite database layer for OHLC candles
  fcsapi_client.py      - FCSAPI client for fetching OHLC data
  cache_layer.py        - Caching layer (calls API only on candle closes)
  indicators.py         - IndicatorEngine class (EMA, SMA, ATR, RSI)
```

## Trading Engine API (via /api/engine/)
- `GET /api/engine/` - Health check / status
- `GET /api/engine/api/candles?symbol=EUR/USD&timeframe=1H&limit=300` - Get OHLC candles
- `GET /api/engine/api/indicators?symbol=EUR/USD&timeframe=1H&include_series=false` - Get indicators
- `POST /api/engine/api/candles/refresh?symbol=EUR/USD&timeframe=1H` - Force refresh candles
- `GET /api/engine/api/symbols` - List available symbols
- `GET /api/engine/api/cache/status?symbol=EUR/USD&timeframe=1H` - Cache status
- `GET /api/engine-status` - Python engine process status

## Indicator Engine
Calculates locally: EMA(20,50,200), SMA(50,100), ATR(100), RSI(20)

## Environment Variables
- `FCSAPI_KEY` - FCSAPI API key for fetching OHLC data (required for live data)

## Key Features
- AI-generated trading signals with entry/SL/TP levels
- Signal categories: Forex, Crypto, Commodities
- Signal status management (active/closed/expired)
- Detailed technical analysis per signal
- Real-time signal generation via SSE streaming
- OHLC candle data storage for 30m, 1H, 4H, Daily timeframes
- Smart caching that only fetches new data on candle closes
- Local technical indicator calculations

## API Endpoints (Express)
- `GET /api/signals?category=` - List signals (filter by category)
- `GET /api/signals/:id` - Get signal details
- `POST /api/signals/generate` - Generate AI signal (SSE stream)
- `PATCH /api/signals/:id/status` - Update signal status
- `GET /api/pairs` - List available trading pairs
