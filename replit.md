# AI Signals - AI-Powered Trading Signals Platform

## Overview
An AI-powered trading signals platform that generates forex, crypto, and commodity trading signals using OpenAI. Now includes a Python-based Trading Signal Engine with FastAPI for OHLC data management, caching, technical indicator calculations, and a StrategyEngine with four automated trading strategies.

## Tech Stack
- **Frontend**: React + TypeScript + Vite + TailwindCSS + Shadcn UI
- **Backend**: Express.js + TypeScript (main server, port 5000)
- **Trading Engine**: Python FastAPI (port 5001, proxied via Express at /api/engine/)
- **Database**: PostgreSQL with Drizzle ORM (signals), SQLite (OHLC candle data + strategy signals)
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
  main.py               - FastAPI app with routes (candles, indicators, strategies, admin)
  database.py           - SQLite database layer for OHLC candles + strategy signals + API usage
  fcsapi_client.py      - FCSAPI client for fetching OHLC data (with usage tracking)
  cache_layer.py        - Caching layer (calls API only on candle closes)
  indicators.py         - IndicatorEngine class (EMA, SMA, ATR, RSI)
  strategy_engine.py    - StrategyEngine with 4 strategies + trailing stop management
  admin.py              - Admin dashboard (HTML), export endpoints, credit monitor, timezone logic
```

## Trading Engine API (via /api/engine/)
- `GET /api/engine/` - Health check / status
- `GET /api/engine/api/candles?symbol=EUR/USD&timeframe=1H&limit=300` - Get OHLC candles
- `GET /api/engine/api/indicators?symbol=EUR/USD&timeframe=1H&include_series=false` - Get indicators
- `POST /api/engine/api/candles/refresh?symbol=EUR/USD&timeframe=1H` - Force refresh candles
- `GET /api/engine/api/symbols` - List available symbols
- `GET /api/engine/api/cache/status?symbol=EUR/USD&timeframe=1H` - Cache status
- `GET /api/engine-status` - Python engine process status

## Strategy Engine API (via /api/engine/)
- `POST /api/engine/api/strategies/evaluate?symbols=EUR/USD,GBP/USD` - Evaluate all strategies
- `POST /api/engine/api/strategies/evaluate/{strategy_name}?symbol=EUR/USD` - Evaluate single strategy
- `POST /api/engine/api/strategies/check-exits` - Check and execute exit conditions
- `GET /api/engine/api/strategy-signals?strategy=&symbol=&status=` - List strategy signals
- `GET /api/engine/api/strategy-signals/active` - List active signals only

## Strategies
1. **MTF EMA (mtf_ema)**: Multi-timeframe using D1/H4/H1. Long when price > D1 200/50 EMA, H4 200 EMA rising, price dips below H4 50 EMA by < 1 ATR, H1 closes back above 20 EMA.
2. **Trend Following (trend_following)**: Entry when close > last 50 days AND SMA50 > SMA100. Exit via 3x ATR(100) trailing stop.
3. **S&P 500 Momentum (sp500_momentum)**: Long on 30m RSI(20) crossing above 70. Exit via 2x ATR(100) trailing stop or RSI back below 70.
4. **Highest/Lowest Close FX (highest_lowest_fx)**: EUR/USD time-sensitive strategy monitoring Tokyo 8am and NY 8am windows for breakouts/reversals.

## Indicator Engine
Calculates locally: EMA(20,50,200), SMA(50,100), ATR(100), RSI(20)

## Environment Variables
- `FCSAPI_KEY` - FCSAPI API key for fetching OHLC data (required for live data)

## Admin Interface (via /api/engine/admin/)
- `GET /api/engine/admin/` - Admin dashboard HTML (signals table, credit monitor, market hours)
- `GET /api/engine/admin/export?format=csv` - Export signals as CSV
- `GET /api/engine/admin/export?format=json` - Export signals as JSON
- `GET /api/engine/admin/api/usage` - FCSAPI usage stats JSON
- `GET /api/engine/admin/api/market-times` - Current market times JSON
- Supports query params: ?strategy=, ?status=, ?symbol=, ?tab= for filtering

## Credit Monitor
- Tracks FCSAPI API calls in SQLite (api_usage table)
- Monthly limit: 500,000 credits
- Alert levels: caution (60%), warning (75%), critical (90%)
- Shows daily history, per-endpoint breakdown

## Timezone Logic
- Tokyo (JST, UTC+9): Session 09:00-15:00
- New York (EST/EDT): Session 09:30-16:00, DST auto-detected
- London (GMT): Session 08:00-16:00
- Strategy windows: Tokyo 8am (23:00 UTC), NY 8am (13:00 UTC)

## Key Features
- AI-generated trading signals with entry/SL/TP levels
- Signal categories: Forex, Crypto, Commodities
- Signal status management (active/closed/expired)
- Detailed technical analysis per signal
- Real-time signal generation via SSE streaming
- OHLC candle data storage for 30m, 1H, 4H, Daily timeframes
- Smart caching that only fetches new data on candle closes
- Local technical indicator calculations
- Idempotent strategy evaluation (no duplicate signals per candle)
- Trailing stop management with automatic exit tracking
- Admin dashboard with signal table, CSV/JSON export, credit monitor, timezone display

## API Endpoints (Express)
- `GET /api/signals?category=` - List signals (filter by category)
- `GET /api/signals/:id` - Get signal details
- `POST /api/signals/generate` - Generate AI signal (SSE stream)
- `PATCH /api/signals/:id/status` - Update signal status
- `GET /api/pairs` - List available trading pairs
