# AI Signals - AI-Powered Trading Signals Platform

## Overview
An AI-powered trading signals platform that generates forex, crypto, and commodity trading signals using OpenAI. Now includes a Python-based Trading Signal Engine with FastAPI for OHLC data management, caching, technical indicator calculations, and a StrategyEngine with four automated trading strategies.

## Tech Stack
- **Frontend**: React + TypeScript + Vite + TailwindCSS + Shadcn UI
- **Backend**: Express.js + TypeScript (main server, port 5000)
- **Trading Engine**: Python FastAPI (port 5001, proxied via Express at /api/engine/)
- **Database**: PostgreSQL with Drizzle ORM (signals), SQLite (OHLC candle data + strategy signals)
- **AI**: OpenAI via Replit AI Integrations (gpt-5-mini for signal generation)
- **Scheduler**: APScheduler (BackgroundScheduler with CronTrigger)
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
  strategy_engine.py    - StrategyEngine orchestrator + trailing stop management
  admin.py              - Admin dashboard (HTML), export endpoints, credit monitor, timezone logic
  strategies/            - Individual strategy modules
    sp500_momentum.py   - S&P 500 Momentum Strategy (ARCA session filter, RSI crossover, ATR stops)
    trend_forex.py      - Forex Trend Following Strategy (EUR/USD, USD/JPY, GBP/USD, 5PM ET eval)
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
3. **S&P 500 Momentum (sp500_momentum)**: SPX only, 30m candles. ARCA session filter (9:30 AM-4:00 PM ET, last valid candle at 3:30 PM). LONG when prev RSI(20) < 70 AND current RSI(20) >= 70, no existing open trade. Stores ATR(100) at entry (fixed for trade). Exit via 2x ATR trailing stop or RSI back below 70. Module: `trading_engine/strategies/sp500_momentum.py`.
4. **Highest/Lowest Close FX (highest_lowest_fx)**: EUR/USD time-sensitive strategy monitoring Tokyo 8am and NY 8am windows for breakouts/reversals.
5. **Forex Trend Following (trend_forex)**: EUR/USD, USD/JPY, GBP/USD only. D1 candles, evaluates at 5:00 PM ET (forex daily close). LONG when close > highest close of last 50 days AND SMA(50) > SMA(100). SHORT when close < lowest close of last 50 days AND SMA(50) < SMA(100). Exit via 3x ATR(100) trailing stop (ATR fixed at entry). Module: `trading_engine/strategies/trend_forex.py`.

## Indicator Engine
- `trading_engine/indicators/` — Package (converted from single file)
  - `__init__.py` — IndicatorEngine class: EMA(20,50,200), SMA(50,100), ATR(100), RSI(20)
  - `validation.py` — `check_data_length(data, period, label)` validates pd.Series/DataFrame length, raises `InsufficientDataError`
  - `ema_slope.py` — `ema(series, period)` computes EMA on pd.Series (index/tz preserved); `calculate_slope(ema_series)` returns current − previous EMA
  - `sma.py` — `SMA(data, period=50)` vectorized via `data.rolling(window=period).mean()`
  - `ema.py` — `EMA(data, period=20)` vectorized via `data.ewm(span=period, adjust=False).mean()`
  - `atr.py` — `ATR(df, period=100)` True Range + Wilder's Smoothing (`ewm(alpha=1/period)`)
  - `rsi.py` — `RSI(data, period=20)` Wilder's Smoothing; returns `(rsi_series, cross_70_bool_series)`
- All functions accept pd.Series or pd.DataFrame and return pd.Series with preserved indices (timezone-aware)
- Performance: all indicators run under 3ms for 10,000 data points

## Environment Variables
- `FCSAPI_KEY` - FCSAPI API key for fetching OHLC data (required for live data)

## Admin Interface (via /api/engine/admin/)
- `GET /api/engine/admin/` - Admin dashboard HTML (signals table, credit monitor, market hours)
- `GET /api/engine/admin/export?format=csv` - Export signals as CSV
- `GET /api/engine/admin/export?format=json` - Export signals as JSON
- `GET /api/engine/admin/api/usage` - FCSAPI usage stats JSON
- `GET /api/engine/admin/api/market-times` - Current market times JSON
- `POST /api/engine/admin/api/settings/key` - Save FCSAPI API key to database
- `POST /api/engine/admin/api/settings/test-connection` - Test FCSAPI connection, returns plan/credits
- `GET /api/engine/admin/api/settings` - Get current key source status
- `GET /api/engine/admin/login` - Login page (HTML form)
- `POST /api/engine/admin/login` - Login submission (form POST)
- `GET /api/engine/admin/logout` - Logout and clear session
- `POST /api/engine/admin/api/users` - Create new admin user
- `PUT /api/engine/admin/api/users/:id` - Update admin user (re-hashes password)
- `DELETE /api/engine/admin/api/users/:id` - Delete admin user (prevents deleting last admin)
- `GET /api/engine/admin/api/users` - List all admin users
- Supports query params: ?strategy=, ?status=, ?symbol=, ?tab= for filtering
- Settings tab: API key management, test connection, credit meter
- User Settings tab: Admin user list, add/edit/delete admin accounts
- All routes protected behind session-based cookie authentication
- Default admin credentials: username=admin, password=pass123 (auto-seeded on first run)

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
