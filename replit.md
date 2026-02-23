# EUR/USD Signal Analyzer - FCS Data Trading Signals

## Overview
A forex trading signal analyzer for EUR/USD that fetches hourly candle data from FCS API, computes ATR(100), identifies highest/lowest hourly closing prices, and generates Long/Short trade entry signals based on Tokyo/NY session rules.

## Tech Stack
- **Frontend**: React + TypeScript + Vite + TailwindCSS + Shadcn UI
- **Backend**: Express.js + TypeScript
- **Database**: PostgreSQL with Drizzle ORM
- **Data Source**: FCS API (forex hourly candles)
- **Routing**: Wouter
- **State**: TanStack React Query

## Project Structure
```
client/src/
  App.tsx          - Main router (/)
  pages/
    analysis.tsx   - EUR/USD analysis dashboard with signal detection
server/
  index.ts    - Express server entry
  routes.ts   - API routes (/api/analysis)
  fcs.ts      - FCS API service + analysis logic (ATR, signals, session detection)
  storage.ts  - Database storage layer
  db.ts       - Database connection
shared/
  schema.ts   - Drizzle schema
```

## Key Features
- Real-time EUR/USD hourly candle data from FCS API
- Highest/Lowest Close calculation since 8am Tokyo time
- ATR(100) computation on hourly candles
- Trailing stop calculation (0.25 * ATR(100))
- Long/Short entry signal detection at 9am/10am ET
- Tokyo and NY session candle views
- Previous trading day high/low tracking
- Rule-by-rule signal validation display

## Trading Rules
### Entry (9am or 10am ET, weekdays only)
- **Long**: Price went below lowest close since 8am Tokyo then closed back above; bullish candle; entry not below prev day low
- **Short**: Price went above highest close since 8am Tokyo then closed back below; bearish candle; entry not above prev day high
### Exit
- Trailing stop of 0.25 * ATR(100), ATR fixed at entry

## API Endpoints
- `GET /api/analysis` - Fetch FCS data and return full analysis (candles, signals, ATR, session data)

## Environment Variables
- `FCS_API_KEY` - FCS API access key
- `FCS_SYMBOL` - Trading symbol (default: EURUSD)
- `DATABASE_URL` - PostgreSQL connection string
