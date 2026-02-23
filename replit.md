# AI Signals - AI-Powered Trading Signals Platform

## Overview
An AI-powered trading signals platform that generates forex, crypto, and commodity trading signals using OpenAI. Users can view active/historical signals, filter by category, and generate new AI-powered signals with detailed technical analysis.

## Tech Stack
- **Frontend**: React + TypeScript + Vite + TailwindCSS + Shadcn UI
- **Backend**: Express.js + TypeScript
- **Database**: PostgreSQL with Drizzle ORM
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
  index.ts    - Express server entry
  routes.ts   - API routes (/api/signals, /api/pairs, /api/signals/generate)
  storage.ts  - Database storage layer
  db.ts       - Database connection
  seed.ts     - Seed data (7 realistic trading signals)
shared/
  schema.ts   - Drizzle schema (users, signals tables)
```

## Key Features
- AI-generated trading signals with entry/SL/TP levels
- Signal categories: Forex, Crypto, Commodities
- Signal status management (active/closed/expired)
- Detailed technical analysis per signal
- Real-time signal generation via SSE streaming
- Category filtering and signal stats

## API Endpoints
- `GET /api/signals?category=` - List signals (filter by category)
- `GET /api/signals/:id` - Get signal details
- `POST /api/signals/generate` - Generate AI signal (SSE stream)
- `PATCH /api/signals/:id/status` - Update signal status
- `GET /api/pairs` - List available trading pairs (14 pairs across forex/crypto/commodities)

## Data Model
- signals table: id (serial), pair, category, direction, entryPrice, stopLoss, takeProfit, status, confidence, analysis, shortSummary, createdAt, updatedAt
- Query keys use array segments: ["/api/signals", category] for list, ["/api/signals", "detail", id] for detail
