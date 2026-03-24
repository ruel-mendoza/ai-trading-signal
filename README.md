# Changelog

## [2.1.0] — 2026-03-24

### 🏗️ Asset Management — Fully Database-Driven

#### Core Architecture
- Rebuilt asset management system to use database as single source of truth
- Bootstrap seeds run **once only** on empty DB — server restarts never re-add removed assets
- `remove_strategy_asset()` always hard-deletes rows regardless of `added_by` value
- `add_strategy_asset()` correctly reactivates inactive rows when re-added
- `sync_strategy_assets_dedup()` removed from `init_db()` auto-call — available for manual admin use only
- `MTF_REMOVE` cleanup block removed — no longer needed with DB-first approach

#### Strategy Files
- **`trend_non_forex.py`** — removed hardcoded `TARGET_SYMBOLS` entry guard from `evaluate()` — assets added via admin panel now evaluate correctly on next scheduler run
- **`trend_forex.py`** — removed hardcoded `TARGET_SYMBOLS` entry guard from `evaluate()`
- **`trend_non_forex.py`** — `TARGET_SYMBOLS` now DB-driven via `_load_symbols()` at module load, hardcoded list kept as exception-only fallback
- **`trend_forex.py`** — same DB-driven pattern applied
- **`multi_timeframe.py`** — `TARGET_ASSETS` and `ALL_ASSETS` now DB-driven via `_build_target_assets()`, hardcoded fallback only on DB exception
- **`multi_timeframe.py`** — `_load_target_assets()` and `get_all_mtf_assets()` read from DB only
- **`trend_non_forex.py`** — `_close_orphaned_signals()` updated to use live DB asset list instead of hardcoded `TARGET_SYMBOLS` for legacy signal detection

#### Database Seeds Updated
| Strategy | Active Assets |
|---|---|
| `mtf_ema` | 11 (SPX, NDX, RUT, DJI, XAU/USD, XAG/USD, OSX, BTC/USD, ETH/USD, GBP/USD, AUD/USD) |
| `trend_non_forex` | 40 (15 ETFs + 25 crypto altcoins) |
| `trend_forex` | 2 (EUR/USD, USD/JPY) |
| `sp500_momentum` | 1 (SPX) |
| `highest_lowest_fx` | 1 (EUR/USD) |

---

### 🛠️ Admin Panel Fixes

#### Asset Management Tab
- Fixed `api_list_strategy_assets()` — inactive assets (`is_active=False`) now correctly excluded from UI display
- Fixed `AttributeError` 500 error when adding assets to `trend_non_forex` — caused by `sub_category=null` payload where `.strip()` was called on `None`
- Fixed all body field parsing in `api_add_strategy_asset()` to handle `null` values safely — `symbol`, `strategy_name`, `asset_class`, `notes`, `sub_category`
- Fixed local import shadowing — `get_active_symbols as _get_tnf_symbols` inside `_get_trend_following_data()` renamed to `_get_tnf_active_symbols`
- Replaced stale top-level `TARGET_SYMBOLS` import from `trend_non_forex.py` with live DB-driven `_get_tnf_symbols()` function

---

### 🐛 Bug Fixes

- Fixed DJI asset — correctly classified as `asset_class=forex`, `sub_category=indices` in `mtf_ema`
- Removed 26 inactive crypto altcoin rows from `mtf_ema` that were appearing in Asset Management UI despite `is_active=False`
- Fixed stale `admin_removed` rows blocking re-adds of manually removed assets to different strategies
- Fixed `is_active` filter — was not correctly excluding `False` boolean values returned from SQLite

---

### 📋 Migration Notes

> No manual DB migration required. All changes are fully backward compatible.
> Existing signals, positions, and candle data are unaffected.
> The `strategy_assets` table is the new single source of truth for all strategy asset lists.
> Assets added or removed via the Admin Panel take effect on the next scheduled evaluation cycle.

---

## [2.0.0] — 2026-03-23

### Initial asset management system
- `strategy_assets` table introduced as DB-backed asset registry
- Admin Panel Asset Management tab added
- FCSAPI auto-verification on asset add
- Sync & Remove Duplicates utility added
- Partner API key management added
- Signal 92-day retention purge added
- WordPress CMS publishing integration added
- WebSocket real-time signal broadcasting added
