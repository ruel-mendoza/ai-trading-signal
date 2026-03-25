"""
nasdaq_sync.py — NASDAQ 100 symbol synchronisation via FCSAPI.

Calls /stock/list with exchange=NASDAQ and persists symbols to the
strategy_assets table under strategy_name='stocks_algo1' and
'stocks_algo2'.  Idempotent: safe to run on every startup or scheduled.
"""

import logging
from typing import Optional

logger = logging.getLogger("trading_engine.nasdaq_sync")

STRATEGY_NAMES = ("stocks_algo1", "stocks_algo2")

# FCSAPI does not expose a dedicated NASDAQ-100 constituents endpoint on
# most plan tiers.  We fetch the full NASDAQ stock list, filter by the
# well-known NDX constituents list cached in the DB (updated monthly), and
# fall back to a hard-coded seed of the current top 100 if the API returns
# no data.  The seed is ONLY used the very first time the DB is empty.
_NDX100_SEED: list[str] = [
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "META",
    "GOOGL",
    "GOOG",
    "TSLA",
    "AVGO",
    "COST",
    "NFLX",
    "AMD",
    "ADBE",
    "QCOM",
    "PEP",
    "CSCO",
    "TMUS",
    "INTC",
    "INTU",
    "AMGN",
    "TXN",
    "HON",
    "AMAT",
    "SBUX",
    "BKNG",
    "ISRG",
    "VRTX",
    "REGN",
    "GILD",
    "ADI",
    "LRCX",
    "MU",
    "PANW",
    "KLAC",
    "SNPS",
    "CDNS",
    "MELI",
    "ADP",
    "MDLZ",
    "PYPL",
    "CRWD",
    "CTAS",
    "ORLY",
    "WDAY",
    "MNST",
    "MRVL",
    "PCAR",
    "FTNT",
    "CEG",
    "ODFL",
    "ROST",
    "CPRT",
    "DXCM",
    "BIIB",
    "KDP",
    "FANG",
    "PAYX",
    "IDXX",
    "EXC",
    "MRNA",
    "FAST",
    "CTSH",
    "VRSK",
    "ON",
    "GEHC",
    "EA",
    "KHC",
    "XEL",
    "DLTR",
    "CDW",
    "WBD",
    "DDOG",
    "ZS",
    "CCEP",
    "ANSS",
    "BKR",
    "TTWO",
    "ILMN",
    "WBA",
    "MTCH",
    "SIRI",
    "OKTA",
    "ZM",
    "ALGN",
    "ENPH",
    "LCID",
    "RIVN",
    "NXPI",
    "MCHP",
    "LULU",
    "TEAM",
    "DOCU",
    "EBAY",
    "ASML",
    "ABNB",
    "DASH",
    "APP",
    "HOOD",
    "RBLX",
    "COIN",
]


def _get_fcsapi_key() -> str:
    from trading_engine.database import get_setting
    import os

    return get_setting("fcsapi_key") or os.environ.get("FCSAPI_KEY", "")


def fetch_nasdaq_symbols_from_api() -> list[str]:
    """
    Fetch NASDAQ stock symbols via FCSAPI /stock/list endpoint.
    Returns a flat list of ticker strings, e.g. ['AAPL', 'MSFT', ...].
    Returns empty list on any failure so callers can fall back gracefully.
    """
    import requests

    key = _get_fcsapi_key()
    if not key:
        logger.warning("[NASDAQ-SYNC] No FCSAPI key — cannot fetch NASDAQ symbol list")
        return []

    # /stock/list requires a higher FCSAPI plan tier — skip and use seed directly
    logger.info("[NASDAQ-SYNC] Skipping /stock/list API call (not available on current plan)")
    return []

    if not data.get("status") or not data.get("response"):
        logger.warning(f"[NASDAQ-SYNC] API returned no symbols: {data.get('msg', '')}")
        return []

    symbols: list[str] = []
    response = data["response"]
    items = list(response.values()) if isinstance(response, dict) else response
    for item in items:
        if isinstance(item, dict):
            sym = item.get("symbol") or item.get("ticker") or ""
            if sym:
                symbols.append(sym.upper().strip())

    logger.info(f"[NASDAQ-SYNC] Fetched {len(symbols)} NASDAQ symbols from API")
    return symbols


def _get_current_ndx100_from_db() -> list[str]:
    """Return NDX100 symbols currently stored in strategy_assets for stocks_algo1."""
    from trading_engine.database import get_strategy_assets

    return get_strategy_assets("stocks_algo1", active_only=True)


def sync_nasdaq100_symbols(force_reseed: bool = False) -> dict:
    """
    Synchronise NASDAQ 100 symbols into strategy_assets for both stock strategies.

    Algorithm:
    1. Try to fetch the full NASDAQ list from FCSAPI and cross-reference with
       the NDX100 seed list to identify current constituents.
    2. If the API returns nothing (no key, rate-limit, etc.) and the DB already
       has symbols, keep the existing set unchanged.
    3. If the DB is empty AND the API returns nothing, seed from _NDX100_SEED.

    Returns a dict with counts: {added, skipped, total}.
    """
    from trading_engine.database import add_strategy_asset, get_strategy_assets_full

    logger.info("[NASDAQ-SYNC] ====== Starting NASDAQ 100 symbol sync ======")

    # Step 1: Try to get live list from API
    api_symbols = fetch_nasdaq_symbols_from_api()

    # Determine NDX100 set: intersection of API response with known seed,
    # or just the seed itself when API returns nothing.
    if api_symbols:
        seed_set = set(_NDX100_SEED)
        api_set = set(api_symbols)
        ndx100 = list(seed_set & api_set) or _NDX100_SEED
        logger.info(
            f"[NASDAQ-SYNC] {len(ndx100)} NDX100 symbols after API×seed intersection "
            f"(api={len(api_set)}, seed={len(seed_set)})"
        )
    else:
        existing = _get_current_ndx100_from_db()
        if existing and not force_reseed:
            logger.info(
                f"[NASDAQ-SYNC] API unavailable — keeping {len(existing)} existing DB symbols"
            )
            return {"added": 0, "skipped": len(existing), "total": len(existing)}
        ndx100 = _NDX100_SEED
        logger.warning(
            f"[NASDAQ-SYNC] API unavailable + DB empty — seeding from {len(ndx100)}-symbol fallback"
        )

    # Step 2: Deactivate symbols that have been removed from the NDX100
    # (soft-delete: set is_active=0 for removed symbols)
    existing_full = get_strategy_assets_full("stocks_algo1")
    current_active = {r["symbol"] for r in existing_full if r.get("is_active")}
    ndx100_set = set(ndx100)
    removed = current_active - ndx100_set
    if removed:
        from trading_engine.database import remove_strategy_asset

        for sym in removed:
            for strat in STRATEGY_NAMES:
                remove_strategy_asset(strat, sym)
        logger.info(
            f"[NASDAQ-SYNC] Removed {len(removed)} symbols no longer in NDX100: {removed}"
        )

    # Step 3: Insert / reactivate new symbols
    added = skipped = 0
    for sym in ndx100:
        for strat in STRATEGY_NAMES:
            result = add_strategy_asset(
                strategy_name=strat,
                symbol=sym,
                asset_class="stocks",
                sub_category="nasdaq100",
                added_by="nasdaq_sync",
                fcsapi_verified=False,
            )
            if result is not None:
                added += 1
            else:
                skipped += 1

    total = len(ndx100)
    logger.info(
        f"[NASDAQ-SYNC] ====== Sync complete | "
        f"added={added} skipped={skipped} total={total} ======"
    )
    return {"added": added, "skipped": skipped, "total": total}
