import logging
from datetime import datetime
from typing import Optional
import pytz
import pandas as pd

from trading_engine.strategies.base import BaseStrategy, SignalResult, Action, Direction
from trading_engine.indicators import IndicatorEngine
from trading_engine.cache_layer import CacheLayer
from trading_engine.database import (
    signal_exists,
    has_open_signal,
    has_any_open_signal_for_asset,
    close_opposite_signal_if_exists,
    insert_signal,
    close_signal,
    open_position as db_open_position,
    get_open_position,
    get_all_open_positions,
    close_position,
    get_active_signals,
    update_signal_stop_loss,
)

logger = logging.getLogger("trading_engine.strategy.trend_non_forex")

STRATEGY_NAME = "trend_non_forex"
TARGET_SYMBOLS = [
    "CORN", "SOYB", "WEAT", "CANE", "WOOD",
    "USO", "UNG", "UGA",
    "SGOL", "SIVR", "CPER", "PPLT", "PALL",
    "DBB", "SLX",
]

SHORT_ELIGIBLE_SYMBOLS = [
    "USO",   # Oil ETF — can trend down sharply
    "UNG",   # Natural gas ETF
    "UGA",   # Gasoline ETF
    "DBB",   # Base metals — can trend down
    "SLX",   # Steel ETF
]

TIMEFRAME = "D1"
MODE = "LONG_ONLY"
SHORT_MODE_SYMBOLS = SHORT_ELIGIBLE_SYMBOLS
SMA_FAST = 50
SMA_SLOW = 100
ATR_PERIOD = 100
LOOKBACK_DAYS = 50
TRAILING_STOP_ATR_MULT = 3.0
RISK_PCT_PER_TRADE = 0.01
MIN_BARS_REQUIRED = ATR_PERIOD + 1

EVAL_HOUR = 16
EVAL_MINUTE = 1
EVAL_WINDOW_MINUTES = 5

ET_ZONE = pytz.timezone("America/New_York")


def _calculate_quantity(portfolio_value: float, atr: float, atr_mult: float = TRAILING_STOP_ATR_MULT) -> Optional[float]:
    """QC algo: quantity = (portfolio_value × RISK_PCT) / (atr_mult × ATR).

    Returns None if ATR is zero or portfolio_value is not provided.
    """
    stop_distance = atr_mult * atr
    if stop_distance <= 0 or portfolio_value <= 0:
        return None
    return round((portfolio_value * RISK_PCT_PER_TRADE) / stop_distance, 4)


class NonForexTrendFollowingStrategy(BaseStrategy):
    def __init__(self, cache: CacheLayer):
        self.cache = cache
        self._batch_prices: dict[str, dict] = {}

    def prefetch_prices(self) -> dict[str, dict]:
        api_client = self.cache.api_client
        logger.info(f"[TREND-NONFX] Batch-fetching prices via stock/latest for {len(TARGET_SYMBOLS)} ETFs")
        self._batch_prices = api_client.get_stock_latest_prices(
            list(TARGET_SYMBOLS), batch_size=9
        )
        logger.info(
            f"[TREND-NONFX] Batch prefetch complete: "
            f"{len(self._batch_prices)}/{len(TARGET_SYMBOLS)} symbols"
        )
        return self._batch_prices

    @property
    def name(self) -> str:
        return STRATEGY_NAME

    def _is_eval_window(self) -> bool:
        now_utc = datetime.now(pytz.utc)
        now_et = now_utc.astimezone(ET_ZONE)
        is_dst = bool(now_et.dst() and now_et.dst().total_seconds() > 0)
        tz_abbr = "EDT" if is_dst else "EST"

        et_minutes = now_et.hour * 60 + now_et.minute
        eval_minutes = EVAL_HOUR * 60 + EVAL_MINUTE
        window_end = eval_minutes + EVAL_WINDOW_MINUTES

        in_window = eval_minutes <= et_minutes <= window_end

        logger.info(
            f"[TREND-NONFX] Timing check | now_ET={now_et.strftime('%H:%M')} {tz_abbr} | "
            f"eval_window=16:01-16:06 ET (post-close) | in_window={in_window} | "
            f"DST={'active' if is_dst else 'inactive'}"
        )
        return in_window

    def _get_advance_price(self, asset: str) -> Optional[dict]:
        from trading_engine.fcsapi_client import STOCK_INDEX_SYMBOLS

        if asset in self._batch_prices:
            quote = self._batch_prices[asset]
            logger.info(
                f"[TREND-NONFX] {asset} | Using batch stock/latest price: close={quote['close']} | "
                f"timestamp={quote.get('timestamp', '')}"
            )
            return quote

        # Index symbols (SPX, NDX, RUT, DJI) must use the advance endpoint with type=index,
        # NOT stock/latest with type=fund&exchange=AMEX which returns wrong fund data.
        if asset in STOCK_INDEX_SYMBOLS:
            logger.info(
                f"[TREND-NONFX] {asset} | Index symbol detected — fetching via advance endpoint "
                f"(type=index, NOT stock/latest)"
            )
            try:
                api_client = self.cache.api_client
                advance_results = api_client.get_advance_data([asset], period="1d", merge="latest,profile")
                logger.debug(
                    f"[TREND-NONFX] {asset} | advance endpoint returned {len(advance_results)} result(s): "
                    f"{[r.get('symbol') for r in advance_results]}"
                )
                for result in advance_results:
                    if result.get("symbol") == asset:
                        current = result.get("current", {})
                        close_price = current.get("close")
                        ts = current.get("timestamp", "")
                        logger.info(
                            f"[TREND-NONFX] {asset} | Advance index price: close={close_price} | "
                            f"high={current.get('high')} | low={current.get('low')} | timestamp={ts}"
                        )
                        logger.debug(
                            f"[TREND-NONFX] {asset} | Raw advance current block: {current}"
                        )
                        if close_price is not None:
                            return {
                                "close": close_price,
                                "high": current.get("high"),
                                "low": current.get("low"),
                                "open": current.get("open"),
                                "timestamp": ts,
                            }
                        else:
                            logger.warning(
                                f"[TREND-NONFX] {asset} | Advance endpoint returned null close — "
                                f"raw current block: {current}"
                            )
                logger.warning(
                    f"[TREND-NONFX] {asset} | Advance endpoint returned no matching result for index. "
                    f"Symbols in response: {[r.get('symbol') for r in advance_results]}"
                )
            except Exception as e:
                logger.error(
                    f"[TREND-NONFX] {asset} | Advance index price request failed: {e}", exc_info=True
                )
            return None

        logger.info(f"[TREND-NONFX] {asset} | Not in batch cache, fetching individually via stock/latest")
        try:
            api_client = self.cache.api_client
            single_result = api_client.get_stock_latest_prices([asset], batch_size=1)
            logger.debug(
                f"[TREND-NONFX] {asset} | stock/latest individual fetch result keys: "
                f"{list(single_result.keys())}"
            )
            if asset in single_result:
                q = single_result[asset]
                logger.info(
                    f"[TREND-NONFX] {asset} | stock/latest price: close={q.get('close')} | "
                    f"timestamp={q.get('timestamp', '')}"
                )
                return q
            else:
                logger.warning(
                    f"[TREND-NONFX] {asset} | stock/latest returned no data. "
                    f"Keys in response: {list(single_result.keys())}"
                )
        except Exception as e:
            logger.error(f"[TREND-NONFX] {asset} | stock/latest request failed: {e}", exc_info=True)
        return None

    def evaluate(
        self,
        asset: str,
        timeframe: str,
        df: pd.DataFrame,
        open_position_data: Optional[dict],
    ) -> SignalResult:
        mode_label = (
            "LONG+SHORT" if asset in SHORT_ELIGIBLE_SYMBOLS
            else "LONG_ONLY"
        )
        logger.info(f"[TREND-NONFX] ====== Evaluating {asset} ({mode_label}) ======")

        if asset not in TARGET_SYMBOLS:
            logger.info(f"[TREND-NONFX] {asset} | Not a target ETF asset - skipping")
            return SignalResult()

        from trading_engine.fcsapi_client import is_symbol_supported
        if not is_symbol_supported(asset):
            logger.warning(f"[TREND-NONFX] {asset} | Symbol not supported by current data provider plan - skipping")
            return SignalResult()

        if not self._is_eval_window():
            logger.info(f"[TREND-NONFX] {asset} | Outside 4:01 PM ET eval window - skipping")
            return SignalResult()

        advance_quote = self._get_advance_price(asset)
        if advance_quote is None:
            logger.warning(f"[TREND-NONFX] {asset} | Cannot get real-time price from v4 advance - skipping")
            return SignalResult()

        logger.info(f"[TREND-NONFX] {asset} | Daily candles: {len(df)} (need {MIN_BARS_REQUIRED})")
        if len(df) < MIN_BARS_REQUIRED:
            logger.warning(
                f"[TREND-NONFX] {asset} | INSUFFICIENT DATA - have {len(df)}, need {MIN_BARS_REQUIRED}"
            )
            return SignalResult()

        closes = df["close"].tolist()
        highs = df["high"].tolist()
        lows = df["low"].tolist()

        sma50_values = IndicatorEngine.sma(closes, SMA_FAST)
        sma100_values = IndicatorEngine.sma(closes, SMA_SLOW)
        atr_values = IndicatorEngine.atr(highs, lows, closes, ATR_PERIOD)

        current_close = advance_quote["close"]
        sma50_val = sma50_values[-1]
        sma100_val = sma100_values[-1]
        atr_val = atr_values[-1]

        if any(v is None for v in [sma50_val, sma100_val, atr_val]):
            none_list = []
            if sma50_val is None: none_list.append("SMA50")
            if sma100_val is None: none_list.append("SMA100")
            if atr_val is None: none_list.append("ATR100")
            logger.warning(f"[TREND-NONFX] {asset} | Indicators returned None: {none_list}")
            return SignalResult()

        prior_closes = closes[-(LOOKBACK_DAYS + 1):-1]
        highest_50d = max(prior_closes)

        sma50_above_sma100 = sma50_val > sma100_val
        close_above_highest = current_close >= highest_50d

        logger.info(f"[TREND-NONFX] {asset} | close={current_close:.5f} (v4 advance pre-close)")
        logger.info(f"[TREND-NONFX] {asset} | SMA(50)={sma50_val:.5f} | SMA(100)={sma100_val:.5f} | ATR_live(100)={atr_val:.5f}")
        logger.info(f"[TREND-NONFX] {asset} | {LOOKBACK_DAYS}-day highest close={highest_50d:.5f}")
        logger.info(
            f"[TREND-NONFX] {asset} | LONG check: close >= highest_close={close_above_highest} "
            f"AND SMA50 > SMA100={sma50_above_sma100} (LONG_ONLY mode)"
        )

        if open_position_data:
            pos_id = open_position_data["id"]
            pos_dir = open_position_data.get("direction", "BUY")

            # Dynamic ATR trailing stop display (QC algo: live ATR × 3 behind current price)
            if pos_dir == "BUY":
                live_trailing_stop = current_close - (atr_val * TRAILING_STOP_ATR_MULT)
                logger.info(
                    f"[TREND-NONFX] {asset} | ACTIVE TRADE #{pos_id} | "
                    f"direction=BUY | entry={open_position_data['entry_price']:.5f} | "
                    f"ATR_live={atr_val:.6f} (dynamic) | "
                    f"current_price={current_close:.5f} | "
                    f"indicative_trailing_stop={live_trailing_stop:.5f} (ratcheted at exit)"
                )
            elif pos_dir == "SELL":
                live_trailing_stop = current_close + (atr_val * TRAILING_STOP_ATR_MULT)
                logger.info(
                    f"[TREND-NONFX] {asset} | ACTIVE SHORT TRADE #{pos_id} | "
                    f"direction=SELL | entry={open_position_data['entry_price']:.5f} | "
                    f"ATR_live={atr_val:.6f} (dynamic) | "
                    f"current_price={current_close:.5f} | "
                    f"indicative_trailing_stop={live_trailing_stop:.5f} (ratcheted at exit)"
                )

        now_et = datetime.now(pytz.utc).astimezone(ET_ZONE)
        signal_timestamp = now_et.strftime("%Y-%m-%dT%H:%M:%S")

        if close_above_highest and sma50_above_sma100:
            if open_position_data and open_position_data.get("direction") == "BUY":
                logger.info(f"[TREND-NONFX] {asset} | IDEMPOTENCY: Existing open LONG position - skipping")
                return SignalResult()

            if has_any_open_signal_for_asset(asset):
                logger.info(
                    f"[TREND-NONFX] {asset} | IDEMPOTENCY BLOCK: "
                    f"An OPEN signal already exists for this asset "
                    f"(cross-strategy check) — entry skipped"
                )
                return SignalResult()

            if has_open_signal(STRATEGY_NAME, asset):
                logger.info(
                    f"[TREND-NONFX] {asset} | IDEMPOTENCY: An OPEN signal already exists for "
                    f"strategy={STRATEGY_NAME}, asset={asset} — duplicate blocked"
                )
                return SignalResult()

            if signal_exists(STRATEGY_NAME, asset, signal_timestamp):
                logger.info(
                    f"[TREND-NONFX] {asset} | IDEMPOTENCY: Signal already exists for "
                    f"signal_timestamp={signal_timestamp} - duplicate blocked on re-run"
                )
                return SignalResult()

            stop_loss_distance = TRAILING_STOP_ATR_MULT * atr_val
            stop_loss = current_close - stop_loss_distance

            logger.info(f"[TREND-NONFX] {asset} | ALL CONDITIONS MET: LONG (4:01 PM ET)")
            logger.info(
                f"[TREND-NONFX] {asset} | ATR_live({ATR_PERIOD}) = {atr_val:.6f} (dynamic — recalculated each bar)"
            )
            logger.info(
                f"[TREND-NONFX] {asset} | GENERATING SIGNAL: BUY @ {current_close:.5f} | "
                f"initial_SL={stop_loss:.5f} ({TRAILING_STOP_ATR_MULT}×ATR_live, closing-rule gate)"
            )

            portfolio_value = None
            try:
                from trading_engine.database import get_setting as _get_setting
                pv_str = _get_setting("portfolio_value")
                if pv_str:
                    portfolio_value = float(pv_str)
            except Exception:
                pass

            suggested_qty = _calculate_quantity(portfolio_value, atr_val) if portfolio_value else None

            signal = {
                "strategy_name": STRATEGY_NAME,
                "asset": asset,
                "direction": "BUY",
                "action": "ENTRY",
                "entry_price": current_close,
                "stop_loss": stop_loss,
                "take_profit": None,
                # atr_at_entry intentionally omitted — dynamic ATR strategy (QC algo behavior)
                "signal_timestamp": signal_timestamp,
                "suggested_quantity": suggested_qty,
                "risk_pct": RISK_PCT_PER_TRADE if suggested_qty else None,
            }
            # Close opposite direction signal if this strategy has one open
            close_opposite_signal_if_exists(STRATEGY_NAME, asset, "BUY")
            signal_id = insert_signal(signal)
            if signal_id:
                db_open_position({
                    "asset": asset,
                    "strategy_name": STRATEGY_NAME,
                    "direction": "BUY",
                    "entry_price": current_close,
                    # atr_at_entry omitted — None is allowed for dynamic ATR strategies
                })
                signal["id"] = signal_id
                signal["status"] = "OPEN"
                logger.info(f"[TREND-NONFX] {asset} | Signal stored with id={signal_id}")
                return SignalResult(
                    action=Action.ENTRY,
                    direction=Direction.LONG,
                    price=current_close,
                    stop_loss=stop_loss,
                    metadata={"signal": signal},
                )
        else:
            logger.info(f"[TREND-NONFX] {asset} | No LONG entry conditions met — checking SHORT eligibility")

        if asset in SHORT_ELIGIBLE_SYMBOLS:
            lowest_50d = min(closes[-(LOOKBACK_DAYS + 1):-1])
            sma50_below_sma100 = sma50_val < sma100_val
            close_below_lowest = current_close <= lowest_50d

            logger.info(
                f"[TREND-NONFX] {asset} | SHORT check: close <= lowest_close={close_below_lowest} "
                f"AND SMA50 < SMA100={sma50_below_sma100} (SHORT eligible asset)"
            )

            if close_below_lowest and sma50_below_sma100:
                if open_position_data and open_position_data.get("direction") == "SELL":
                    logger.info(f"[TREND-NONFX] {asset} | IDEMPOTENCY: Existing open SHORT position - skipping")
                    return SignalResult()

                if has_any_open_signal_for_asset(asset):
                    logger.info(
                        f"[TREND-NONFX] {asset} | IDEMPOTENCY BLOCK: "
                        f"An OPEN signal already exists for this asset "
                        f"(cross-strategy check) — entry skipped"
                    )
                    return SignalResult()

                if has_open_signal(STRATEGY_NAME, asset):
                    logger.info(f"[TREND-NONFX] {asset} | IDEMPOTENCY: Open signal exists — duplicate blocked")
                    return SignalResult()

                if signal_exists(STRATEGY_NAME, asset, signal_timestamp):
                    logger.info(f"[TREND-NONFX] {asset} | Signal already exists for {signal_timestamp} — blocked")
                    return SignalResult()

                stop_loss_distance = TRAILING_STOP_ATR_MULT * atr_val
                stop_loss = current_close + stop_loss_distance

                portfolio_value = None
                try:
                    from trading_engine.database import get_setting as _get_setting
                    pv_str = _get_setting("portfolio_value")
                    if pv_str:
                        portfolio_value = float(pv_str)
                except Exception:
                    pass

                suggested_qty = _calculate_quantity(portfolio_value, atr_val) if portfolio_value else None

                logger.info(f"[TREND-NONFX] {asset} | ALL SHORT CONDITIONS MET")
                logger.info(
                    f"[TREND-NONFX] {asset} | ATR_live({ATR_PERIOD}) = {atr_val:.6f} (dynamic — recalculated each bar)"
                )
                logger.info(
                    f"[TREND-NONFX] {asset} | GENERATING SIGNAL: SELL @ {current_close:.5f} | "
                    f"initial_SL={stop_loss:.5f} (entry + {TRAILING_STOP_ATR_MULT}×ATR_live)"
                )

                signal = {
                    "strategy_name": STRATEGY_NAME,
                    "asset": asset,
                    "direction": "SELL",
                    "action": "ENTRY",
                    "entry_price": current_close,
                    "stop_loss": stop_loss,
                    "take_profit": None,
                    # atr_at_entry intentionally omitted — dynamic ATR strategy (QC algo behavior)
                    "signal_timestamp": signal_timestamp,
                    "suggested_quantity": suggested_qty,
                    "risk_pct": RISK_PCT_PER_TRADE if suggested_qty else None,
                }
                # Close opposite direction signal if this strategy has one open
                close_opposite_signal_if_exists(STRATEGY_NAME, asset, "SELL")
                signal_id = insert_signal(signal)
                if signal_id:
                    db_open_position({
                        "asset": asset,
                        "strategy_name": STRATEGY_NAME,
                        "direction": "SELL",
                        "entry_price": current_close,
                        # atr_at_entry omitted — None is allowed for dynamic ATR strategies
                    })
                    signal["id"] = signal_id
                    signal["status"] = "OPEN"
                    logger.info(f"[TREND-NONFX] {asset} | SHORT signal stored with id={signal_id}")
                    return SignalResult(
                        action=Action.ENTRY,
                        direction=Direction.SHORT,
                        price=current_close,
                        stop_loss=stop_loss,
                        metadata={"signal": signal},
                    )

        return SignalResult()

    def _close_orphaned_signals(self) -> list[dict]:
        """Close any OPEN signals that have no corresponding open_positions record,
        and close any position records for assets no longer in TARGET_SYMBOLS
        (legacy assets removed from the strategy).
        """
        orphans = get_active_signals(strategy_name=STRATEGY_NAME)
        if not orphans:
            return []

        positions = get_all_open_positions(strategy_name=STRATEGY_NAME)
        position_assets = {p["asset"] for p in positions}

        closed: list[dict] = []
        for sig in orphans:
            asset = sig["asset"]
            sig_id = sig["id"]
            direction = sig.get("direction", "")
            entry_price = sig.get("entry_price", 0)
            ts_raw = sig.get("signal_timestamp") or ""

            is_orphaned = asset not in position_assets
            is_legacy = asset not in TARGET_SYMBOLS

            if not is_orphaned and not is_legacy:
                continue

            hours_open = None
            try:
                entry_time = datetime.strptime(str(ts_raw)[:19], "%Y-%m-%dT%H:%M:%S")
                entry_time = pytz.timezone("America/New_York").localize(entry_time)
                now_et = datetime.now(pytz.utc).astimezone(pytz.timezone("America/New_York"))
                hours_open = (now_et - entry_time).total_seconds() / 3600
            except (ValueError, TypeError):
                pass

            reason_tag = []
            if is_legacy:
                reason_tag.append(f"asset '{asset}' removed from strategy TARGET_SYMBOLS")
            if is_orphaned:
                reason_tag.append("no open_positions record (cleared on restart)")

            exit_price = entry_price
            try:
                candles = self.cache.get_candles(asset, TIMEFRAME, 5)
                if candles:
                    exit_price = candles[-1]["close"]
            except Exception:
                pass

            hours_str = f"{hours_open:.1f}h" if hours_open is not None else "unknown duration"
            exit_reason = (
                f"Orphaned/legacy signal close | {'; '.join(reason_tag)} | "
                f"open for {hours_str}"
            )
            logger.warning(
                f"[TREND-NONFX-EXIT] ORPHAN/LEGACY SIGNAL | id={sig_id} | {asset} {direction} | "
                f"is_orphaned={is_orphaned} | is_legacy={is_legacy} | hours_open={hours_str} | Closing"
            )
            close_signal(sig_id, exit_reason, exit_price=exit_price)
            if is_legacy and asset in position_assets:
                close_position(STRATEGY_NAME, asset)
            closed.append({
                "asset": asset, "direction": direction,
                "entry_price": entry_price, "exit_price": exit_price,
                "exit_reason": "orphaned_or_legacy",
            })

        return closed

    def check_exits(self) -> list[dict]:
        closed_signals = []

        # Safety net: close orphaned signals and legacy assets removed from strategy
        orphan_closes = self._close_orphaned_signals()
        closed_signals.extend(orphan_closes)
        if orphan_closes:
            logger.warning(
                f"[TREND-NONFX-EXIT] {len(orphan_closes)} orphaned/legacy signal(s) closed"
            )

        positions = get_all_open_positions(strategy_name=STRATEGY_NAME)
        logger.info(f"[TREND-NONFX-EXIT] ====== Checking exits (closing-rule gate) | {len(positions)} open position(s) ======")

        if not positions:
            return closed_signals

        for pos in positions:
            asset = pos["asset"]
            pos_id = pos["id"]
            entry_price = pos["entry_price"]
            direction = pos["direction"]
            logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} {direction} | entry={entry_price:.5f}")

            if direction not in ("BUY", "SELL"):
                logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | Unknown direction {direction} — skipping")
                continue

            # ── STEP 1: Fetch candles and compute live ATR (QC algo: dynamic, not stored) ──
            try:
                candles_for_atr = self.cache.get_candles(asset, TIMEFRAME, 300)
            except Exception as e:
                logger.error(f"[TREND-NONFX-EXIT] Position #{pos_id} | Exception fetching candles for ATR: {e}")
                continue

            if len(candles_for_atr) < ATR_PERIOD + 1:
                logger.warning(
                    f"[TREND-NONFX-EXIT] Position #{pos_id} | Insufficient candles for ATR: "
                    f"{len(candles_for_atr)} (need {ATR_PERIOD + 1})"
                )
                continue

            closes_for_atr = [c["close"] for c in candles_for_atr]
            highs_for_atr  = [c["high"]  for c in candles_for_atr]
            lows_for_atr   = [c["low"]   for c in candles_for_atr]
            atr_values_exit = IndicatorEngine.atr(highs_for_atr, lows_for_atr, closes_for_atr, ATR_PERIOD)
            live_atr = atr_values_exit[-1] if atr_values_exit and atr_values_exit[-1] is not None else None

            if live_atr is None:
                logger.warning(f"[TREND-NONFX-EXIT] Position #{pos_id} | Live ATR returned None — skipping")
                continue

            logger.info(
                f"[TREND-NONFX-EXIT] Position #{pos_id} | ATR_live({ATR_PERIOD})={live_atr:.6f} (dynamic, not stored)"
            )

            # ── STEP 2: Get current price (advance API first, candle fallback) ──
            advance_quote = self._get_advance_price(asset)
            if advance_quote is not None:
                current_close = advance_quote["close"]
                logger.info(
                    f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} | "
                    f"Price source: advance API | close={current_close:.5f} | "
                    f"timestamp={advance_quote.get('timestamp', 'N/A')}"
                )
            else:
                logger.warning(
                    f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} | "
                    f"Advance API returned None — using last cached candle"
                )
                if len(candles_for_atr) < 2:
                    logger.warning(f"[TREND-NONFX-EXIT] Position #{pos_id} | Insufficient candles for price fallback")
                    continue
                current_close = candles_for_atr[-1]["close"]
                logger.info(
                    f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} | "
                    f"Price source: cached candle | close={current_close:.5f}"
                )

            # ── STEP 3: QC ratchet trailing stop ──
            # Retrieve the stored stop_loss from the open signal (persisted between runs)
            active_sigs_for_stop = get_active_signals(strategy_name=STRATEGY_NAME, asset=asset)
            stored_stop = active_sigs_for_stop[0].get("stop_loss") if active_sigs_for_stop else None

            if direction == "SELL":
                # SHORT: stop moves DOWN (lower stop is better for shorts — tighter above price)
                new_stop = current_close + (live_atr * TRAILING_STOP_ATR_MULT)
                if stored_stop is not None:
                    trailing_stop = min(stored_stop, new_stop)
                else:
                    trailing_stop = new_stop

                if stored_stop is None or trailing_stop < stored_stop:
                    for sig in active_sigs_for_stop:
                        update_signal_stop_loss(sig["id"], trailing_stop)
                    logger.info(
                        f"[TREND-NONFX-EXIT] Position #{pos_id} | "
                        f"SHORT trailing stop updated: {stored_stop} → {trailing_stop:.5f} "
                        f"(price={current_close:.5f} + {TRAILING_STOP_ATR_MULT}×ATR_live={live_atr:.6f})"
                    )
                else:
                    logger.info(
                        f"[TREND-NONFX-EXIT] Position #{pos_id} | "
                        f"SHORT trailing stop held at {trailing_stop:.5f} "
                        f"(new_stop={new_stop:.5f} would have moved stop up — not allowed)"
                    )

                logger.info(
                    f"[TREND-NONFX-EXIT] Position #{pos_id} | SHORT | close={current_close:.5f} | "
                    f"ATR_live={live_atr:.6f} | trailing_stop={trailing_stop:.5f} | "
                    f"exit_trigger(close >= stop)={current_close >= trailing_stop}"
                )

                if current_close >= trailing_stop:
                    exit_reason = (
                        f"Closing-rule SHORT exit | 4:01 PM close={current_close:.5f} >= "
                        f"SL_level={trailing_stop:.5f} ({TRAILING_STOP_ATR_MULT}×ATR_live={live_atr:.6f})"
                    )
                    logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | EXIT: SHORT closing-rule gate triggered")
                    for sig in active_sigs_for_stop:
                        close_signal(sig["id"], exit_reason)
                    close_position(STRATEGY_NAME, asset)
                    closed_signals.append({**pos, "exit_price": current_close, "exit_reason": "closing_rule_short"})
                else:
                    logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | Holding SHORT — close below SL level")
                continue

            # BUY: stop moves UP (higher stop is better for longs — tighter below price)
            new_stop = current_close - (live_atr * TRAILING_STOP_ATR_MULT)
            if stored_stop is not None:
                trailing_stop = max(stored_stop, new_stop)
            else:
                trailing_stop = new_stop

            if stored_stop is None or trailing_stop > stored_stop:
                for sig in active_sigs_for_stop:
                    update_signal_stop_loss(sig["id"], trailing_stop)
                logger.info(
                    f"[TREND-NONFX-EXIT] Position #{pos_id} | "
                    f"Trailing stop updated: {stored_stop} → {trailing_stop:.5f} "
                    f"(price={current_close:.5f} - {TRAILING_STOP_ATR_MULT}×ATR_live={live_atr:.6f})"
                )
            else:
                logger.info(
                    f"[TREND-NONFX-EXIT] Position #{pos_id} | "
                    f"Trailing stop held at {trailing_stop:.5f} "
                    f"(new_stop={new_stop:.5f} would have moved stop down — not allowed)"
                )

            logger.info(
                f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} | TRAILING STOP CHECK | "
                f"API_price={current_close:.5f} | ATR_live={live_atr:.6f} | mult={TRAILING_STOP_ATR_MULT} | "
                f"trailing_stop={trailing_stop:.5f} | "
                f"exit_trigger(close <= stop)={current_close <= trailing_stop}"
            )

            if current_close <= trailing_stop:
                exit_reason = (
                    f"Closing-rule exit | 4:01 PM close={current_close:.5f} <= "
                    f"SL_level={trailing_stop:.5f} ({TRAILING_STOP_ATR_MULT}×ATR_live={live_atr:.6f})"
                )
                logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | EXIT: closing-rule gate triggered")
                for sig in active_sigs_for_stop:
                    close_signal(sig["id"], exit_reason)
                close_position(STRATEGY_NAME, asset)
                closed_signals.append({**pos, "exit_price": current_close, "exit_reason": "closing_rule"})
            else:
                logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | Holding BUY — close above SL level")

        return closed_signals
