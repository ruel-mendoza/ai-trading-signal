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

logger = logging.getLogger("trading_engine.strategy.trend_forex")

STRATEGY_NAME = "trend_forex"
TARGET_SYMBOLS = [
    "EUR/USD",
    "USD/JPY",
]
TIMEFRAME = "D1"
SMA_FAST = 50
SMA_SLOW = 100
ATR_PERIOD = 100
LOOKBACK_DAYS = 50
TRAILING_STOP_ATR_MULT = 3.0
RISK_PCT_PER_TRADE = 0.01
MIN_BARS_REQUIRED = ATR_PERIOD + 1

FOREX_CLOSE_HOUR = 17
FOREX_CLOSE_MINUTE = 1
EVAL_WINDOW_MINUTES = 2

ET_ZONE = pytz.timezone("America/New_York")

MODE = "LONG_SHORT"
# All forex pairs are eligible for both LONG and SHORT per QC algo


def get_active_symbols() -> list[str]:
    """Return active forex trend symbols from DB.
    Falls back to hardcoded TARGET_SYMBOLS.
    """
    try:
        from trading_engine.database import get_strategy_assets
        symbols = get_strategy_assets(STRATEGY_NAME)
        if symbols:
            return symbols
    except Exception as e:
        logger.error(
            f"[TREND-FOREX] Failed to load assets from DB: "
            f"{e} — using hardcoded fallback"
        )
    return list(TARGET_SYMBOLS)


def _calculate_quantity(
    portfolio_value: float,
    atr: float,
    atr_mult: float = TRAILING_STOP_ATR_MULT,
) -> Optional[float]:
    """QC algo: quantity = (portfolio_value × RISK_PCT) / (atr_mult × ATR)."""
    stop_distance = atr_mult * atr
    if stop_distance <= 0 or portfolio_value <= 0:
        return None
    return round((portfolio_value * RISK_PCT_PER_TRADE) / stop_distance, 4)


class ForexTrendFollowingStrategy(BaseStrategy):
    def __init__(self, cache: CacheLayer):
        self.cache = cache

    @property
    def name(self) -> str:
        return STRATEGY_NAME

    def _is_forex_close_window(self) -> bool:
        now_utc = datetime.now(pytz.utc)
        now_et = now_utc.astimezone(ET_ZONE)
        is_dst = bool(now_et.dst() and now_et.dst().total_seconds() > 0)
        tz_abbr = "EDT" if is_dst else "EST"

        et_minutes = now_et.hour * 60 + now_et.minute
        close_minutes = FOREX_CLOSE_HOUR * 60 + FOREX_CLOSE_MINUTE
        window_end = close_minutes + EVAL_WINDOW_MINUTES

        in_window = close_minutes <= et_minutes <= window_end

        logger.info(
            f"[TREND-FOREX] Timing check | now_ET={now_et.strftime('%H:%M')} {tz_abbr} | "
            f"post-close=17:01 ET | window=17:01-17:03 ET | in_window={in_window} | "
            f"DST={'active' if is_dst else 'inactive'}"
        )
        return in_window

    def _get_advance_price(self, asset: str) -> Optional[dict]:
        try:
            api_client = self.cache.api_client
            quotes = api_client.get_advance_data([asset], period="1d", merge="latest,profile")
            if quotes and len(quotes) > 0:
                quote = quotes[0]
                current = quote.get("current", {})
                close_price = current.get("close")
                timestamp = current.get("timestamp", "")
                update_time = quote.get("update_time", "")
                profile_name = quote.get("profile", {}).get("name", "")
                if close_price is not None:
                    close_price = float(close_price)
                    logger.info(
                        f"[TREND-FOREX] {asset} | v4 advance quote: close={close_price} | "
                        f"timestamp={timestamp} | update_time={update_time} | name={profile_name}"
                    )
                    return {
                        "close": close_price,
                        "high": current.get("high"),
                        "low": current.get("low"),
                        "open": current.get("open"),
                        "change": current.get("change"),
                        "change_pct": current.get("change_pct"),
                        "timestamp": timestamp,
                        "update_time": update_time,
                    }
                else:
                    logger.warning(f"[TREND-FOREX] {asset} | v4 advance returned null close price")
            else:
                logger.warning(f"[TREND-FOREX] {asset} | v4 advance returned no quotes")
        except Exception as e:
            logger.error(f"[TREND-FOREX] {asset} | v4 advance request failed: {e}")
        return None

    def evaluate(
        self,
        asset: str,
        timeframe: str,
        df: pd.DataFrame,
        open_position_data: Optional[dict],
        batch_price: Optional[float] = None,
    ) -> SignalResult:
        logger.info(f"[TREND-FOREX] ====== Evaluating {asset} (LONG_SHORT) ======")

        if asset not in TARGET_SYMBOLS:
            logger.info(f"[TREND-FOREX] {asset} | Not a target asset (strictly EUR/USD, USD/JPY) - skipping")
            return SignalResult()

        from trading_engine.fcsapi_client import is_symbol_supported
        if not is_symbol_supported(asset):
            logger.warning(f"[TREND-FOREX] {asset} | Symbol not supported by current data provider plan - skipping")
            return SignalResult()

        if not self._is_forex_close_window():
            logger.info(f"[TREND-FOREX] {asset} | Outside 5:01 PM ET eval window - skipping")
            return SignalResult()

        if batch_price is not None:
            logger.info(f"[TREND-FOREX] {asset} | Using v3 batch price: {batch_price:.5f} (pre-fetched, 0 extra credits)")
            advance_quote = {"close": batch_price}
        else:
            logger.info(f"[TREND-FOREX] {asset} | No batch price available, falling back to v4 advance call")
            advance_quote = self._get_advance_price(asset)

        logger.info(f"[TREND-FOREX] {asset} | Daily candles: {len(df)} (need {MIN_BARS_REQUIRED})")
        if len(df) < MIN_BARS_REQUIRED:
            logger.warning(
                f"[TREND-FOREX] {asset} | INSUFFICIENT DATA - have {len(df)}, need {MIN_BARS_REQUIRED}"
            )
            return SignalResult()

        closes = df["close"].tolist()
        highs = df["high"].tolist()
        lows = df["low"].tolist()

        sma50_values = IndicatorEngine.sma(closes, SMA_FAST)
        sma100_values = IndicatorEngine.sma(closes, SMA_SLOW)
        atr_values = IndicatorEngine.atr(highs, lows, closes, ATR_PERIOD)

        sma50_val = sma50_values[-1]
        sma100_val = sma100_values[-1]
        atr_val = atr_values[-1]

        if any(v is None for v in [sma50_val, sma100_val, atr_val]):
            none_list = []
            if sma50_val is None: none_list.append("SMA50")
            if sma100_val is None: none_list.append("SMA100")
            if atr_val is None: none_list.append("ATR100")
            logger.warning(f"[TREND-FOREX] {asset} | Indicators returned None: {none_list}")
            return SignalResult()

        if advance_quote and advance_quote.get("close") is not None:
            current_close = float(advance_quote["close"])
            logger.info(f"[TREND-FOREX] {asset} | Using v4 advance pre-close: {current_close:.5f}")
        else:
            current_close = closes[-1]
            logger.info(f"[TREND-FOREX] {asset} | Using cached candle close: {current_close:.5f} (advance unavailable)")

        prior_closes = closes[-(LOOKBACK_DAYS + 1):-1]
        highest_50d  = max(prior_closes)
        lowest_50d   = min(prior_closes)

        sma50_above_sma100 = sma50_val > sma100_val
        close_above_highest = current_close >= highest_50d

        sma50_below_sma100 = sma50_val < sma100_val
        close_below_lowest  = current_close <= lowest_50d

        logger.info(f"[TREND-FOREX] {asset} | close={current_close:.5f} (5:01 PM ET)")
        logger.info(
            f"[TREND-FOREX] {asset} | SMA(50)={sma50_val:.5f} | SMA(100)={sma100_val:.5f} | "
            f"ATR_live(100)={atr_val:.5f}"
        )
        logger.info(
            f"[TREND-FOREX] {asset} | 50d-high={highest_50d:.5f} | 50d-low={lowest_50d:.5f}"
        )
        logger.info(
            f"[TREND-FOREX] {asset} | LONG check: close >= highest_close={close_above_highest} "
            f"AND SMA50 > SMA100={sma50_above_sma100} (LONG_SHORT mode)"
        )
        logger.info(
            f"[TREND-FOREX] {asset} | SHORT check: "
            f"close <= lowest_close={close_below_lowest} "
            f"AND SMA50 < SMA100={sma50_below_sma100} (LONG_SHORT mode)"
        )

        # ── Active position monitoring ──
        if open_position_data:
            pos_id  = open_position_data["id"]
            pos_dir = open_position_data.get("direction", "BUY")

            if pos_dir == "BUY":
                trailing_stop_preview = current_close - (atr_val * TRAILING_STOP_ATR_MULT)
                logger.info(
                    f"[TREND-FOREX] {asset} | ACTIVE LONG #{pos_id} | "
                    f"entry={open_position_data['entry_price']:.5f} | "
                    f"ATR_live={atr_val:.6f} (dynamic) | "
                    f"indicative_trailing_stop={trailing_stop_preview:.5f} "
                    f"(actual stop updated by check_exits)"
                )
            elif pos_dir == "SELL":
                trailing_stop_preview = current_close + (atr_val * TRAILING_STOP_ATR_MULT)
                logger.info(
                    f"[TREND-FOREX] {asset} | ACTIVE SHORT #{pos_id} | "
                    f"entry={open_position_data['entry_price']:.5f} | "
                    f"ATR_live={atr_val:.6f} (dynamic) | "
                    f"indicative_trailing_stop={trailing_stop_preview:.5f} "
                    f"(actual stop updated by check_exits)"
                )

            logger.info(
                f"[TREND-FOREX] {asset} | IDEMPOTENCY: "
                f"Existing open {pos_dir} position — skipping entry evaluation"
            )
            return SignalResult()

        now_et = datetime.now(pytz.utc).astimezone(ET_ZONE)
        signal_timestamp = now_et.strftime("%Y-%m-%dT%H:%M:%S")

        # ── LONG ENTRY ──
        if close_above_highest and sma50_above_sma100:
            if has_any_open_signal_for_asset(
                asset,
                exclude_strategies=["highest_lowest_fx"],
            ):
                logger.info(
                    f"[TREND-FOREX] {asset} | IDEMPOTENCY BLOCK: "
                    f"An OPEN signal already exists for this asset "
                    f"(cross-strategy check) — entry skipped"
                )
                return SignalResult()

            if has_open_signal(STRATEGY_NAME, asset):
                logger.info(
                    f"[TREND-FOREX] {asset} | IDEMPOTENCY: An OPEN signal already exists for "
                    f"strategy={STRATEGY_NAME}, asset={asset} - duplicate blocked"
                )
                return SignalResult()

            if signal_exists(STRATEGY_NAME, asset, signal_timestamp):
                logger.info(
                    f"[TREND-FOREX] {asset} | IDEMPOTENCY: Signal already exists for "
                    f"signal_timestamp={signal_timestamp} - duplicate blocked on re-run"
                )
                return SignalResult()

            stop_loss_distance = TRAILING_STOP_ATR_MULT * atr_val
            stop_loss = current_close - stop_loss_distance

            logger.info(f"[TREND-FOREX] {asset} | ALL CONDITIONS MET: LONG (5:01 PM ET)")
            logger.info(
                f"[TREND-FOREX] {asset} | ATR_live({ATR_PERIOD}) = {atr_val:.6f} "
                f"(dynamic — recalculated each bar)"
            )
            logger.info(
                f"[TREND-FOREX] {asset} | GENERATING SIGNAL: BUY @ {current_close:.5f} | "
                f"initial_SL={stop_loss:.5f} ({TRAILING_STOP_ATR_MULT}×ATR_live)"
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
            # (e.g. was SHORT, now going LONG — close the SHORT first)
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
                logger.info(f"[TREND-FOREX] {asset} | LONG signal stored with id={signal_id}")
                return SignalResult(
                    action=Action.ENTRY,
                    direction=Direction.LONG,
                    price=current_close,
                    stop_loss=stop_loss,
                    metadata={"signal": signal},
                )

        # ── SHORT ENTRY (all forex pairs per QC algo) ──
        if sma50_below_sma100 and close_below_lowest:
            if has_any_open_signal_for_asset(
                asset,
                exclude_strategies=["highest_lowest_fx"],
            ):
                logger.info(
                    f"[TREND-FOREX] {asset} | IDEMPOTENCY BLOCK: "
                    f"An OPEN signal already exists for this asset "
                    f"(cross-strategy check) — entry skipped"
                )
                return SignalResult()

            if has_open_signal(STRATEGY_NAME, asset):
                logger.info(
                    f"[TREND-FOREX] {asset} | IDEMPOTENCY: Open signal exists — duplicate blocked"
                )
                return SignalResult()

            if signal_exists(STRATEGY_NAME, asset, signal_timestamp):
                logger.info(
                    f"[TREND-FOREX] {asset} | IDEMPOTENCY: "
                    f"Signal at {signal_timestamp} already exists — blocked"
                )
                return SignalResult()

            stop_loss_distance = TRAILING_STOP_ATR_MULT * atr_val
            stop_loss = current_close + stop_loss_distance

            logger.info(f"[TREND-FOREX] {asset} | ALL SHORT CONDITIONS MET (5:01 PM ET)")
            logger.info(
                f"[TREND-FOREX] {asset} | ATR_live({ATR_PERIOD}) = {atr_val:.6f} (dynamic)"
            )
            logger.info(
                f"[TREND-FOREX] {asset} | GENERATING SIGNAL: SELL @ {current_close:.5f} | "
                f"initial_stop={stop_loss:.5f} (entry + {TRAILING_STOP_ATR_MULT}×ATR_live)"
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
                "direction": "SELL",
                "action": "ENTRY",
                "entry_price": current_close,
                "stop_loss": stop_loss,
                "take_profit": None,
                # atr_at_entry intentionally omitted — dynamic ATR strategy
                "signal_timestamp": signal_timestamp,
                "suggested_quantity": suggested_qty,
                "risk_pct": RISK_PCT_PER_TRADE if suggested_qty else None,
            }
            # Close opposite direction signal if this strategy has one open
            # (e.g. was LONG, now going SHORT — close the LONG first)
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
                logger.info(f"[TREND-FOREX] {asset} | SHORT signal stored id={signal_id}")
                return SignalResult(
                    action=Action.ENTRY,
                    direction=Direction.SHORT,
                    price=current_close,
                    stop_loss=stop_loss,
                    metadata={"signal": signal},
                )
        else:
            logger.info(
                f"[TREND-FOREX] {asset} | "
                f"No LONG or SHORT conditions met — no action (LONG_SHORT mode)"
            )

        return SignalResult()

    def check_exits(self) -> list[dict]:
        closed_signals = []
        positions = get_all_open_positions(strategy_name=STRATEGY_NAME)
        logger.info(
            f"[TREND-FOREX-EXIT] ====== Checking exits (closing-rule gate) | "
            f"{len(positions)} open position(s) ======"
        )

        if not positions:
            return closed_signals

        for pos in positions:
            asset = pos["asset"]
            pos_id = pos["id"]
            entry_price = pos["entry_price"]
            direction = pos["direction"]
            logger.info(
                f"[TREND-FOREX-EXIT] Position #{pos_id} | {asset} {direction} | "
                f"entry={entry_price:.5f}"
            )

            if direction not in ("BUY", "SELL"):
                logger.info(
                    f"[TREND-FOREX-EXIT] Position #{pos_id} | Unknown direction {direction} — skipping"
                )
                continue

            # ── STEP 1: Compute live ATR from latest candles (QC algo: never stored) ──
            try:
                candles_for_atr = self.cache.get_candles(asset, TIMEFRAME, 300)
            except Exception as e:
                logger.error(
                    f"[TREND-FOREX-EXIT] Position #{pos_id} | Exception fetching candles for ATR: {e}"
                )
                continue

            if len(candles_for_atr) < ATR_PERIOD + 1:
                logger.warning(
                    f"[TREND-FOREX-EXIT] Position #{pos_id} | "
                    f"Insufficient candles for ATR: {len(candles_for_atr)}"
                )
                continue

            closes_atr = [c["close"] for c in candles_for_atr]
            highs_atr  = [c["high"]  for c in candles_for_atr]
            lows_atr   = [c["low"]   for c in candles_for_atr]
            atr_series = IndicatorEngine.atr(highs_atr, lows_atr, closes_atr, ATR_PERIOD)
            live_atr = atr_series[-1] if atr_series and atr_series[-1] is not None else None

            if live_atr is None:
                logger.warning(
                    f"[TREND-FOREX-EXIT] Position #{pos_id} | Live ATR returned None — skipping"
                )
                continue

            logger.info(
                f"[TREND-FOREX-EXIT] Position #{pos_id} | "
                f"ATR_live({ATR_PERIOD})={live_atr:.6f} (dynamic, recalculated)"
            )

            # ── STEP 2: Get current price (advance API first, candle fallback) ──
            advance_quote = self._get_advance_price(asset)
            if advance_quote is not None:
                current_close = float(advance_quote["close"])
                logger.info(
                    f"[TREND-FOREX-EXIT] Position #{pos_id} | "
                    f"Using v4 advance pre-close price: {current_close:.5f}"
                )
            else:
                logger.warning(
                    f"[TREND-FOREX-EXIT] Position #{pos_id} | "
                    f"v4 advance unavailable, using last cached candle"
                )
                if len(candles_for_atr) < 2:
                    logger.warning(
                        f"[TREND-FOREX-EXIT] Position #{pos_id} | Insufficient candles for price fallback"
                    )
                    continue
                current_close = candles_for_atr[-1]["close"]

            # ── STEP 3: QC ratchet trailing stop and exit check ──
            active_sigs_stop = get_active_signals(strategy_name=STRATEGY_NAME, asset=asset)
            stored_stop = active_sigs_stop[0].get("stop_loss") if active_sigs_stop else None

            if direction == "SELL":
                # SHORT: new_stop = price + (ATR × 3), ratchet DOWN with min()
                new_stop = current_close + (live_atr * TRAILING_STOP_ATR_MULT)
                if stored_stop is not None:
                    trailing_stop = min(stored_stop, new_stop)
                else:
                    trailing_stop = new_stop

                if stored_stop is None or trailing_stop < stored_stop:
                    for sig in active_sigs_stop:
                        update_signal_stop_loss(sig["id"], trailing_stop)
                    logger.info(
                        f"[TREND-FOREX-EXIT] Position #{pos_id} | SELL | "
                        f"Trailing stop updated: {stored_stop} → {trailing_stop:.5f} "
                        f"(price={current_close:.5f} + {TRAILING_STOP_ATR_MULT}×ATR={live_atr:.6f})"
                    )
                else:
                    logger.info(
                        f"[TREND-FOREX-EXIT] Position #{pos_id} | SELL | "
                        f"Trailing stop held: {trailing_stop:.5f} "
                        f"(new_stop={new_stop:.5f} would move stop up — not allowed)"
                    )

                logger.info(
                    f"[TREND-FOREX-EXIT] Position #{pos_id} | SELL | "
                    f"close={current_close:.5f} | stop={trailing_stop:.5f} | "
                    f"exit_trigger(close >= stop)={current_close >= trailing_stop}"
                )

                if current_close >= trailing_stop:
                    exit_reason = (
                        f"SHORT closing-rule exit | 5:01 PM close={current_close:.5f} >= "
                        f"stop={trailing_stop:.5f} "
                        f"({TRAILING_STOP_ATR_MULT}×ATR_live={live_atr:.6f})"
                    )
                    logger.info(
                        f"[TREND-FOREX-EXIT] Position #{pos_id} | EXIT: SHORT trailing stop triggered"
                    )
                    for sig in active_sigs_stop:
                        close_signal(sig["id"], exit_reason)
                    close_position(STRATEGY_NAME, asset)
                    closed_signals.append({
                        **pos,
                        "exit_price": current_close,
                        "exit_reason": "closing_rule_short",
                    })
                else:
                    logger.info(
                        f"[TREND-FOREX-EXIT] Position #{pos_id} | Holding SHORT — close below stop level"
                    )
                continue

            # BUY: new_stop = price - (ATR × 3), ratchet UP with max()
            new_stop = current_close - (live_atr * TRAILING_STOP_ATR_MULT)
            if stored_stop is not None:
                trailing_stop = max(stored_stop, new_stop)
            else:
                trailing_stop = new_stop

            if stored_stop is None or trailing_stop > stored_stop:
                for sig in active_sigs_stop:
                    update_signal_stop_loss(sig["id"], trailing_stop)
                logger.info(
                    f"[TREND-FOREX-EXIT] Position #{pos_id} | BUY | "
                    f"Trailing stop updated: {stored_stop} → {trailing_stop:.5f} "
                    f"(price={current_close:.5f} - {TRAILING_STOP_ATR_MULT}×ATR={live_atr:.6f})"
                )
            else:
                logger.info(
                    f"[TREND-FOREX-EXIT] Position #{pos_id} | BUY | "
                    f"Trailing stop held: {trailing_stop:.5f} "
                    f"(new_stop={new_stop:.5f} would move stop down — not allowed)"
                )

            logger.info(
                f"[TREND-FOREX-EXIT] Position #{pos_id} | CLOSING-RULE CHECK | "
                f"close={current_close:.5f} | stop={trailing_stop:.5f} | "
                f"exit_trigger(close <= stop)={current_close <= trailing_stop} | "
                f"(intraday spikes ignored — only 5:01 PM close matters)"
            )

            if current_close <= trailing_stop:
                exit_reason = (
                    f"Closing-rule exit | 5:01 PM close={current_close:.5f} <= "
                    f"stop={trailing_stop:.5f} "
                    f"({TRAILING_STOP_ATR_MULT}×ATR_live={live_atr:.6f})"
                )
                logger.info(f"[TREND-FOREX-EXIT] Position #{pos_id} | EXIT: closing-rule gate triggered")
                for sig in active_sigs_stop:
                    close_signal(sig["id"], exit_reason)
                close_position(STRATEGY_NAME, asset)
                closed_signals.append({**pos, "exit_price": current_close, "exit_reason": "closing_rule"})
            else:
                logger.info(f"[TREND-FOREX-EXIT] Position #{pos_id} | Holding BUY — close above SL level")

        return closed_signals
