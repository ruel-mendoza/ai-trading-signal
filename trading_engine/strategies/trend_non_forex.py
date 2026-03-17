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
    insert_signal,
    close_signal,
    open_position as db_open_position,
    get_open_position,
    get_all_open_positions,
    update_position_tracking,
    close_position,
    get_active_signals,
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
        self._batch_prices = api_client.get_stock_latest_prices(list(TARGET_SYMBOLS), batch_size=9)
        logger.info(f"[TREND-NONFX] Batch prefetch complete: {len(self._batch_prices)}/{len(TARGET_SYMBOLS)} symbols")
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
        mode_label = "LONG+SHORT" if asset in SHORT_ELIGIBLE_SYMBOLS else "LONG_ONLY"
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
        close_above_highest = current_close > highest_50d

        logger.info(f"[TREND-NONFX] {asset} | close={current_close:.5f} (v4 advance pre-close)")
        logger.info(f"[TREND-NONFX] {asset} | SMA(50)={sma50_val:.5f} | SMA(100)={sma100_val:.5f} | ATR(100)={atr_val:.5f}")
        logger.info(f"[TREND-NONFX] {asset} | {LOOKBACK_DAYS}-day highest close={highest_50d:.5f}")
        logger.info(
            f"[TREND-NONFX] {asset} | LONG check: close > highest_close={close_above_highest} "
            f"AND SMA50 > SMA100={sma50_above_sma100} (LONG_ONLY mode)"
        )

        if open_position_data:
            pos_id = open_position_data["id"]
            pos_dir = open_position_data.get("direction", "BUY")
            pos_atr = open_position_data.get("atr_at_entry")

            if pos_dir == "BUY":
                stored_highest = open_position_data.get("highest_price_since_entry") or open_position_data["entry_price"]
                new_highest = max(stored_highest, current_close)
                if new_highest > stored_highest:
                    update_position_tracking(pos_id, highest_price=new_highest)
                    logger.info(
                        f"[TREND-NONFX] {asset} | PEAK UPDATE #{pos_id} | "
                        f"prev_highest={stored_highest:.5f} → new_highest={new_highest:.5f} (persisted to DB)"
                    )
                if pos_atr is not None:
                    trailing_stop = new_highest - (pos_atr * TRAILING_STOP_ATR_MULT)
                    logger.info(
                        f"[TREND-NONFX] {asset} | ACTIVE TRADE #{pos_id} | "
                        f"direction=BUY | entry={open_position_data['entry_price']:.5f} | "
                        f"ATR_at_entry={pos_atr:.6f} (FIXED from DB) | "
                        f"highest_since_entry={new_highest:.5f} | "
                        f"current_trailing_stop={trailing_stop:.5f}"
                    )

            elif pos_dir == "SELL":
                stored_trough = open_position_data.get("lowest_price_since_entry") or open_position_data["entry_price"]
                new_trough = min(stored_trough, current_close)
                if new_trough < stored_trough:
                    update_position_tracking(pos_id, lowest_price=new_trough)
                    logger.info(
                        f"[TREND-NONFX] {asset} | SHORT TROUGH UPDATE #{pos_id} | "
                        f"prev_trough={stored_trough:.5f} → new_trough={new_trough:.5f} (persisted to DB)"
                    )
                if pos_atr is not None:
                    trailing_stop = new_trough + (pos_atr * TRAILING_STOP_ATR_MULT)
                    logger.info(
                        f"[TREND-NONFX] {asset} | ACTIVE SHORT TRADE #{pos_id} | "
                        f"direction=SELL | entry={open_position_data['entry_price']:.5f} | "
                        f"ATR_at_entry={pos_atr:.6f} (FIXED from DB) | "
                        f"trough_since_entry={new_trough:.5f} | "
                        f"current_trailing_stop={trailing_stop:.5f}"
                    )

            if pos_atr is None:
                logger.warning(
                    f"[TREND-NONFX] {asset} | ACTIVE TRADE #{pos_id} | "
                    f"ATR_at_entry=MISSING in DB — trailing stop cannot be calculated"
                )

        now_et = datetime.now(pytz.utc).astimezone(ET_ZONE)
        signal_timestamp = now_et.strftime("%Y-%m-%dT%H:%M:%S")

        if close_above_highest and sma50_above_sma100:
            if open_position_data and open_position_data.get("direction") == "BUY":
                logger.info(f"[TREND-NONFX] {asset} | IDEMPOTENCY: Existing open LONG position - skipping")
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
                f"[TREND-NONFX] {asset} | ATR({ATR_PERIOD}) at entry = {atr_val:.6f} "
                f"(FIXED for trade lifetime)"
            )
            logger.info(
                f"[TREND-NONFX] {asset} | GENERATING SIGNAL: BUY @ {current_close:.5f} | "
                f"SL={stop_loss:.5f} ({TRAILING_STOP_ATR_MULT}x ATR, closing-rule gate)"
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
                "atr_at_entry": round(atr_val, 6),
                "signal_timestamp": signal_timestamp,
                "suggested_quantity": suggested_qty,
                "risk_pct": RISK_PCT_PER_TRADE if suggested_qty else None,
            }
            signal_id = insert_signal(signal)
            if signal_id:
                db_open_position({
                    "asset": asset,
                    "strategy_name": STRATEGY_NAME,
                    "direction": "BUY",
                    "entry_price": current_close,
                    "atr_at_entry": round(atr_val, 6),
                })
                signal["id"] = signal_id
                signal["status"] = "OPEN"
                logger.info(f"[TREND-NONFX] {asset} | Signal stored with id={signal_id}")
                return SignalResult(
                    action=Action.ENTRY,
                    direction=Direction.LONG,
                    price=current_close,
                    stop_loss=stop_loss,
                    atr_at_entry=round(atr_val, 6),
                    metadata={"signal": signal},
                )
        else:
            logger.info(f"[TREND-NONFX] {asset} | No LONG entry conditions met — checking SHORT eligibility")

        if asset in SHORT_ELIGIBLE_SYMBOLS:
            lowest_50d = min(closes[-(LOOKBACK_DAYS + 1):-1])
            sma50_below_sma100 = sma50_val < sma100_val
            close_below_lowest = current_close < lowest_50d

            logger.info(
                f"[TREND-NONFX] {asset} | SHORT check: close < lowest_close={close_below_lowest} "
                f"AND SMA50 < SMA100={sma50_below_sma100} (SHORT eligible asset)"
            )

            if close_below_lowest and sma50_below_sma100:
                if open_position_data and open_position_data.get("direction") == "SELL":
                    logger.info(f"[TREND-NONFX] {asset} | IDEMPOTENCY: Existing open SHORT position - skipping")
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
                logger.info(f"[TREND-NONFX] {asset} | ATR({ATR_PERIOD}) at entry = {atr_val:.6f} (FIXED for trade lifetime)")
                logger.info(
                    f"[TREND-NONFX] {asset} | GENERATING SIGNAL: SELL @ {current_close:.5f} | "
                    f"initial_trailing_stop={stop_loss:.5f} (entry + {TRAILING_STOP_ATR_MULT}x ATR)"
                )

                signal = {
                    "strategy_name": STRATEGY_NAME,
                    "asset": asset,
                    "direction": "SELL",
                    "action": "ENTRY",
                    "entry_price": current_close,
                    "stop_loss": stop_loss,
                    "take_profit": None,
                    "atr_at_entry": round(atr_val, 6),
                    "signal_timestamp": signal_timestamp,
                    "suggested_quantity": suggested_qty,
                    "risk_pct": RISK_PCT_PER_TRADE if suggested_qty else None,
                }
                signal_id = insert_signal(signal)
                if signal_id:
                    db_open_position({
                        "asset": asset,
                        "strategy_name": STRATEGY_NAME,
                        "direction": "SELL",
                        "entry_price": current_close,
                        "atr_at_entry": round(atr_val, 6),
                    })
                    signal["id"] = signal_id
                    signal["status"] = "OPEN"
                    logger.info(f"[TREND-NONFX] {asset} | SHORT signal stored with id={signal_id}")
                    return SignalResult(
                        action=Action.ENTRY,
                        direction=Direction.SHORT,
                        price=current_close,
                        stop_loss=stop_loss,
                        atr_at_entry=round(atr_val, 6),
                        metadata={"signal": signal},
                    )

        return SignalResult()

    def check_exits(self) -> list[dict]:
        closed_signals = []
        positions = get_all_open_positions(strategy_name=STRATEGY_NAME)
        logger.info(f"[TREND-NONFX-EXIT] ====== Checking exits (closing-rule gate) | {len(positions)} open position(s) ======")

        if not positions:
            return closed_signals

        for pos in positions:
            asset = pos["asset"]
            pos_id = pos["id"]
            entry_price = pos["entry_price"]
            direction = pos["direction"]
            atr_at_entry = pos["atr_at_entry"]
            logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} {direction} | entry={entry_price:.5f}")

            if direction == "SELL":
                if atr_at_entry is None:
                    logger.warning(f"[TREND-NONFX-EXIT] Position #{pos_id} | No atr_at_entry for SHORT — skipping")
                    continue

                logger.info(
                    f"[TREND-NONFX-EXIT] Position #{pos_id} | ATR locked at entry: {atr_at_entry:.6f} (read from DB)"
                )

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
                        f"Advance API returned None — falling back to cached candles"
                    )
                    try:
                        candles = self.cache.get_candles(asset, TIMEFRAME, 300)
                    except Exception as e:
                        logger.error(
                            f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} | "
                            f"Exception fetching candles: {e}", exc_info=True
                        )
                        continue
                    if len(candles) < 2:
                        logger.warning(
                            f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} | "
                            f"Insufficient candles: {len(candles)}"
                        )
                        continue
                    current_close = candles[-1]["close"]

                stored_trough = pos.get("lowest_price_since_entry") or entry_price
                trough_close = min(stored_trough, current_close)
                update_position_tracking(pos_id, lowest_price=trough_close)

                trailing_stop = trough_close + (atr_at_entry * TRAILING_STOP_ATR_MULT)

                logger.info(
                    f"[TREND-NONFX-EXIT] Position #{pos_id} | SHORT | close={current_close:.5f} | "
                    f"trough={trough_close:.5f} | ATR_at_entry={atr_at_entry:.6f} (FIXED) | "
                    f"trailing_stop={trailing_stop:.5f} | "
                    f"price_above_stop={current_close > trailing_stop}"
                )

                if current_close > trailing_stop:
                    exit_reason = (
                        f"Closing-rule SHORT exit | 4:01 PM close={current_close:.5f} > "
                        f"SL_level={trailing_stop:.5f} (trough={trough_close:.5f} + "
                        f"{TRAILING_STOP_ATR_MULT}x ATR={atr_at_entry:.6f})"
                    )
                    logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | EXIT: SHORT closing-rule gate triggered")
                    active_sigs = get_active_signals(strategy_name=STRATEGY_NAME, asset=asset)
                    for sig in active_sigs:
                        close_signal(sig["id"], exit_reason)
                    close_position(STRATEGY_NAME, asset)
                    closed_signals.append({**pos, "exit_price": current_close, "exit_reason": "closing_rule_short"})
                else:
                    logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | Holding SHORT — close below SL level")
                continue

            if direction != "BUY":
                logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | Unknown direction {direction} — skipping")
                continue

            if atr_at_entry is None:
                logger.warning(f"[TREND-NONFX-EXIT] Position #{pos_id} | No atr_at_entry - skipping")
                continue

            logger.info(
                f"[TREND-NONFX-EXIT] Position #{pos_id} | ATR locked at entry: {atr_at_entry:.6f} (read from DB)"
            )

            logger.debug(
                f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} | "
                f"Calling _get_advance_price() ..."
            )
            advance_quote = self._get_advance_price(asset)
            if advance_quote is not None:
                current_close = advance_quote["close"]
                logger.info(
                    f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} | "
                    f"Price source: advance API | close={current_close:.5f} | "
                    f"timestamp={advance_quote.get('timestamp', 'N/A')}"
                )
                logger.debug(
                    f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} | "
                    f"Full advance quote: {advance_quote}"
                )
            else:
                logger.warning(
                    f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} | "
                    f"Advance API returned None — falling back to cached candles"
                )
                try:
                    candles = self.cache.get_candles(asset, TIMEFRAME, 300)
                except Exception as e:
                    logger.error(
                        f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} | "
                        f"Exception fetching candles: {e}", exc_info=True
                    )
                    continue

                if len(candles) < 2:
                    logger.warning(
                        f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} | "
                        f"Insufficient candles: {len(candles)}"
                    )
                    continue
                current_close = candles[-1]["close"]
                logger.info(
                    f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} | "
                    f"Price source: cached candle | close={current_close:.5f}"
                )

            stored_highest = pos.get("highest_price_since_entry") or entry_price
            highest_close = max(stored_highest, current_close)
            update_position_tracking(pos_id, highest_price=highest_close)

            trailing_stop = highest_close - (atr_at_entry * TRAILING_STOP_ATR_MULT)

            logger.info(
                f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} | TRAILING STOP CHECK | "
                f"API_price={current_close:.5f} | stored_highest={stored_highest:.5f} | "
                f"effective_highest={highest_close:.5f} | "
                f"atr_at_entry={atr_at_entry:.6f} | mult={TRAILING_STOP_ATR_MULT} | "
                f"trailing_stop={trailing_stop:.5f} | "
                f"price_below_stop={current_close < trailing_stop}"
            )
            logger.debug(
                f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} | "
                f"Comparison: {current_close:.5f} < {trailing_stop:.5f} = {current_close < trailing_stop}"
            )

            if current_close < trailing_stop:
                exit_reason = (
                    f"Closing-rule exit | 4:01 PM close={current_close:.5f} < "
                    f"SL_level={trailing_stop:.5f} (highest={highest_close:.5f} - "
                    f"{TRAILING_STOP_ATR_MULT}x ATR={atr_at_entry:.6f})"
                )
                logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | EXIT: closing-rule gate triggered")
                active_sigs = get_active_signals(strategy_name=STRATEGY_NAME, asset=asset)
                for sig in active_sigs:
                    close_signal(sig["id"], exit_reason)
                close_position(STRATEGY_NAME, asset)
                closed_signals.append({**pos, "exit_price": current_close, "exit_reason": "closing_rule"})
            else:
                logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | Holding BUY — close above SL level")

        return closed_signals
