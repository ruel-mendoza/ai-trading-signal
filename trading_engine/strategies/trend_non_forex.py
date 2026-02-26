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
    "SPX",
    "NDX",
    "XAU/USD",
    "XAG/USD",
    "OSX",
    "BTC/USD",
    "ETH/USD",
]
TIMEFRAME = "D1"
SMA_FAST = 50
SMA_SLOW = 100
ATR_PERIOD = 100
LOOKBACK_DAYS = 50
TRAILING_STOP_ATR_MULT = 3.0
MIN_BARS_REQUIRED = ATR_PERIOD + 1

EVAL_HOUR = 16
EVAL_MINUTE = 0
EVAL_WINDOW_MINUTES = 30

ET_ZONE = pytz.timezone("America/New_York")


class NonForexTrendFollowingStrategy(BaseStrategy):
    def __init__(self, cache: CacheLayer):
        self.cache = cache

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
            f"eval_window=16:00-16:30 ET | in_window={in_window} | "
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
                    logger.info(
                        f"[TREND-NONFX] {asset} | v4 advance quote: close={close_price} | "
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
                    logger.warning(f"[TREND-NONFX] {asset} | v4 advance returned null close price")
            else:
                logger.warning(f"[TREND-NONFX] {asset} | v4 advance returned no quotes")
        except Exception as e:
            logger.error(f"[TREND-NONFX] {asset} | v4 advance request failed: {e}")
        return None

    def evaluate(
        self,
        asset: str,
        timeframe: str,
        df: pd.DataFrame,
        open_position_data: Optional[dict],
    ) -> SignalResult:
        logger.info(f"[TREND-NONFX] ====== Evaluating {asset} ======")

        if asset not in TARGET_SYMBOLS:
            logger.info(f"[TREND-NONFX] {asset} | Not a target asset - skipping")
            return SignalResult()

        from trading_engine.fcsapi_client import is_symbol_supported
        if not is_symbol_supported(asset):
            logger.warning(f"[TREND-NONFX] {asset} | Symbol not supported by current data provider plan - skipping")
            return SignalResult()

        if not self._is_eval_window():
            logger.info(f"[TREND-NONFX] {asset} | Outside 4:00 PM ET window - skipping")
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

        logger.info(f"[TREND-NONFX] {asset} | close={current_close:.5f} (v4 advance real-time)")
        logger.info(f"[TREND-NONFX] {asset} | SMA(50)={sma50_val:.5f} | SMA(100)={sma100_val:.5f} | ATR(100)={atr_val:.5f}")
        logger.info(f"[TREND-NONFX] {asset} | 50-day highest close={highest_50d:.5f} (prior {LOOKBACK_DAYS} bars)")
        logger.info(
            f"[TREND-NONFX] {asset} | LONG check: close ({current_close:.5f}) > highest_50d ({highest_50d:.5f}) = {close_above_highest} "
            f"AND SMA50 ({sma50_val:.5f}) > SMA100 ({sma100_val:.5f}) = {sma50_above_sma100}"
        )

        if open_position_data:
            pos_id = open_position_data["id"]
            pos_atr = open_position_data.get("atr_at_entry")
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
                    f"direction={open_position_data['direction']} | entry={open_position_data['entry_price']:.5f} | "
                    f"ATR_at_entry={pos_atr:.6f} (FIXED from DB, not recalculated) | "
                    f"highest_since_entry={new_highest:.5f} | "
                    f"current_trailing_stop={trailing_stop:.5f}"
                )
            else:
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
                    f"signal_timestamp={signal_timestamp} (unique constraint: strategy+asset+timestamp) "
                    f"- duplicate blocked on re-run"
                )
                return SignalResult()

            stop_loss_distance = TRAILING_STOP_ATR_MULT * atr_val
            stop_loss = current_close - stop_loss_distance

            logger.info(f"[TREND-NONFX] {asset} | ALL CONDITIONS MET: LONG")
            logger.info(
                f"[TREND-NONFX] {asset} | ATR({ATR_PERIOD}) at entry = {atr_val:.6f} "
                f"(FIXED for trade lifetime)"
            )
            logger.info(
                f"[TREND-NONFX] {asset} | GENERATING SIGNAL: BUY @ {current_close:.5f} | "
                f"initial_trailing_stop={stop_loss:.5f} (entry - {TRAILING_STOP_ATR_MULT}x ATR)"
            )

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
            logger.info(f"[TREND-NONFX] {asset} | LONG conditions not met — no action")

        return SignalResult()

    def check_exits(self) -> list[dict]:
        closed_signals = []
        positions = get_all_open_positions(strategy_name=STRATEGY_NAME)
        logger.info(f"[TREND-NONFX-EXIT] ====== Checking exits | {len(positions)} open position(s) ======")

        if not positions:
            return closed_signals

        for pos in positions:
            asset = pos["asset"]
            pos_id = pos["id"]
            entry_price = pos["entry_price"]
            direction = pos["direction"]
            atr_at_entry = pos["atr_at_entry"]
            logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | {asset} {direction} | entry={entry_price:.5f}")

            if atr_at_entry is None:
                logger.warning(f"[TREND-NONFX-EXIT] Position #{pos_id} | No atr_at_entry - skipping")
                continue

            logger.info(
                f"[TREND-NONFX-EXIT] Position #{pos_id} | ATR locked at entry: {atr_at_entry:.6f} (read from DB, not recalculated)"
            )

            advance_quote = self._get_advance_price(asset)
            if advance_quote is not None:
                current_close = advance_quote["close"]
                logger.info(
                    f"[TREND-NONFX-EXIT] Position #{pos_id} | Using v4 advance real-time price: {current_close:.5f}"
                )
            else:
                logger.warning(
                    f"[TREND-NONFX-EXIT] Position #{pos_id} | v4 advance unavailable, falling back to cached candles"
                )
                try:
                    candles = self.cache.get_candles(asset, TIMEFRAME, 300)
                except Exception as e:
                    logger.error(f"[TREND-NONFX-EXIT] Position #{pos_id} | Exception fetching candles: {e}")
                    continue

                if len(candles) < 2:
                    logger.warning(f"[TREND-NONFX-EXIT] Position #{pos_id} | Insufficient candles: {len(candles)}")
                    continue
                current_close = candles[-1]["close"]

            stored_highest = pos.get("highest_price_since_entry") or entry_price
            highest_close = max(stored_highest, current_close)
            update_position_tracking(pos_id, highest_price=highest_close)

            trailing_stop = highest_close - (atr_at_entry * TRAILING_STOP_ATR_MULT)

            logger.info(
                f"[TREND-NONFX-EXIT] Position #{pos_id} | BUY tracking | "
                f"close={current_close:.5f} | prev_highest={stored_highest:.5f} | "
                f"new_highest={highest_close:.5f} | "
                f"trailing_stop = {highest_close:.5f} - ({atr_at_entry:.6f} x {TRAILING_STOP_ATR_MULT}) = {trailing_stop:.5f}"
            )
            logger.info(
                f"[TREND-NONFX-EXIT] Position #{pos_id} | Exit check: "
                f"close ({current_close:.5f}) < trailing_stop ({trailing_stop:.5f}) = "
                f"{current_close < trailing_stop}"
            )

            if current_close < trailing_stop:
                exit_reason = (
                    f"Trailing stop hit | close={current_close:.5f}, "
                    f"stop={trailing_stop:.5f}, highest_since_entry={highest_close:.5f}, "
                    f"ATR_at_entry={atr_at_entry:.6f} (fixed)"
                )
                logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | EXIT: trailing_stop")
                active_sigs = get_active_signals(strategy_name=STRATEGY_NAME, asset=asset)
                for sig in active_sigs:
                    close_signal(sig["id"], exit_reason)
                close_position(STRATEGY_NAME, asset)
                closed_signals.append({**pos, "exit_price": current_close, "exit_reason": "trailing_stop"})
            else:
                logger.info(f"[TREND-NONFX-EXIT] Position #{pos_id} | Holding LONG position")

        return closed_signals
