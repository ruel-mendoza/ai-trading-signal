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
)

logger = logging.getLogger("trading_engine.strategy.sp500_momentum")

STRATEGY_NAME = "sp500_momentum"
SYMBOL = "SPX"
TIMEFRAME = "30m"
RSI_PERIOD = 20
ATR_PERIOD = 100
RSI_THRESHOLD = 70
SMA_PERIOD = 200
MIN_BARS_REQUIRED = max(RSI_PERIOD + 1, ATR_PERIOD + 1)

ARCA_SESSION_START_HOUR = 9
ARCA_SESSION_START_MIN = 30
ARCA_SESSION_END_HOUR = 16
ARCA_SESSION_END_MIN = 0
LAST_VALID_CANDLE_HOUR = 15
LAST_VALID_CANDLE_MIN = 30

ET_ZONE = pytz.timezone("America/New_York")


class SP500MomentumStrategy(BaseStrategy):
    def __init__(self, cache: CacheLayer):
        self.cache = cache

    @property
    def name(self) -> str:
        return STRATEGY_NAME

    def _is_within_arca_session(self, candle_time_str: str) -> bool:
        try:
            candle_utc = datetime.strptime(candle_time_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pytz.utc)
        except ValueError:
            try:
                candle_utc = datetime.strptime(candle_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.utc)
            except ValueError:
                logger.warning(f"[SP500-MOM] Cannot parse candle time: {candle_time_str}")
                return False

        candle_et = candle_utc.astimezone(ET_ZONE)
        candle_et_minutes = candle_et.hour * 60 + candle_et.minute

        session_start_minutes = ARCA_SESSION_START_HOUR * 60 + ARCA_SESSION_START_MIN
        last_valid_minutes = LAST_VALID_CANDLE_HOUR * 60 + LAST_VALID_CANDLE_MIN

        in_session = session_start_minutes <= candle_et_minutes <= last_valid_minutes

        is_dst = bool(candle_et.dst() and candle_et.dst().total_seconds() > 0)
        tz_abbr = "EDT" if is_dst else "EST"

        logger.info(
            f"[SP500-MOM] Session filter | candle_utc={candle_time_str} | "
            f"candle_ET={candle_et.strftime('%H:%M')} {tz_abbr} | "
            f"ARCA window=09:30-15:30 ET (last valid) | "
            f"in_session={in_session}"
        )
        return in_session

    def _get_advance_price(self, asset: str) -> Optional[dict]:
        try:
            api_client = self.cache.api_client
            quotes = api_client.get_advance_data([asset], period="30m", merge="latest,profile")
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
                        f"[SP500-MOM] {asset} | v4 advance quote: close={close_price} | "
                        f"timestamp={timestamp} | update_time={update_time} | name={profile_name}"
                    )
                    return {
                        "close": close_price,
                        "high": float(current["high"]) if current.get("high") else None,
                        "low": float(current["low"]) if current.get("low") else None,
                        "open": float(current["open"]) if current.get("open") else None,
                        "change": current.get("change"),
                        "change_pct": current.get("change_pct"),
                        "timestamp": timestamp,
                        "update_time": update_time,
                    }
                else:
                    logger.warning(f"[SP500-MOM] {asset} | v4 advance returned null close price")
            else:
                logger.warning(f"[SP500-MOM] {asset} | v4 advance returned no quotes")
        except Exception as e:
            logger.error(f"[SP500-MOM] {asset} | v4 advance request failed: {e}")
        return None

    def evaluate(
        self,
        asset: str,
        timeframe: str,
        df: pd.DataFrame,
        open_position_data: Optional[dict],
    ) -> SignalResult:
        logger.info(f"[SP500-MOM] ====== Evaluating {asset} ======")

        if asset != SYMBOL:
            logger.info(f"[SP500-MOM] {asset} | Not SPX - skipping")
            return SignalResult()

        logger.info(f"[SP500-MOM] {asset} | 30m candles: {len(df)} (need {MIN_BARS_REQUIRED})")
        if len(df) < MIN_BARS_REQUIRED:
            logger.warning(f"[SP500-MOM] {asset} | INSUFFICIENT DATA - have {len(df)}, need {MIN_BARS_REQUIRED}")
            return SignalResult()

        now_et = datetime.now(pytz.utc).astimezone(ET_ZONE)
        is_dst = bool(now_et.dst() and now_et.dst().total_seconds() > 0)
        tz_abbr = "EDT" if is_dst else "EST"
        logger.info(f"[SP500-MOM] {asset} | Current time: {now_et.strftime('%H:%M')} {tz_abbr} | DST={'active' if is_dst else 'inactive'}")

        latest_candle = df.iloc[-1]
        candle_time_str = str(latest_candle.get("timestamp", ""))

        if not self._is_within_arca_session(candle_time_str):
            logger.info(f"[SP500-MOM] {asset} | Outside ARCA session - skipping")
            return SignalResult()

        advance_quote = self._get_advance_price(asset)

        closes = df["close"].tolist()
        highs = df["high"].tolist()
        lows = df["low"].tolist()

        if advance_quote and advance_quote.get("close") is not None:
            current_close = float(advance_quote["close"])
            logger.info(f"[SP500-MOM] {asset} | Using v4 advance close: {current_close}")
        else:
            current_close = closes[-1]
            logger.info(f"[SP500-MOM] {asset} | Using cached candle close: {current_close} (advance unavailable)")

        rsi_values = IndicatorEngine.rsi(closes, RSI_PERIOD)
        atr_values = IndicatorEngine.atr(highs, lows, closes, ATR_PERIOD)

        current_rsi = rsi_values[-1]
        prev_rsi = rsi_values[-2] if len(rsi_values) >= 2 else None
        atr_val = atr_values[-1]

        logger.info(f"[SP500-MOM] {asset} | price={current_close:.2f}")
        if prev_rsi is not None:
            logger.info(f"[SP500-MOM] {asset} | RSI({RSI_PERIOD}): current={current_rsi:.4f}, prev={prev_rsi:.4f}")
        else:
            logger.info(f"[SP500-MOM] {asset} | RSI({RSI_PERIOD}): current={current_rsi}, prev=None")
        if atr_val is not None:
            logger.info(f"[SP500-MOM] {asset} | ATR({ATR_PERIOD}): {atr_val:.5f}")
        else:
            logger.info(f"[SP500-MOM] {asset} | ATR({ATR_PERIOD}): None")

        if current_rsi is None or atr_val is None:
            none_indicators = []
            if current_rsi is None: none_indicators.append("RSI_current")
            if atr_val is None: none_indicators.append(f"ATR{ATR_PERIOD}")
            logger.warning(f"[SP500-MOM] {asset} | Indicators returned None: {none_indicators}")
            return SignalResult()

        # Fetch D1 candles for SMA200 filter
        try:
            d1_candles = self.cache.get_candles(asset, "D1", 250)
        except Exception as e:
            logger.error(f"[SP500-MOM] {asset} | Exception fetching D1 candles for SMA200: {e}")
            return SignalResult()

        if len(d1_candles) < SMA_PERIOD:
            logger.warning(
                f"[SP500-MOM] {asset} | Insufficient D1 candles for SMA200: "
                f"{len(d1_candles)} (need {SMA_PERIOD})"
            )
            return SignalResult()

        d1_closes = [float(c["close"]) for c in d1_candles]
        sma200_vals = IndicatorEngine.sma(d1_closes, SMA_PERIOD)
        sma200_val = sma200_vals[-1] if sma200_vals and sma200_vals[-1] is not None else None

        if sma200_val is None:
            logger.warning(f"[SP500-MOM] {asset} | D1 SMA200 returned None — skipping")
            return SignalResult()

        above_sma200 = current_close > sma200_val
        logger.info(
            f"[SP500-MOM] {asset} | D1 SMA200={sma200_val:.2f} | "
            f"price={current_close:.2f} | above_sma200={above_sma200}"
        )

        if open_position_data and open_position_data.get("direction") == "BUY":
            pos_id = open_position_data.get("id")
            logger.info(
                f"[SP500-MOM] {asset} | ACTIVE LONG #{pos_id} | "
                f"entry={open_position_data['entry_price']:.2f} | "
                f"current={current_close:.2f} | "
                f"RSI({RSI_PERIOD})={current_rsi:.4f} | "
                f"exit_when_RSI_below={RSI_THRESHOLD}"
            )
            logger.info(f"[SP500-MOM] {asset} | IDEMPOTENCY: Existing open LONG position - skipping entry")
            return SignalResult()

        rsi_above_threshold = current_rsi > RSI_THRESHOLD
        entry_conditions_met = rsi_above_threshold and above_sma200

        logger.info(
            f"[SP500-MOM] {asset} | Entry check: "
            f"RSI({RSI_PERIOD})={current_rsi:.4f} > {RSI_THRESHOLD}: {rsi_above_threshold} | "
            f"price > D1 SMA200: {above_sma200} | "
            f"all_met={entry_conditions_met}"
        )

        if not entry_conditions_met:
            logger.info(f"[SP500-MOM] {asset} | Entry conditions not met — no action")
            return SignalResult()

        if has_any_open_signal_for_asset(asset):
            logger.info(
                f"[SP500-MOM] {asset} | IDEMPOTENCY BLOCK: "
                f"An OPEN signal already exists for this asset "
                f"(cross-strategy check) — entry skipped"
            )
            return SignalResult()

        if has_open_signal(STRATEGY_NAME, asset):
            logger.info(
                f"[SP500-MOM] {asset} | IDEMPOTENCY: An OPEN signal already exists for "
                f"strategy={STRATEGY_NAME}, asset={asset} — duplicate blocked"
            )
            return SignalResult()

        now_et = datetime.now(pytz.utc).astimezone(ET_ZONE)
        signal_timestamp = now_et.strftime("%Y-%m-%dT%H:%M:%S")

        if signal_exists(STRATEGY_NAME, asset, signal_timestamp):
            logger.info(f"[SP500-MOM] {asset} | Signal already exists for timestamp {signal_timestamp} - blocked")
            return SignalResult()

        stop_loss_distance = 2.0 * atr_val  # legacy reference — ATR stop kept for DB only; exit is RSI-based
        stop_loss = current_close - stop_loss_distance

        logger.info(f"[SP500-MOM] {asset} | ALL CONDITIONS MET: LONG")
        logger.info(
            f"[SP500-MOM] {asset} | ATR({ATR_PERIOD})={atr_val:.6f} "
            f"(reference only — exit is RSI-based, not ATR trailing stop)"
        )
        logger.info(
            f"[SP500-MOM] {asset} | GENERATING SIGNAL: BUY @ {current_close:.2f} | "
            f"RSI({RSI_PERIOD})={current_rsi:.4f} > {RSI_THRESHOLD} | "
            f"above_D1_SMA200=True | "
            f"ref_stop={stop_loss:.2f} (ATR-based, not used for exit — RSI exit only)"
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
        close_opposite_signal_if_exists(STRATEGY_NAME, asset, "BUY")
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
            logger.info(f"[SP500-MOM] {asset} | Signal stored with id={signal_id}")
            return SignalResult(
                action=Action.ENTRY,
                direction=Direction.LONG,
                price=current_close,
                stop_loss=stop_loss,
                atr_at_entry=round(atr_val, 6),
                metadata={"signal": signal},
            )

        return SignalResult()

    def check_exits(self) -> list[dict]:
        closed_signals = []
        positions = get_all_open_positions(strategy_name=STRATEGY_NAME)
        logger.info(f"[SP500-MOM-EXIT] ====== Checking exits | {len(positions)} open position(s) ======")

        if not positions:
            return closed_signals

        for pos in positions:
            asset = pos["asset"]
            pos_id = pos["id"]
            entry_price = pos["entry_price"]
            atr_at_entry = pos["atr_at_entry"]
            logger.info(f"[SP500-MOM-EXIT] Position #{pos_id} | {asset} | entry={entry_price:.2f}")

            if atr_at_entry is None:
                logger.warning(f"[SP500-MOM-EXIT] Position #{pos_id} | No atr_at_entry - skipping")
                continue

            advance_quote = self._get_advance_price(asset)

            try:
                candles = self.cache.get_candles(asset, TIMEFRAME, 300)
            except Exception as e:
                logger.error(f"[SP500-MOM-EXIT] Position #{pos_id} | Exception fetching candles: {e}")
                continue

            if len(candles) < RSI_PERIOD + 1:
                logger.warning(f"[SP500-MOM-EXIT] Position #{pos_id} | Insufficient candles: {len(candles)} (need {RSI_PERIOD + 1})")
                continue

            closes = [c["close"] for c in candles]

            if advance_quote and advance_quote.get("close") is not None:
                current_close = float(advance_quote["close"])
                logger.info(f"[SP500-MOM-EXIT] Position #{pos_id} | Using v4 advance close: {current_close:.2f}")
            else:
                current_close = closes[-1]
                logger.info(f"[SP500-MOM-EXIT] Position #{pos_id} | Using cached candle close: {current_close:.2f} (advance unavailable)")

            rsi_values = IndicatorEngine.rsi(closes, RSI_PERIOD)
            current_rsi = rsi_values[-1] if rsi_values else None

            rsi_below_threshold = current_rsi is not None and current_rsi < RSI_THRESHOLD

            logger.info(
                f"[SP500-MOM-EXIT] Position #{pos_id} | "
                f"close={current_close:.2f} | RSI({RSI_PERIOD})={current_rsi:.4f if current_rsi is not None else 'None'} | "
                f"exit_trigger(RSI < {RSI_THRESHOLD})={rsi_below_threshold}"
            )

            if rsi_below_threshold:
                exit_reason = (
                    f"RSI below {RSI_THRESHOLD} | "
                    f"RSI={current_rsi:.4f} | close={current_close:.2f}"
                )
                logger.info(f"[SP500-MOM-EXIT] Position #{pos_id} | EXIT: RSI < {RSI_THRESHOLD}")
                active_sigs = get_active_signals(strategy_name=STRATEGY_NAME, asset=asset)
                for sig in active_sigs:
                    close_signal(sig["id"], exit_reason)
                close_position(STRATEGY_NAME, asset)
                closed_signals.append({
                    **pos,
                    "exit_price": current_close,
                    "exit_reason": "rsi_below_threshold",
                })
            else:
                logger.info(
                    f"[SP500-MOM-EXIT] Position #{pos_id} | "
                    f"Holding — RSI {current_rsi:.4f} still >= {RSI_THRESHOLD}"
                )

        return closed_signals
