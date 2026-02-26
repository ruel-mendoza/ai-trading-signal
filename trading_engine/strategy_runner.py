import logging
import traceback
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from trading_engine.strategies.base import Action, Direction, SignalResult
from trading_engine.database import (
    get_candles,
    get_open_position,
    get_active_signals,
    insert_signal,
    close_signal,
    open_position,
    close_position,
    update_position_tracking,
    signal_exists,
)

logger = logging.getLogger("trading_engine.strategy_runner")

STRATEGY_ASSET_CONFIG: dict[str, dict] = {
    "mtf_ema": {
        "assets": [
            "SPX", "NDX", "RUT",
            "XAU/USD", "XAG/USD", "OSX",
            "BTC/USD", "ETH/USD",
            "EUR/USD", "USD/JPY", "GBP/USD", "AUD/USD",
        ],
        "timeframe": "1H",
    },
    "trend_following": {
        "assets": ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "NZD/USD", "USD/CAD", "USD/CHF", "EUR/GBP"],
        "timeframe": "D1",
    },
    "sp500_momentum": {
        "assets": ["SPX"],
        "timeframe": "30m",
    },
    "highest_lowest_fx": {
        "assets": ["EUR/USD"],
        "timeframe": "1H",
    },
    "trend_forex": {
        "assets": ["EUR/USD", "USD/JPY", "GBP/USD", "AUD/USD", "NZD/USD", "USD/CAD", "USD/CHF", "EUR/GBP"],
        "timeframe": "D1",
    },
    "trend_non_forex": {
        "assets": ["SPX", "NDX", "XAU/USD", "XAG/USD", "OSX", "BTC/USD", "ETH/USD"],
        "timeframe": "D1",
    },
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(strategy: str, asset: str, decision: str, detail: str = ""):
    ts = _utc_now_iso()
    msg = f"[{ts}] [{strategy}] [{asset}] [{decision}]"
    if detail:
        msg += f" {detail}"
    logger.info(msg)


def _log_warn(strategy: str, asset: str, decision: str, detail: str = ""):
    ts = _utc_now_iso()
    msg = f"[{ts}] [{strategy}] [{asset}] [{decision}]"
    if detail:
        msg += f" {detail}"
    logger.warning(msg)


def _log_error(strategy: str, asset: str, decision: str, detail: str = ""):
    ts = _utc_now_iso()
    msg = f"[{ts}] [{strategy}] [{asset}] [{decision}]"
    if detail:
        msg += f" {detail}"
    logger.error(msg)


def _candles_to_dataframe(candles: list[dict]) -> pd.DataFrame:
    if not candles:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close"])
    df = pd.DataFrame(candles)
    for col in ("open", "high", "low", "close"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _update_trailing_stop(
    strategy_name: str,
    asset: str,
    pos: dict,
    latest_close: float,
):
    pos_id = pos["id"]
    direction = pos["direction"]

    if direction == "BUY":
        stored_highest = pos.get("highest_price_since_entry") or pos["entry_price"]
        new_highest = max(stored_highest, latest_close)
        if new_highest > stored_highest:
            update_position_tracking(pos_id, highest_price=new_highest)
            _log(
                strategy_name, asset, "TRAILING_UPDATE",
                f"highest {stored_highest:.6f} -> {new_highest:.6f}"
            )
    elif direction == "SELL":
        stored_lowest = pos.get("lowest_price_since_entry") or pos["entry_price"]
        new_lowest = min(stored_lowest, latest_close)
        if new_lowest < stored_lowest:
            update_position_tracking(pos_id, lowest_price=new_lowest)
            _log(
                strategy_name, asset, "TRAILING_UPDATE",
                f"lowest {stored_lowest:.6f} -> {new_lowest:.6f}"
            )


def _handle_entry(
    strategy_name: str,
    asset: str,
    timeframe: str,
    result: SignalResult,
    signal_timestamp: str,
) -> Optional[dict]:
    direction_str = "BUY" if result.direction == Direction.LONG else "SELL"

    if signal_exists(strategy_name, asset, signal_timestamp):
        _log(
            strategy_name, asset, "IDEMPOTENCY_SKIP",
            f"signal already exists for candle {signal_timestamp}"
        )
        return None

    atr_locked = round(result.atr_at_entry, 6) if result.atr_at_entry else None

    signal = {
        "strategy_name": strategy_name,
        "asset": asset,
        "direction": direction_str,
        "entry_price": result.price,
        "stop_loss": result.stop_loss,
        "take_profit": result.metadata.get("take_profit"),
        "atr_at_entry": atr_locked,
        "signal_timestamp": signal_timestamp,
    }
    signal_id = insert_signal(signal)
    if not signal_id:
        _log_error(
            strategy_name, asset, "INSERT_FAILED",
            "insert_signal returned None (duplicate or DB error)"
        )
        return None

    pos_id = open_position({
        "asset": asset,
        "strategy_name": strategy_name,
        "direction": direction_str,
        "entry_price": result.price,
        "atr_at_entry": atr_locked,
    })

    _log(
        strategy_name, asset, "ENTRY",
        f"{direction_str} @ {result.price:.6f} | SL={result.stop_loss} | "
        f"ATR_locked={atr_locked} | signal_id={signal_id} | position_id={pos_id}"
    )

    signal["id"] = signal_id
    signal["status"] = "OPEN"
    return signal


def _handle_exit(
    strategy_name: str,
    asset: str,
    pos: dict,
    result: SignalResult,
) -> dict:
    exit_reason = result.metadata.get("exit_reason", "Strategy exit signal")
    exit_price = result.price

    active_sigs = get_active_signals(strategy_name=strategy_name, asset=asset)
    for sig in active_sigs:
        close_signal(sig["id"], exit_reason, exit_price=exit_price)

    close_position(strategy_name, asset)

    _log(
        strategy_name, asset, "EXIT",
        f"@ {exit_price:.6f} | reason={exit_reason} | closed {len(active_sigs)} signal(s)"
    )

    return {
        **pos,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
    }


def run_strategy(
    strategy,
    strategy_name: str,
    asset: str,
    timeframe: str,
    candle_limit: int = 300,
) -> Optional[dict]:
    _log(strategy_name, asset, "EVALUATE_START", f"timeframe={timeframe}")

    candles = get_candles(asset, timeframe, candle_limit)
    if not candles:
        _log_warn(strategy_name, asset, "NO_DATA", f"no candles in DB for {timeframe}")
        return None

    df = _candles_to_dataframe(candles)
    latest_close = float(df["close"].iloc[-1])
    signal_timestamp = str(df["timestamp"].iloc[-1])

    pos = get_open_position(strategy_name, asset)

    if pos:
        _update_trailing_stop(strategy_name, asset, pos, latest_close)
        pos = get_open_position(strategy_name, asset)

        atr_at_entry = pos.get("atr_at_entry")
        if atr_at_entry is None:
            _log_warn(
                strategy_name, asset, "ATR_LOCK_MISSING",
                f"position #{pos['id']} has no atr_at_entry — trailing stop may be unreliable"
            )
        else:
            _log(
                strategy_name, asset, "ATR_LOCK_VERIFIED",
                f"position #{pos['id']} | atr_at_entry={atr_at_entry:.6f} (fixed at open)"
            )

    try:
        result: SignalResult = strategy.evaluate(asset, timeframe, df, pos)
    except Exception:
        _log_error(
            strategy_name, asset, "EVALUATE_ERROR",
            f"exception:\n{traceback.format_exc()}"
        )
        return None

    if result is None or result.is_none:
        _log(strategy_name, asset, "NO_ACTION")
        return None

    if result.is_entry:
        if pos:
            _log(
                strategy_name, asset, "ENTRY_BLOCKED",
                f"position already open (id={pos['id']})"
            )
            return None

        if result.atr_at_entry is None:
            _log_warn(
                strategy_name, asset, "ATR_LOCK_WARNING",
                "ENTRY signal has no atr_at_entry — trailing stop will not function"
            )

        return _handle_entry(strategy_name, asset, timeframe, result, signal_timestamp)

    if result.is_exit:
        if not pos:
            _log(
                strategy_name, asset, "EXIT_IGNORED",
                "no open position"
            )
            return None
        return _handle_exit(strategy_name, asset, pos, result)

    return None


def run_all(
    strategies: dict,
    config: Optional[dict] = None,
) -> list[dict]:
    config = config or STRATEGY_ASSET_CONFIG
    results: list[dict] = []
    errors: list[dict] = []

    ts = _utc_now_iso()
    logger.info(
        f"[{ts}] [RUNNER] [ALL] [RUN_ALL_START] "
        f"strategies={len(config)}"
    )

    for strategy_name, cfg in config.items():
        if strategy_name not in strategies:
            _log_warn(strategy_name, "*", "STRATEGY_MISSING", "not in provided strategies dict — skipping")
            continue

        strategy = strategies[strategy_name]
        assets = cfg["assets"]
        timeframe = cfg["timeframe"]

        for asset in assets:
            try:
                result = run_strategy(strategy, strategy_name, asset, timeframe)
                if result:
                    results.append(result)
            except Exception:
                tb = traceback.format_exc()
                _log_error(strategy_name, asset, "UNHANDLED_ERROR", tb)
                errors.append({
                    "strategy": strategy_name,
                    "asset": asset,
                    "error": tb,
                })

    ts = _utc_now_iso()
    logger.info(
        f"[{ts}] [RUNNER] [ALL] [RUN_ALL_COMPLETE] "
        f"signals={len(results)} | errors={len(errors)}"
    )
    return results
