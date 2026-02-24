import pandas as pd
from trading_engine.indicators.validation import check_data_length


def ATR(df: pd.DataFrame, period: int = 100) -> pd.Series:
    check_data_length(df, period + 1, label=f"ATR({period})")

    high = df["high"]
    low = df["low"]
    close = df["close"]

    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = tr.ewm(alpha=1.0 / period, adjust=False).mean()
    atr.name = f"ATR_{period}"
    return atr
