import pandas as pd
from trading_engine.indicators.validation import check_data_length


def true_range(df: pd.DataFrame) -> pd.Series:
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

    tr.iloc[0] = high.iloc[0] - low.iloc[0]
    tr.name = "TR"
    return tr


def ATR(df: pd.DataFrame, period: int = 100) -> pd.Series:
    check_data_length(df, period + 1, label=f"ATR({period})")

    tr = true_range(df)
    atr = tr.rolling(window=period).mean()
    atr.name = f"ATR_{period}"
    return atr


def latest(df: pd.DataFrame, period: int = 100) -> float | None:
    series = ATR(df, period)
    val = series.iloc[-1]
    if pd.isna(val):
        return None
    return float(val)
