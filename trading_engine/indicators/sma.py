import pandas as pd
from trading_engine.indicators.validation import check_data_length


def SMA(data: pd.Series, period: int = 50) -> pd.Series:
    check_data_length(data, period, label=f"SMA({period})")
    result = data.rolling(window=period).mean()
    result.name = f"SMA_{period}"
    return result


def SMA50(data: pd.Series) -> pd.Series:
    return SMA(data, period=50)


def SMA100(data: pd.Series) -> pd.Series:
    return SMA(data, period=100)


def latest(data: pd.Series, period: int = 50) -> float | None:
    series = SMA(data, period)
    val = series.iloc[-1]
    if pd.isna(val):
        return None
    return float(val)
