import pandas as pd
from trading_engine.indicators.validation import check_data_length


def EMA(data: pd.Series, period: int = 20) -> pd.Series:
    check_data_length(data, period, label=f"EMA({period})")
    result = data.ewm(span=period, adjust=False).mean()
    result.name = f"EMA_{period}"
    return result


def EMA20(data: pd.Series) -> pd.Series:
    return EMA(data, period=20)


def EMA50(data: pd.Series) -> pd.Series:
    return EMA(data, period=50)


def EMA200(data: pd.Series) -> pd.Series:
    return EMA(data, period=200)


def latest(data: pd.Series, period: int = 20) -> float | None:
    series = EMA(data, period)
    val = series.iloc[-1]
    if pd.isna(val):
        return None
    return float(val)
