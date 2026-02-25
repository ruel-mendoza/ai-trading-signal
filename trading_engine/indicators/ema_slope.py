import pandas as pd
from trading_engine.indicators.validation import check_data_length


def ema(series: pd.Series, period: int) -> pd.Series:
    check_data_length(series, period, label=f"EMA({period})")
    return series.ewm(span=period, adjust=False).mean()


def calculate_slope(ema_series: pd.Series) -> float:
    check_data_length(ema_series, 2, label="EMA slope")
    current = ema_series.iloc[-1]
    previous = ema_series.iloc[-2]
    if pd.isna(current) or pd.isna(previous):
        return 0.0
    return float(current - previous)


def calculate_slope_series(ema_series: pd.Series) -> pd.Series:
    check_data_length(ema_series, 2, label="EMA slope series")
    return ema_series.diff()
