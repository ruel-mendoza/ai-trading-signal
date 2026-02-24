import logging
import pandas as pd
from trading_engine.indicators.validation import check_data_length

logger = logging.getLogger("trading_engine.indicators.ema_slope")


def ema(series: pd.Series, period: int) -> pd.Series:
    check_data_length(series, period, label=f"EMA({period})")
    return series.ewm(span=period, adjust=False).mean()


def calculate_slope(ema_series: pd.Series) -> pd.Series:
    check_data_length(ema_series, 2, label="EMA slope")
    return ema_series.diff()
