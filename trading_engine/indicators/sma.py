import pandas as pd
from trading_engine.indicators.validation import check_data_length


def SMA(data: pd.Series, period: int = 50) -> pd.Series:
    check_data_length(data, period, label=f"SMA({period})")
    return data.rolling(window=period).mean()
