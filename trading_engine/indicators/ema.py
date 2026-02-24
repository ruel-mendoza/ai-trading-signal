import pandas as pd
from trading_engine.indicators.validation import check_data_length


def EMA(data: pd.Series, period: int = 20) -> pd.Series:
    check_data_length(data, period, label=f"EMA({period})")
    return data.ewm(span=period, adjust=False).mean()
