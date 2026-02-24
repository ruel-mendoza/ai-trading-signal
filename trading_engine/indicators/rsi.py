import pandas as pd
from trading_engine.indicators.validation import check_data_length


def RSI(data: pd.Series, period: int = 20) -> tuple[pd.Series, pd.Series]:
    check_data_length(data, period + 1, label=f"RSI({period})")

    delta = data.diff()
    gains = delta.clip(lower=0)
    losses = (-delta).clip(lower=0)

    avg_gain = gains.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1.0 / period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))

    rsi.name = f"RSI_{period}"

    prev_rsi = rsi.shift(1)
    cross_70 = (prev_rsi < 70) & (rsi >= 70)
    cross_70.name = f"RSI_{period}_cross_70"

    return rsi, cross_70
