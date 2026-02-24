import logging
import pandas as pd

logger = logging.getLogger("trading_engine.indicators.validation")


class InsufficientDataError(Exception):
    pass


def check_data_length(
    data: pd.Series | pd.DataFrame,
    required_period: int,
    label: str = "",
) -> None:
    length = len(data)
    if length < required_period:
        tag = f" for {label}" if label else ""
        raise InsufficientDataError(
            f"Insufficient data{tag}: need at least {required_period} "
            f"data points but received {length}."
        )
