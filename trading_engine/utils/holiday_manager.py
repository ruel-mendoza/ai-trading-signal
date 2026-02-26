import logging
from datetime import date, datetime
from typing import Union

import holidays

logger = logging.getLogger("trading_engine.utils.holiday_manager")

_us_holidays = holidays.US()
_jp_holidays = holidays.JP()


def is_trading_holiday(dt: Union[date, datetime]) -> bool:
    check_date = dt.date() if isinstance(dt, datetime) else dt

    in_us = check_date in _us_holidays
    in_jp = check_date in _jp_holidays

    if in_us or in_jp:
        reasons = []
        if in_us:
            reasons.append(f"US: {_us_holidays.get(check_date)}")
        if in_jp:
            reasons.append(f"JP: {_jp_holidays.get(check_date)}")
        logger.info(
            f"[HOLIDAY] {check_date.isoformat()} is a trading holiday — {', '.join(reasons)}"
        )
        return True

    return False
