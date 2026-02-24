from trading_engine.strategies.base import BaseStrategy
from trading_engine.strategies.sp500_momentum import SP500MomentumStrategy
from trading_engine.strategies.trend_forex import ForexTrendFollowingStrategy
from trading_engine.strategy_engine import StrategyEngine

STRATEGY_REGISTRY: dict[str, type] = {
    "mtf_ema": StrategyEngine,
    "trend_following": StrategyEngine,
    "sp500_momentum": SP500MomentumStrategy,
    "highest_lowest_fx": StrategyEngine,
    "trend_forex": ForexTrendFollowingStrategy,
}


def get_strategy_keys() -> list[str]:
    return list(STRATEGY_REGISTRY.keys())


def get_strategy_class(key: str) -> type:
    if key not in STRATEGY_REGISTRY:
        raise KeyError(
            f"Unknown strategy key '{key}'. "
            f"Available: {', '.join(STRATEGY_REGISTRY.keys())}"
        )
    return STRATEGY_REGISTRY[key]
