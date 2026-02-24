from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import pandas as pd


class Action(str, Enum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    NONE = "NONE"


class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass
class SignalResult:
    action: Action = Action.NONE
    direction: Optional[Direction] = None
    price: Optional[float] = None
    stop_loss: Optional[float] = None
    atr_at_entry: Optional[float] = None
    metadata: dict = field(default_factory=dict)

    @property
    def is_entry(self) -> bool:
        return self.action == Action.ENTRY

    @property
    def is_exit(self) -> bool:
        return self.action == Action.EXIT

    @property
    def is_none(self) -> bool:
        return self.action == Action.NONE

    def to_dict(self) -> dict:
        return {
            "action": self.action.value,
            "direction": self.direction.value if self.direction else None,
            "price": self.price,
            "stop_loss": self.stop_loss,
            "atr_at_entry": self.atr_at_entry,
            "metadata": self.metadata,
        }


class BaseStrategy(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @abstractmethod
    def evaluate(
        self,
        asset: str,
        timeframe: str,
        df: pd.DataFrame,
        open_position: Optional[dict],
    ) -> SignalResult:
        ...
