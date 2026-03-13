"""
Singleton registry for the StrategyEngine instance.
main.py registers the engine here after creation;
admin.py and other modules read it to avoid circular imports.
"""
from typing import Optional

_engine = None


def register(engine) -> None:
    global _engine
    _engine = engine


def get_engine():
    return _engine
