import logging

from sqlalchemy import (
    Column,
    Integer,
    Float,
    Text,
    CheckConstraint,
    UniqueConstraint,
    Index,
    ForeignKey,
    func,
)
from sqlalchemy.orm import DeclarativeBase

logger = logging.getLogger("trading_engine.models")

VALID_TIMEFRAMES = ["30m", "1H", "4H", "D1"]


class Base(DeclarativeBase):
    pass


class Candle(Base):
    __tablename__ = "candles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    asset = Column(Text, nullable=False)
    timeframe = Column(Text, nullable=False)
    timestamp = Column(Text, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    created_at = Column(Text, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("asset", "timeframe", "timestamp", name="uq_candle_asset_tf_ts"),
        CheckConstraint("timeframe IN ('30m', '1H', '4H', 'D1')", name="ck_candle_timeframe"),
        Index("idx_candle_asset_tf_ts", "asset", "timeframe", "timestamp"),
        Index("idx_candle_asset_tf", "asset", "timeframe"),
        Index("idx_candle_timestamp", "timestamp"),
    )


class Signal(Base):
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    asset = Column(Text, nullable=False)
    strategy_name = Column(Text, nullable=False)
    direction = Column(Text, nullable=False)
    entry_price = Column(Float, nullable=False)
    stop_loss = Column(Float)
    take_profit = Column(Float)
    atr_at_entry = Column(Float)
    status = Column(Text, nullable=False, default="OPEN")
    signal_timestamp = Column(Text, nullable=False)
    created_at = Column(Text, server_default=func.now())
    updated_at = Column(Text, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("asset", "strategy_name", "signal_timestamp", name="uq_signal_idempotency"),
        CheckConstraint("direction IN ('BUY', 'SELL')", name="ck_signal_direction"),
        CheckConstraint("status IN ('OPEN', 'CLOSED')", name="ck_signal_status"),
        Index("idx_signal_asset_strategy_ts", "asset", "strategy_name", "signal_timestamp"),
        Index("idx_signal_status_compound", "asset", "strategy_name", "status"),
        Index("idx_signal_status", "status"),
    )


class OpenPosition(Base):
    __tablename__ = "open_positions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    asset = Column(Text, nullable=False)
    strategy_name = Column(Text, nullable=False)
    direction = Column(Text, nullable=False)
    entry_price = Column(Float, nullable=False)
    atr_at_entry = Column(Float, nullable=False)
    highest_price_since_entry = Column(Float)
    lowest_price_since_entry = Column(Float)
    opened_at = Column(Text, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("asset", "strategy_name", name="uq_open_position_asset_strategy"),
        CheckConstraint("direction IN ('BUY', 'SELL')", name="ck_position_direction"),
        Index("idx_open_position_asset_strategy", "asset", "strategy_name"),
        Index("idx_open_position_asset", "asset"),
    )


class APIUsageLog(Base):
    __tablename__ = "api_usage_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    endpoint = Column(Text, nullable=False)
    credits_used = Column(Integer, default=1)
    timestamp = Column(Text, server_default=func.now())

    __table_args__ = (
        Index("idx_api_usage_log_timestamp", "timestamp"),
        Index("idx_api_usage_log_endpoint", "endpoint"),
    )


class CacheMetadata(Base):
    __tablename__ = "cache_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    asset = Column(Text, nullable=False)
    timeframe = Column(Text, nullable=False)
    last_fetched = Column(Text, nullable=False)
    last_candle_close = Column(Text)

    __table_args__ = (
        UniqueConstraint("asset", "timeframe", name="uq_cache_asset_tf"),
    )


class AppSetting(Base):
    __tablename__ = "app_settings"

    key = Column(Text, primary_key=True)
    value = Column(Text, nullable=False)
    updated_at = Column(Text, server_default=func.now(), onupdate=func.now())


class AdminUser(Base):
    __tablename__ = "admin_users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(Text, nullable=False, unique=True)
    password_hash = Column(Text, nullable=False)
    created_at = Column(Text, server_default=func.now())


class AdminSession(Base):
    __tablename__ = "admin_sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    token = Column(Text, nullable=False, unique=True)
    user_id = Column(Integer, ForeignKey("admin_users.id", ondelete="CASCADE"), nullable=False)
    expires_at = Column(Text, nullable=False)
    created_at = Column(Text, server_default=func.now())

    __table_args__ = (
        Index("idx_admin_sessions_token", "token"),
    )
