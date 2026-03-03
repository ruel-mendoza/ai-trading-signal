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
    exit_price = Column(Float)
    exit_reason = Column(Text)
    signal_timestamp = Column(Text, nullable=False)
    created_at = Column(Text, server_default=func.now())
    updated_at = Column(Text, server_default=func.now(), onupdate=func.now())
    wp_post_id = Column(Integer, nullable=True)
    publish_status = Column(Text, nullable=False, server_default="PENDING")
    wp_last_sync = Column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("asset", "strategy_name", "signal_timestamp", name="uq_signal_idempotency"),
        CheckConstraint("direction IN ('BUY', 'SELL')", name="ck_signal_direction"),
        CheckConstraint("status IN ('OPEN', 'CLOSED')", name="ck_signal_status"),
        Index("idx_signal_asset_strategy_ts", "asset", "strategy_name", "signal_timestamp"),
        Index("idx_signal_status_compound", "asset", "strategy_name", "status"),
        Index("idx_signal_status", "status"),
        Index("idx_signal_status_created", "status", "created_at"),
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


class SchedulerJobLog(Base):
    __tablename__ = "scheduler_job_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Text, nullable=False)
    strategy_name = Column(Text, nullable=False)
    started_at = Column(Text, nullable=False)
    finished_at = Column(Text)
    duration_seconds = Column(Float)
    status = Column(Text, nullable=False, default="RUNNING")
    assets_evaluated = Column(Integer, default=0)
    signals_generated = Column(Integer, default=0)
    errors = Column(Integer, default=0)
    error_detail = Column(Text)

    __table_args__ = (
        CheckConstraint("status IN ('RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED')", name="ck_job_log_status"),
        Index("idx_job_log_job_id", "job_id"),
        Index("idx_job_log_started", "started_at"),
        Index("idx_job_log_strategy", "strategy_name"),
    )


class SignalMetrics(Base):
    __tablename__ = "signal_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_name = Column(Text, nullable=False)
    asset = Column(Text)
    period = Column(Text, nullable=False, default="all_time")
    total_signals = Column(Integer, default=0)
    open_signals = Column(Integer, default=0)
    closed_signals = Column(Integer, default=0)
    won = Column(Integer, default=0)
    lost = Column(Integer, default=0)
    win_rate = Column(Float, default=0.0)
    avg_gain_pct = Column(Float, default=0.0)
    avg_loss_pct = Column(Float, default=0.0)
    best_gain_pct = Column(Float)
    worst_loss_pct = Column(Float)
    avg_duration_hours = Column(Float)
    last_signal_at = Column(Text)
    computed_at = Column(Text, nullable=False)

    __table_args__ = (
        UniqueConstraint("strategy_name", "asset", "period", name="uq_signal_metrics_key"),
        Index("idx_signal_metrics_strategy", "strategy_name"),
        Index("idx_signal_metrics_period", "period"),
    )


class WordPressCredential(Base):
    __tablename__ = "wordpress_credentials"

    id = Column(Integer, primary_key=True, autoincrement=True)
    label = Column(Text, nullable=False)
    wp_url = Column(Text, nullable=False)
    wp_username = Column(Text, nullable=False)
    app_password_encrypted = Column(Text, nullable=False)
    is_active = Column(Integer, nullable=False, server_default="1")
    created_at = Column(Text, server_default=func.now())
    updated_at = Column(Text, server_default=func.now(), onupdate=func.now())
