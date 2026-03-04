import json
import logging
from datetime import datetime

logger = logging.getLogger("trading_engine.services.notification_service")


def log_recovery_event(
    strategy_name: str,
    missed_window_time: str,
    assets_affected: list[str],
    status: str,
):
    from trading_engine.database import insert_recovery_notification

    execution_time = datetime.utcnow().isoformat()
    assets_json = json.dumps(assets_affected)

    record_id = insert_recovery_notification(
        strategy_name=strategy_name,
        missed_window_time=missed_window_time,
        execution_time=execution_time,
        assets_affected=assets_json,
        status=status,
    )

    logger.info(
        f"[RECOVERY-NOTIFY] Logged recovery event id={record_id} | "
        f"strategy={strategy_name} | missed_window={missed_window_time} | "
        f"assets={assets_affected} | status={status}"
    )

    return record_id
