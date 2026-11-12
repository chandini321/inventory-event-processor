
import logging
from datetime import datetime, timezone

logger = logging.getLogger("inventory-scheduler")


def run_scheduled_notification():

    current_time = datetime.now(timezone.utc).isoformat()

    logger.info(
        "Scheduled inventory batch notification triggered at %s",
        current_time
    )

    notification = {
        "status": "SUCCESS",
        "message": "Inventory batch notification triggered",
        "generated_at": current_time
    }

    return notification
