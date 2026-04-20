"""Lakebase data retention — auto-purge sensor readings older than 1 day."""

from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger("iot_digital_twin.services.retention")

RETENTION_INTERVAL_SEC = 3600  # run every hour
RETENTION_DAYS = 1


async def run_retention_loop(db) -> None:
    """Periodically delete sensor_readings older than RETENTION_DAYS.

    Runs as a long-lived background task.  If the DB connection is lost
    the loop sleeps and retries on the next tick.
    """
    logger.info(
        "Retention service started (interval=%ds, keep=%dd)",
        RETENTION_INTERVAL_SEC,
        RETENTION_DAYS,
    )
    while True:
        await asyncio.sleep(RETENTION_INTERVAL_SEC)
        try:
            result = await db.execute(
                "DELETE FROM sensor_readings "
                "WHERE event_time < NOW() - make_interval(days => $1)",
                RETENTION_DAYS,
            )
            logger.info("Retention purge complete: %s", result)
        except Exception as exc:
            logger.warning("Retention purge failed: %s", exc)
