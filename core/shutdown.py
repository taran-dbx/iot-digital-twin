"""Graceful shutdown — SIGTERM handler and cleanup helper."""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.state import AppState

logger = logging.getLogger("iot_digital_twin.shutdown")


def install_sigterm_handler(state: AppState) -> None:
    """Register a SIGTERM handler that sets the shutdown event."""

    def _handle_sigterm(signum: int, frame: object) -> None:
        logger.info("SIGTERM received — initiating graceful shutdown")
        state.shutdown_event.set()

    signal.signal(signal.SIGTERM, _handle_sigterm)


async def shutdown(state: AppState) -> None:
    """Cancel background tasks and close the DB pool.  Must finish within ~10 s."""
    tasks_to_cancel: list[asyncio.Task] = []

    for task in (state.refresh_task, state.retention_task, state.simulator_task):
        if task and not task.done():
            task.cancel()
            tasks_to_cancel.append(task)

    if tasks_to_cancel:
        await asyncio.wait(tasks_to_cancel, timeout=8)
        for t in tasks_to_cancel:
            if not t.done():
                logger.warning("Task %s did not finish in time", t.get_name())

    if state.db_available and state.db:
        try:
            await asyncio.wait_for(state.db.close(), timeout=2)
        except asyncio.TimeoutError:
            logger.warning("Lakebase pool close timed out")

    # Close the additive app-backend metadata pool (iot-app-backend project).
    app_backend_db = getattr(state, "app_backend_db", None)
    if app_backend_db:
        try:
            await asyncio.wait_for(app_backend_db.close(), timeout=2)
        except asyncio.TimeoutError:
            logger.warning("app-backend Lakebase pool close timed out")
        except Exception as exc:
            logger.warning("app-backend Lakebase close error: %s", exc)

    # Close per-pipeline Lakebase pools
    pipeline_dbs = getattr(state, "pipeline_dbs", {}) or {}
    for pid, handle in list(pipeline_dbs.items()):
        try:
            await asyncio.wait_for(handle.close(), timeout=2)
        except asyncio.TimeoutError:
            logger.warning("Pipeline %s Lakebase close timed out", pid)
        except Exception as exc:
            logger.warning("Pipeline %s Lakebase close error: %s", pid, exc)
