"""Custom Lakebase sink for the IoT Data Simulator.

Writes sensor records to a Lakebase Autoscaling PostgreSQL database using
asyncpg.  Time-series features (partitioning, tiering, retention) are provided
by LakeTS — installed and configured through the app's provisioning flow.

Authentication modes:
  1. Databricks Apps auto-injected env vars (PGHOST, PGPASSWORD, etc.)
  2. Lakebase Autoscaling SDK with OAuth token refresh
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import asyncpg

from iot_simulator.models import SensorRecord
from iot_simulator.sinks.base import Sink

__all__ = ["LakebaseSink"]

logger = logging.getLogger("iot_simulator.sinks.lakebase")

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sensor_readings (
    event_time TIMESTAMPTZ NOT NULL,
    sensor_name TEXT NOT NULL,
    industry TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    unit TEXT,
    sensor_type TEXT,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    nominal_value DOUBLE PRECISION,
    fault_active BOOLEAN DEFAULT FALSE,
    metadata JSONB DEFAULT '{}'::jsonb,
    source TEXT DEFAULT 'app'
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_sensor_industry
ON sensor_readings (sensor_name, industry, event_time DESC);
"""

_INSERT_SQL = """
INSERT INTO sensor_readings
    (event_time, sensor_name, industry, value, unit,
     sensor_type, min_value, max_value, nominal_value,
     fault_active, metadata, source)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'lakebase')
"""


class LakebaseSink(Sink):
    """Stream sensor records to a Lakebase PostgreSQL table.

    Parameters
    ----------
    pg_host, pg_database, pg_user, pg_password, pg_port:
        Explicit connection parameters.  When *all* are ``None`` the sink
        reads ``PGHOST``, ``PGDATABASE``, ``PGUSER``, ``PGPASSWORD``,
        ``PGPORT`` from the environment (auto-injected by Databricks Apps).
    pool_min / pool_max:
        asyncpg connection-pool sizing.
    rate_hz / batch_size / **kwargs:
        Forwarded to :class:`Sink`.
    """

    def __init__(
        self,
        *,
        pg_host: str | None = None,
        pg_database: str | None = None,
        pg_user: str | None = None,
        pg_password: str | None = None,
        pg_port: int | None = None,
        pool_min: int = 2,
        pool_max: int = 8,
        rate_hz: float | None = None,
        batch_size: int = 100,
        **kwargs: Any,
    ) -> None:
        super().__init__(rate_hz=rate_hz, batch_size=batch_size, **kwargs)
        self._pg_host = pg_host or os.getenv("PGHOST")
        self._pg_database = pg_database or os.getenv("PGDATABASE", "databricks_postgres")
        self._pg_user = pg_user or os.getenv("PGUSER")
        self._pg_password = pg_password or os.getenv("PGPASSWORD")
        self._pg_port = pg_port or int(os.getenv("PGPORT", "5432"))
        self._pool_min = pool_min
        self._pool_max = pool_max
        self._pool: asyncpg.Pool | None = None
        self._uses_static_creds = False
        self._refresh_task: asyncio.Task | None = None

    def _generate_oauth_credentials(self) -> tuple[str, str]:
        """Generate OAuth credentials via the Databricks SDK."""
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        endpoint_name = os.getenv("LAKEBASE_ENDPOINT")
        if not endpoint_name:
            raise RuntimeError("LAKEBASE_ENDPOINT env var not set")
        pg_user = os.getenv("DATABRICKS_CLIENT_ID") or w.current_user.me().user_name
        cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
        return pg_user, cred.token

    async def connect(self) -> None:
        """Create the connection pool and ensure the target table exists."""
        # If credentials aren't set, generate via Databricks SDK
        if not self._pg_user or not self._pg_password:
            endpoint_name = os.getenv("LAKEBASE_ENDPOINT")
            if endpoint_name:
                self._pg_user, self._pg_password = self._generate_oauth_credentials()
            self._uses_static_creds = False
        else:
            self._uses_static_creds = True

        self._pool = await asyncpg.create_pool(
            host=self._pg_host,
            database=self._pg_database,
            user=self._pg_user,
            password=self._pg_password,
            port=self._pg_port,
            min_size=self._pool_min,
            max_size=self._pool_max,
            ssl="require",
        )

        async with self._pool.acquire() as conn:
            try:
                await conn.execute(_CREATE_TABLE_SQL)
                await conn.execute(_CREATE_INDEX_SQL)
            except asyncpg.InsufficientPrivilegeError:
                logger.info(
                    "sensor_readings table exists (owned by another role) — "
                    "skipping DDL, DML OK"
                )

        logger.info(
            "LakebaseSink connected - host=%s db=%s static=%s",
            self._pg_host,
            self._pg_database,
            self._uses_static_creds,
        )

        if not self._uses_static_creds:
            self._refresh_task = asyncio.create_task(self._token_refresh_loop())

    async def _token_refresh_loop(self) -> None:
        """Periodically refresh the OAuth token and recreate the pool."""
        while True:
            await asyncio.sleep(50 * 60)  # 50 minutes
            try:
                self._pg_user, self._pg_password = self._generate_oauth_credentials()
                old_pool = self._pool
                self._pool = await asyncpg.create_pool(
                    host=self._pg_host,
                    database=self._pg_database,
                    user=self._pg_user,
                    password=self._pg_password,
                    port=self._pg_port,
                    min_size=self._pool_min,
                    max_size=self._pool_max,
                    ssl="require",
                )
                if old_pool:
                    await old_pool.close()
                logger.info("LakebaseSink token refreshed and pool recreated")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("LakebaseSink token refresh failed: %s", exc)

    async def write(self, records: list[SensorRecord]) -> None:
        """Batch-insert sensor records."""
        if self._pool is None:
            raise RuntimeError("LakebaseSink is not connected")

        rows = [
            (
                datetime.fromtimestamp(r.timestamp, tz=timezone.utc),
                r.sensor_name,
                r.industry,
                r.value,
                r.unit,
                r.sensor_type,
                r.min_value,
                r.max_value,
                r.nominal_value,
                r.fault_active,
                json.dumps(r.metadata),
            )
            for r in records
        ]

        async with self._pool.acquire() as conn:
            await conn.executemany(_INSERT_SQL, rows)

        logger.debug("Inserted %d records into sensor_readings", len(records))

    async def flush(self) -> None:
        """No-op — writes are committed per batch."""

    async def close(self) -> None:
        """Cancel refresh task and close the connection pool."""
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
        logger.info("LakebaseSink closed")
