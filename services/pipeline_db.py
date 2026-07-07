"""Per-pipeline Lakebase connection resolver.

Dashboard endpoints (sensor_readings queries, LakeTS analytics, lakebase-health)
must query the *pipeline's* Lakebase, not the app backend's shared Lakebase.
See ``project_per_pipeline_dashboard`` memory: each pipeline can write to its
own Lakebase target, resolved from ``source_config.lb_project_id`` /
``lb_branch_id`` / ``lb_database``.

This module provides:
- ``get_pipeline_lakebase(state, pipeline_id)``: returns a cached, OAuth-refreshing
  ``LakebaseConnectionManager`` scoped to the pipeline's Lakebase endpoint.
- Resolution rules:
    1. Look up the pipeline row in ``state.db`` (app backend).
    2. Read ``source_config.lb_project_id``, ``lb_branch_id``, ``lb_database``.
    3. Ask the Databricks SDK for the endpoint host.
    4. Instantiate a ``LakebaseConnectionManager`` bound to that host +
       ``LAKEBASE_ENDPOINT=projects/<project>/branches/<branch>/endpoints/primary``
       so OAuth credential generation hits the right project.
- Pools are cached in ``state.pipeline_dbs`` by ``pipeline_id``.

- ``get_industry_lakebase(state, industry)``: same idea, but for **industry-first**
  real-time pipelines, which have no row in the legacy ``pipelines`` table to read
  ``sink_config`` from. Their Lakebase location is deterministic instead of
  config-driven (see ``services.pipelines_project_service``):
    - project = ``PIPELINES_PROJECT`` (``"iot-pipelines"``, shared across all
      industries)
    - branch = the industry id (e.g. ``"pharmaceutical"``)
    - database = ``"databricks_postgres"``
    - endpoint = ``primary``
  Pools are cached in ``state.pipeline_dbs`` under ``f"industry:{industry}"``.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from core.state import AppState

logger = logging.getLogger("iot_digital_twin.services.pipeline_db")


async def _load_pipeline_sink_config(
    state: AppState, pipeline_id: str
) -> dict[str, Any] | None:
    """Fetch the pipeline row and return its ``source_config`` dict (or None)."""
    if not state.db_available or not state.db:
        return None
    try:
        row = await state.db.fetchrow(
            "SELECT source_config FROM pipelines WHERE id = $1::uuid", pipeline_id
        )
    except Exception as exc:
        logger.warning("Pipeline %s lookup failed: %s", pipeline_id, exc)
        return None
    if not row:
        return None
    sc = row["source_config"]
    if isinstance(sc, str):
        try:
            sc = json.loads(sc)
        except Exception:
            return None
    return sc if isinstance(sc, dict) else None


def _resolve_endpoint_host(endpoint_name: str) -> str | None:
    """Return the PG hostname for a Lakebase endpoint (``projects/.../endpoints/primary``)."""
    from databricks.sdk import WorkspaceClient

    try:
        w = WorkspaceClient()
        ep = w.postgres.get_endpoint(name=endpoint_name)
        host = ep.status.hosts.host if ep.status and ep.status.hosts else None
        return host
    except Exception as exc:
        logger.warning("Could not resolve endpoint %s: %s", endpoint_name, exc)
        return None


async def get_pipeline_lakebase(
    state: AppState, pipeline_id: str
) -> "PipelineLakebaseHandle | None":
    """Return a connection manager for the pipeline's Lakebase, or None.

    Caches the manager in ``state.pipeline_dbs`` so repeat calls reuse the
    same asyncpg pool.  Pools auto-refresh their OAuth token every 50 min
    (see ``LakebaseConnectionManager._token_refresh_loop``).
    """
    cache: dict[str, "PipelineLakebaseHandle"] = getattr(state, "pipeline_dbs", None)
    if cache is None:
        cache = {}
        state.pipeline_dbs = cache

    if pipeline_id in cache:
        return cache[pipeline_id]

    sc = await _load_pipeline_sink_config(state, pipeline_id)
    if sc is None:
        return None

    project_id = sc.get("lb_project_id") or os.getenv("LAKEBASE_PROJECT_ID", "")
    branch_id = sc.get("lb_branch_id") or "production"
    database = sc.get("lb_database") or os.getenv("PGDATABASE", "databricks_postgres")
    endpoint_name = f"projects/{project_id}/branches/{branch_id}/endpoints/primary"

    if not project_id:
        logger.warning("Pipeline %s: no lb_project_id in sink_config", pipeline_id)
        return None

    host = _resolve_endpoint_host(endpoint_name)
    if not host:
        return None

    from backend import LakebaseConnectionManager

    manager = LakebaseConnectionManager(
        pg_host=host,
        pg_database=database,
        endpoint_name=endpoint_name,
    )
    handle = PipelineLakebaseHandle(
        manager=manager, endpoint_name=endpoint_name, project_id=project_id,
        branch_id=branch_id, database=database, host=host,
    )
    try:
        await handle.initialize()
    except Exception as exc:
        logger.warning("Pipeline %s Lakebase initialize failed: %s", pipeline_id, exc)
        return None

    cache[pipeline_id] = handle
    logger.info(
        "Pipeline %s bound to Lakebase endpoint=%s host=%s db=%s",
        pipeline_id, endpoint_name, host, database,
    )
    return handle


async def get_industry_lakebase(
    state: AppState, industry: str
) -> "PipelineLakebaseHandle | None":
    """Return a connection manager for an industry-first pipeline's Lakebase, or None.

    Unlike ``get_pipeline_lakebase``, this does not read a ``pipelines`` row —
    industry-first pipelines live in ``pipeline_setup`` and have no legacy
    ``sink_config`` to read. Instead the location is deterministic: the shared
    ``PIPELINES_PROJECT`` (``iot-pipelines``), one branch per industry, database
    ``databricks_postgres``, ``primary`` endpoint (see
    ``services.pipelines_project_service.provision_industry_branch``).

    Caches the manager in ``state.pipeline_dbs`` under ``f"industry:{industry}"``
    so repeat calls reuse the same asyncpg pool. Never raises — logs and
    returns None on any resolution/connection failure so callers can degrade
    to an empty/graceful payload.
    """
    from services.pipelines_project_service import (
        PG_DATABASE,
        PIPELINES_PROJECT,
        PRIMARY_ENDPOINT_ID,
        endpoint_resource_name,
    )

    cache: dict[str, "PipelineLakebaseHandle"] = getattr(state, "pipeline_dbs", None)
    if cache is None:
        cache = {}
        state.pipeline_dbs = cache

    cache_key = f"industry:{industry}"
    if cache_key in cache:
        return cache[cache_key]

    project_id = PIPELINES_PROJECT
    branch_id = industry
    database = PG_DATABASE
    endpoint_name = endpoint_resource_name(branch_id, PRIMARY_ENDPOINT_ID, project_id)

    host = _resolve_endpoint_host(endpoint_name)
    if not host:
        logger.warning(
            "Industry %s: could not resolve Lakebase endpoint host (%s)",
            industry, endpoint_name,
        )
        return None

    from backend import LakebaseConnectionManager

    manager = LakebaseConnectionManager(
        pg_host=host,
        pg_database=database,
        endpoint_name=endpoint_name,
    )
    handle = PipelineLakebaseHandle(
        manager=manager, endpoint_name=endpoint_name, project_id=project_id,
        branch_id=branch_id, database=database, host=host,
    )
    try:
        await handle.initialize()
    except Exception as exc:
        logger.warning("Industry %s Lakebase initialize failed: %s", industry, exc)
        return None

    cache[cache_key] = handle
    logger.info(
        "Industry %s bound to Lakebase endpoint=%s host=%s db=%s",
        industry, endpoint_name, host, database,
    )
    return handle


class PipelineLakebaseHandle:
    """Wraps a ``LakebaseConnectionManager`` with the endpoint metadata.

    Exposes ``fetch``, ``fetchrow``, ``fetchval``, ``execute`` — the same
    interface the routes already use against ``state.db``.
    """

    def __init__(
        self,
        *,
        manager: Any,
        endpoint_name: str,
        project_id: str,
        branch_id: str,
        database: str,
        host: str,
    ) -> None:
        self._manager = manager
        self.endpoint_name = endpoint_name
        self.project_id = project_id
        self.branch_id = branch_id
        self.database = database
        self.host = host

    async def initialize(self) -> None:
        await self._manager.initialize()

    async def fetch(self, query: str, *args):
        return await self._manager.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        return await self._manager.fetchrow(query, *args)

    async def fetchval(self, query: str, *args):
        return await self._manager.fetchval(query, *args)

    async def execute(self, query: str, *args) -> str:
        return await self._manager.execute(query, *args)

    async def executemany(self, query: str, args: list):
        return await self._manager.executemany(query, args)

    async def close(self) -> None:
        await self._manager.close()
