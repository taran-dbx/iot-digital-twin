"""Per-mode data-freshness ("is data flowing?") signal for the readiness gate.

A deployed pipeline mode is only truly ``running`` when its Databricks job is up
AND rows are actually landing in the mode's sink within a short freshness window.
This module answers *"is data flowing for (industry, mode)?"* — the signal
``services.pipeline_view`` uses to gate ``running`` vs ``starting`` (warming up).

Design contract (spec §1.10, readiness gate):

* **Defensive & fast** — on ANY error (missing table, Lakebase/DBSQL unavailable,
  timeout) the mode is treated as **not flowing** and this NEVER raises, so the
  status degrades gracefully to ``starting`` rather than 500-ing the whole list.
* **Cheap** — a single existence probe (``... WHERE event_time > now-window LIMIT 1``)
  per mode, and only for modes whose job is actually up (the caller filters).
* **Short cache** — a per-(industry, mode) TTL cache (default 15s) so polling the
  admin list every few seconds does not hammer Lakebase / DBSQL.

Per-mode sinks (spec §2.5):

* ``realtime``   → Lakebase ChronoTable ``public.sensor_readings`` on the
  per-industry branch (project ``iot-pipelines``, db ``databricks_postgres``).
* ``microbatch`` → Delta ``iot_<industry>.microbatch.sensor_data`` (via DBSQL).
* ``zerobus``    → Delta ``iot_<industry>.zerobus.iot_sensor_data`` (via DBSQL).

The canonical event/ingest timestamp column is ``event_time`` for every sink
(SSOT ``models/sensor_schema.py``).
"""

from __future__ import annotations

import logging
import time
from typing import Any, Awaitable, Callable, Mapping

from services import industry_catalog_service as cat
from services import pipeline_view as pv

logger = logging.getLogger("iot_digital_twin.services.pipeline_freshness")

# Freshness window: a mode is "flowing" if its sink has a row newer than this.
# realtime uses a 5-min trigger, so 10 min gives comfortable margin (spec §1.10).
FRESHNESS_WINDOW_MINUTES = 10

# Short TTL so a fast poll loop reuses the last probe instead of re-querying.
_CACHE_TTL_SECONDS = 15.0

# The realtime Lakebase sink (ChronoTable) — schema.table + timestamp column.
_REALTIME_TABLE = "public.sensor_readings"

# Delta sinks per DBSQL-backed mode: (schema, table). Mirrors health_checks._MODE_TABLES.
_DELTA_TABLES: dict[str, tuple[str, str]] = {
    "microbatch": ("microbatch", "sensor_data"),
    "zerobus": ("zerobus", "iot_sensor_data"),
}

_EVENT_TIME_COLUMN = "event_time"

# Injectable DBSQL runner type (keeps the checks pure + unit-testable).
RunSql = Callable[[str], Awaitable[dict]]

# Module TTL cache: (industry, mode) -> (monotonic_deadline, flowing).
_cache: dict[tuple[str, str], tuple[float, bool]] = {}


def _cache_get(industry: str, mode: str) -> bool | None:
    """Return the cached flowing verdict if still fresh, else ``None``."""
    entry = _cache.get((industry, mode))
    if entry is None:
        return None
    deadline, flowing = entry
    if time.monotonic() >= deadline:
        return None
    return flowing


def _cache_put(industry: str, mode: str, flowing: bool) -> None:
    """Store a flowing verdict with a short TTL."""
    _cache[(industry, mode)] = (time.monotonic() + _CACHE_TTL_SECONDS, flowing)


def clear_cache() -> None:
    """Drop all cached freshness verdicts (used by tests)."""
    _cache.clear()


async def _realtime_flowing(handle: Any, window_minutes: int) -> bool:
    """True when the Lakebase ``sensor_readings`` sink has a row inside the window.

    Uses a cheap ``LIMIT 1`` existence probe on the ``event_time`` index. Never
    raises — any error is treated as not-flowing.
    """
    sql = (
        f"SELECT 1 FROM {_REALTIME_TABLE} "
        f"WHERE {_EVENT_TIME_COLUMN} > NOW() - make_interval(mins => $1) LIMIT 1"
    )
    try:
        row = await handle.fetchval(sql, window_minutes)
        return row is not None
    except Exception as exc:
        logger.info("realtime freshness probe failed (treating as not flowing): %s", exc)
        return False


async def _dbsql_flowing(
    run_sql: RunSql, industry: str, mode: str, window_minutes: int
) -> bool:
    """True when the mode's Delta sink has a row inside the window (via DBSQL).

    Cheap ``LIMIT 1`` existence probe; never raises — any error (missing table,
    warehouse timeout) is treated as not-flowing.
    """
    schema, table = _DELTA_TABLES[mode]
    catalog = cat.catalog_name(industry)
    fqn = f"{catalog}.{schema}.{table}"
    sql = (
        f"SELECT 1 FROM {fqn} "
        f"WHERE {_EVENT_TIME_COLUMN} > current_timestamp() - INTERVAL {int(window_minutes)} MINUTES "
        f"LIMIT 1"
    )
    try:
        result = await run_sql(sql)
        return bool((result or {}).get("data"))
    except Exception as exc:
        logger.info(
            "%s freshness probe failed for %s (treating as not flowing): %s",
            mode, industry, exc,
        )
        return False


async def mode_flowing(
    state: Any,
    industry: str,
    mode: str,
    *,
    window_minutes: int = FRESHNESS_WINDOW_MINUTES,
    run_sql: RunSql | None = None,
    use_cache: bool = True,
) -> bool:
    """Return whether ``mode`` for ``industry`` has fresh rows in its sink.

    Defensive: resolving the Lakebase handle / warehouse or querying may fail;
    every failure path returns ``False`` (not flowing) and never raises.
    """
    if not industry or not mode:
        return False
    if use_cache:
        cached = _cache_get(industry, mode)
        if cached is not None:
            return cached

    flowing = False
    try:
        if mode == "realtime":
            from services.pipeline_db import get_industry_lakebase

            handle = await get_industry_lakebase(state, industry)
            if handle is not None:
                flowing = await _realtime_flowing(handle, window_minutes)
        elif mode in _DELTA_TABLES:
            if run_sql is not None:
                flowing = await _dbsql_flowing(run_sql, industry, mode, window_minutes)
    except Exception as exc:  # pragma: no cover - blanket safety net
        logger.warning("freshness check for %s/%s failed: %s", industry, mode, exc)
        flowing = False

    if use_cache:
        _cache_put(industry, mode, flowing)
    return flowing


def _make_run_sql(setup: Mapping) -> RunSql | None:
    """Build a warehouse-bound DBSQL runner for a setup, or ``None`` if no warehouse."""
    warehouse_id = setup.get("warehouse_id")
    if not warehouse_id:
        return None
    from services.dbsql_service import execute_dbsql

    async def run_sql(sql: str) -> dict:
        return await execute_dbsql(sql, warehouse_id)

    return run_sql


def _mode_job_running(deployment: Mapping, job_running: Mapping[str, Any]) -> bool:
    """True when the mode's job run is coarsely RUNNING (worth a freshness probe)."""
    job_id = deployment.get("job_id")
    if not job_id:
        return False
    running, degraded, _starting, _detail = pv._runtime(job_running.get(str(job_id)))
    return running and not degraded


async def flowing_lookup(
    state: Any,
    setup: Mapping,
    deployments: list[dict],
    job_running: Mapping[str, Any] | None = None,
    *,
    window_minutes: int = FRESHNESS_WINDOW_MINUTES,
    run_sql: RunSql | None = None,
) -> dict[str, bool]:
    """Return ``{mode: flowing}`` for the pipeline's deployed, RUNNING modes.

    Only probes modes whose job run is up (RUNNING and not degraded) — a stopped /
    starting / degraded mode never needs the flowing signal, keeping the sweep
    cheap. Never raises: a resolution failure yields ``{}`` (nothing flowing).
    """
    job_running = job_running or {}
    industry = setup.get("industry")
    if not industry:
        return {}

    to_probe = [
        d.get("mode")
        for d in (deployments or [])
        if pv.is_deployed(d)
        and d.get("mode")
        and _mode_job_running(d, job_running)
    ]
    if not to_probe:
        return {}

    if run_sql is None and any(m in _DELTA_TABLES for m in to_probe):
        run_sql = _make_run_sql(setup)

    result: dict[str, bool] = {}
    for mode in to_probe:
        result[mode] = await mode_flowing(
            state, industry, mode, window_minutes=window_minutes, run_sql=run_sql
        )
    return result
