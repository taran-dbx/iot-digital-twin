"""Latency + throughput KPI endpoints for the per-mode dashboards.

Two industry-scoped endpoints feed the dashboard's latency/throughput tiles:

  * ``GET /api/latency/streaming?industry=&mode=realtime|microbatch`` reads the
    latest row of the per-mode streaming-metrics Delta table
    (``iot_<industry>.metrics.streaming_metrics_<mode>`` — see
    ``models.metrics_schema``) via DBSQL and surfaces the e2e / processing /
    queuing p50+p99 percentiles, input/processed rows-per-sec, trigger interval
    and the latest batch id/timestamp, plus a short ascending series for a
    sparkline.
  * ``GET /api/latency/zerobus?industry=`` derives ingest KPIs for the pharma
    zerobus target (``iot_<industry>.zerobus.iot_sensor_data``) from the system
    table ``system.lakeflow.zerobus_ingest``: commit freshness (seconds since
    the last commit), throughput (records/min + totals), committed bytes and
    any recent ingest error messages. Zerobus is not a Spark stream, so there
    are no p50/p99 percentiles — freshness + throughput + error count are the
    "latency/health" signals.

Both endpoints are defensive: a catalog/schema/table that hasn't been
provisioned yet, a window with no rows, or an inaccessible system table
degrades to a 200 response with a nulled/zeroed payload and a human-readable
``reason`` — never a 500. Warehouse resolution + DBSQL execution reuse the same
helpers as ``routes/sensor_data.py`` (``resolve_warehouse_id`` /
``execute_dbsql``), so DBSQL always runs as the app service principal.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from core.auth import get_current_user
from core.state import AppState, get_app_state
from models import metrics_schema
from services import industry_catalog_service as _icat
from services.dbsql_service import (
    execute_dbsql,
    resolve_warehouse_id,
    validate_catalog,
)

logger = logging.getLogger("iot_digital_twin.routes.streaming_latency")
router = APIRouter()

# Zerobus target table (schema.table) relative to the industry catalog — mirrors
# the layout in ``services/industry_catalog_service.py`` and the selection in
# ``routes/sensor_data.py::_DBSQL_MODE_TABLES``.
_ZEROBUS_TABLE = "zerobus.iot_sensor_data"


def _is_missing_table_error(exc: Exception) -> bool:
    """True when a DBSQL query failed because the target table/schema/catalog
    doesn't exist yet — "no data yet", not a server fault."""
    msg = str(exc).lower()
    return (
        "table_or_view_not_found" in msg
        or "table or view not found" in msg
        or "schema_not_found" in msg
        or "catalog_not_found" in msg
        or ("does not exist" in msg and ("table" in msg or "view" in msg or "schema" in msg))
    )


def _num(value: Any) -> float | None:
    """Coerce a DBSQL scalar to ``float`` (or ``None`` when null/blank/bad)."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    """Coerce a DBSQL scalar to ``int`` (or ``None`` when null/blank/bad)."""
    n = _num(value)
    return int(n) if n is not None else None


def _first_non_null(rows: list[dict[str, Any]], column: str) -> Any:
    """Return the first non-null value of ``column`` scanning newest-first rows.

    The metrics table can hold nulls in a given latency column for the most
    recent batch (e.g. a trigger with no input), so the latest *non-null* value
    is a truer "current" reading than blindly taking row 0.
    """
    for row in rows:
        val = row.get(column)
        if val is not None and val != "":
            return val
    return None


# ---------------------------------------------------------------------------
# Streaming latency (realtime / microbatch) — per-mode metrics Delta table
# ---------------------------------------------------------------------------
def _empty_streaming_payload(table: str, mode: str, reason: str) -> dict[str, Any]:
    """Nulled streaming-latency payload (all KPI keys present) with a reason."""
    return {
        "table": table,
        "mode": mode,
        "has_data": False,
        "last_batch_id": None,
        "last_timestamp": None,
        "num_input_rows": None,
        "input_rps": None,
        "processed_rps": None,
        "trigger_ms": None,
        "e2e_p50": None,
        "e2e_p99": None,
        "processing_p50": None,
        "processing_p99": None,
        "queuing_p50": None,
        "queuing_p99": None,
        "series": [],
        "reason": reason,
    }


@router.get("/api/latency/streaming")
async def get_streaming_latency(
    industry: str = Query(..., description="Industry id — resolves iot_<industry>"),
    mode: str = Query(..., description="Streaming mode: realtime or microbatch"),
    minutes: int = Query(
        60, ge=1, le=10080, description="Lookback window (minutes) for the sparkline series"
    ),
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Latest latency + throughput KPIs from ``streaming_metrics_<mode>``.

    Reads the latest rows of the per-mode metrics Delta table, surfacing the
    latest non-null e2e/processing/queuing p50+p99, input/processed rps, trigger
    interval, num_input_rows and the latest batch id/timestamp, plus a short
    ascending series for a sparkline. Missing table / no rows → 200 with a
    nulled payload (never 500).
    """
    normalized_mode = (mode or "").strip().lower()
    if normalized_mode not in metrics_schema.ALL_METRIC_MODES:
        return JSONResponse(
            status_code=400,
            content={
                "error": f"mode must be one of {metrics_schema.ALL_METRIC_MODES}"
            },
        )

    try:
        catalog = _icat.catalog_name(industry)
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Invalid industry"})
    if not validate_catalog(catalog):
        return JSONResponse(status_code=400, content={"error": "Invalid catalog name"})

    table = metrics_schema.metrics_table_fqn(catalog, normalized_mode)

    warehouse_id = await resolve_warehouse_id(state.db, state.db_available, user_email)
    if not warehouse_id:
        return JSONResponse(status_code=400, content={"error": "No SQL warehouse configured"})

    sql = f"""
        SELECT batch_id, timestamp, num_input_rows, input_rps, processed_rps,
               trigger_ms, e2e_p50, e2e_p99, processing_p50, processing_p99,
               queuing_p50, queuing_p99
        FROM {table}
        WHERE timestamp >= TIMESTAMPADD(MINUTE, -{int(minutes)}, CURRENT_TIMESTAMP())
        ORDER BY timestamp DESC, batch_id DESC
        LIMIT 60
    """

    try:
        result = await execute_dbsql(sql, warehouse_id)
    except Exception as exc:
        if _is_missing_table_error(exc):
            logger.info(
                "streaming latency: %s not provisioned/populated yet — returning empty",
                table,
            )
            return _empty_streaming_payload(
                table, normalized_mode, "Metrics table not provisioned yet"
            )
        # Never 500 — degrade gracefully so the dashboard tile shows "—".
        logger.warning("streaming latency query failed for %s: %s", table, exc)
        return _empty_streaming_payload(
            table,
            normalized_mode,
            f"Metrics query failed: {type(exc).__name__}: {str(exc)[:200]}",
        )

    rows = result.get("data") or []
    if not rows:
        return _empty_streaming_payload(
            table, normalized_mode, "No metrics rows in the requested window"
        )

    latest = rows[0]
    # Ascending series (oldest → newest) for the sparkline.
    series = [
        {
            "batch_id": _int(r.get("batch_id")),
            "timestamp": r.get("timestamp"),
            "e2e_p50": _num(r.get("e2e_p50")),
            "e2e_p99": _num(r.get("e2e_p99")),
            "processing_p50": _num(r.get("processing_p50")),
            "processing_p99": _num(r.get("processing_p99")),
            "input_rps": _num(r.get("input_rps")),
            "processed_rps": _num(r.get("processed_rps")),
            "trigger_ms": _num(r.get("trigger_ms")),
        }
        for r in reversed(rows)
    ]

    return {
        "table": table,
        "mode": normalized_mode,
        "has_data": True,
        "last_batch_id": _int(latest.get("batch_id")),
        "last_timestamp": latest.get("timestamp"),
        "num_input_rows": _int(_first_non_null(rows, "num_input_rows")),
        "input_rps": _num(_first_non_null(rows, "input_rps")),
        "processed_rps": _num(_first_non_null(rows, "processed_rps")),
        "trigger_ms": _num(_first_non_null(rows, "trigger_ms")),
        "e2e_p50": _num(_first_non_null(rows, "e2e_p50")),
        "e2e_p99": _num(_first_non_null(rows, "e2e_p99")),
        "processing_p50": _num(_first_non_null(rows, "processing_p50")),
        "processing_p99": _num(_first_non_null(rows, "processing_p99")),
        "queuing_p50": _num(_first_non_null(rows, "queuing_p50")),
        "queuing_p99": _num(_first_non_null(rows, "queuing_p99")),
        "series": series,
    }


# ---------------------------------------------------------------------------
# Zerobus latency/health — system.lakeflow.zerobus_ingest
# ---------------------------------------------------------------------------
def _empty_zerobus_payload(table_name: str, reason: str) -> dict[str, Any]:
    """Nulled/zeroed zerobus-latency payload with a human-readable reason."""
    return {
        "table_name": table_name,
        "has_data": False,
        "last_commit_time": None,
        "seconds_since_commit": None,
        "committed_records_total": 0,
        "committed_bytes_total": 0,
        "records_per_min": 0.0,
        "recent_error_count": 0,
        "recent_errors": [],
        "reason": reason,
    }


@router.get("/api/latency/zerobus")
async def get_zerobus_latency(
    industry: str = Query(..., description="Industry id — resolves iot_<industry>"),
    minutes: int = Query(
        60, ge=1, le=10080, description="Lookback window in minutes"
    ),
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Zerobus ingest latency/health KPIs for ``iot_<industry>.zerobus.iot_sensor_data``.

    Derives commit freshness (``seconds_since_commit`` from ``MAX(commit_time)``),
    throughput (``committed_records_total`` / ``records_per_min`` /
    ``committed_bytes_total``), a recent error count and up to 3 recent error
    messages from ``system.lakeflow.zerobus_ingest``. Inaccessible system table
    or an empty window → 200 with a zeroed payload (never 500).
    """
    try:
        catalog = _icat.catalog_name(industry)
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Invalid industry"})
    if not validate_catalog(catalog):
        return JSONResponse(status_code=400, content={"error": "Invalid catalog name"})

    target_table = f"{catalog}.{_ZEROBUS_TABLE}"

    warehouse_id = await resolve_warehouse_id(state.db, state.db_available, user_email)
    if not warehouse_id:
        return JSONResponse(status_code=400, content={"error": "No SQL warehouse configured"})

    # ``target_table`` comes from our own catalog/config, not arbitrary user
    # input, but escape single quotes anyway before it goes into the literal.
    safe_target = target_table.replace("'", "''")
    window = int(minutes)

    summary_sql = f"""
        SELECT MAX(commit_time) AS last_commit_time,
               COALESCE(SUM(committed_records), 0) AS committed_records_total,
               COALESCE(SUM(committed_bytes), 0) AS committed_bytes_total,
               COALESCE(SUM(size(COALESCE(errors, array()))), 0) AS recent_error_count,
               (unix_timestamp(now()) - unix_timestamp(MAX(commit_time))) AS seconds_since_commit
        FROM system.lakeflow.zerobus_ingest
        WHERE table_name = '{safe_target}'
          AND commit_time >= now() - INTERVAL {window} MINUTES
    """
    errors_sql = f"""
        SELECT e.error_message AS error_message
        FROM system.lakeflow.zerobus_ingest
        LATERAL VIEW explode(errors) t AS e
        WHERE table_name = '{safe_target}'
          AND commit_time >= now() - INTERVAL {window} MINUTES
          AND e.error_message IS NOT NULL
        ORDER BY e.timestamp DESC
        LIMIT 3
    """

    try:
        summary_result = await execute_dbsql(summary_sql, warehouse_id)
        errors_result = await execute_dbsql(errors_sql, warehouse_id)
    except Exception as exc:
        logger.warning("Zerobus latency query failed for %s: %s", target_table, exc)
        return _empty_zerobus_payload(
            target_table,
            f"Zerobus system tables not accessible: {type(exc).__name__}: {str(exc)[:200]}",
        )

    summary_rows = summary_result.get("data") or []
    if not summary_rows:
        return _empty_zerobus_payload(
            target_table, "No Zerobus ingest rows for this table in the requested window"
        )

    summary = summary_rows[0]
    last_commit_time = summary.get("last_commit_time")
    committed_records_total = _int(summary.get("committed_records_total")) or 0
    committed_bytes_total = _int(summary.get("committed_bytes_total")) or 0
    recent_error_count = _int(summary.get("recent_error_count")) or 0
    seconds_since_commit = _num(summary.get("seconds_since_commit"))
    records_per_min = round(committed_records_total / window, 2) if window else 0.0

    recent_errors = [
        str(r.get("error_message"))
        for r in (errors_result.get("data") or [])
        if r.get("error_message") is not None
    ][:3]

    return {
        "table_name": target_table,
        "has_data": last_commit_time is not None or committed_records_total > 0,
        "last_commit_time": last_commit_time,
        "seconds_since_commit": (
            round(seconds_since_commit, 1) if seconds_since_commit is not None else None
        ),
        "committed_records_total": committed_records_total,
        "committed_bytes_total": committed_bytes_total,
        "records_per_min": records_per_min,
        "recent_error_count": recent_error_count,
        "recent_errors": recent_errors,
    }
