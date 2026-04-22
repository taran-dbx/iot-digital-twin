"""Sensor data query endpoints: ingest, timeseries, sensors, data, aggregates, latest, streaming-metrics, dbsql-timeseries."""

from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import APIRouter, Query, Request, Depends
from fastapi.responses import JSONResponse

from core.auth import get_current_user
from core.state import AppState, get_app_state
from services.simulator_service import broadcast
from services.dbsql_service import (
    validate_catalog,
    resolve_warehouse_id,
    execute_dbsql,
    get_pipeline_catalog,
)

logger = logging.getLogger("iot_digital_twin.routes.sensor_data")
router = APIRouter()


@router.post("/api/sss/ingest")
async def sss_ingest(request: Request, state: AppState = Depends(get_app_state)):
    """Receive aggregated sensor data from the SSS Databricks Job."""
    try:
        data = await request.json()
        if not isinstance(data, list):
            data = [data]
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    state.sources["sss"].status = "active"
    state.sources["sss"].error = None
    t0 = time.time()

    e2e_ms = None
    ts_fields = [d.get("window_end") or d.get("timestamp") for d in data if d.get("window_end") or d.get("timestamp")]
    if ts_fields:
        import datetime as _dt
        now = time.time()
        epoch_vals = []
        for ts in ts_fields:
            if isinstance(ts, (int, float)):
                epoch_vals.append(float(ts))
            elif isinstance(ts, str):
                try:
                    epoch_vals.append(_dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
                except Exception:
                    pass
        if epoch_vals:
            avg_ts = sum(epoch_vals) / len(epoch_vals)
            e2e_ms = (now - avg_ts) * 1000

    await broadcast(state, {
        "type": "sensor_update",
        "source": "sss",
        "sensors": data,
        "write_latency_ms": 0,
        "e2e_latency_ms": round(e2e_ms, 1) if e2e_ms is not None else 0,
    })

    write_ms = (time.time() - t0) * 1000
    state.sources["sss"].record_write(len(data), write_ms, e2e_ms)

    return {"status": "ok", "records": len(data)}


@router.get("/api/sss/aggregates")
async def sss_aggregates(
    minutes: int = Query(30, ge=1, le=1440, description="Lookback window in minutes"),
    state: AppState = Depends(get_app_state),
):
    """Return recent windowed aggregates from the sensor_aggregates table."""
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    try:
        rows = await state.db.fetch(
            """
            SELECT window_start, window_end, sensor_name, industry,
                   avg_value, min_value, max_value, record_count,
                   unit, sensor_type
            FROM sensor_aggregates
            WHERE window_start > NOW() - make_interval(mins => $1)
            ORDER BY window_start DESC
            LIMIT 500
            """,
            minutes,
        )
        return [dict(r) for r in rows]
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/timeseries")
async def get_timeseries(
    minutes: int = Query(5, ge=1, le=1440, description="Lookback window in minutes"),
    source: str | None = Query(None, description="Filter by source"),
    sensor_type: str | None = Query(None, description="Filter by sensor type"),
    industry: str | None = Query(None, description="Filter by industry"),
    state: AppState = Depends(get_app_state),
):
    """Unified time-series endpoint with auto-resolution."""
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})

    try:
        use_raw = minutes <= 10 or source is not None

        if use_raw and minutes <= 10:
            resolution = "raw"
            query = """
                SELECT event_time AS time, sensor_name, sensor_type, industry,
                       value, unit, fault_active
                FROM sensor_readings
                WHERE event_time > NOW() - make_interval(mins => $1)
            """
            params: list[Any] = [minutes]
            if source:
                query += f" AND source = ${len(params) + 1}"
                params.append(source)
            if sensor_type:
                query += f" AND sensor_type = ${len(params) + 1}"
                params.append(sensor_type)
            if industry:
                query += f" AND industry = ${len(params) + 1}"
                params.append(industry)
            query += " ORDER BY event_time ASC LIMIT 5000"
            rows = await state.db.fetch(query, *params)
            data = [
                {
                    "time": str(r["time"]),
                    "sensor_name": r["sensor_name"],
                    "sensor_type": r["sensor_type"],
                    "value": r["value"],
                    "unit": r.get("unit"),
                    "fault_active": r.get("fault_active", False),
                }
                for r in rows
            ]

        elif use_raw and source is not None:
            resolution = "raw_agg"
            # date_trunc interval must be a SQL literal — cannot be parameterized.
            # Use a whitelist to prevent injection.
            interval_literal = "1 minute" if minutes <= 60 else "5 minutes"
            query = f"""
                SELECT date_trunc('{interval_literal}', event_time) AS time,
                       sensor_name, sensor_type, industry,
                       AVG(value) AS value,
                       MIN(value) AS min_value,
                       MAX(value) AS max_value,
                       COUNT(*) AS sample_count
                FROM sensor_readings
                WHERE event_time > NOW() - make_interval(mins => $1)
                  AND source = $2
            """
            params = [minutes, source]
            if sensor_type:
                query += f" AND sensor_type = ${len(params) + 1}"
                params.append(sensor_type)
            if industry:
                query += f" AND industry = ${len(params) + 1}"
                params.append(industry)
            query += """
                GROUP BY time, sensor_name, sensor_type, industry
                ORDER BY time ASC
                LIMIT 5000
            """
            rows = await state.db.fetch(query, *params)
            data = [
                {
                    "time": str(r["time"]),
                    "sensor_name": r["sensor_name"],
                    "sensor_type": r["sensor_type"],
                    "value": float(r["value"]),
                    "min_value": float(r["min_value"]),
                    "max_value": float(r["max_value"]),
                    "sample_count": r["sample_count"],
                }
                for r in rows
            ]

        else:
            # Aggregate from raw sensor_readings using date_trunc.
            # Use a whitelist for the interval literal to prevent injection.
            interval_literal = "1 minute" if minutes <= 60 else "5 minutes"
            resolution = "1m" if minutes <= 60 else "5m"
            query = f"""
                SELECT date_trunc('{interval_literal}', event_time) AS time,
                       sensor_name, sensor_type, industry,
                       AVG(value) AS value,
                       MIN(value) AS min_value,
                       MAX(value) AS max_value,
                       COUNT(*) AS sample_count,
                       BOOL_OR(COALESCE(fault_active, false)) AS fault_active
                FROM sensor_readings
                WHERE event_time > NOW() - make_interval(mins => $1)
            """
            params = [minutes]
            if sensor_type:
                query += f" AND sensor_type = ${len(params) + 1}"
                params.append(sensor_type)
            if industry:
                query += f" AND industry = ${len(params) + 1}"
                params.append(industry)
            query += """
                GROUP BY time, sensor_name, sensor_type, industry
                ORDER BY time ASC
                LIMIT 5000
            """
            rows = await state.db.fetch(query, *params)
            data = [
                {
                    "time": str(r["time"]),
                    "sensor_name": r["sensor_name"],
                    "sensor_type": r["sensor_type"],
                    "value": float(r["value"]),
                    "min_value": float(r["min_value"]),
                    "max_value": float(r["max_value"]),
                    "sample_count": r["sample_count"],
                    "fault_active": r.get("fault_active", False),
                }
                for r in rows
            ]

        return {"resolution": resolution, "minutes": minutes, "count": len(data), "rows": data}

    except Exception as exc:
        logger.warning("Timeseries query failed: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/sensors")
async def list_sensors(state: AppState = Depends(get_app_state)):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    rows = await state.db.fetch(
        """
        SELECT DISTINCT ON (sensor_name)
            sensor_name, industry, sensor_type, unit, value,
            event_time, fault_active
        FROM sensor_readings
        ORDER BY sensor_name, event_time DESC
        """
    )
    return [dict(r) for r in rows]


@router.get("/api/data")
async def get_data(
    sensor: str = Query(..., description="Sensor name"),
    minutes: int = Query(5, ge=1, le=1440, description="Lookback window in minutes"),
    state: AppState = Depends(get_app_state),
):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    rows = await state.db.fetch(
        """
        SELECT event_time, value, fault_active
        FROM sensor_readings
        WHERE sensor_name = $1
          AND event_time > NOW() - make_interval(mins => $2)
        ORDER BY event_time ASC
        """,
        sensor,
        minutes,
    )
    return [dict(r) for r in rows]


@router.get("/api/aggregates")
async def get_aggregates(
    interval: str = Query("1m", description="Aggregate interval: 1m or 5m"),
    minutes: int = Query(30, ge=1, le=1440, description="Lookback window in minutes"),
    state: AppState = Depends(get_app_state),
):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    ALLOWED_VIEWS = {"1m": "sensor_1m", "5m": "sensor_5m"}
    if interval not in ALLOWED_VIEWS:
        return JSONResponse(status_code=422, content={"error": "interval must be '1m' or '5m'"})
    view = ALLOWED_VIEWS[interval]
    try:
        rows = await state.db.fetch(
            f"""
            SELECT *
            FROM {view}
            WHERE bucket > NOW() - make_interval(mins => $1)
            ORDER BY bucket DESC
            """,
            minutes,
        )
        return [dict(r) for r in rows]
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": f"Aggregate query failed: {exc}"})


@router.get("/api/latest")
async def get_latest(state: AppState = Depends(get_app_state)):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    rows = await state.db.fetch(
        """
        SELECT DISTINCT ON (sensor_name)
            sensor_name, industry, sensor_type, unit, value,
            event_time, fault_active, source
        FROM sensor_readings
        ORDER BY sensor_name, event_time DESC
        """
    )
    return [dict(r) for r in rows]


@router.get("/api/pipeline-stats")
async def get_pipeline_stats(
    minutes: int = Query(5, ge=1, le=60, description="Lookback window in minutes"),
    industry: str | None = Query(None, description="Filter by industry"),
    state: AppState = Depends(get_app_state),
):
    """Return throughput and freshness stats for the pipeline dashboard."""
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    try:
        query = """
            SELECT COUNT(*) AS total_records,
                   COUNT(*) / GREATEST(EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) / 60, 1) AS records_per_min,
                   MAX(event_time) AS latest_event,
                   MIN(event_time) AS oldest_event,
                   EXTRACT(EPOCH FROM (NOW() - MAX(event_time))) AS freshness_sec
            FROM sensor_readings
            WHERE event_time > NOW() - make_interval(mins => $1)
        """
        params: list[Any] = [minutes]
        if industry:
            query += " AND industry = $2"
            params.append(industry)

        row = await state.db.fetchrow(query, *params)
        if not row:
            return {"total_records": 0, "records_per_min": 0, "latest_event": None, "oldest_event": None, "freshness_sec": None}
        return {
            "total_records": row["total_records"],
            "records_per_min": round(float(row["records_per_min"] or 0), 1),
            "latest_event": str(row["latest_event"]) if row["latest_event"] else None,
            "oldest_event": str(row["oldest_event"]) if row["oldest_event"] else None,
            "freshness_sec": round(float(row["freshness_sec"] or 0), 1) if row["freshness_sec"] else None,
        }
    except Exception as exc:
        logger.warning("Pipeline stats query failed: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


async def _probe_lakets_available(state: AppState) -> bool:
    """Probe ``lakets._version`` — return True when LakeTS is installed.

    Returns False on any failure (missing schema, permissions, etc.) so the
    caller can gracefully surface ``LakeTS not installed`` in the response.
    """
    if not state.db_available or not state.db:
        return False
    try:
        version = await state.db.fetchval(
            "SELECT version FROM lakets._version ORDER BY installed_at DESC LIMIT 1"
        )
        return bool(version)
    except Exception as exc:  # pragma: no cover — surfaced via reason field
        logger.debug("lakets._version probe failed: %s", exc)
        return False


@router.get("/api/pipelines/{pipeline_id}/lakets/gapfill")
async def get_lakets_gapfill(
    pipeline_id: str,
    minutes: int = Query(30, ge=1, le=1440, description="Lookback window in minutes"),
    state: AppState = Depends(get_app_state),
):
    """Return 1-minute gap-filled average values per sensor via LakeTS.

    Uses ``lakets.time_bucket`` to bucket raw readings, then ``lakets.locf``
    inside a window (``LAG`` over partition) so missing buckets carry the
    previous value forward — a continuous line even when a sensor stops
    reporting for a few seconds.
    """
    if not await _probe_lakets_available(state):
        return {"rows": [], "reason": "LakeTS not installed"}

    try:
        rows = await state.db.fetch(
            """
            WITH base AS (
                SELECT lakets.time_bucket('1 minute'::interval, event_time) AS bucket,
                       sensor_name,
                       AVG(value) AS avg_value
                FROM sensor_readings
                WHERE event_time > NOW() - make_interval(mins => $1)
                GROUP BY bucket, sensor_name
            )
            SELECT bucket,
                   sensor_name,
                   lakets.locf(
                       avg_value,
                       LAG(avg_value) OVER (
                           PARTITION BY sensor_name ORDER BY bucket
                       )
                   ) AS value
            FROM base
            ORDER BY bucket ASC
            LIMIT 5000
            """,
            minutes,
        )
    except Exception as exc:
        logger.warning("LakeTS gapfill query failed for pipeline %s: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})

    data = [
        {
            "bucket": str(r["bucket"]) if r["bucket"] is not None else None,
            "sensor_name": r["sensor_name"],
            "value": float(r["value"]) if r["value"] is not None else None,
        }
        for r in rows
    ]
    return {"rows": data, "resolution": "1 min", "minutes": minutes}


@router.get("/api/pipelines/{pipeline_id}/lakets/rate")
async def get_lakets_rate(
    pipeline_id: str,
    minutes: int = Query(30, ge=1, le=1440, description="Lookback window in minutes"),
    state: AppState = Depends(get_app_state),
):
    """Return per-minute value rate-of-change per sensor via ``lakets.rate``.

    Buckets raw readings with ``lakets.time_bucket``, then invokes
    ``lakets.rate`` with the current bucket value and a ``LAG`` of the previous
    bucket value / timestamp to compute per-second change.
    """
    if not await _probe_lakets_available(state):
        return {"rows": [], "reason": "LakeTS not installed"}

    try:
        rows = await state.db.fetch(
            """
            WITH base AS (
                SELECT lakets.time_bucket('1 minute'::interval, event_time) AS bucket,
                       sensor_name,
                       AVG(value) AS avg_value
                FROM sensor_readings
                WHERE event_time > NOW() - make_interval(mins => $1)
                GROUP BY bucket, sensor_name
            )
            SELECT bucket,
                   sensor_name,
                   lakets.rate(
                       avg_value,
                       LAG(avg_value) OVER (
                           PARTITION BY sensor_name ORDER BY bucket
                       ),
                       bucket,
                       LAG(bucket) OVER (
                           PARTITION BY sensor_name ORDER BY bucket
                       )
                   ) AS value
            FROM base
            ORDER BY bucket ASC
            LIMIT 5000
            """,
            minutes,
        )
    except Exception as exc:
        logger.warning("LakeTS rate query failed for pipeline %s: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})

    data = [
        {
            "bucket": str(r["bucket"]) if r["bucket"] is not None else None,
            "sensor_name": r["sensor_name"],
            "value": float(r["value"]) if r["value"] is not None else None,
        }
        for r in rows
    ]
    return {"rows": data, "resolution": "1 min", "minutes": minutes}


@router.get("/api/pipelines/{pipeline_id}/lakets/histogram")
async def get_lakets_histogram(
    pipeline_id: str,
    sensor: str = Query(..., description="Sensor name to compute the distribution for"),
    minutes: int = Query(30, ge=1, le=1440, description="Lookback window in minutes"),
    buckets: int = Query(20, ge=2, le=200, description="Number of histogram buckets"),
    state: AppState = Depends(get_app_state),
):
    """Return a value-distribution histogram for a given sensor via LakeTS.

    Uses ``lakets.histogram(value, min, max, num_buckets)`` to assign each
    reading to a bucket index, then aggregates counts + emits each bucket's
    lower/upper numeric bounds.
    """
    if not await _probe_lakets_available(state):
        return {"rows": [], "reason": "LakeTS not installed"}

    try:
        rows = await state.db.fetch(
            """
            WITH bounds AS (
                SELECT MIN(value) AS min_v, MAX(value) AS max_v
                FROM sensor_readings
                WHERE sensor_name = $1
                  AND event_time > NOW() - make_interval(mins => $2)
            ),
            bucketed AS (
                SELECT lakets.histogram(
                           r.value,
                           b.min_v,
                           b.max_v,
                           $3::int
                       ) AS bucket_idx,
                       b.min_v,
                       b.max_v
                FROM sensor_readings r
                CROSS JOIN bounds b
                WHERE r.sensor_name = $1
                  AND r.event_time > NOW() - make_interval(mins => $2)
                  AND b.min_v IS NOT NULL
                  AND b.max_v IS NOT NULL
            )
            SELECT bucket_idx,
                   MIN(min_v) AS min_v,
                   MIN(max_v) AS max_v,
                   COUNT(*) AS cnt
            FROM bucketed
            WHERE bucket_idx IS NOT NULL
            GROUP BY bucket_idx
            ORDER BY bucket_idx ASC
            """,
            sensor,
            minutes,
            buckets,
        )
    except Exception as exc:
        logger.warning("LakeTS histogram query failed for pipeline %s: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})

    data: list[dict[str, Any]] = []
    for r in rows:
        min_v = float(r["min_v"]) if r["min_v"] is not None else 0.0
        max_v = float(r["max_v"]) if r["max_v"] is not None else 0.0
        width = (max_v - min_v) / buckets if buckets else 0.0
        bucket_idx = int(r["bucket_idx"])
        lower = min_v + width * bucket_idx
        upper = min_v + width * (bucket_idx + 1)
        data.append({
            "bucket_idx": bucket_idx,
            "lower": round(lower, 4),
            "upper": round(upper, 4),
            "count": int(r["cnt"] or 0),
        })

    return {"rows": data, "resolution": "1 min", "minutes": minutes}


@router.get("/api/pipeline/{pipeline_id}/streaming-metrics")
async def get_streaming_metrics(
    pipeline_id: str,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Query streaming_metrics table from the pipeline's UC catalog via DBSQL."""
    catalog = await get_pipeline_catalog(state.db, state.db_available, pipeline_id, user_email)
    if not catalog:
        return JSONResponse(status_code=404, content={"error": "Pipeline not found or no UC catalog"})
    if not validate_catalog(catalog):
        return JSONResponse(status_code=400, content={"error": "Invalid catalog name"})

    metrics_table = f"{catalog}.metrics.streaming_metrics"
    warehouse_id = await resolve_warehouse_id(state.db, state.db_available, user_email)
    if not warehouse_id:
        return JSONResponse(status_code=400, content={"error": "No SQL warehouse configured"})

    try:
        # Summary: extract key metrics from the JSON progress column
        summary_sql = f"""
            SELECT COUNT(*) AS total_rows,
                   MAX(timestamp) AS last_write,
                   MAX(batch_id) AS last_batch,
                   AVG(progress:inputRowsPerSecond::DOUBLE) AS avg_input_rps,
                   AVG(progress:processedRowsPerSecond::DOUBLE) AS avg_processed_rps,
                   AVG(progress:durationMs.triggerExecution::LONG) AS avg_batch_duration_ms,
                   SUM(progress:numInputRows::LONG) AS total_input_rows,
                   AVG(progress:e2eLatencyMs.p50::DOUBLE) AS avg_e2e_p50_ms,
                   AVG(progress:e2eLatencyMs.p99::DOUBLE) AS avg_e2e_p99_ms,
                   AVG(progress:processingLatencyMs.p50::DOUBLE) AS avg_processing_p50_ms,
                   AVG(progress:processingLatencyMs.p99::DOUBLE) AS avg_processing_p99_ms,
                   AVG(progress:sourceQueuingLatencyMs.p50::DOUBLE) AS avg_queuing_p50_ms,
                   AVG(progress:sourceQueuingLatencyMs.p99::DOUBLE) AS avg_queuing_p99_ms
            FROM {metrics_table}
        """
        # Recent rows: explode the JSON into useful columns
        rows_sql = f"""
            SELECT query_id, query_name, batch_id, timestamp,
                   progress:numInputRows::LONG AS num_input_rows,
                   progress:inputRowsPerSecond::DOUBLE AS input_rows_per_second,
                   progress:processedRowsPerSecond::DOUBLE AS processed_rows_per_second,
                   progress:durationMs.triggerExecution::LONG AS batch_duration_ms,
                   progress:durationMs.addBatch::LONG AS add_batch_ms,
                   progress:durationMs.latestOffset::LONG AS latest_offset_ms,
                   progress:durationMs.walCommit::LONG AS wal_commit_ms,
                   progress:sink.numOutputRows::LONG AS num_output_rows,
                   progress:e2eLatencyMs.p50::DOUBLE AS e2e_p50_ms,
                   progress:e2eLatencyMs.p99::DOUBLE AS e2e_p99_ms,
                   progress:processingLatencyMs.p50::DOUBLE AS processing_p50_ms,
                   progress:sourceQueuingLatencyMs.p50::DOUBLE AS queuing_p50_ms,
                   progress:observedMetrics AS observed_metrics,
                   progress
            FROM {metrics_table}
            ORDER BY batch_id DESC
            LIMIT 50
        """

        summary_result = await execute_dbsql(summary_sql, warehouse_id)
        rows_result = await execute_dbsql(rows_sql, warehouse_id)

        summary = summary_result["data"][0] if summary_result["data"] else {}
        return {
            "table": metrics_table,
            "summary": {
                "total_rows": int(summary.get("total_rows", 0) or 0),
                "last_write": summary.get("last_write"),
                "last_batch": int(summary.get("last_batch", 0) or 0),
                "avg_input_rps": round(float(summary.get("avg_input_rps", 0) or 0), 2),
                "avg_processed_rps": round(float(summary.get("avg_processed_rps", 0) or 0), 2),
                "avg_batch_duration_ms": round(float(summary.get("avg_batch_duration_ms", 0) or 0), 1),
                "total_input_rows": int(summary.get("total_input_rows", 0) or 0),
                "avg_e2e_p50_ms": round(float(summary.get("avg_e2e_p50_ms", 0) or 0), 1),
                "avg_e2e_p99_ms": round(float(summary.get("avg_e2e_p99_ms", 0) or 0), 1),
                "avg_processing_p50_ms": round(float(summary.get("avg_processing_p50_ms", 0) or 0), 1),
                "avg_processing_p99_ms": round(float(summary.get("avg_processing_p99_ms", 0) or 0), 1),
                "avg_queuing_p50_ms": round(float(summary.get("avg_queuing_p50_ms", 0) or 0), 1),
                "avg_queuing_p99_ms": round(float(summary.get("avg_queuing_p99_ms", 0) or 0), 1),
            },
            "rows": rows_result["data"],
        }

    except Exception as exc:
        logger.warning("Streaming metrics query failed for pipeline %s: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/pipeline/{pipeline_id}/dbsql-timeseries")
async def get_dbsql_timeseries(
    pipeline_id: str,
    minutes: int = Query(1440, ge=1, le=10080, description="Lookback window in minutes"),
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Query time-series sensor data from pipeline's UC catalog via DBSQL."""
    catalog = await get_pipeline_catalog(state.db, state.db_available, pipeline_id, user_email)
    if not catalog:
        return JSONResponse(status_code=404, content={"error": "Pipeline not found or no UC catalog"})
    if not validate_catalog(catalog):
        return JSONResponse(status_code=400, content={"error": "Invalid catalog name"})

    table = f"{catalog}.raw.sensor_data"
    warehouse_id = await resolve_warehouse_id(state.db, state.db_available, user_email)
    if not warehouse_id:
        return JSONResponse(status_code=400, content={"error": "No SQL warehouse configured"})

    # Choose aggregation interval based on lookback range
    if minutes <= 60:
        interval = "MINUTE"
    elif minutes <= 1440:
        interval = "HOUR"
    else:
        interval = "HOUR"

    try:
        sql = f"""
            SELECT DATE_TRUNC('{interval}', timestamp) AS time,
                   sensor_name, sensor_type, industry,
                   AVG(value) AS value,
                   MIN(value) AS min_value,
                   MAX(value) AS max_value,
                   COUNT(*) AS sample_count
            FROM {table}
            WHERE timestamp >= TIMESTAMPADD(MINUTE, -{minutes}, CURRENT_TIMESTAMP())
            GROUP BY 1, 2, 3, 4
            ORDER BY time ASC
            LIMIT 5000
        """
        result = await execute_dbsql(sql, warehouse_id)
        return {
            "source": "dbsql",
            "resolution": interval.lower(),
            "minutes": minutes,
            "count": result["row_count"],
            "rows": result["data"],
        }

    except Exception as exc:
        logger.warning("DBSQL timeseries query failed for pipeline %s: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})
