"""Sensor data query endpoints: ingest, timeseries, sensors, data, aggregates, latest, streaming-metrics, dbsql-timeseries."""

from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import APIRouter, Query, Request, Depends
from fastapi.responses import JSONResponse

from core.auth import get_current_user
from core.state import AppState, get_app_state
from services.pipeline_db import get_pipeline_lakebase, PipelineLakebaseHandle
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
    """Receive aggregated sensor data from the SSS Databricks Job.

    When the request carries an ``X-Pipeline-Id`` header, also INSERT the rows
    into that pipeline's Lakebase ``sensor_readings`` table via the app's
    per-pipeline OAuth pool (``get_pipeline_lakebase``). This shifts the
    write responsibility from Spark executors (which suffer from OAuth token
    expiry on long-lived connections) to the app, which already handles token
    rotation cleanly.
    """
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

    # If the consumer identifies a pipeline, insert rows into its Lakebase.
    pipeline_id = request.headers.get("X-Pipeline-Id") or request.headers.get("x-pipeline-id")
    lakebase_inserted = 0
    lakebase_error: str | None = None
    lb_resolved = False

    # Diagnostic: stamp every call's outcome on the sources object so we can
    # see at a glance (via /api/sources) what headers the consumer sent and
    # whether the INSERT path executed.
    import time as _t
    src = state.sources["sss"]
    # Attach counters on first use
    if not hasattr(src, "_lb_counters"):
        src._lb_counters = {"with_header": 0, "no_header": 0, "inserted": 0, "errors": 0, "last_keys": None, "last_headers": None}
    headers_snapshot = {k: v for k, v in request.headers.items() if k.lower() in ("x-pipeline-id", "content-type", "user-agent", "host")}
    src._lb_counters["last_headers"] = headers_snapshot
    if data:
        src._lb_counters["last_keys"] = list((data[0].keys() if isinstance(data[0], dict) else []))

    if not pipeline_id:
        src._lb_counters["no_header"] += 1
        msg = f"X-Pipeline-Id missing — skipped {len(data)} rows (last_keys={src._lb_counters['last_keys']})"
        src.error = msg
    else:
        src._lb_counters["with_header"] += 1
    if pipeline_id:
        try:
            lb = await get_pipeline_lakebase(state, pipeline_id)
            lb_resolved = lb is not None
            if lb is None:
                lakebase_error = "get_pipeline_lakebase returned None"
                logger.error("sss_ingest: get_pipeline_lakebase(%s) returned None", pipeline_id)
                state.sources["sss"].error = lakebase_error
            if lb is not None:
                import datetime as _dt
                # Use ingestion time as event_time so the dashboard's "last 5 min"
                # window shows live data, not timestamps from whatever point in
                # the Kafka backlog the consumer is currently chewing through.
                # The original producer timestamp is still on the record via
                # the broadcasted row field but we don't persist it separately.
                now_utc = _dt.datetime.now(_dt.timezone.utc)
                src._lb_counters["last_event_time"] = now_utc.isoformat()
                rows: list[tuple] = []
                for d in data:
                    event_time = now_utc
                    rows.append((
                        event_time,
                        d.get("sensor_name"),
                        d.get("industry"),
                        float(d.get("value")) if d.get("value") is not None else None,
                        d.get("unit"),
                        d.get("sensor_type"),
                        bool(d.get("fault_active")) if d.get("fault_active") is not None else False,
                        "sss",
                    ))
                # asyncpg: use executemany-style INSERT for batch performance
                in_txn_count = await lb.executemany(
                    """
                    INSERT INTO sensor_readings
                      (event_time, sensor_name, industry, value, unit,
                       sensor_type, fault_active, source)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    """,
                    rows,
                )
                lakebase_inserted = len(rows)
                src._lb_counters["inserted"] += lakebase_inserted
                # executemany now returns COUNT(*) AS SEEN INSIDE THE TRANSACTION
                # post-INSERT. If that is > 0 but a fresh SELECT shows 0, the
                # commit is the problem. If in_txn_count is 0 or -1, INSERT is
                # not taking effect at all inside the transaction itself.
                src._lb_counters["in_txn_count"] = in_txn_count
                # Separate pool-acquired fetchval AFTER commit, to see if
                # another connection sees the newly-committed rows.
                try:
                    current_count = await lb.fetchval(
                        "SELECT COUNT(*) FROM sensor_readings WHERE source = 'sss' AND event_time > NOW() - INTERVAL '5 minutes'"
                    )
                    src._lb_counters["last_post_verify_count_5m"] = current_count
                except Exception as _e:
                    src._lb_counters["last_post_verify_count_5m"] = f"err: {_e}"
        except Exception as exc:
            lakebase_error = f"{type(exc).__name__}: {str(exc)[:200]}"
            logger.error(
                "sss_ingest Lakebase insert failed for pipeline %s (lb_resolved=%s): %s",
                pipeline_id, lb_resolved, exc, exc_info=True,
            )
            state.sources["sss"].error = lakebase_error

    await broadcast(state, {
        "type": "sensor_update",
        "source": "sss",
        "sensors": data,
        "write_latency_ms": 0,
        "e2e_latency_ms": round(e2e_ms, 1) if e2e_ms is not None else 0,
    })

    write_ms = (time.time() - t0) * 1000
    state.sources["sss"].record_write(len(data), write_ms, e2e_ms)

    return {
        "status": "ok",
        "records": len(data),
        "lakebase_inserted": lakebase_inserted,
        **({"lakebase_error": lakebase_error} if lakebase_error else {}),
    }


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
            # date_trunc takes unit NAMES ('minute', 'hour'), not intervals.
            # For 5-min buckets we use arithmetic since date_trunc can't do it.
            if minutes <= 60:
                bucket_expr = "date_trunc('minute', event_time)"
            else:
                bucket_expr = (
                    "to_timestamp(floor(extract(epoch from event_time) / 300) * 300) "
                    "AT TIME ZONE 'UTC'"
                )
            query = f"""
                SELECT {bucket_expr} AS time,
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
            # date_trunc takes unit NAMES ('minute', 'hour'), not intervals.
            # For 5-min buckets we use arithmetic since date_trunc can't do it.
            if minutes <= 60:
                bucket_expr = "date_trunc('minute', event_time)"
                resolution = "1m"
            else:
                bucket_expr = (
                    "to_timestamp(floor(extract(epoch from event_time) / 300) * 300) "
                    "AT TIME ZONE 'UTC'"
                )
                resolution = "5m"
            query = f"""
                SELECT {bucket_expr} AS time,
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


@router.get("/api/pipelines/{pipeline_id}/stats")
async def get_pipeline_stats_scoped(
    pipeline_id: str,
    minutes: int = Query(5, ge=1, le=60, description="Lookback window in minutes"),
    state: AppState = Depends(get_app_state),
):
    """Return throughput and freshness stats for a specific pipeline's Lakebase."""
    lb = await get_pipeline_lakebase(state, pipeline_id)
    if lb is None:
        return JSONResponse(status_code=503, content={"error": "Pipeline Lakebase not resolvable"})
    try:
        row = await lb.fetchrow(
            """
            SELECT COUNT(*) AS total_records,
                   COUNT(*) / GREATEST(EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) / 60, 1) AS records_per_min,
                   MAX(event_time) AS latest_event,
                   MIN(event_time) AS oldest_event,
                   EXTRACT(EPOCH FROM (NOW() - MAX(event_time))) AS freshness_sec
            FROM sensor_readings
            WHERE event_time > NOW() - make_interval(mins => $1)
            """,
            minutes,
        )
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
        logger.warning("Pipeline %s stats query failed: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/pipelines/{pipeline_id}/timeseries")
async def get_pipeline_timeseries(
    pipeline_id: str,
    minutes: int = Query(5, ge=1, le=1440, description="Lookback window in minutes"),
    sensor_type: str | None = Query(None, description="Filter by sensor type"),
    state: AppState = Depends(get_app_state),
):
    """Pipeline-scoped timeseries — queries the pipeline's own Lakebase."""
    lb = await get_pipeline_lakebase(state, pipeline_id)
    if lb is None:
        return JSONResponse(status_code=503, content={"error": "Pipeline Lakebase not resolvable"})

    try:
        use_raw = minutes <= 10
        if use_raw:
            resolution = "raw"
            query = """
                SELECT event_time AS time, sensor_name, sensor_type, industry,
                       value, unit, fault_active
                FROM sensor_readings
                WHERE event_time > NOW() - make_interval(mins => $1)
            """
            params: list[Any] = [minutes]
            if sensor_type:
                query += f" AND sensor_type = ${len(params) + 1}"
                params.append(sensor_type)
            query += " ORDER BY event_time ASC LIMIT 5000"
            rows = await lb.fetch(query, *params)
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
        else:
            # date_trunc takes unit NAMES ('minute', 'hour'), not intervals.
            # For 5-min buckets we use arithmetic since date_trunc can't do it.
            if minutes <= 60:
                bucket_expr = "date_trunc('minute', event_time)"
                resolution = "1m"
            else:
                bucket_expr = (
                    "to_timestamp(floor(extract(epoch from event_time) / 300) * 300) "
                    "AT TIME ZONE 'UTC'"
                )
                resolution = "5m"
            query = f"""
                SELECT {bucket_expr} AS time,
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
            query += """
                GROUP BY time, sensor_name, sensor_type, industry
                ORDER BY time ASC
                LIMIT 5000
            """
            rows = await lb.fetch(query, *params)
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
        logger.warning("Pipeline %s timeseries query failed: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


# Deprecated alias — kept for backward compat with the workspace-wide
# /api/pipeline-stats caller.  New code should use /api/pipelines/:id/stats.
@router.get("/api/pipeline-stats")
async def get_pipeline_stats(
    minutes: int = Query(5, ge=1, le=60, description="Lookback window in minutes"),
    industry: str | None = Query(None, description="Filter by industry"),
    state: AppState = Depends(get_app_state),
):
    """Legacy workspace-scoped stats against app backend Lakebase (no pipeline filter)."""
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


@router.get("/api/pipelines/{pipeline_id}/lakebase-perf")
async def get_lakebase_perf(
    pipeline_id: str,
    minutes: int = Query(5, ge=1, le=60, description="Lookback window in minutes"),
    state: AppState = Depends(get_app_state),
):
    """Return insertion-time write performance for a pipeline's Lakebase.

    Throughput (``rows_per_sec`` / ``rows_per_min`` / ``series``)
    -----------------------------------------------------------
    The app stamps ``event_time = NOW()`` at ingest (see ``sss_ingest``), so
    for this table ``event_time`` *is* the Lakebase insertion time. Rows are
    bucketed by ``event_time`` per-minute (``date_trunc('minute', ...)``) over
    the lookback window. Therefore each bucket counts rows *inserted* in that
    minute (not rows produced by upstream sensors in that minute):

      * ``rows_per_sec`` = total rows in window / (minutes * 60)
      * ``rows_per_min`` = total rows in window / minutes
      * ``series[i].rows_per_sec`` = that minute-bucket's row count / 60

    Ingest lag (``p50_insert_lag_ms`` / ``p95_insert_lag_ms``)
    ---------------------------------------------------------
    The ``sensor_readings`` table has no producer-originated timestamp column,
    so a true event->insert lag cannot be computed in SQL. Instead the lag is
    surfaced from the ``sss`` source's rolling end-to-end latency window, which
    is measured at insert time in ``sss_ingest`` as
    ``e2e_ms = (now() - avg(producer event-time)) * 1000``. So the lag here is
    the **producer event-time -> Lakebase insert** latency:

      * ``p50_insert_lag_ms`` <- ``avg_e2e_latency_ms`` (rolling mean)
      * ``p95_insert_lag_ms`` <- ``p95_e2e_latency_ms`` (rolling p95)
      * ``lag_sample_count``  <- number of samples in the rolling window
      * ``lag_basis`` / ``lag_definition`` describe the above for the UI

    When the rolling window holds 0 samples (no recent ingests) p50/p95 are
    returned as ``null`` and ``lag_sample_count`` is 0, so the UI can render
    "—" rather than a misleading ``0``. When no lb is resolvable we return
    graceful zero throughput and null lag.
    """
    lag_basis = "producer_event_to_lakebase_insert"
    lag_definition = (
        "Ingest lag = now() - producer event-time, measured at Lakebase insert"
    )

    lb = await get_pipeline_lakebase(state, pipeline_id)
    if lb is None:
        return {
            "rows_per_sec": 0,
            "rows_per_min": 0,
            "p50_insert_lag_ms": None,
            "p95_insert_lag_ms": None,
            "lag_basis": lag_basis,
            "lag_definition": lag_definition,
            "lag_sample_count": 0,
            "series": [],
        }

    try:
        rows = await lb.fetch(
            """
            SELECT date_trunc('minute', event_time) AS bucket,
                   COUNT(*) AS cnt
            FROM sensor_readings
            WHERE event_time > NOW() - make_interval(mins => $1)
            GROUP BY bucket
            ORDER BY bucket ASC
            LIMIT 5000
            """,
            minutes,
        )
    except Exception as exc:
        logger.warning("Pipeline %s lakebase-perf query failed: %s", pipeline_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})

    total = sum(int(r["cnt"] or 0) for r in rows)
    window_sec = max(minutes * 60, 1)
    rows_per_sec = total / window_sec
    rows_per_min = round(total / minutes) if minutes else 0

    # Each per-minute bucket holds `cnt` rows over 60 seconds.
    series = [
        {
            "t": str(r["bucket"]) if r["bucket"] is not None else None,
            "rows_per_sec": round(int(r["cnt"] or 0) / 60.0, 3),
        }
        for r in rows
    ]

    # Insert-lag from the sss source's rolling e2e latency window. The window
    # measures producer event-time -> Lakebase insert latency (see docstring).
    # With 0 samples we return null (not a fake 0) so the UI can show "—".
    src = state.sources.get("sss")
    lag_sample_count = len(src.e2e_latency_samples) if src is not None else 0
    if lag_sample_count > 0:
        p50_lag: float | None = round(src.avg_e2e_latency_ms, 1)
        p95_lag: float | None = round(src.p95_e2e_latency_ms, 1)
    else:
        p50_lag = None
        p95_lag = None

    return {
        "rows_per_sec": round(rows_per_sec, 3),
        "rows_per_min": rows_per_min,
        "p50_insert_lag_ms": p50_lag,
        "p95_insert_lag_ms": p95_lag,
        "lag_basis": lag_basis,
        "lag_definition": lag_definition,
        "lag_sample_count": lag_sample_count,
        "series": series,
    }


async def _probe_lakets_available(
    lb: PipelineLakebaseHandle | None,
) -> tuple[bool, str | None]:
    """Probe ``lakets._version`` on the pipeline's Lakebase.

    On success: ``(True, None)``.
    On failure: ``(False, <human-readable reason>)`` so the caller can surface
    a specific message to the client (e.g. permissions vs missing schema).
    """
    if lb is None:
        return False, "Pipeline Lakebase not resolvable"
    try:
        version = await lb.fetchval(
            "SELECT version FROM lakets._version ORDER BY installed_at DESC LIMIT 1"
        )
        if not version:
            return False, "LakeTS not installed (lakets._version empty)"
        return True, None
    except Exception as exc:
        reason = f"LakeTS probe failed: {type(exc).__name__}: {exc}"
        logger.warning("lakets._version probe failed: %s", exc)
        return False, reason


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
    lb = await get_pipeline_lakebase(state, pipeline_id)
    installed, reason = await _probe_lakets_available(lb)
    if not installed or lb is None:
        return {"rows": [], "reason": reason or "LakeTS not installed"}

    try:
        rows = await lb.fetch(
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
    lb = await get_pipeline_lakebase(state, pipeline_id)
    installed, reason = await _probe_lakets_available(lb)
    if not installed or lb is None:
        return {"rows": [], "reason": reason or "LakeTS not installed"}

    try:
        rows = await lb.fetch(
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
    lb = await get_pipeline_lakebase(state, pipeline_id)
    installed, reason = await _probe_lakets_available(lb)
    if not installed or lb is None:
        return {"rows": [], "reason": reason or "LakeTS not installed"}

    try:
        rows = await lb.fetch(
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
    request: Request,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Query streaming_metrics table from the pipeline's UC catalog via DBSQL."""
    from services import app_permissions

    catalog = await get_pipeline_catalog(
        state.db, state.db_available, pipeline_id, user_email,
        is_admin=app_permissions.is_admin(request),
    )
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


async def _resolve_zerobus_target(
    state: AppState, pipeline_id: str, user_email: str, catalog: str,
) -> str:
    """Resolve the fully-qualified target Delta table for a Zerobus pipeline.

    Uses ``source_config.zerobus.table_name`` when present, otherwise defaults
    to ``<uc_catalog>.raw.iot_sensor_data``. The system tables store the
    fully-qualified name, so the returned value is used verbatim in the filter.
    """
    default_target = f"{catalog}.raw.iot_sensor_data"
    if not state.db_available or not state.db:
        return default_target
    try:
        import json as _json

        row = await state.db.fetchrow(
            "SELECT source_config FROM pipelines WHERE id = $1::uuid AND user_email = $2",
            pipeline_id, user_email,
        )
        if not row or row["source_config"] is None:
            return default_target
        sc = row["source_config"]
        if isinstance(sc, str):
            sc = _json.loads(sc or "{}")
        if not isinstance(sc, dict):
            return default_target
        zb = sc.get("zerobus")
        if isinstance(zb, dict):
            table_name = zb.get("table_name")
            if isinstance(table_name, str) and table_name.strip():
                return table_name.strip()
    except Exception as exc:
        logger.warning(
            "Failed to resolve zerobus target for pipeline %s: %s", pipeline_id, exc
        )
    return default_target


def _graceful_zerobus_metrics(table_name: str, reason: str) -> dict[str, Any]:
    """Return a zeroed-out metrics payload with a human-readable ``reason``."""
    return {
        "table_name": table_name,
        "total_records": 0,
        "total_bytes": 0,
        "records_per_sec": 0,
        "records_per_min": 0,
        "last_commit_time": None,
        "stream_status": "none",
        "error_count": 0,
        "series": [],
        "recent_errors": [],
        "reason": reason,
    }


@router.get("/api/pipelines/{pipeline_id}/zerobus-metrics")
async def get_zerobus_metrics(
    pipeline_id: str,
    minutes: int = Query(60, ge=1, le=10080, description="Lookback window in minutes"),
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Return Zerobus ingest metrics for a pipeline's target Delta table.

    Sourced from the Databricks system tables ``system.lakeflow.zerobus_ingest``
    and ``system.lakeflow.zerobus_stream`` via DBSQL. When those tables are not
    accessible (permissions / Beta not enabled) or hold no rows for the target,
    a graceful zeroed payload with ``stream_status:"none"`` and a ``reason`` is
    returned.
    """
    catalog = await get_pipeline_catalog(state.db, state.db_available, pipeline_id, user_email)
    if not catalog:
        return JSONResponse(status_code=404, content={"error": "Pipeline not found or no UC catalog"})
    if not validate_catalog(catalog):
        return JSONResponse(status_code=400, content={"error": "Invalid catalog name"})

    target_table = await _resolve_zerobus_target(state, pipeline_id, user_email, catalog)

    warehouse_id = await resolve_warehouse_id(state.db, state.db_available, user_email)
    if not warehouse_id:
        return JSONResponse(status_code=400, content={"error": "No SQL warehouse configured"})

    # The target table name is a literal in the WHERE clause. Escape single
    # quotes to avoid breaking the SQL string (the value comes from our own
    # config / catalog, not arbitrary user input, but be defensive anyway).
    safe_target = target_table.replace("'", "''")

    try:
        summary_sql = f"""
            SELECT COALESCE(SUM(committed_records), 0) AS total_records,
                   COALESCE(SUM(committed_bytes), 0) AS total_bytes,
                   MAX(commit_time) AS last_commit_time,
                   COALESCE(SUM(size(COALESCE(errors, array()))), 0) AS error_count
            FROM system.lakeflow.zerobus_ingest
            WHERE table_name = '{safe_target}'
              AND commit_time >= now() - INTERVAL {int(minutes)} MINUTES
        """
        series_sql = f"""
            SELECT date_trunc('minute', commit_time) AS bucket,
                   COALESCE(SUM(committed_records), 0) AS records
            FROM system.lakeflow.zerobus_ingest
            WHERE table_name = '{safe_target}'
              AND commit_time >= now() - INTERVAL {int(minutes)} MINUTES
            GROUP BY bucket
            ORDER BY bucket ASC
            LIMIT 5000
        """
        stream_sql = f"""
            SELECT opened_time, closed_time, errors
            FROM system.lakeflow.zerobus_stream
            WHERE table_name = '{safe_target}'
            ORDER BY COALESCE(closed_time, opened_time, event_time) DESC
            LIMIT 1
        """

        summary_result = await execute_dbsql(summary_sql, warehouse_id)
        series_result = await execute_dbsql(series_sql, warehouse_id)
        stream_result = await execute_dbsql(stream_sql, warehouse_id)
    except Exception as exc:
        logger.warning("Zerobus metrics query failed for pipeline %s: %s", pipeline_id, exc)
        return _graceful_zerobus_metrics(
            target_table,
            f"Zerobus system tables not accessible: {type(exc).__name__}: {str(exc)[:200]}",
        )

    summary_rows = summary_result.get("data") or []
    if not summary_rows:
        return _graceful_zerobus_metrics(
            target_table,
            "No Zerobus ingest rows for this table in the requested window",
        )

    summary = summary_rows[0]
    total_records = int(summary.get("total_records", 0) or 0)
    total_bytes = int(summary.get("total_bytes", 0) or 0)
    last_commit_time = summary.get("last_commit_time")
    error_count = int(summary.get("error_count", 0) or 0)

    window_sec = max(int(minutes) * 60, 1)
    records_per_sec = total_records / window_sec
    records_per_min = total_records / int(minutes) if minutes else 0

    series = [
        {
            "t": r.get("bucket"),
            "records_per_min": float(r.get("records", 0) or 0),
        }
        for r in (series_result.get("data") or [])
    ]

    # Stream status from the latest stream row's open/close times.
    stream_rows = stream_result.get("data") or []
    stream_status = "none"
    recent_errors: list[str] = []
    if stream_rows:
        latest = stream_rows[0]
        opened = latest.get("opened_time")
        closed = latest.get("closed_time")
        if opened and not closed:
            stream_status = "open"
        elif closed:
            stream_status = "closed"
        errors = latest.get("errors")
        if isinstance(errors, (list, tuple)):
            recent_errors = [str(e) for e in errors if e is not None][:5]

    return {
        "table_name": target_table,
        "total_records": total_records,
        "total_bytes": total_bytes,
        "records_per_sec": round(records_per_sec, 4),
        "records_per_min": round(records_per_min, 2),
        "last_commit_time": last_commit_time,
        "stream_status": stream_status,
        "error_count": error_count,
        "series": series,
        "recent_errors": recent_errors,
    }


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
