"""Tests for routes/sensor_data.py — focused on the LakeTS analytics endpoints."""

from __future__ import annotations

import datetime as _dt
from typing import Any, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from core.state import AppState


def _build_app_with_state(state: AppState) -> FastAPI:
    from routes.sensor_data import router as sensor_router

    app = FastAPI()
    app.state.app_state = state
    app.include_router(sensor_router)
    return app


def _mock_db(
    fetchval_return: Any = None,
    fetch_return: list | None = None,
) -> MagicMock:
    db = MagicMock()
    db.fetchval = AsyncMock(return_value=fetchval_return)
    db.fetch = AsyncMock(return_value=fetch_return or [])
    db.execute = AsyncMock(return_value=None)
    db.fetchrow = AsyncMock(return_value=None)
    return db


def _state_with_db(db: MagicMock) -> AppState:
    state = AppState()
    state.db = db
    state.db_available = True
    state.db_error = None
    return state


def _mock_pipeline_lakebase(db: MagicMock) -> MagicMock:
    """Build a MagicMock that looks like a ``PipelineLakebaseHandle``."""
    lb = MagicMock()
    lb.fetch = db.fetch
    lb.fetchrow = db.fetchrow
    lb.fetchval = db.fetchval
    lb.execute = db.execute
    return lb


def _patch_pipeline_lakebase(db: MagicMock | None):
    """Patch ``get_pipeline_lakebase`` to return a handle wrapping ``db`` (or None)."""
    handle = _mock_pipeline_lakebase(db) if db is not None else None
    return patch("routes.sensor_data.get_pipeline_lakebase", AsyncMock(return_value=handle))


# ---------------------------------------------------------------------------
# Gapfill endpoint
# ---------------------------------------------------------------------------
class TestLaketsGapfill:
    @pytest.mark.asyncio
    async def test_lakets_not_installed_returns_reason(self) -> None:
        # fetchval raises — treated as "not installed".
        db = _mock_db()
        db.fetchval = AsyncMock(
            side_effect=Exception('relation "lakets._version" does not exist')
        )
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
          async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/pipelines/pipe-1/lakets/gapfill?minutes=30")

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows"] == []
        assert body["reason"].startswith("LakeTS probe failed")
        assert "lakets._version" in body["reason"]

    @pytest.mark.asyncio
    async def test_happy_path_returns_rows_shape(self) -> None:
        bucket_a = _dt.datetime(2026, 4, 21, 10, 0, tzinfo=_dt.timezone.utc)
        bucket_b = _dt.datetime(2026, 4, 21, 10, 1, tzinfo=_dt.timezone.utc)
        fetch_rows = [
            {"bucket": bucket_a, "sensor_name": "temp-01", "value": 22.5},
            {"bucket": bucket_b, "sensor_name": "temp-01", "value": 23.1},
        ]
        db = _mock_db(fetchval_return="0.1.0", fetch_return=fetch_rows)
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
          async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/api/pipelines/pipe-1/lakets/gapfill?minutes=15"
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["resolution"] == "1 min"
        assert body["minutes"] == 15
        assert isinstance(body["rows"], list)
        assert len(body["rows"]) == 2
        first = body["rows"][0]
        assert {"bucket", "sensor_name", "value"}.issubset(first.keys())
        assert first["sensor_name"] == "temp-01"
        assert first["value"] == 22.5


# ---------------------------------------------------------------------------
# Rate endpoint
# ---------------------------------------------------------------------------
class TestLaketsRate:
    @pytest.mark.asyncio
    async def test_lakets_not_installed_returns_reason(self) -> None:
        db = _mock_db(fetchval_return=None)  # probe returns None
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
          async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/pipelines/pipe-1/lakets/rate?minutes=30")

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows"] == []
        assert "not installed" in body["reason"].lower()

    @pytest.mark.asyncio
    async def test_happy_path_returns_rows_shape(self) -> None:
        bucket = _dt.datetime(2026, 4, 21, 10, 0, tzinfo=_dt.timezone.utc)
        fetch_rows = [
            {"bucket": bucket, "sensor_name": "pressure-02", "value": 0.15},
        ]
        db = _mock_db(fetchval_return="0.1.0", fetch_return=fetch_rows)
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
          async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/pipelines/pipe-1/lakets/rate?minutes=30")

        assert resp.status_code == 200
        body = resp.json()
        assert body["minutes"] == 30
        assert body["resolution"] == "1 min"
        assert len(body["rows"]) == 1
        first = body["rows"][0]
        assert first["sensor_name"] == "pressure-02"
        assert first["value"] == 0.15


# ---------------------------------------------------------------------------
# Histogram endpoint
# ---------------------------------------------------------------------------
class TestLaketsHistogram:
    @pytest.mark.asyncio
    async def test_lakets_not_installed_returns_reason(self) -> None:
        db = _mock_db(fetchval_return=None)  # probe returns None
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
          async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/api/pipelines/pipe-1/lakets/histogram?sensor=temp-01&minutes=30"
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows"] == []
        assert "not installed" in body["reason"].lower()

    @pytest.mark.asyncio
    async def test_happy_path_returns_rows_shape(self) -> None:
        fetch_rows = [
            {"bucket_idx": 0, "min_v": 0.0, "max_v": 10.0, "cnt": 5},
            {"bucket_idx": 1, "min_v": 0.0, "max_v": 10.0, "cnt": 12},
            {"bucket_idx": 2, "min_v": 0.0, "max_v": 10.0, "cnt": 3},
        ]
        db = _mock_db(fetchval_return="0.1.0", fetch_return=fetch_rows)
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
          async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/api/pipelines/pipe-1/lakets/histogram"
                "?sensor=temp-01&minutes=30&buckets=20"
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["resolution"] == "1 min"
        assert body["minutes"] == 30
        assert len(body["rows"]) == 3
        first = body["rows"][0]
        assert {"bucket_idx", "lower", "upper", "count"}.issubset(first.keys())
        assert first["bucket_idx"] == 0
        assert first["count"] == 5
        # First bucket width is (10-0)/20 = 0.5 — lower=0.0, upper=0.5
        assert first["lower"] == 0.0
        assert first["upper"] == 0.5

    @pytest.mark.asyncio
    async def test_missing_sensor_query_param_returns_422(self) -> None:
        db = _mock_db(fetchval_return="0.1.0")
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
          async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/api/pipelines/pipe-1/lakets/histogram?minutes=30"
            )

        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Lakebase insertion-time performance endpoint
# ---------------------------------------------------------------------------
class TestLakebasePerf:
    @pytest.mark.asyncio
    async def test_lb_none_returns_graceful_zeros(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(None):  # lb resolves to None
          async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/pipelines/pipe-1/lakebase-perf?minutes=5")

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows_per_sec"] == 0
        assert body["rows_per_min"] == 0
        # No lb resolvable => no lag samples, so p50/p95 are null (not fake 0).
        assert body["p50_insert_lag_ms"] is None
        assert body["p95_insert_lag_ms"] is None
        assert body["lag_sample_count"] == 0
        assert body["series"] == []
        # Self-describing lag metadata is always present.
        assert body["lag_basis"] == "producer_event_to_lakebase_insert"
        assert "Ingest lag" in body["lag_definition"]

    @pytest.mark.asyncio
    async def test_no_lag_samples_returns_null_p50_p95(self) -> None:
        # lb resolves, rows come back, but the sss source has zero e2e samples.
        bucket_a = _dt.datetime(2026, 4, 21, 10, 0, tzinfo=_dt.timezone.utc)
        fetch_rows = [{"bucket": bucket_a, "cnt": 60}]
        db = _mock_db(fetch_return=fetch_rows)
        state = _state_with_db(db)
        # Do NOT seed any e2e latency samples on the sss source.

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
          async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/pipelines/pipe-1/lakebase-perf?minutes=5")

        assert resp.status_code == 200
        body = resp.json()
        # Rates still computed from the SQL buckets.
        assert body["rows_per_min"] == 12
        # No samples => null p50/p95 and a zero sample count (UI shows "—").
        assert body["p50_insert_lag_ms"] is None
        assert body["p95_insert_lag_ms"] is None
        assert body["lag_sample_count"] == 0
        assert body["lag_basis"] == "producer_event_to_lakebase_insert"
        assert "Ingest lag" in body["lag_definition"]

    @pytest.mark.asyncio
    async def test_happy_path_computes_rates_and_keys(self) -> None:
        bucket_a = _dt.datetime(2026, 4, 21, 10, 0, tzinfo=_dt.timezone.utc)
        bucket_b = _dt.datetime(2026, 4, 21, 10, 1, tzinfo=_dt.timezone.utc)
        # Two 1-minute buckets: 120 rows + 60 rows over a 5-minute window.
        fetch_rows = [
            {"bucket": bucket_a, "cnt": 120},
            {"bucket": bucket_b, "cnt": 60},
        ]
        db = _mock_db(fetch_return=fetch_rows)
        state = _state_with_db(db)
        # Seed rolling e2e latency metrics on the sss source.
        src = state.sources["sss"]
        for v in (100.0, 200.0, 300.0, 400.0):
            src.record_write(1, 1.0, v)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
          async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/pipelines/pipe-1/lakebase-perf?minutes=5")

        assert resp.status_code == 200
        body = resp.json()
        assert {
            "rows_per_sec",
            "rows_per_min",
            "p50_insert_lag_ms",
            "p95_insert_lag_ms",
            "lag_basis",
            "lag_definition",
            "lag_sample_count",
            "series",
        }.issubset(body.keys())
        # 180 rows over 5 minutes (300 s) => 0.6 rows/sec, 36 rows/min.
        assert body["rows_per_min"] == 36
        assert body["rows_per_sec"] == pytest.approx(0.6, abs=0.01)
        # Lag pulled from rolling e2e metrics (avg / p95).
        assert body["p50_insert_lag_ms"] > 0
        assert body["p95_insert_lag_ms"] > 0
        # Self-describing lag metadata + a non-zero sample count (4 seeded).
        assert body["lag_basis"] == "producer_event_to_lakebase_insert"
        assert "Ingest lag" in body["lag_definition"]
        assert body["lag_sample_count"] == 4
        # Series carries one entry per bucket with a per-sec rate.
        assert len(body["series"]) == 2
        first = body["series"][0]
        assert {"t", "rows_per_sec"}.issubset(first.keys())
        # 120 rows in a 60-second bucket => 2.0 rows/sec.
        assert first["rows_per_sec"] == pytest.approx(2.0, abs=0.01)

    @pytest.mark.asyncio
    async def test_query_error_returns_500(self) -> None:
        db = _mock_db()
        db.fetch = AsyncMock(side_effect=Exception("boom"))
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with _patch_pipeline_lakebase(db):
          async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/pipelines/pipe-1/lakebase-perf?minutes=5")

        assert resp.status_code == 500
        assert "error" in resp.json()


# ---------------------------------------------------------------------------
# Zerobus ingest-metrics endpoint
# ---------------------------------------------------------------------------
def _patch_zerobus_deps(
    catalog: str | None = "cat1",
    warehouse_id: str | None = "wh-123",
    source_config: dict | None = None,
):
    """Patch get_pipeline_catalog / resolve_warehouse_id / source_config lookup.

    Returns a list of context managers to enter via ``ExitStack`` / nested ``with``.
    """
    patches = [
        patch(
            "routes.sensor_data.get_pipeline_catalog",
            AsyncMock(return_value=catalog),
        ),
        patch(
            "routes.sensor_data.resolve_warehouse_id",
            AsyncMock(return_value=warehouse_id),
        ),
        patch(
            "routes.sensor_data._resolve_zerobus_target",
            AsyncMock(
                return_value=(source_config or {})
                .get("zerobus", {})
                .get("table_name")
                or f"{catalog}.raw.iot_sensor_data"
            ),
        ),
    ]
    return patches


class TestZerobusMetrics:
    @pytest.mark.asyncio
    async def test_pipeline_unknown_returns_404(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with patch(
            "routes.sensor_data.get_pipeline_catalog", AsyncMock(return_value=None)
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipelines/pipe-1/zerobus-metrics?minutes=60"
                )

        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_happy_path_computes_totals_series_status_errors(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)

        last_commit = "2026-04-21T10:05:00.000+00:00"
        # First execute_dbsql call -> ingest aggregate summary
        ingest_summary = {
            "columns": ["total_records", "total_bytes", "last_commit_time", "error_count"],
            "data": [
                {
                    "total_records": 600,
                    "total_bytes": 120000,
                    "last_commit_time": last_commit,
                    "error_count": 2,
                }
            ],
            "row_count": 1,
        }
        # Second call -> per-minute series
        ingest_series = {
            "columns": ["bucket", "records"],
            "data": [
                {"bucket": "2026-04-21T10:00:00.000+00:00", "records": 120},
                {"bucket": "2026-04-21T10:01:00.000+00:00", "records": 240},
            ],
            "row_count": 2,
        }
        # Third call -> latest stream row (open, with errors)
        stream_result = {
            "columns": ["opened_time", "closed_time", "errors"],
            "data": [
                {
                    "opened_time": "2026-04-21T09:55:00.000+00:00",
                    "closed_time": None,
                    "errors": ["transient ack timeout", "retry succeeded"],
                }
            ],
            "row_count": 1,
        }

        exec_mock = AsyncMock(side_effect=[ingest_summary, ingest_series, stream_result])

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with patch(
            "routes.sensor_data.get_pipeline_catalog", AsyncMock(return_value="cat1")
        ), patch(
            "routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value="wh-1")
        ), patch(
            "routes.sensor_data._resolve_zerobus_target",
            AsyncMock(return_value="cat1.raw.iot_sensor_data"),
        ), patch(
            "routes.sensor_data.execute_dbsql", exec_mock
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipelines/pipe-1/zerobus-metrics?minutes=60"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["table_name"] == "cat1.raw.iot_sensor_data"
        assert body["total_records"] == 600
        assert body["total_bytes"] == 120000
        # 600 records over a 60-minute window => 10 rec/min, ~0.167 rec/sec
        assert body["records_per_min"] == pytest.approx(10.0, abs=0.01)
        assert body["records_per_sec"] == pytest.approx(600 / (60 * 60), abs=0.001)
        assert body["last_commit_time"] == last_commit
        assert body["error_count"] == 2
        # Stream is open (closed_time is null).
        assert body["stream_status"] == "open"
        # Series carries one entry per minute bucket with records_per_min.
        assert len(body["series"]) == 2
        assert {"t", "records_per_min"}.issubset(body["series"][0].keys())
        assert body["series"][0]["records_per_min"] == 120.0
        assert body["series"][1]["records_per_min"] == 240.0
        # Errors flattened from the stream errors array (capped at ~5).
        assert "transient ack timeout" in body["recent_errors"]
        assert len(body["recent_errors"]) <= 5

    @pytest.mark.asyncio
    async def test_closed_stream_status(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        ingest_summary = {
            "columns": ["total_records", "total_bytes", "last_commit_time", "error_count"],
            "data": [
                {"total_records": 10, "total_bytes": 100, "last_commit_time": None, "error_count": 0}
            ],
            "row_count": 1,
        }
        ingest_series = {"columns": [], "data": [], "row_count": 0}
        stream_result = {
            "columns": ["opened_time", "closed_time", "errors"],
            "data": [
                {
                    "opened_time": "2026-04-21T09:55:00.000+00:00",
                    "closed_time": "2026-04-21T10:10:00.000+00:00",
                    "errors": None,
                }
            ],
            "row_count": 1,
        }
        exec_mock = AsyncMock(side_effect=[ingest_summary, ingest_series, stream_result])

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with patch(
            "routes.sensor_data.get_pipeline_catalog", AsyncMock(return_value="cat1")
        ), patch(
            "routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value="wh-1")
        ), patch(
            "routes.sensor_data._resolve_zerobus_target",
            AsyncMock(return_value="cat1.raw.iot_sensor_data"),
        ), patch(
            "routes.sensor_data.execute_dbsql", exec_mock
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipelines/pipe-1/zerobus-metrics?minutes=60"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["stream_status"] == "closed"
        assert body["recent_errors"] == []

    @pytest.mark.asyncio
    async def test_custom_table_name_from_source_config(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        ingest_summary = {
            "columns": ["total_records", "total_bytes", "last_commit_time", "error_count"],
            "data": [
                {"total_records": 5, "total_bytes": 50, "last_commit_time": None, "error_count": 0}
            ],
            "row_count": 1,
        }
        ingest_series = {"columns": [], "data": [], "row_count": 0}
        stream_result = {"columns": [], "data": [], "row_count": 0}
        exec_mock = AsyncMock(side_effect=[ingest_summary, ingest_series, stream_result])

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with patch(
            "routes.sensor_data.get_pipeline_catalog", AsyncMock(return_value="cat1")
        ), patch(
            "routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value="wh-1")
        ), patch(
            "routes.sensor_data._resolve_zerobus_target",
            AsyncMock(return_value="cat1.custom_schema.my_zerobus_table"),
        ), patch(
            "routes.sensor_data.execute_dbsql", exec_mock
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipelines/pipe-1/zerobus-metrics?minutes=30"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["table_name"] == "cat1.custom_schema.my_zerobus_table"
        # The fully-qualified target must be passed into the SQL filter.
        ingest_sql = exec_mock.await_args_list[0].args[0]
        assert "cat1.custom_schema.my_zerobus_table" in ingest_sql

    @pytest.mark.asyncio
    async def test_system_tables_inaccessible_returns_graceful_zeros(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        exec_mock = AsyncMock(
            side_effect=Exception(
                "INSUFFICIENT_PERMISSIONS: cannot access system.lakeflow.zerobus_ingest"
            )
        )

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with patch(
            "routes.sensor_data.get_pipeline_catalog", AsyncMock(return_value="cat1")
        ), patch(
            "routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value="wh-1")
        ), patch(
            "routes.sensor_data._resolve_zerobus_target",
            AsyncMock(return_value="cat1.raw.iot_sensor_data"),
        ), patch(
            "routes.sensor_data.execute_dbsql", exec_mock
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipelines/pipe-1/zerobus-metrics?minutes=60"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["table_name"] == "cat1.raw.iot_sensor_data"
        assert body["total_records"] == 0
        assert body["total_bytes"] == 0
        assert body["records_per_sec"] == 0
        assert body["records_per_min"] == 0
        assert body["last_commit_time"] is None
        assert body["stream_status"] == "none"
        assert body["error_count"] == 0
        assert body["series"] == []
        assert body["recent_errors"] == []
        assert "reason" in body

    @pytest.mark.asyncio
    async def test_no_rows_returns_graceful_zeros_with_reason(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        # Ingest summary returns no data rows (no ingests in window).
        empty_summary = {"columns": [], "data": [], "row_count": 0}
        empty_series = {"columns": [], "data": [], "row_count": 0}
        empty_stream = {"columns": [], "data": [], "row_count": 0}
        exec_mock = AsyncMock(side_effect=[empty_summary, empty_series, empty_stream])

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with patch(
            "routes.sensor_data.get_pipeline_catalog", AsyncMock(return_value="cat1")
        ), patch(
            "routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value="wh-1")
        ), patch(
            "routes.sensor_data._resolve_zerobus_target",
            AsyncMock(return_value="cat1.raw.iot_sensor_data"),
        ), patch(
            "routes.sensor_data.execute_dbsql", exec_mock
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipelines/pipe-1/zerobus-metrics?minutes=60"
                )

        assert resp.status_code == 200
        body = resp.json()
        assert body["total_records"] == 0
        assert body["stream_status"] == "none"
        assert body["series"] == []
        assert body["recent_errors"] == []
        assert "reason" in body

    @pytest.mark.asyncio
    async def test_no_warehouse_returns_400(self) -> None:
        db = _mock_db()
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        with patch(
            "routes.sensor_data.get_pipeline_catalog", AsyncMock(return_value="cat1")
        ), patch(
            "routes.sensor_data.resolve_warehouse_id", AsyncMock(return_value="")
        ), patch(
            "routes.sensor_data._resolve_zerobus_target",
            AsyncMock(return_value="cat1.raw.iot_sensor_data"),
        ):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    "/api/pipelines/pipe-1/zerobus-metrics?minutes=60"
                )

        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# /api/sss/aggregates — graceful when the sensor_aggregates table is missing
# ---------------------------------------------------------------------------
class _UndefinedTable(Exception):
    """Stand-in for asyncpg UndefinedTableError (SQLSTATE 42P01)."""

    sqlstate = "42P01"


@pytest.mark.asyncio
class TestSssAggregates:
    async def test_missing_table_returns_empty_not_500(self) -> None:
        db = _mock_db()
        db.fetch = AsyncMock(
            side_effect=_UndefinedTable('relation "sensor_aggregates" does not exist')
        )
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/sss/aggregates?minutes=30")
        assert resp.status_code == 200
        assert resp.json() == []

    async def test_other_db_error_still_500(self) -> None:
        db = _mock_db()
        db.fetch = AsyncMock(side_effect=Exception("connection reset by peer"))
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/sss/aggregates?minutes=30")
        assert resp.status_code == 500

    async def test_happy_path_returns_rows(self) -> None:
        db = _mock_db(fetch_return=[{"sensor_name": "s1", "avg_value": 1.5}])
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/sss/aggregates?minutes=30")
        assert resp.status_code == 200
        assert resp.json() == [{"sensor_name": "s1", "avg_value": 1.5}]

    async def test_503_when_db_unavailable(self) -> None:
        state = AppState()
        state.db = None
        state.db_available = False
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/sss/aggregates?minutes=30")
        assert resp.status_code == 503
