"""Tests for routes/sensor_data.py — focused on the LakeTS analytics endpoints."""

from __future__ import annotations

import datetime as _dt
from typing import Any, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

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
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/pipelines/pipe-1/lakets/gapfill?minutes=30")

        assert resp.status_code == 200
        body = resp.json()
        assert body == {"rows": [], "reason": "LakeTS not installed"}

    @pytest.mark.asyncio
    async def test_happy_path_returns_rows_shape(self) -> None:
        bucket_a = _dt.datetime(2026, 4, 21, 10, 0, tzinfo=_dt.timezone.utc)
        bucket_b = _dt.datetime(2026, 4, 21, 10, 1, tzinfo=_dt.timezone.utc)
        fetch_rows = [
            {"bucket": bucket_a, "sensor_name": "temp-01", "value": 22.5},
            {"bucket": bucket_b, "sensor_name": "temp-01", "value": 23.1},
        ]
        db = _mock_db(fetchval_return="0.1.2", fetch_return=fetch_rows)
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
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
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/pipelines/pipe-1/lakets/rate?minutes=30")

        assert resp.status_code == 200
        body = resp.json()
        assert body == {"rows": [], "reason": "LakeTS not installed"}

    @pytest.mark.asyncio
    async def test_happy_path_returns_rows_shape(self) -> None:
        bucket = _dt.datetime(2026, 4, 21, 10, 0, tzinfo=_dt.timezone.utc)
        fetch_rows = [
            {"bucket": bucket, "sensor_name": "pressure-02", "value": 0.15},
        ]
        db = _mock_db(fetchval_return="0.1.2", fetch_return=fetch_rows)
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
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
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/api/pipelines/pipe-1/lakets/histogram?sensor=temp-01&minutes=30"
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body == {"rows": [], "reason": "LakeTS not installed"}

    @pytest.mark.asyncio
    async def test_happy_path_returns_rows_shape(self) -> None:
        fetch_rows = [
            {"bucket_idx": 0, "min_v": 0.0, "max_v": 10.0, "cnt": 5},
            {"bucket_idx": 1, "min_v": 0.0, "max_v": 10.0, "cnt": 12},
            {"bucket_idx": 2, "min_v": 0.0, "max_v": 10.0, "cnt": 3},
        ]
        db = _mock_db(fetchval_return="0.1.2", fetch_return=fetch_rows)
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
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
        db = _mock_db(fetchval_return="0.1.2")
        state = _state_with_db(db)

        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/api/pipelines/pipe-1/lakets/histogram?minutes=30"
            )

        assert resp.status_code == 422
