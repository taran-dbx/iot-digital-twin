"""Tests for routes/pipelines.py — focused on the lakebase-health endpoint."""

from __future__ import annotations

import datetime as _dt
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from core.state import AppState


def _build_app_with_state(state: AppState) -> FastAPI:
    from routes.pipelines import router as pipelines_router

    app = FastAPI()
    app.state.app_state = state
    app.include_router(pipelines_router)
    return app


def _mock_db_for_health(row: dict | None = None, raises: Exception | None = None) -> MagicMock:
    db = MagicMock()
    if raises is not None:
        db.fetchrow = AsyncMock(side_effect=raises)
    else:
        db.fetchrow = AsyncMock(return_value=row)
    db.fetch = AsyncMock(return_value=[])
    db.execute = AsyncMock(return_value=None)
    db.fetchval = AsyncMock(return_value=None)
    return db


def _state_with_db(db: MagicMock, available: bool = True) -> AppState:
    state = AppState()
    state.db = db
    state.db_available = available
    state.db_error = None
    return state


class TestLakebaseHealth:
    """GET /api/pipelines/:id/lakebase-health returns sink observability signals."""

    @pytest_asyncio.fixture()
    async def happy_client(self) -> AsyncGenerator[AsyncClient, None]:
        latest_at = _dt.datetime(2026, 4, 21, 10, 15, 30, tzinfo=_dt.timezone.utc)
        db = _mock_db_for_health(
            row={"rows_5m": 1500, "latest_at": latest_at, "latency_sec": 2.5}
        )
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client

    @pytest.mark.asyncio
    async def test_happy_path_returns_shape(self, happy_client: AsyncClient) -> None:
        resp = await happy_client.get("/api/pipelines/pipe-xyz/lakebase-health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["rows_5m"] == 1500
        assert body["latest_at"] == "2026-04-21T10:15:30+00:00"
        assert body["latency_sec"] == 2.5
        # 1500 / 300 = 5.0
        assert body["inserts_per_sec_recent"] == 5.0
        assert "error" not in body

    @pytest.mark.asyncio
    async def test_no_rows_returns_zeroes(self) -> None:
        db = _mock_db_for_health(
            row={"rows_5m": 0, "latest_at": None, "latency_sec": None}
        )
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/pipelines/pipe-xyz/lakebase-health")

        assert resp.status_code == 200
        body = resp.json()
        assert body == {
            "rows_5m": 0,
            "latest_at": None,
            "latency_sec": None,
            "inserts_per_sec_recent": 0.0,
        }

    @pytest.mark.asyncio
    async def test_db_unavailable_returns_zeroes_with_error(self) -> None:
        state = AppState()
        state.db = None
        state.db_available = False
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/pipelines/pipe-xyz/lakebase-health")

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows_5m"] == 0
        assert body["latest_at"] is None
        assert body["latency_sec"] is None
        assert body["inserts_per_sec_recent"] == 0.0
        assert "error" in body

    @pytest.mark.asyncio
    async def test_db_error_returns_zeroes_with_error(self) -> None:
        db = _mock_db_for_health(raises=RuntimeError("connection reset"))
        state = _state_with_db(db)
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/pipelines/pipe-xyz/lakebase-health")

        assert resp.status_code == 200
        body = resp.json()
        assert body["rows_5m"] == 0
        assert body["inserts_per_sec_recent"] == 0.0
        assert "connection reset" in body["error"]
