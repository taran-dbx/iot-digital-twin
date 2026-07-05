"""Tests for routes/lakets.py — /api/lakets/sync/check."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from routes.lakets import router as lakets_router
from core.state import AppState


@pytest_asyncio.fixture()
async def client():
    app = FastAPI()
    app.include_router(lakets_router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


def _mock_lb(fetch_return=None, fetchval_return=None, fetch_side=None, fetchval_side=None) -> MagicMock:
    lb = MagicMock()
    lb.fetch = AsyncMock(return_value=fetch_return or [], side_effect=fetch_side)
    lb.fetchval = AsyncMock(return_value=fetchval_return, side_effect=fetchval_side)
    return lb


def _build_app_with_state(state: AppState) -> FastAPI:
    app = FastAPI()
    app.state.app_state = state
    app.include_router(lakets_router)
    return app


def _patch_pipeline_lakebase(lb):
    return patch("routes.lakets.get_pipeline_lakebase", AsyncMock(return_value=lb))


def _ready_conn() -> MagicMock:
    conn = MagicMock()
    conn.fetchval = AsyncMock(side_effect=[
        "databricks_postgres",  # current_database
        True,                    # wal2delta available
        "0.1.0",                 # wal2delta installed
        True,                    # lakets.enable_sync
    ])
    conn.close = AsyncMock(return_value=None)
    return conn


class TestSyncCheckRoute:
    @pytest.mark.asyncio
    async def test_direct_mode_returns_ready(self, client: AsyncClient) -> None:
        conn = _ready_conn()
        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            response = await client.post("/api/lakets/sync/check", json={
                "lakebase": {"mode": "direct", "host": "h", "user": "u", "password": "p"},
            })
        assert response.status_code == 200
        body = response.json()
        assert body["ready_to_enable"] is True
        assert body["current_database"] == "databricks_postgres"

    @pytest.mark.asyncio
    async def test_returns_warning_when_not_ready(self, client: AsyncClient) -> None:
        conn = MagicMock()
        conn.fetchval = AsyncMock(side_effect=[
            "custom_db", True, "0.1.0", True,
        ])
        conn.close = AsyncMock(return_value=None)
        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            response = await client.post("/api/lakets/sync/check", json={
                "lakebase": {"mode": "direct", "host": "h", "user": "u", "password": "p"},
            })
        body = response.json()
        assert body["ready_to_enable"] is False
        assert "databricks_postgres" in body["reason"]

    @pytest.mark.asyncio
    async def test_connection_failure_surfaces_as_not_ready(self, client: AsyncClient) -> None:
        with patch(
            "asyncpg.connect",
            new=AsyncMock(side_effect=RuntimeError("host unreachable")),
        ):
            response = await client.post("/api/lakets/sync/check", json={
                "lakebase": {"mode": "direct", "host": "h", "user": "u", "password": "p"},
            })
        body = response.json()
        assert body["ready_to_enable"] is False
        assert "host unreachable" in body["error"]

    @pytest.mark.asyncio
    async def test_autoscale_missing_project_id_surfaces_error(
        self, client: AsyncClient
    ) -> None:
        response = await client.post("/api/lakets/sync/check", json={
            "lakebase": {"mode": "autoscale"},
        })
        body = response.json()
        assert body["ready_to_enable"] is False
        assert "project_id" in body["error"].lower()

    @pytest.mark.asyncio
    async def test_invalid_json_returns_400(self, client: AsyncClient) -> None:
        response = await client.post(
            "/api/lakets/sync/check",
            content="not json",
            headers={"content-type": "application/json"},
        )
        assert response.status_code == 400


class TestCdfStatus:
    @pytest.mark.asyncio
    async def test_lb_none_returns_none_status(self) -> None:
        state = AppState()
        app = _build_app_with_state(state)
        with _patch_pipeline_lakebase(None):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as c:
                resp = await c.get("/api/pipelines/pipe-1/cdf-status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "NONE"
        assert body["enabled"] is False
        assert body["streaming"] is False

    @pytest.mark.asyncio
    async def test_streaming_when_wal2delta_reports_streaming(self) -> None:
        lb = _mock_lb(
            fetch_return=[{"status": "STREAMING"}],
            fetchval_return=4242,  # hot_rows count
        )
        state = AppState()
        app = _build_app_with_state(state)
        with _patch_pipeline_lakebase(lb):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as c:
                resp = await c.get("/api/pipelines/pipe-1/cdf-status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "STREAMING"
        assert body["enabled"] is True
        assert body["streaming"] is True
        assert body["hot_rows"] == 4242
        assert body["cold_rows"] is None

    @pytest.mark.asyncio
    async def test_snapshotting_status_enabled_not_streaming(self) -> None:
        lb = _mock_lb(
            fetch_return=[{"status": "SNAPSHOTTING"}],
            fetchval_return=10,
        )
        state = AppState()
        app = _build_app_with_state(state)
        with _patch_pipeline_lakebase(lb):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as c:
                resp = await c.get("/api/pipelines/pipe-1/cdf-status")
        body = resp.json()
        assert body["status"] == "SNAPSHOTTING"
        assert body["enabled"] is True
        assert body["streaming"] is False

    @pytest.mark.asyncio
    async def test_query_error_fails_closed_to_none(self) -> None:
        lb = _mock_lb(fetch_side=Exception('relation "wal2delta.tables" does not exist'))
        state = AppState()
        app = _build_app_with_state(state)
        with _patch_pipeline_lakebase(lb):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as c:
                resp = await c.get("/api/pipelines/pipe-1/cdf-status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "NONE"
        assert body["enabled"] is False
        assert body["streaming"] is False
        assert "reason" in body
