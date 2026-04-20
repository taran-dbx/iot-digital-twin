"""Tests for routes/lakets.py — /api/lakets/sync/check."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from routes.lakets import router as lakets_router


@pytest_asyncio.fixture()
async def client():
    app = FastAPI()
    app.include_router(lakets_router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


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
