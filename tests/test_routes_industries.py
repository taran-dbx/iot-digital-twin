"""Tests for routes/industries.py — catalog + mode start/stop endpoints."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from core.state import AppState


def _build_app_with_state(state: AppState) -> FastAPI:
    from routes.industries import router as industries_router

    app = FastAPI()
    app.state.app_state = state
    app.include_router(industries_router)
    return app


def _state() -> AppState:
    state = AppState()
    state.db = MagicMock()
    state.db_available = True
    state.db_error = None
    return state


# ---------------------------------------------------------------------------
# GET /api/industries/catalog
# ---------------------------------------------------------------------------
class TestIndustriesCatalog:
    @pytest.mark.asyncio
    async def test_catalog_endpoint_returns_industries_and_is_admin(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        fake_catalog = [{"id": "pharmaceutical", "modes": []}]
        with patch(
            "routes.industries.industry_service.get_catalog",
            AsyncMock(return_value=fake_catalog),
        ), patch("routes.industries.app_permissions.is_admin", MagicMock(return_value=False)):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/industries/catalog")

        assert resp.status_code == 200
        body = resp.json()
        assert body["is_admin"] is False
        assert body["industries"][0]["id"] == "pharmaceutical"

    @pytest.mark.asyncio
    async def test_catalog_endpoint_reports_admin_true(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with patch(
            "routes.industries.industry_service.get_catalog", AsyncMock(return_value=[])
        ), patch("routes.industries.app_permissions.is_admin", MagicMock(return_value=True)):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/api/industries/catalog")

        assert resp.status_code == 200
        assert resp.json() == {"industries": [], "is_admin": True}


# ---------------------------------------------------------------------------
# POST /api/industries/{industry}/modes/{mode}/start
# ---------------------------------------------------------------------------
class TestStartIndustryMode:
    @pytest.mark.asyncio
    async def test_start_rejects_unknown_industry(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with patch(
            "routes.industries.industry_service.start_mode", AsyncMock()
        ) as start_mock:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/api/industries/banking/modes/realtime/start")

        assert resp.status_code == 400
        assert "banking" in resp.json()["error"]
        start_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_rejects_unknown_mode(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with patch(
            "routes.industries.industry_service.start_mode", AsyncMock()
        ) as start_mock:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/api/industries/pharmaceutical/modes/nightly/start"
                )

        assert resp.status_code == 400
        assert "nightly" in resp.json()["error"]
        start_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_curated_industry_and_mode_calls_service(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        fake_result = {"pipeline": "iot-pharmaceutical-realtime", "status": "starting"}
        start_mock = AsyncMock(return_value=fake_result)
        with patch("routes.industries.industry_service.start_mode", start_mock):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/api/industries/pharmaceutical/modes/realtime/start"
                )

        assert resp.status_code == 200
        assert resp.json() == fake_result
        start_mock.assert_awaited_once_with(state, "pharmaceutical", "realtime")


# ---------------------------------------------------------------------------
# POST /api/industries/{industry}/modes/{mode}/stop
# ---------------------------------------------------------------------------
class TestStopIndustryMode:
    @pytest.mark.asyncio
    async def test_stop_rejects_unknown_industry(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with patch("routes.industries.industry_service.stop_mode", AsyncMock()) as stop_mock:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/api/industries/banking/modes/realtime/stop")

        assert resp.status_code == 400
        stop_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stop_rejects_unknown_mode(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        with patch("routes.industries.industry_service.stop_mode", AsyncMock()) as stop_mock:
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/api/industries/pharmaceutical/modes/nightly/stop"
                )

        assert resp.status_code == 400
        stop_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stop_curated_industry_and_mode_calls_service(self) -> None:
        state = _state()
        app = _build_app_with_state(state)
        transport = ASGITransport(app=app)

        fake_result = {"pipeline": "iot-manufacturing-microbatch", "status": "idle"}
        stop_mock = AsyncMock(return_value=fake_result)
        with patch("routes.industries.industry_service.stop_mode", stop_mock):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/api/industries/manufacturing/modes/microbatch/stop"
                )

        assert resp.status_code == 200
        assert resp.json() == fake_result
        stop_mock.assert_awaited_once_with(state, "manufacturing", "microbatch")
