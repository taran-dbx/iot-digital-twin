"""Tests for routes/industries_admin.py — admin pipeline management API.

Unit tests drive the routes through an httpx ``AsyncClient`` with the repos,
provisioning service, and teardown service mocked, plus the ``is_admin`` gate
and app-backend DB pool stubbed. A skippable live smoke test ensures the
app-backend tables against the real ``iot-app-backend`` project.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from core.state import AppState
from routes import industries_admin
from services import pipeline_setup_repo, resource_ledger, teardown_service

MOD = "routes.industries_admin"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _state(with_db: bool = True) -> AppState:
    state = AppState()
    if with_db:
        db = MagicMock()
        db.fetch = AsyncMock(return_value=[])
        db.fetchrow = AsyncMock(return_value=None)
        db.execute = AsyncMock(return_value="DELETE 1")
        state.app_backend_db = db
        state.app_backend_available = True
    return state


def _app(state: AppState) -> FastAPI:
    app = FastAPI()
    app.state.app_state = state
    app.include_router(industries_admin.router)
    return app


def _client(state: AppState) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=_app(state)), base_url="http://test")


def _admin(value: bool = True):
    return patch(f"{MOD}.app_permissions.is_admin", MagicMock(return_value=value))


# ---------------------------------------------------------------------------
# Admin gating + DB availability
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestGuards:
    async def test_non_admin_forbidden(self) -> None:
        with _admin(False):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines")
        assert resp.status_code == 403

    async def test_db_unavailable_returns_503(self) -> None:
        with _admin(True):
            async with _client(_state(with_db=False)) as c:
                resp = await c.get("/api/admin/pipelines")
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# POST /api/admin/industries/{industry}/setup
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestCreateOrUpdateSetup:
    async def test_creates_new_draft_setup(self) -> None:
        created = {"id": "s1", "industry": "pharmaceutical", "status": "draft"}
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
            AsyncMock(return_value=None),
        ), patch(
            f"{MOD}.pipeline_setup_repo.create_setup", AsyncMock(return_value=created)
        ) as create_mock:
            async with _client(_state()) as c:
                resp = await c.post(
                    "/api/admin/industries/pharmaceutical/setup",
                    json={
                        "flavor": "classic",
                        "source_type": "kafka",
                        "enabled_modes": ["realtime"],
                    },
                )
        assert resp.status_code == 200
        body = resp.json()
        assert body["created"] is True
        assert body["setup"]["id"] == "s1"
        # created with draft status + one-per-industry enforced via prior lookup
        _, kwargs = create_mock.call_args
        assert kwargs["industry"] == "pharmaceutical"
        assert kwargs["status"] == "draft"

    async def test_updates_existing_setup_in_place(self) -> None:
        existing = {"id": "s1", "industry": "pharmaceutical"}
        updated = {"id": "s1", "industry": "pharmaceutical", "flavor": "classic"}
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
            AsyncMock(return_value=existing),
        ), patch(
            f"{MOD}.pipeline_setup_repo.update_setup", AsyncMock(return_value=updated)
        ) as update_mock, patch(
            f"{MOD}.pipeline_setup_repo.create_setup", AsyncMock()
        ) as create_mock:
            async with _client(_state()) as c:
                resp = await c.post(
                    "/api/admin/industries/pharmaceutical/setup",
                    json={"flavor": "classic"},
                )
        assert resp.status_code == 200
        body = resp.json()
        assert body["created"] is False
        assert body["setup"]["flavor"] == "classic"
        # one-per-industry: no second row created
        create_mock.assert_not_awaited()
        # only non-null fields forwarded to update
        args, kwargs = update_mock.call_args
        assert args[1] == "s1"
        assert kwargs == {"flavor": "classic"}


# ---------------------------------------------------------------------------
# POST /api/admin/industries/{industry}/provision
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestProvision:
    async def test_404_when_no_setup(self) -> None:
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
            AsyncMock(return_value=None),
        ):
            async with _client(_state()) as c:
                resp = await c.post("/api/admin/industries/pharmaceutical/provision")
        assert resp.status_code == 404

    async def test_provisions_and_returns_resources(self) -> None:
        setup = {"id": "s1", "industry": "pharmaceutical", "warehouse_id": "wh1"}
        summary = {"industry": "pharmaceutical", "sections": [], "catalog": "iot_pharmaceutical"}
        resources = [{"id": "r1", "resource_type": "uc_catalog"}]
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
            AsyncMock(return_value=setup),
        ), patch(
            f"{MOD}.industry_provisioning_service.provision_industry",
            AsyncMock(return_value=summary),
        ) as prov_mock, patch(
            f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=resources)
        ):
            async with _client(_state()) as c:
                resp = await c.post("/api/admin/industries/pharmaceutical/provision")
        assert resp.status_code == 200
        body = resp.json()
        assert body["setup_id"] == "s1"
        assert body["resources"] == resources
        args, kwargs = prov_mock.call_args
        assert args[0] == "pharmaceutical"
        assert args[1] == "s1"
        assert kwargs["warehouse_id"] == "wh1"

    async def test_failed_preflight_returns_409(self) -> None:
        setup = {"id": "s1", "industry": "pharmaceutical"}
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
            AsyncMock(return_value=setup),
        ), patch(
            f"{MOD}.industry_provisioning_service.provision_industry",
            AsyncMock(side_effect=RuntimeError("external location missing")),
        ):
            async with _client(_state()) as c:
                resp = await c.post("/api/admin/industries/pharmaceutical/provision")
        assert resp.status_code == 409
        assert "external location" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# GET /api/admin/pipelines
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestListPipelines:
    async def test_lists_with_health_rollup(self) -> None:
        setups = [{"id": "s1", "industry": "pharmaceutical", "status": "deployed",
                   "enabled_modes": ["realtime"]}]
        deployments = [{"id": "d1", "mode": "realtime", "status": "running"}]
        resources = [
            {"id": "r1", "resource_type": "uc_catalog", "status": "active"},
            {"id": "r2", "resource_type": "job", "status": "active"},
        ]
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.list_setups", AsyncMock(return_value=setups)
        ), patch(
            f"{MOD}.deployment_repo.list_deployments",
            AsyncMock(return_value=deployments),
        ), patch(
            f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=resources)
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines")
        assert resp.status_code == 200
        items = resp.json()["pipelines"]
        assert len(items) == 1
        health = items[0]["health"]
        assert health["overall"] == "healthy"
        assert health["resources_healthy"] == 2
        assert health["jobs_running"] == 1


# ---------------------------------------------------------------------------
# GET /api/admin/pipelines/{setup_id}/health
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestPipelineHealth:
    async def test_404_when_setup_missing(self) -> None:
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=None)
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines/nope/health")
        assert resp.status_code == 404

    async def test_returns_grouped_resource_health(self) -> None:
        setup = {"id": "s1", "industry": "pharmaceutical", "status": "deployed",
                 "enabled_modes": ["realtime"]}
        resources = [
            {"id": "r1", "resource_type": "uc_catalog", "identifier": "iot_x",
             "status": "active", "metadata": {}},
            {"id": "r2", "resource_type": "job", "identifier": "j",
             "status": "error", "metadata": {}},
        ]
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)
        ), patch(
            f"{MOD}.deployment_repo.list_deployments", AsyncMock(return_value=[])
        ), patch(
            f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=resources)
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines/s1/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["health"]["overall"] == "degraded"
        assert body["resources"]["uc_catalog"][0]["health"] == "healthy"
        assert body["resources"]["job"][0]["health"] == "degraded"


# ---------------------------------------------------------------------------
# DELETE /api/admin/pipelines/{setup_id}
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestDeletePipeline:
    async def test_404_when_setup_missing(self) -> None:
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=None)
        ):
            async with _client(_state()) as c:
                resp = await c.delete("/api/admin/pipelines/nope")
        assert resp.status_code == 404

    async def test_tears_down_and_returns_result(self) -> None:
        setup = {"id": "s1", "industry": "pharmaceutical"}
        teardown_result = {"success": True, "setup_id": "s1", "deleted_setup": True,
                           "steps": []}
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)
        ), patch(
            f"{MOD}.teardown_service.teardown_setup",
            AsyncMock(return_value=teardown_result),
        ) as td_mock:
            async with _client(_state()) as c:
                resp = await c.delete("/api/admin/pipelines/s1")
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        td_mock.assert_awaited_once()


# ---------------------------------------------------------------------------
# Health rollup unit tests
# ---------------------------------------------------------------------------
class TestRollupHealth:
    def test_draft_setup_reports_draft(self) -> None:
        out = industries_admin._rollup_health({"status": "draft"}, [], [])
        assert out["overall"] == "draft"

    def test_no_resources_is_unknown(self) -> None:
        out = industries_admin._rollup_health({"status": "deployed"}, [], [])
        assert out["overall"] == "unknown"

    def test_all_active_is_healthy(self) -> None:
        res = [{"resource_type": "uc_catalog", "status": "active"}]
        out = industries_admin._rollup_health({"status": "deployed"}, [], res)
        assert out["overall"] == "healthy"

    def test_mixed_is_degraded(self) -> None:
        res = [
            {"resource_type": "uc_catalog", "status": "active"},
            {"resource_type": "job", "status": "error"},
        ]
        out = industries_admin._rollup_health({"status": "deployed"}, [], res)
        assert out["overall"] == "degraded"

    def test_none_active_is_down(self) -> None:
        res = [{"resource_type": "job", "status": "error"}]
        out = industries_admin._rollup_health({"status": "deployed"}, [], res)
        assert out["overall"] == "down"

    def test_deleted_resources_ignored(self) -> None:
        res = [
            {"resource_type": "uc_catalog", "status": "active"},
            {"resource_type": "job", "status": "deleted"},
        ]
        out = industries_admin._rollup_health({"status": "deployed"}, [], res)
        assert out["overall"] == "healthy"
        assert out["resources_total"] == 1


# ---------------------------------------------------------------------------
# Live smoke test (skippable) — ensure tables against real iot-app-backend
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture()
async def schema_ready(app_backend_conn):
    from models import app_schema

    await app_schema.ensure_app_schema(app_backend_conn)
    return app_backend_conn


@pytest.mark.asyncio
async def test_smoke_setup_provision_list_delete_roundtrip(schema_ready) -> None:
    """End-to-end against the live project: create → list → health → delete."""
    db = schema_ready
    setup = await pipeline_setup_repo.create_setup(
        db, industry="smoke_test_industry", status="draft", enabled_modes=["realtime"]
    )
    await resource_ledger.record_resource(
        db, setup_id=setup["id"], resource_type="uc_catalog",
        identifier="iot_smoke", teardown_order=10,
    )

    resources = await resource_ledger.list_resources(db, setup["id"])
    assert len(resources) == 1
    health = industries_admin._rollup_health(setup, [], resources)
    assert health["overall"] == "draft"  # status=draft

    result = await teardown_service.teardown_setup(db, setup["id"])
    assert result["deleted_setup"] is True
    assert await pipeline_setup_repo.get_setup(db, setup["id"]) is None
