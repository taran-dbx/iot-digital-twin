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
from services import validation_state_machine as sm

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

    async def test_create_only_on_existing_industry_rejected_409(self) -> None:
        existing = {"id": "s1", "industry": "pharmaceutical"}
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
            AsyncMock(return_value=existing),
        ), patch(
            f"{MOD}.pipeline_setup_repo.create_setup", AsyncMock()
        ) as create_mock, patch(
            f"{MOD}.pipeline_setup_repo.update_setup", AsyncMock()
        ) as update_mock:
            async with _client(_state()) as c:
                resp = await c.post(
                    "/api/admin/industries/pharmaceutical/setup",
                    json={"create_only": True, "flavor": "classic"},
                )
        assert resp.status_code == 409
        detail = resp.json()["detail"]
        assert detail["setup_id"] == "s1"
        # neither a second create nor an in-place update happens
        create_mock.assert_not_awaited()
        update_mock.assert_not_awaited()

    async def test_create_only_409_serializes_uuid_setup_id(self) -> None:
        """Regression: real Lakebase returns the ``id`` column as a
        ``uuid.UUID``. FastAPI renders the HTTPException detail with plain
        ``json.dumps`` (no jsonable_encoder), so a raw UUID there raised
        TypeError → the intended 409 surfaced as a 500. The route must coerce
        ``setup_id`` to str so the guard returns a clean 409.
        """
        import uuid as _uuid

        setup_uuid = _uuid.UUID("6aa89e69-b749-4480-a5d6-f381b37e3dcf")
        existing = {"id": setup_uuid, "industry": "pharmaceutical"}
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
            AsyncMock(return_value=existing),
        ), patch(
            f"{MOD}.pipeline_setup_repo.create_setup", AsyncMock()
        ), patch(
            f"{MOD}.pipeline_setup_repo.update_setup", AsyncMock()
        ):
            async with _client(_state()) as c:
                resp = await c.post(
                    "/api/admin/industries/pharmaceutical/setup",
                    json={"create_only": True},
                )
        assert resp.status_code == 409
        assert resp.json()["detail"]["setup_id"] == str(setup_uuid)


# ---------------------------------------------------------------------------
# GET /api/admin/industries/{industry}/setup
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestGetSetupForIndustry:
    async def test_404_when_no_setup(self) -> None:
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
            AsyncMock(return_value=None),
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/industries/pharmaceutical/setup")
        assert resp.status_code == 404

    async def test_returns_existing_setup_with_contract(self) -> None:
        setup = {"id": "s1", "industry": "pharmaceutical", "status": "draft",
                 "enabled_modes": ["realtime"]}
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
            AsyncMock(return_value=setup),
        ), patch(
            f"{MOD}.deployment_repo.list_deployments", AsyncMock(return_value=[])
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/industries/pharmaceutical/setup")
        assert resp.status_code == 200
        body = resp.json()
        assert body["setup"]["id"] == "s1"
        assert body["pipeline"]["setup_id"] == "s1"
        assert body["pipeline"]["status"] == "draft"


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

    async def test_zerobus_only_setup_passes_enable_realtime_false(self) -> None:
        # A zerobus-only setup must NOT trigger realtime Lakebase branch
        # provisioning (which may not exist on this workspace) — the route
        # gates enable_realtime on enabled_modes.
        setup = {"id": "s1", "industry": "pharmaceutical", "warehouse_id": "wh1",
                 "enabled_modes": ["zerobus"]}
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
            AsyncMock(return_value=setup),
        ), patch(
            f"{MOD}.industry_provisioning_service.provision_industry",
            AsyncMock(return_value={"sections": []}),
        ) as prov_mock, patch(
            f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=[])
        ):
            async with _client(_state()) as c:
                resp = await c.post("/api/admin/industries/pharmaceutical/provision")
        assert resp.status_code == 200
        assert prov_mock.call_args.kwargs["enable_realtime"] is False

    async def test_realtime_setup_passes_enable_realtime_true(self) -> None:
        setup = {"id": "s1", "industry": "pharmaceutical", "warehouse_id": "wh1",
                 "enabled_modes": ["zerobus", "realtime"]}
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup_by_industry",
            AsyncMock(return_value=setup),
        ), patch(
            f"{MOD}.industry_provisioning_service.provision_industry",
            AsyncMock(return_value={"sections": []}),
        ) as prov_mock, patch(
            f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=[])
        ):
            async with _client(_state()) as c:
                resp = await c.post("/api/admin/industries/pharmaceutical/provision")
        assert resp.status_code == 200
        assert prov_mock.call_args.kwargs["enable_realtime"] is True

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
    async def test_lists_with_contract_and_resource_health(self) -> None:
        setups = [{"id": "s1", "industry": "pharmaceutical", "status": "deployed",
                   "enabled_modes": ["realtime"]}]
        # Deployed realtime but CDF not done -> unhealthy + not startable + stopped.
        deployments = [{"id": "d1", "mode": "realtime", "status": "deployed",
                        "job_id": "j1"}]
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
        ), patch(
            f"{MOD}._job_running_lookup", MagicMock(return_value={"j1": False})
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines")
        assert resp.status_code == 200
        items = resp.json()["pipelines"]
        assert len(items) == 1
        pipe = items[0]
        # contract fields
        assert pipe["setup_id"] == "s1"
        assert pipe["industry"] == "pharmaceutical"
        assert pipe["status"] == "stopped"
        assert pipe["health"] == "unhealthy"
        assert pipe["startable"] is False
        assert pipe["ready"] is False
        assert pipe["modes"][0] == {
            "mode": "realtime", "status": "stopped",
            "health": "unhealthy", "startable": False,
            "runtime_detail": None, "tasks": [],
            "deployed": True, "flowing": False,
        }
        # resource ledger rollup preserved under resource_health
        assert pipe["resource_health"]["overall"] == "healthy"
        assert pipe["resource_health"]["jobs_running"] == 1

    async def test_draft_pipeline_when_no_deployments(self) -> None:
        setups = [{"id": "s1", "industry": "pharmaceutical", "status": "draft",
                   "enabled_modes": ["realtime"]}]
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.list_setups", AsyncMock(return_value=setups)
        ), patch(
            f"{MOD}.deployment_repo.list_deployments", AsyncMock(return_value=[])
        ), patch(
            f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=[])
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines")
        pipe = resp.json()["pipelines"][0]
        assert pipe["status"] == "draft"
        assert pipe["health"] == "draft"
        assert pipe["startable"] is False

    async def test_running_and_flowing_reports_running(self) -> None:
        # Deployed + RUNNING job + fresh data in the sink → running.
        setup = {"id": "s1", "industry": "pharmaceutical", "status": "deployed",
                 "enabled_modes": ["microbatch"], "validation_state": {"steps": {
                     k: {"status": sm.PASSED}
                     for k in sm.mode_required_keys("microbatch")}}}
        deployments = [{"id": "d1", "mode": "microbatch", "status": "deployed",
                        "job_id": "j1"}]
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.list_setups", AsyncMock(return_value=[setup])
        ), patch(
            f"{MOD}.deployment_repo.list_deployments",
            AsyncMock(return_value=deployments),
        ), patch(
            f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=[])
        ), patch(
            f"{MOD}._job_running_lookup", MagicMock(return_value={"j1": True})
        ), patch(
            f"{MOD}._flowing_lookup", AsyncMock(return_value={"microbatch": True})
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines")
        pipe = resp.json()["pipelines"][0]
        assert pipe["status"] == "running"
        assert pipe["modes"][0]["status"] == "running"
        assert pipe["modes"][0]["flowing"] is True

    async def test_running_but_not_flowing_reports_starting(self) -> None:
        # Deployed + RUNNING job but NO fresh data yet → starting (warming up).
        setup = {"id": "s1", "industry": "pharmaceutical", "status": "deployed",
                 "enabled_modes": ["microbatch"]}
        deployments = [{"id": "d1", "mode": "microbatch", "status": "deployed",
                        "job_id": "j1"}]
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.list_setups", AsyncMock(return_value=[setup])
        ), patch(
            f"{MOD}.deployment_repo.list_deployments",
            AsyncMock(return_value=deployments),
        ), patch(
            f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=[])
        ), patch(
            f"{MOD}._job_running_lookup", MagicMock(return_value={"j1": True})
        ), patch(
            f"{MOD}._flowing_lookup", AsyncMock(return_value={})
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines")
        pipe = resp.json()["pipelines"][0]
        assert pipe["status"] == "starting"
        assert pipe["modes"][0]["status"] == "starting"


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
        # per-mode contract + ledger rollup under resource_health
        assert body["setup_id"] == "s1"
        assert body["status"] == "draft"  # no deployment rows
        assert body["resource_health"]["overall"] == "degraded"
        assert body["resources"]["uc_catalog"][0]["health"] == "healthy"
        assert body["resources"]["job"][0]["health"] == "degraded"

    async def test_no_ws_falls_back_to_deploy_gate_view(self) -> None:
        # Default path: no WorkspaceClient (no creds) -> Tier-1 checks skipped, the
        # endpoint still returns the ledger view + an empty checks list + e2e desc.
        setup = {"id": "s1", "industry": "pharmaceutical", "status": "draft",
                 "enabled_modes": ["realtime"]}
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)
        ), patch(
            f"{MOD}.deployment_repo.list_deployments", AsyncMock(return_value=[])
        ), patch(
            f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=[])
        ), patch(
            f"{MOD}._gather_api_health", AsyncMock(return_value=None)
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines/s1/health")
        body = resp.json()
        assert body["checks"] == []
        assert body["cluster_state"] is None
        assert body["health"] == "draft"          # deploy-gate view preserved
        assert body["e2e_test"]["tier"] == "e2e"
        assert body["e2e_test"]["spins_interactive_cluster"] is True

    async def test_tier1_checks_become_primary_health(self) -> None:
        # When the Tier-1 sweep runs, its rollup is the primary health/startable
        # and checks[] + cluster_state are surfaced; deploy-gate view is preserved.
        setup = {"id": "s1", "industry": "pharmaceutical", "status": "deployed",
                 "enabled_modes": ["microbatch"]}
        deployments = [{"id": "d1", "mode": "microbatch", "status": "deployed",
                        "job_id": "j1"}]
        api = {
            "checks": [
                {"name": "uc_catalog_schemas", "tier": "api", "status": "pass",
                 "detail": "ok"},
                {"name": "kafka_topics", "tier": "e2e", "status": "skip",
                 "detail": "cluster-only"},
            ],
            "health": "healthy",
            "startable": True,
            "cluster_state": "TERMINATED",
        }
        with _admin(True), patch(
            f"{MOD}.pipeline_setup_repo.get_setup", AsyncMock(return_value=setup)
        ), patch(
            f"{MOD}.deployment_repo.list_deployments", AsyncMock(return_value=deployments)
        ), patch(
            f"{MOD}.resource_ledger.list_resources", AsyncMock(return_value=[])
        ), patch(
            f"{MOD}._job_running_lookup", MagicMock(return_value={"j1": True})
        ), patch(
            f"{MOD}._gather_api_health", AsyncMock(return_value=api)
        ):
            async with _client(_state()) as c:
                resp = await c.get("/api/admin/pipelines/s1/health")
        body = resp.json()
        assert body["health"] == "healthy"
        assert body["startable"] is True
        assert body["cluster_state"] == "TERMINATED"
        assert len(body["checks"]) == 2
        # deploy-gate rollup preserved for admin detail
        assert "deploy_health" in body and "deploy_startable" in body
        # Tier-2 topic check is present and marked e2e
        topic = [c for c in body["checks"] if c["name"] == "kafka_topics"][0]
        assert topic["tier"] == "e2e"


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
