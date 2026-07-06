"""Admin pipeline management API (spec §2.3–2.9, plan Task 2.4 + Phase 6 backend).

Exposes the app-backend data model (``pipeline_setup`` / ``pipeline_deployment``
/ ``provisioned_resource`` in the ``iot-app-backend`` project) and the
provisioning + teardown engine over admin-gated HTTP endpoints:

    POST   /api/admin/industries/{industry}/setup      create/update a setup (one per industry)
    POST   /api/admin/industries/{industry}/provision  run provision_industry(); return resources
    GET    /api/admin/pipelines                         management list + rolled-up health
    GET    /api/admin/pipelines/{setup_id}/health       per-resource health for one pipeline
    DELETE /api/admin/pipelines/{setup_id}              teardown (reverse order) + cascade-delete

All endpoints are gated by ``app_permissions.is_admin`` (same check the rest of
the app uses) and require the additive app-backend DB pool
(``state.app_backend_db``), returning 503 when it is unavailable so a Lakebase
outage degrades gracefully instead of 500-ing.

Additive only: this router never touches ``PGHOST`` / ``LAKEBASE_ENDPOINT`` / the
legacy ``iot-lakets-demo`` connection or the ``pipelines`` / ``user_settings`` /
``source_bindings`` wiring.

TODO (Phase 4 — separate task): the validation-flow routes
``POST /api/admin/industries/{industry}/validate`` (start / resume the persisted
validation state machine) and ``GET /api/admin/industries/{industry}/validate/state``
(poll live step state) will be added here, backed by ``services/validation_service.py``.
They are intentionally NOT implemented in this task.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from core.state import AppState, get_app_state
from services import (
    app_permissions,
    dbsql_service,
    deployment_repo,
    deployment_service,
    health_checks,
    industry_provisioning_service,
    pipeline_setup_repo,
    pipeline_view,
    pipelines_project_service,
    resource_ledger,
    teardown_service,
)

logger = logging.getLogger("iot_digital_twin.routes.industries_admin")

router = APIRouter()

# Resource statuses that count as live/healthy in the ledger.
_HEALTHY_STATUSES = {"active"}
_IGNORED_STATUSES = {"deleted"}


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------
def _require_admin(request: Request) -> None:
    """Raise 403 unless the caller passes the admin check."""
    if not app_permissions.is_admin(request):
        raise HTTPException(status_code=403, detail="admin access required")


def _require_db(state: AppState) -> Any:
    """Return the app-backend DB pool, or raise 503 if it is unavailable."""
    db = getattr(state, "app_backend_db", None)
    if db is None:
        raise HTTPException(
            status_code=503,
            detail="app-backend metadata database is unavailable",
        )
    return db


# ---------------------------------------------------------------------------
# Live job-running lookup (Jobs API) — resolves REAL running state per job so
# ``status`` is never a fabricated "idle/running".  Best-effort: a missing
# WorkspaceClient (tests / no creds) or a Jobs-API error degrades a deployed
# mode to "stopped" (not running) rather than 500-ing the whole list.
# ---------------------------------------------------------------------------
def _job_running_lookup(deployments: list[dict]) -> dict[str, dict]:
    """Return ``{job_id: {running, degraded, detail, tasks}}`` for every deployed mode.

    Per-mode runtime derived from the Jobs API: ``running`` (run life-cycle RUNNING),
    ``degraded`` (run up but a task crashed — e.g. producer died so data isn't
    flowing), a human ``detail``, and the per-task states for the UI.
    """
    job_ids = pipeline_view.collect_job_ids(deployments)
    if not job_ids:
        return {}
    try:
        w = deployment_service._new_ws()
    except Exception as exc:  # pragma: no cover - no creds in tests
        logger.warning("Jobs API client unavailable; modes report not-running: %s", exc)
        return {}
    lookup: dict[str, dict] = {}
    for job_id in job_ids:
        try:
            status = deployment_service.pipeline_status(job_id, w=w)
            lookup[str(job_id)] = {
                "running": status.get("state") == "RUNNING",
                "degraded": bool(status.get("degraded")),
                "detail": status.get("detail"),
                "tasks": status.get("tasks") or [],
            }
        except Exception as exc:  # pragma: no cover - best-effort
            logger.warning("pipeline_status(%s) failed; treating as not-running: %s", job_id, exc)
            lookup[str(job_id)] = {"running": False, "degraded": False, "detail": None, "tasks": []}
    return lookup


# ---------------------------------------------------------------------------
# Health rollup (ledger-derived; live checks are Phase 4)
# ---------------------------------------------------------------------------
def _rollup_health(
    setup: dict, deployments: list[dict], resources: list[dict]
) -> dict:
    """Compute an overall health verdict + counts from ledger + deployment rows.

    Draft setups report ``status='draft'`` (not startable / "coming soon").
    Otherwise: ``healthy`` when every non-deleted resource is active,
    ``degraded`` when some are active and some are not, ``down`` when none are
    active, and ``unknown`` when there are no resources yet.
    """
    live = [r for r in resources if r.get("status") not in _IGNORED_STATUSES]
    total = len(live)
    healthy = sum(1 for r in live if r.get("status") in _HEALTHY_STATUSES)

    if setup.get("status") == "draft":
        overall = "draft"
    elif total == 0:
        overall = "unknown"
    elif healthy == total:
        overall = "healthy"
    elif healthy == 0:
        overall = "down"
    else:
        overall = "degraded"

    jobs = [r for r in live if r.get("resource_type") == "job"]
    jobs_running = sum(1 for r in jobs if r.get("status") in _HEALTHY_STATUSES)

    return {
        "overall": overall,
        "resources_healthy": healthy,
        "resources_total": total,
        "jobs_running": jobs_running,
        "jobs_total": len(jobs),
        "enabled_modes": setup.get("enabled_modes") or [],
        "deployment_count": len(deployments),
    }


# ---------------------------------------------------------------------------
# Tier-1 live API/DB health checks (spec §1.16) — fast, NO interactive cluster.
# Best-effort: a missing WorkspaceClient (tests / no creds) degrades to ``None``
# so the endpoint still returns the ledger-backed inventory instead of 500-ing.
# ---------------------------------------------------------------------------
async def _gather_api_health(setup: dict, deployments: list[dict]) -> dict | None:
    """Run the Tier-1 API/DB health sweep for one pipeline, or ``None`` if no ws."""
    try:
        ws = deployment_service._new_ws()
    except Exception as exc:  # pragma: no cover - no creds in tests
        logger.info("Tier-1 API health checks skipped (no workspace client): %s", exc)
        return None

    warehouse_id = setup.get("warehouse_id")
    run_sql = None
    if warehouse_id:
        async def run_sql(sql: str) -> dict:  # noqa: A001 - local binding
            return await dbsql_service.execute_dbsql(sql, warehouse_id)

    async def open_read_conn(branch: str):
        return await pipelines_project_service.get_read_conn(branch, ws=ws)

    try:
        return await health_checks.gather_checks(
            setup,
            deployments,
            ws=ws,
            run_sql=run_sql,
            open_read_conn=open_read_conn,
        )
    except Exception as exc:  # pragma: no cover - gather_checks is self-guarding
        logger.warning("Tier-1 API health checks failed: %s", exc)
        return None


def _e2e_test_descriptor(industry: str) -> dict:
    """Describe the opt-in Tier-2 interactive end-to-end test (spec §1.16)."""
    return {
        "tier": "e2e",
        "available": True,
        "spins_interactive_cluster": True,
        "eta_minutes": 10,
        "description": (
            "Opt-in end-to-end test: spins the interactive validation cluster "
            "(~10 min) to verify source DNS/TCP/TLS connectivity, Kafka topics, "
            "and a produce→consume→land→metrics round-trip. Distinct from the "
            "fast Tier-1 API health above."
        ),
        "start_endpoint": f"POST /api/admin/industries/{industry}/validate/start",
        "state_endpoint": f"GET /api/admin/industries/{industry}/validate/state",
    }


def _group_resource_health(resources: list[dict]) -> dict:
    """Group ledger rows by ``resource_type`` with a per-resource health pill."""
    groups: dict[str, list[dict]] = {}
    for res in resources:
        status = res.get("status")
        if status in _HEALTHY_STATUSES:
            pill = "healthy"
        elif status in _IGNORED_STATUSES:
            pill = "deleted"
        else:
            pill = "degraded"
        groups.setdefault(res.get("resource_type") or "unknown", []).append(
            {
                "id": res.get("id"),
                "identifier": res.get("identifier"),
                "parent_ref": res.get("parent_ref"),
                "teardown_order": res.get("teardown_order"),
                "status": status,
                "health": pill,
                "metadata": res.get("metadata") or {},
            }
        )
    return groups


# ---------------------------------------------------------------------------
# POST /api/admin/industries/{industry}/setup — create/update (one per industry)
# ---------------------------------------------------------------------------
@router.post("/api/admin/industries/{industry}/setup")
async def create_or_update_setup(
    industry: str, request: Request, state: AppState = Depends(get_app_state)
):
    """Create or update the single ``pipeline_setup`` for an industry (draft).

    Enforces one-setup-per-industry (the DB has ``UNIQUE(industry)``): if a setup
    already exists for ``industry`` it is UPDATED in place; otherwise a new draft
    row is created.

    A caller that explicitly intends to create a *brand-new* pipeline for an
    already-configured industry (body/query ``create_only`` / ``intent=create``)
    is rejected with **409** and pointed at the existing ``setup_id`` — so a
    second pipeline is never silently created for the same industry.
    """
    _require_admin(request)
    db = _require_db(state)

    try:
        body = await request.json()
    except Exception:
        body = {}

    create_only = bool(body.get("create_only")) or (
        body.get("intent") == "create"
        or request.query_params.get("intent") == "create"
    )

    fields = {
        "flavor": body.get("flavor"),
        "source_type": body.get("source_type"),
        "bootstrap_servers": body.get("bootstrap_servers"),
        "enabled_modes": body.get("enabled_modes"),
        "cluster_spec": body.get("cluster_spec"),
        "warehouse_id": body.get("warehouse_id"),
    }

    existing = await pipeline_setup_repo.get_setup_by_industry(db, industry)
    if existing is not None:
        if create_only:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": f"a pipeline setup already exists for industry '{industry}'",
                    # ``existing["id"]`` is a ``uuid.UUID`` (asyncpg maps the pg
                    # uuid column) — coerce to str, since FastAPI renders the
                    # HTTPException detail with plain json.dumps (no
                    # jsonable_encoder), and a raw UUID there raises TypeError
                    # → the intended 409 would otherwise surface as a 500.
                    "setup_id": str(existing["id"]),
                    "industry": industry,
                },
            )
        update_fields = {k: v for k, v in fields.items() if v is not None}
        setup = await pipeline_setup_repo.update_setup(
            db, existing["id"], **update_fields
        )
        return {"created": False, "setup": setup}

    caller_email = app_permissions._caller_email(request)
    setup = await pipeline_setup_repo.create_setup(
        db,
        industry=industry,
        status="draft",
        user_email=caller_email,
        **fields,
    )
    return {"created": True, "setup": setup}


# ---------------------------------------------------------------------------
# GET /api/admin/industries/{industry}/setup — existing setup or 404
# ---------------------------------------------------------------------------
@router.get("/api/admin/industries/{industry}/setup")
async def get_setup_for_industry(
    industry: str, request: Request, state: AppState = Depends(get_app_state)
):
    """Return the industry's single ``pipeline_setup`` (with its contract view), or 404.

    Lets the frontend detect that an industry is already configured (one-per-
    industry) before offering a "create" path.
    """
    _require_admin(request)
    db = _require_db(state)

    setup = await pipeline_setup_repo.get_setup_by_industry(db, industry)
    if setup is None:
        raise HTTPException(
            status_code=404, detail=f"no pipeline setup for industry '{industry}'"
        )

    deployments = await deployment_repo.list_deployments(db, setup["id"])
    running = _job_running_lookup(deployments)
    return {
        "setup": setup,
        "deployments": deployments,
        "pipeline": pipeline_view.build_pipeline_view(setup, deployments, running),
    }


# ---------------------------------------------------------------------------
# POST /api/admin/industries/{industry}/provision — run the provisioning engine
# ---------------------------------------------------------------------------
@router.post("/api/admin/industries/{industry}/provision")
async def provision_industry_endpoint(
    industry: str, request: Request, state: AppState = Depends(get_app_state)
):
    """Provision all resources for an industry and return the ledger.

    Resolves the setup by industry (or by ``setup_id`` in the body), then calls
    ``industry_provisioning_service.provision_industry`` bound to the
    app-backend DB so every resource is recorded in ``provisioned_resource``.
    """
    _require_admin(request)
    db = _require_db(state)

    try:
        body = await request.json()
    except Exception:
        body = {}

    setup_id = body.get("setup_id")
    if setup_id:
        setup = await pipeline_setup_repo.get_setup(db, setup_id)
    else:
        setup = await pipeline_setup_repo.get_setup_by_industry(db, industry)

    if setup is None:
        raise HTTPException(
            status_code=404,
            detail=f"no pipeline setup for industry '{industry}' — create one first",
        )

    try:
        summary = await industry_provisioning_service.provision_industry(
            industry,
            setup["id"],
            db=db,
            warehouse_id=setup.get("warehouse_id"),
        )
    except RuntimeError as exc:  # failed pre-flight is a clean 409 for the admin
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    resources = await resource_ledger.list_resources(db, setup["id"])
    return {"setup_id": setup["id"], "summary": summary, "resources": resources}


# ---------------------------------------------------------------------------
# GET /api/admin/pipelines — management list + rolled-up health
# ---------------------------------------------------------------------------
@router.get("/api/admin/pipelines")
async def list_pipelines(request: Request, state: AppState = Depends(get_app_state)):
    """Return every setup with its per-mode status/health/startability contract.

    Each pipeline object carries the frontend contract
    (``{setup_id, industry, status, health, modes:[{mode,status,health,startable}],
    startable, ready}``) plus the raw ``setup`` / ``deployments`` / ledger
    ``resource_health`` for admin detail.  ``status`` / ``health`` are DERIVED
    from real rows + live Jobs-API run state — never fabricated.
    """
    _require_admin(request)
    db = _require_db(state)

    setups = await pipeline_setup_repo.list_setups(db)

    # Resolve live job-running state once for all deployments in the sweep.
    all_deployments: dict[str, list[dict]] = {}
    combined: list[dict] = []
    for setup in setups:
        deployments = await deployment_repo.list_deployments(db, setup["id"])
        all_deployments[setup["id"]] = deployments
        combined.extend(deployments)
    running = _job_running_lookup(combined)

    items = []
    for setup in setups:
        deployments = all_deployments[setup["id"]]
        resources = await resource_ledger.list_resources(db, setup["id"])
        view = pipeline_view.build_pipeline_view(setup, deployments, running)
        items.append(
            {
                **view,
                "setup": setup,
                "deployments": deployments,
                "resource_health": _rollup_health(setup, deployments, resources),
            }
        )
    return {"pipelines": items}


# ---------------------------------------------------------------------------
# GET /api/admin/pipelines/{setup_id}/health — per-resource health
# ---------------------------------------------------------------------------
@router.get("/api/admin/pipelines/{setup_id}/health")
async def pipeline_health(
    setup_id: str, request: Request, state: AppState = Depends(get_app_state)
):
    """Return per-pipeline health: **Tier-1 live API/DB checks** + ledger inventory.

    Primary health is the fast Tier-1 sweep (spec §1.16) — UC/DBSQL, Lakebase
    endpoints, the ChronoTable + CDF sync, grants/secrets, metrics freshness, and
    Jobs-API run state — run from the app backend with **no interactive cluster**.
    Each check is ``{name, tier, status, detail}``; they aggregate into the
    top-level ``health`` / ``startable`` and surface the informational
    ``cluster_state``.  The opt-in Tier-2 interactive end-to-end test (which spins
    the cluster ~10 min) is described under ``e2e_test``.

    The deploy-gate rollup (``build_pipeline_view``) is preserved under
    ``deploy_health`` / ``deploy_startable`` and the ledger inventory under
    ``resource_health`` / ``resources`` for admin detail.  When no WorkspaceClient
    is available (tests / no creds), the live checks are skipped and the endpoint
    falls back to the deploy-gate view.
    """
    _require_admin(request)
    db = _require_db(state)

    setup = await pipeline_setup_repo.get_setup(db, setup_id)
    if setup is None:
        raise HTTPException(status_code=404, detail="pipeline setup not found")

    deployments = await deployment_repo.list_deployments(db, setup_id)
    resources = await resource_ledger.list_resources(db, setup_id)
    running = _job_running_lookup(deployments)

    view = pipeline_view.build_pipeline_view(setup, deployments, running)
    response = {
        **view,
        "setup": setup,
        "deployments": deployments,
        "resource_health": _rollup_health(setup, deployments, resources),
        "resources": _group_resource_health(resources),
        "e2e_test": _e2e_test_descriptor(setup.get("industry") or ""),
    }

    # Tier-1 live health is the PRIMARY health for this endpoint (spec §1.16).
    api = await _gather_api_health(setup, deployments)
    if api is not None:
        response["checks"] = api["checks"]
        response["cluster_state"] = api["cluster_state"]
        response["deploy_health"] = view["health"]
        response["deploy_startable"] = view["startable"]
        response["health"] = api["health"]
        response["startable"] = api["startable"]
        response["ready"] = api["startable"]
    else:
        response["checks"] = []
        response["cluster_state"] = None
    return response


# ---------------------------------------------------------------------------
# DELETE /api/admin/pipelines/{setup_id} — teardown + cascade-delete
# ---------------------------------------------------------------------------
@router.delete("/api/admin/pipelines/{setup_id}")
async def delete_pipeline(
    setup_id: str, request: Request, state: AppState = Depends(get_app_state)
):
    """Tear down every provisioned resource (reverse order) then delete metadata.

    Delegates to ``teardown_service.teardown_setup``, which walks the ledger in
    descending ``teardown_order`` and, after the per-resource deletes, drops the
    ``pipeline_setup`` row — whose ``ON DELETE CASCADE`` FKs remove the
    deployments and ledger rows.
    """
    _require_admin(request)
    db = _require_db(state)

    setup = await pipeline_setup_repo.get_setup(db, setup_id)
    if setup is None:
        raise HTTPException(status_code=404, detail="pipeline setup not found")

    # Register a deleter for EVERY resource_type the ledger records so teardown
    # actually removes the cloud resource (UC catalog, Lakebase branch/endpoints,
    # external metrics table + its S3 objects, secrets, SP, grants) in reverse
    # dependency order — instead of silently skipping and orphaning it.  Shared
    # resources are never deleted; Kafka topic-delete stays skipped (DeleteTopic
    # denied, §1.8).  Building the registry needs a live WorkspaceClient; if that
    # is unavailable (tests / no creds) fall back to the default (all skipped).
    deleters = None
    try:
        from services import resource_deleters

        deleters = resource_deleters.make_all_deleters(
            warehouse_id=setup.get("warehouse_id"),
        )
    except Exception as exc:  # pragma: no cover - best-effort
        logger.warning("deleter registry unavailable; resources will be skipped: %s", exc)

    result = await teardown_service.teardown_setup(db, setup_id, deleters)
    return result
