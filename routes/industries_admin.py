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
    deployment_repo,
    industry_provisioning_service,
    pipeline_setup_repo,
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

    Enforces one-setup-per-industry: if a setup already exists for ``industry``
    it is updated in place; otherwise a new draft row is created.
    """
    _require_admin(request)
    db = _require_db(state)

    try:
        body = await request.json()
    except Exception:
        body = {}

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
    """Return every setup with its deployments and a rolled-up health summary."""
    _require_admin(request)
    db = _require_db(state)

    setups = await pipeline_setup_repo.list_setups(db)
    items = []
    for setup in setups:
        deployments = await deployment_repo.list_deployments(db, setup["id"])
        resources = await resource_ledger.list_resources(db, setup["id"])
        items.append(
            {
                "setup": setup,
                "deployments": deployments,
                "health": _rollup_health(setup, deployments, resources),
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
    """Return per-resource health for one pipeline by walking the ledger.

    Health is currently derived from the ``provisioned_resource`` ledger status.
    Live (cheap) status checks — Lakebase endpoint state, CDF ``STREAMING`` — are
    layered on by Phase 4's validation/health service; this endpoint is the
    ledger-backed inventory that surface reads.
    """
    _require_admin(request)
    db = _require_db(state)

    setup = await pipeline_setup_repo.get_setup(db, setup_id)
    if setup is None:
        raise HTTPException(status_code=404, detail="pipeline setup not found")

    deployments = await deployment_repo.list_deployments(db, setup_id)
    resources = await resource_ledger.list_resources(db, setup_id)

    return {
        "setup": setup,
        "deployments": deployments,
        "health": _rollup_health(setup, deployments, resources),
        "resources": _group_resource_health(resources),
    }


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

    # Register a job deleter so ``job`` ledger rows are actually deleted (reverse
    # order).  Building it needs a live WorkspaceClient; if that is unavailable
    # (tests / no creds) fall back to the default registry (jobs skipped, not
    # failed).  Kafka topic-delete stays skipped — DeleteTopic is denied (§1.8).
    deleters = None
    try:
        from services.deployment_service import make_job_deleter

        deleters = {"job": make_job_deleter()}
    except Exception as exc:  # pragma: no cover - best-effort
        logger.warning("job deleter unavailable; jobs will be skipped: %s", exc)

    result = await teardown_service.teardown_setup(db, setup_id, deleters)
    return result
