"""LakeTS-specific routes — sync readiness preflight etc."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from core.state import AppState, get_app_state
from services import lakets_service
from services.lakets_deploy_service import deploy_retention_job
from services.pipeline_db import get_pipeline_lakebase

logger = logging.getLogger("iot_digital_twin.routes.lakets")

router = APIRouter()


async def _open_lakebase_conn(lb: dict) -> Any:
    """Open a direct asyncpg connection using the same resolution as provisioning.

    Mirrors ``services/provisioning_service.provision_lakebase_table`` — autoscale
    mode asks the Databricks SDK for a short-lived OAuth credential; direct mode
    uses plain creds from the body.
    """
    import asyncpg

    mode = lb.get("mode", "autoscale")
    database = lb.get("database", "databricks_postgres")

    if mode == "autoscale":
        from databricks.sdk import WorkspaceClient
        project_id = lb.get("project_id", "")
        branch_id = lb.get("branch_id", "production")
        if not project_id:
            raise ValueError("project_id is required in autoscale mode")

        w = WorkspaceClient()
        parent = f"projects/{project_id}/branches/{branch_id}"
        endpoints = list(w.postgres.list_endpoints(parent=parent))
        if not endpoints:
            raise RuntimeError(f"No endpoints for {parent}")

        endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
        cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)
        return await asyncpg.connect(
            host=endpoint.status.hosts.host,
            database=database,
            user=w.current_user.me().user_name,
            password=cred.token,
            port=5432,
            ssl="require",
        )

    return await asyncpg.connect(
        host=lb.get("host", ""),
        database=database,
        user=lb.get("user", ""),
        password=lb.get("password", ""),
        port=int(lb.get("port", 5432)),
        ssl="require",
    )


@router.post("/api/lakets/sync/check")
async def check_sync(request: Request):
    """Probe a Lakebase for Lakehouse Sync readiness.

    Accepts the same ``lb`` body shape as ``/api/settings/provision``.
    Returns the readiness dict (booleans + a human-readable ``reason``).
    Never raises — connection errors are folded into the result dict.
    """
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    lb = body.get("lakebase") or body.get("lb") or body
    try:
        conn = await _open_lakebase_conn(lb)
    except Exception as exc:
        logger.warning("Sync check could not connect: %s", exc)
        return {
            "database_compatible": False,
            "wal2delta_available": False,
            "wal2delta_installed": False,
            "lakets_sync_functions_present": False,
            "ready_to_enable": False,
            "error": f"Connection failed: {exc}"[:200],
        }

    try:
        return await lakets_service.check_lakehouse_sync_readiness(conn)
    finally:
        await conn.close()


@router.get("/api/pipelines/{pipeline_id}/cdf-status")
async def get_cdf_status(
    pipeline_id: str,
    state: AppState = Depends(get_app_state),
):
    """Report whether Lakebase CDF (Lakehouse Sync) is streaming for a pipeline.

    Queries the pipeline's Lakebase ``wal2delta.tables`` (the Lakehouse Sync
    bookkeeping schema). A row status of ``STREAMING`` means CDF is actively
    replicating; ``SNAPSHOTTING`` means the initial backfill is in progress.
    If the schema/table is missing or the query errors, we fail closed to
    ``status="NONE"`` with a human-readable ``reason``.

    ``hot_rows`` is a COUNT of ``sensor_readings`` (the hot Lakebase tier).
    ``cold_rows`` stays ``None`` — the cold tier lives in Unity Catalog and is
    not queried here.
    """
    none_result = {
        "enabled": False,
        "streaming": False,
        "status": "NONE",
        "reason": "Lakebase Sync not enabled",
        "hot_rows": None,
        "cold_rows": None,
    }

    try:
        lb = await get_pipeline_lakebase(state, pipeline_id)
    except Exception as exc:
        return {**none_result, "reason": f"Pipeline Lakebase not resolvable: {exc}"[:200]}

    if lb is None:
        return {**none_result, "reason": "Pipeline Lakebase not resolvable"}

    try:
        rows = await lb.fetch("SELECT status FROM wal2delta.tables LIMIT 50")
    except Exception as exc:
        logger.warning("cdf-status wal2delta probe failed for %s: %s", pipeline_id, exc)
        return {**none_result, "reason": f"Lakebase Sync not enabled: {exc}"[:200]}

    statuses = {str(r["status"]).upper() for r in rows if r["status"] is not None}

    if "STREAMING" in statuses:
        status = "STREAMING"
    elif "SNAPSHOTTING" in statuses:
        status = "SNAPSHOTTING"
    elif statuses:
        status = "UNKNOWN"
    else:
        return {**none_result, "reason": "Lakebase Sync not enabled (no wal2delta tables)"}

    hot_rows: int | None = None
    try:
        hot_rows = await lb.fetchval("SELECT COUNT(*) FROM sensor_readings")
        hot_rows = int(hot_rows) if hot_rows is not None else None
    except Exception as exc:
        logger.warning("cdf-status hot_rows count failed for %s: %s", pipeline_id, exc)

    return {
        "enabled": True,
        "streaming": status == "STREAMING",
        "status": status,
        "reason": f"wal2delta tables report status={status}",
        "hot_rows": hot_rows,
        "cold_rows": None,
    }


@router.post("/api/lakets/retention/deploy")
async def deploy_retention(request: Request):
    """Upload the retention notebook and create/update a daily Databricks Job."""
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    lb = body.get("lakebase") or body.get("lb") or body
    project_id = (lb.get("project_id") or "").strip()
    if not project_id:
        return JSONResponse(
            status_code=400,
            content={"error": "lakebase.project_id is required"},
        )

    try:
        return deploy_retention_job(
            project_id=project_id,
            branch_id=(lb.get("branch_id") or "production").strip(),
            database=(lb.get("database") or "databricks_postgres").strip(),
        )
    except Exception as exc:
        logger.exception("Retention job deploy failed")
        return JSONResponse(status_code=500, content={"error": str(exc)[:500]})
