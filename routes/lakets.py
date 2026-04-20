"""LakeTS-specific routes — sync readiness preflight etc."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from services import lakets_service
from services.lakets_deploy_service import deploy_retention_job

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
