"""Industry provisioning orchestrator (plan Phase 2 — 2.2 + 2.4 + LakeTS 2.3).

``provision_industry(industry, setup_id)`` runs the full additive provisioning
sequence for one industry pipeline and records every resource in the
``provisioned_resource`` ledger:

1. Pre-flight checks (external location + warehouse).
2. Shared identity + secrets — the ``iot-sss-industries`` SP and ``iot-secrets``
   scope (shared; recorded but never torn down per-industry).
3. Canonical ``iot_<industry>`` UC catalog + schemas + volume + external metrics
   table (§2.5, §1.11).
4. (real-time) never-expiring Lakebase branch + primary/replica endpoints
   (§2.1–2.2), then LakeTS setup on the branch: install, ``sensor_readings``
   ChronoTable, ~24h partition pre-create, LakeTS ops jobs (§1.9–1.10).
5. Grants for the shared SP on the catalog, incl. ``EXTERNAL USE SCHEMA`` on the
   metrics schema (§1.11) and ``CREATE TABLE`` on the zerobus schema (§1.10).

Additive only — does not touch ``app.yaml``, ``backend.py`` live wiring, or the
legacy ``iot-lakets-demo`` project.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any

from services import (
    industry_catalog_service as cat,
    industry_setup_service as setup,
    pipelines_project_service as pps,
    provisioning_service,
)
from services.industry_catalog_service import TEARDOWN_ORDER_SP
from services.resource_ledger import record_resource

logger = logging.getLogger("iot_digital_twin.services.industry_provisioning")


def _new_ws() -> Any:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


async def provision_industry(
    industry: str,
    setup_id: Any,
    *,
    db: Any | None = None,
    ws: Any | None = None,
    warehouse_id: str | None = None,
    enable_realtime: bool = True,
    deploy_ops_jobs: bool = True,
    lakets_sql_path: str | None = None,
    run_preflight: bool = True,
) -> dict:
    """Provision all resources for one industry pipeline; record them in the ledger.

    Returns a summary dict with a ``sections`` list (one per phase) and the key
    identifiers (``catalog``, ``branch``, ``metrics_table``, ``sp_principal``).
    Raises ``RuntimeError`` on failed pre-flight; otherwise best-effort per phase
    (each phase's granular status is in its section).
    """
    ws = ws or _new_ws()
    summary: dict[str, Any] = {"industry": industry, "setup_id": setup_id, "sections": []}

    def _section(name: str, payload: Any) -> None:
        summary["sections"].append({"phase": name, "result": payload})

    # 1. Pre-flight.
    if run_preflight:
        checks = cat.preflight_checks(ws=ws, warehouse_id=warehouse_id)
        _section("preflight", checks)
        cat.raise_on_failed_preflight(checks)

    # 2. Shared identity + secrets (shared — recorded but never torn down here).
    sp_result = await provisioning_service.ensure_shared_streaming_sp(
        SimpleNamespace(workspace_client=ws)
    )
    _section("shared_sp", sp_result)
    principal = sp_result.get("client_id") or cat.resolve_streaming_sp_principal(ws)
    summary["sp_principal"] = principal
    if db is not None and setup_id is not None:
        await record_resource(
            db, setup_id=setup_id,
            resource_type="service_principal",
            identifier=provisioning_service.SHARED_STREAMING_SP_DISPLAY_NAME,
            teardown_order=TEARDOWN_ORDER_SP,
            metadata={"shared": True, "application_id": principal},
        )
    scope_result = await cat.ensure_secret_scope(ws=ws, db=db, setup_id=setup_id)
    _section("secret_scope", scope_result)

    # 3. Canonical UC catalog + schemas + volume + external metrics table.
    catalog_result = await cat.provision_industry_catalog(
        industry, ws=ws, warehouse_id=warehouse_id,
        db=db, setup_id=setup_id,
    )
    _section("uc_catalog", catalog_result)
    summary["catalog"] = catalog_result.get("catalog")
    summary["metrics_table"] = catalog_result.get("metrics_table")

    # 4. Real-time Lakebase branch + LakeTS setup on the branch.
    if enable_realtime:
        branch_result = await pps.provision_industry_branch(
            industry, ws=ws, db=db, setup_id=setup_id,
        )
        _section("lakebase_branch", branch_result)
        branch = branch_result["branch"]
        summary["branch"] = branch

        write_mgr = None
        try:
            write_mgr = await pps.get_write_conn(branch, ws=ws)
            lakets_result = await setup.setup_lakets_on_branch(
                branch, conn=write_mgr, ws=ws,
                project_id=branch_result["project"],
                deploy_ops_jobs=deploy_ops_jobs,
                lakets_sql_path=lakets_sql_path,
                db=db, setup_id=setup_id,
            )
            _section("lakets_setup", lakets_result)
        finally:
            if write_mgr is not None:
                try:
                    await write_mgr.close()
                except Exception:  # pragma: no cover - best-effort
                    pass

    # 5. Grants for the shared SP on the catalog.
    grants_result = await cat.grant_streaming_sp(
        industry, ws=ws, warehouse_id=warehouse_id, principal=principal,
        db=db, setup_id=setup_id,
    )
    _section("grants", grants_result)

    return summary
