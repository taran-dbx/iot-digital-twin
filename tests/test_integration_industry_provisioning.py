"""End-to-end integration test for the industry provisioning engine.

Provisions a THROWAWAY industry (``spiketest``) against the real
``fe-vm-shared-interop`` workspace + ``iot-pipelines`` Lakebase project:

- canonical ``iot_spiketest`` catalog + 5 schemas + checkpoint volume +
  external metrics table,
- a never-expiring Lakebase branch + primary/replica endpoints,
- LakeTS install + ``sensor_readings`` ChronoTable + ~24h partition pre-create,
- shared-SP grants on the catalog.

Asserts each resource exists, then performs FULL cleanup (drop catalog CASCADE +
best-effort S3-object delete, delete the branch, drop the ChronoTable).
``pytest.skip`` when the workspace / project is unreachable.  Ops-job deploy is
disabled (``deploy_ops_jobs=False``) to keep the run fast and avoid leaving
scheduled jobs behind.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

INDUSTRY = "spiketest"
CATALOG = "iot_spiketest"
_VENDORED_LAKETS = str(
    Path(__file__).resolve().parent.parent / "databricks" / "vendor" / "lakets.sql"
)


def _ws_or_skip():
    try:
        from databricks.sdk import WorkspaceClient
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"databricks-sdk unavailable: {exc}")
    profile = os.getenv("DATABRICKS_CONFIG_PROFILE", "fe-vm-shared-interop")
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", profile)
    try:
        ws = WorkspaceClient(profile=profile)
        ws.current_user.me()
        return ws
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"workspace {profile} unreachable: {exc}")


def _warehouse_or_skip(ws):
    whs = list(ws.warehouses.list())
    if not whs:
        pytest.skip("no SQL warehouse available")
    return whs[0].id


async def _catalog_exists(ws, wh, catalog: str) -> bool:
    from services.dbsql_service import execute_dbsql

    res = await execute_dbsql(f"SHOW CATALOGS LIKE '{catalog}'", wh)
    return res["row_count"] > 0


@pytest.mark.asyncio
async def test_integration_full_provision_and_teardown() -> None:
    from services import industry_provisioning_service as ips
    from services import industry_catalog_service as ics
    from services import pipelines_project_service as pps
    from services.dbsql_service import execute_dbsql

    ws = _ws_or_skip()
    wh = _warehouse_or_skip(ws)

    # Pre-flight must pass (external location + warehouse) or the environment
    # can't support the test.
    checks = ics.preflight_checks(ws=ws, warehouse_id=wh)
    if any(c["status"] == "fail" for c in checks):
        pytest.skip(f"pre-flight failed: {checks}")

    metrics_location = None
    try:
        summary = await ips.provision_industry(
            INDUSTRY, setup_id=None, ws=ws, warehouse_id=wh,
            enable_realtime=True, deploy_ops_jobs=False,
            lakets_sql_path=_VENDORED_LAKETS,
        )

        assert summary["catalog"] == CATALOG
        assert summary["branch"] == INDUSTRY

        # 1. Catalog + schemas exist.
        assert await _catalog_exists(ws, wh, CATALOG)
        schemas = await execute_dbsql(f"SHOW SCHEMAS IN {CATALOG}", wh)
        names = {r.get("databaseName") or r.get("namespace") for r in schemas["data"]}
        assert {"realtime", "microbatch", "zerobus", "metrics", "checkpoints"} <= names

        # 2. External metrics table exists.
        mt = await execute_dbsql(
            f"SHOW TABLES IN {CATALOG}.metrics", wh
        )
        tbls = {r.get("tableName") for r in mt["data"]}
        assert "streaming_metrics" in tbls
        metrics_location = summary["metrics_table"]

        # 3. Checkpoint volume exists.
        vols = await execute_dbsql(f"SHOW VOLUMES IN {CATALOG}.checkpoints", wh)
        vnames = {r.get("volume_name") or r.get("volumeName") for r in vols["data"]}
        assert "stream_checkpoints" in vnames

        # 4. Lakebase branch is present with a resolvable primary host.
        info = summary["sections"]
        branch_section = next(s for s in info if s["phase"] == "lakebase_branch")
        assert branch_section["result"]["primary_host"]

        # 5. LakeTS ChronoTable was created (registered).
        lakets_section = next(s for s in info if s["phase"] == "lakets_setup")
        steps = {s["step"]: s["status"] for s in lakets_section["result"]["steps"]}
        assert steps.get("create_chronotable") in ("created", "already_exists")
        assert steps.get("precreate_partitions") == "ok"

    finally:
        # ---- FULL CLEANUP ------------------------------------------------
        # Drop the catalog (CASCADE removes schemas/tables/volume).
        try:
            await execute_dbsql(f"DROP CATALOG IF EXISTS {CATALOG} CASCADE", wh)
        except Exception:
            pass

        # Best-effort S3-object delete for the external metrics table + catalog
        # managed location (dropping the catalog does not remove external data).
        try:
            import boto3

            s3 = boto3.client("s3", region_name="us-east-1")
            bucket = "taran-shared-interop"
            for prefix in (
                f"iot-digital-twin/{CATALOG}/",
                f"{CATALOG}/",
            ):
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                    if objs:
                        s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
        except Exception:
            pass

        # Delete the Lakebase branch (removes its primary endpoint); delete the
        # replica first.
        try:
            ws.postgres.delete_endpoint(
                name=pps.endpoint_resource_name(INDUSTRY, "replica")
            )
        except Exception:
            pass
        try:
            ws.postgres.delete_branch(name=pps.branch_resource_name(INDUSTRY))
        except Exception:
            pass
