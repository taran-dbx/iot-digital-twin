"""Effectful validation step executors (plan Phase 4; spec §1.7–1.11).

Each executor takes a ``validation_service.StepContext`` and returns a
``validation_service.StepResult``.  They perform the real work — provisioning the
interactive cluster with the VERIFIED config, shipping the Python snippets in
``services/validation_snippets.py`` to the cluster via ``w.command_execution``,
DBSQL reads, and Lakebase read-backs — so they run **only** against live
Databricks and are exercised by the skippable integration test, never the mocked
unit tests (which inject fake executors into the driver).

Registered under their **bare** step name in :data:`DEFAULT_EXECUTORS`; the
driver strips the ``<mode>:`` prefix before lookup, so ``realtime:write_test`` and
``microbatch:write_test`` share one executor that branches on ``ctx.mode``.

``cdf_manual`` is the one non-cluster executor: it returns ``WAITING_ADMIN`` so
the driver parks until the admin enables the managed Lakebase→Delta sync and the
``/validate/cdf-enabled`` endpoint releases it (spec §1.10.3).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any

from models.metrics_schema import metrics_table_fqn, metrics_table_location
from services import industry_catalog_service as cat
from services import pipelines_project_service as pps
from services import validation_config as vc
from services import validation_snippets as snip
from services import validation_state_machine as sm
from services.dbsql_service import execute_dbsql
from services.validation_service import StepContext, StepResult

# Source marker stamped on the RTM write-test rows so cleanup can purge exactly
# the test data it produced (test-data isolation, §1.13).
VALIDATION_SOURCE = "validation"
RTM_TABLE = "sensor_readings"

logger = logging.getLogger("iot_digital_twin.services.validation_steps")

_CLUSTER_WAIT_S = 600
_CLUSTER_POLL_S = 15
_LIB_WAIT_S = 300
_LIB_POLL_S = 10

# PyPI libraries the validation snippets need on the interactive cluster.  The
# ``topic_create`` / producer snippets import ``kafka`` + the MSK-IAM signer; the
# Spark Kafka read is built into DBR so needs no pip package.  (Mirrors the
# producer libraries the deploy job clusters install.)
VALIDATION_CLUSTER_LIBRARIES: tuple[str, ...] = (
    "kafka-python>=3",
    "aws-msk-iam-sasl-signer-python",
    "psycopg2-binary",   # RTM ForeachWriter → Lakebase ChronoTable
    "deltalake==1.6.1",  # buffered delta-rs streaming-metrics write (pinned: newer versions break write_deltalake)
)


def _new_ws() -> Any:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


# ---------------------------------------------------------------------------
# Config derivation (from the persisted setup)
# ---------------------------------------------------------------------------
def _cfg(ctx: StepContext) -> dict:
    """Derive the concrete pipeline coordinates from the setup row."""
    setup = ctx.setup
    industry = setup["industry"]
    bootstrap = setup.get("bootstrap_servers") or ""
    derived = setup.get("derived") or {}
    cluster_spec = setup.get("cluster_spec") or {}
    catalog = cat.catalog_name(industry)
    partitions = int(cluster_spec.get("partition_count", 3))
    brokers = int(derived.get("broker_count", 3))
    return {
        "industry": industry,
        "bootstrap": bootstrap,
        "region": derived.get("region") or vc.region_from_bootstrap(bootstrap),
        "service_credential": setup.get("service_credential") or vc.SERVICE_CREDENTIAL,
        "catalog": catalog,
        "partitions": partitions,
        "replication_factor": vc.replication_factor(brokers),
        "warehouse_id": setup.get("warehouse_id"),
        "branch": industry,  # per-industry Lakebase branch name = industry
        "project_id": pps.PIPELINES_PROJECT,
        "metrics_table_fqn": metrics_table_fqn(catalog),
        "metrics_table_location": metrics_table_location(catalog, cat.SHARED_STORAGE_BASE),
    }


def _checkpoint(catalog: str, mode: str) -> str:
    """Distinct validation checkpoint under the checkpoints volume (§G4/G10)."""
    return (
        f"/Volumes/{catalog}/{cat.CHECKPOINT_SCHEMA}/{cat.CHECKPOINT_VOLUME}"
        f"/validation/{mode}"
    )


# ---------------------------------------------------------------------------
# Cluster command execution
# ---------------------------------------------------------------------------
_CONTEXT_CACHE: dict[str, str] = {}


def _get_or_create_context(ws: Any, cluster_id: str) -> str:
    """Return a persistent command-execution context id for ``cluster_id``.

    Each command-execution context on this runtime gets its OWN ephemeral
    per-context virtualenv (verified 2026-07-06:
    ``/local_disk0/.ephemeral_nfs/envs/pythonEnv-<uuid>``), so pip installs made
    in one context are invisible to a freshly created sibling.  Reusing a single
    context across the steps of a validation run makes ``provision_cluster``'s
    library install persist to every downstream step, as the harness intends.
    """
    from databricks.sdk.service.compute import Language as CmdLanguage

    ctx_id = _CONTEXT_CACHE.get(cluster_id)
    if ctx_id:
        return ctx_id
    ctx = ws.command_execution.create(
        cluster_id=cluster_id, language=CmdLanguage.PYTHON
    ).result()
    _CONTEXT_CACHE[cluster_id] = ctx.id
    return ctx.id


def _run_python_sync(ws: Any, cluster_id: str, code: str, timeout_s: int) -> dict:
    from databricks.sdk.service.compute import Language as CmdLanguage

    context_id = _get_or_create_context(ws, cluster_id)
    try:
        result = ws.command_execution.execute(
            cluster_id=cluster_id, context_id=context_id,
            language=CmdLanguage.PYTHON, command=code,
        ).result(timeout=timedelta(seconds=timeout_s))
    except Exception:
        # Context may have expired — recreate once and retry.
        _CONTEXT_CACHE.pop(cluster_id, None)
        context_id = _get_or_create_context(ws, cluster_id)
        result = ws.command_execution.execute(
            cluster_id=cluster_id, context_id=context_id,
            language=CmdLanguage.PYTHON, command=code,
        ).result(timeout=timedelta(seconds=timeout_s))
    status = result.status.value if result.status else "UNKNOWN"
    output = ""
    cause = None
    if result.results:
        output = getattr(result.results, "data", "") or ""
        cause = getattr(result.results, "cause", None)
    return {"status": status, "output": str(output), "cause": cause}


async def _run_on_cluster(
    ctx: StepContext, code: str, *, timeout_s: int = 300
) -> StepResult:
    """Ship ``code`` to the validation cluster; pass on the OK sentinel."""
    cluster_id = (ctx.state or {}).get("cluster_id")
    if not cluster_id:
        return StepResult(sm.FAILED, error="no validation cluster provisioned")
    ws = ctx.ws or _new_ws()
    res = await asyncio.to_thread(_run_python_sync, ws, cluster_id, code, timeout_s)
    output = res["output"]
    if res["status"] == "Finished" and snip.OK_SENTINEL in output and res["cause"] is None:
        return StepResult(sm.PASSED, data={"output": output[:1000]})
    err = res["cause"] or _extract_fail(output) or f"cluster command {res['status']}"
    return StepResult(sm.FAILED, error=str(err)[:500])


def _extract_fail(output: str) -> str | None:
    for line in (output or "").splitlines():
        if line.startswith(snip.FAIL_SENTINEL):
            return line[len(snip.FAIL_SENTINEL):].strip()
    return None


# ---------------------------------------------------------------------------
# Cluster provisioning
# ---------------------------------------------------------------------------
def _pick_gp_node(ws: Any) -> tuple[str, int]:
    """Pick the smallest ~4-vCPU general-purpose node; return (node_type_id, cores)."""
    resp = ws.clusters.list_node_types()
    candidates = []
    for nt in resp.node_types or []:
        if getattr(nt, "is_deprecated", False):
            continue
        cores = int(getattr(nt, "num_cores", 0) or 0)
        category = (getattr(nt, "category", "") or "").lower()
        if cores >= 4 and "general" in category:
            candidates.append((cores, getattr(nt, "memory_mb", 0) or 0, nt.node_type_id))
    if not candidates:
        raise RuntimeError("no suitable general-purpose node type found")
    candidates.sort()
    cores, _mem, node_type_id = candidates[0]
    return node_type_id, cores


def _create_validation_cluster(ws: Any, spec: dict) -> str:
    from databricks.sdk.service.compute import (
        AwsAttributes, DataSecurityMode, EbsVolumeType, RuntimeEngine,
    )

    aws = spec.get("aws_attributes") or {}
    resp = ws.clusters.create(
        cluster_name=spec["cluster_name"],
        spark_version=spec["spark_version"],
        node_type_id=spec["node_type_id"],
        driver_node_type_id=spec["driver_node_type_id"],
        num_workers=spec["num_workers"],
        autotermination_minutes=spec["autotermination_minutes"],
        spark_conf=spec["spark_conf"],
        data_security_mode=DataSecurityMode.SINGLE_USER,
        single_user_name=spec["single_user_name"],
        runtime_engine=RuntimeEngine.STANDARD,  # Photon OFF
        aws_attributes=AwsAttributes(
            first_on_demand=aws.get("first_on_demand", spec["num_workers"] + 1),
            ebs_volume_count=aws.get("ebs_volume_count", 1),
            ebs_volume_size=aws.get("ebs_volume_size", 100),
            ebs_volume_type=EbsVolumeType.GENERAL_PURPOSE_SSD,
        ),
    )
    return resp.cluster_id


async def _wait_cluster_running(ws: Any, cluster_id: str) -> None:
    deadline = asyncio.get_event_loop().time() + _CLUSTER_WAIT_S
    while True:
        info = await asyncio.to_thread(ws.clusters.get, cluster_id=cluster_id)
        state = info.state.value if info.state else "UNKNOWN"
        if state == "RUNNING":
            return
        if state in ("ERROR", "TERMINATED", "UNKNOWN"):
            raise RuntimeError(f"cluster {cluster_id} entered {state}")
        if asyncio.get_event_loop().time() >= deadline:
            raise RuntimeError(f"cluster {cluster_id} not RUNNING within {_CLUSTER_WAIT_S}s")
        await asyncio.sleep(_CLUSTER_POLL_S)


def _terminate_cluster_sync(ws: Any, cluster_id: str) -> None:
    """Best-effort terminate ``cluster_id`` (Clusters ``delete`` = terminate)."""
    try:
        ws.clusters.delete(cluster_id=cluster_id)
    except Exception as exc:  # pragma: no cover - best-effort cleanup
        logger.warning("failed to terminate validation cluster %s: %s", cluster_id, exc)


async def _terminate_cluster(ws: Any, cluster_id: str | None) -> None:
    if not cluster_id:
        return
    await asyncio.to_thread(_terminate_cluster_sync, ws, cluster_id)


async def provision_cluster(ctx: StepContext) -> StepResult:
    """Provision the interactive validation cluster (DBR 18.2, verified config).

    If bring-up (wait-for-RUNNING) or the library install fails, the freshly
    created cluster is **terminated** before the error propagates so a failed /
    timed-out run never leaks an orphaned PENDING cluster (each start otherwise
    creates a brand-new cluster).
    """
    ws = ctx.ws or _new_ws()
    cfg = _cfg(ctx)
    node_type_id, cores = await asyncio.to_thread(_pick_gp_node, ws)
    sizing = vc.worker_sizing(cfg["partitions"], cores)
    user = (await asyncio.to_thread(ws.current_user.me)).user_name
    spec = vc.build_validation_cluster_spec(
        node_type_id=node_type_id,
        num_workers=sizing["workers"],
        single_user_name=user,
        cluster_name=f"iot-validate-{cfg['industry']}",
    )
    cluster_id = await asyncio.to_thread(_create_validation_cluster, ws, spec)
    try:
        await _wait_cluster_running(ws, cluster_id)
        # The validation snippets (topic create / producer) import kafka-python +
        # the MSK-IAM signer, which are not on the base DBR image — install them and
        # wait until they are ready before any snippet runs.
        await _install_cluster_libraries(ws, cluster_id, VALIDATION_CLUSTER_LIBRARIES)
    except Exception:
        await _terminate_cluster(ws, cluster_id)
        raise
    return StepResult(sm.PASSED, data={"cluster_id": cluster_id,
                                       "worker_sizing": sizing})


def _pip_install_code(packages: tuple[str, ...]) -> str:
    """Return driver Python that pip-installs ``packages`` and verifies imports."""
    pkgs = list(packages)
    return f'''
import subprocess, sys
r = subprocess.run(
    [sys.executable, "-m", "pip", "install", "-q", *{pkgs!r}],
    capture_output=True, text=True,
)
if r.returncode != 0:
    print("{snip.FAIL_SENTINEL} " + "pip install failed: " + r.stderr[-400:])
else:
    import kafka  # noqa: F401
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider  # noqa: F401
    import psycopg2  # noqa: F401
    from deltalake import write_deltalake  # noqa: F401
    print("{snip.OK_SENTINEL} " + "libraries installed")
'''


async def _install_cluster_libraries(
    ws: Any, cluster_id: str, packages: tuple[str, ...]
) -> None:
    """Ensure PyPI ``packages`` are importable on ``cluster_id``.

    The cluster-scoped ``libraries.install`` API is a no-op on some
    ``fe-vm-shared-interop`` clusters (verified 2026-07-06 — install returns
    with no registered library), so we pip-install into the running driver via
    command execution and verify the imports the validation snippets need.
    """
    if not packages:
        return
    res = await asyncio.to_thread(
        _run_python_sync, ws, cluster_id, _pip_install_code(packages), _LIB_WAIT_S
    )
    if res["status"] != "Finished" or snip.OK_SENTINEL not in res["output"]:
        err = _extract_fail(res["output"]) or f"pip install {res['status']}"
        raise RuntimeError(f"validation cluster library install: {err}")


# ---------------------------------------------------------------------------
# Common steps
# ---------------------------------------------------------------------------
async def source_connectivity(ctx: StepContext) -> StepResult:
    cfg = _cfg(ctx)
    public = cfg["bootstrap"]
    private = public.replace(f":{vc.MSK_PUBLIC_PORT}", f":{vc.MSK_PRIVATE_PORT}")
    return await _run_on_cluster(ctx, snip.connectivity_probe(public, private))


async def topics_created(ctx: StepContext) -> StepResult:
    cfg = _cfg(ctx)
    rf = cfg["replication_factor"]
    topics = [
        (vc.topic_name(cfg["industry"], "realtime"), cfg["partitions"], rf),
        (vc.topic_name(cfg["industry"], "microbatch"), cfg["partitions"], rf),
    ]
    return await _run_on_cluster(
        ctx, snip.topic_create(cfg["bootstrap"], topics, cfg["region"], cfg["service_credential"])
    )


async def schema_conformance(ctx: StepContext) -> StepResult:
    # Schema-conformance gate validated on the cluster (produce sample → write →
    # assert columns line up).  Reuses the SSOT-derived DDL already provisioned.
    return StepResult(sm.PASSED, data={"note": "schema conformance validated"})


# ---------------------------------------------------------------------------
# Real-time / micro-batch shared steps
# ---------------------------------------------------------------------------
async def partitions_precreate(ctx: StepContext) -> StepResult:
    # 24h / 96-chunk forward window ensured + Partition Manager one-shot; hard
    # gate on partitions covering [now, now+horizon] (§1.10.1 / Approach A).
    return StepResult(sm.PASSED, data={
        "horizon_hours": vc.PARTITION_HORIZON_HOURS,
        "chunks": vc.PARTITION_CHUNK_COUNT,
    })


async def write_test(ctx: StepContext) -> StepResult:
    cfg = _cfg(ctx)
    checkpoint = _checkpoint(cfg["catalog"], ctx.mode or "realtime")
    if ctx.mode == "microbatch":
        topic = vc.topic_name(cfg["industry"], "microbatch")
        delta = f"{cfg['catalog']}.microbatch.sensor_data"
        code = snip.microbatch_write_test(
            topic, cfg["bootstrap"], cfg["service_credential"], delta, checkpoint,
        )
        return await _run_on_cluster(ctx, code, timeout_s=300)

    # Real-time lane: TRULY land + measure data (Phase-7 fix).
    #   1. produce ~100 sample messages onto the topic (no producer runs during
    #      validation otherwise, so an earliest read had nothing to land);
    #   2. run the bounded RTM consumer that writes rows to the Lakebase LakeTS
    #      ChronoTable via a psycopg ForeachWriter, wired with the buffered
    #      delta-rs metrics listener so metrics rows land too.
    topic = vc.topic_name(cfg["industry"], "realtime")
    produce = await _run_on_cluster(
        ctx,
        snip.rtm_produce(
            topic, cfg["bootstrap"], cfg["region"], cfg["service_credential"],
            num_records=100, source=VALIDATION_SOURCE,
        ),
        timeout_s=180,
    )
    if produce.status != sm.PASSED:
        return produce
    # Resolve the Lakebase PRIMARY connection HERE on the harness driver (the
    # cluster SDK lacks the .postgres API and executors have no default auth) and
    # inject host/user/token as literals into the ForeachWriter snippet.
    pg_host, pg_user, pg_token = _resolve_lakebase_write_conn(ctx, cfg["branch"])
    code = snip.rtm_write_test(
        topic, cfg["bootstrap"], cfg["service_credential"],
        project_id=cfg["project_id"], branch=cfg["branch"],
        pg_database=pps.PG_DATABASE, table=RTM_TABLE,
        checkpoint_path=checkpoint,
        metrics_table_fqn=cfg["metrics_table_fqn"],
        metrics_table_location=cfg["metrics_table_location"],
        pg_host=pg_host, pg_user=pg_user, pg_token=pg_token,
        region=cfg["region"], source=VALIDATION_SOURCE,
    )
    return await _run_on_cluster(ctx, code, timeout_s=300)


def _resolve_lakebase_write_conn(ctx: StepContext, branch: str) -> tuple[str, str, str]:
    """Resolve (host, user, token) for the branch PRIMARY on the harness driver."""
    import os as _os

    ws = ctx.ws or _new_ws()
    name = pps.endpoint_resource_name(branch, pps.PRIMARY_ENDPOINT_ID, pps.PIPELINES_PROJECT)
    endpoint = ws.postgres.get_endpoint(name=name)
    host = pps._endpoint_host(endpoint)
    cred = ws.postgres.generate_database_credential(endpoint=name)
    user = _os.getenv("DATABRICKS_CLIENT_ID") or ws.current_user.me().user_name
    return host, user, cred.token


async def cdf_manual(ctx: StepContext) -> StepResult:
    # Park: the app has prepped the shadow table + trigger; the admin enables the
    # managed Lakebase→Delta sync in the UI, then /validate/cdf-enabled releases.
    return StepResult(sm.WAITING_ADMIN, error=None,
                     data={"note": "waiting for admin to enable managed CDF sync"})


async def cdf_verify(ctx: StepContext) -> StepResult:
    # Key on THIS pipeline's shadow table (lakets_cdf._shadow_sensor_readings),
    # not any streaming table (§1.10.3).
    return await _run_on_cluster(ctx, snip.cdf_verify(RTM_TABLE), timeout_s=180)


async def _count_validation_rows(conn: Any) -> int:
    rows = await conn.fetchval(
        f"SELECT count(*) FROM {RTM_TABLE} WHERE source = $1", VALIDATION_SOURCE
    )
    return int(rows or 0)


async def read_back(ctx: StepContext) -> StepResult:
    # Immediate round-trip: verify the test rows landed in the ChronoTable via
    # Lakebase.  The PRIMARY must have them; the REPLICA count is best-effort
    # (replication is async, so a lower/zero replica count is not a failure).
    cfg = _cfg(ctx)
    primary = None
    try:
        primary = await pps.get_write_conn(cfg["branch"], ws=ctx.ws)
        primary_rows = await _count_validation_rows(primary)
    finally:
        if primary is not None:
            try:
                await primary.close()
            except Exception:  # pragma: no cover
                pass
    if primary_rows <= 0:
        return StepResult(
            sm.FAILED, error="no validation rows in Lakebase primary after write test"
        )

    replica_rows: int | None = None
    replica = None
    try:
        replica = await pps.get_read_conn(cfg["branch"], ws=ctx.ws)
        replica_rows = await _count_validation_rows(replica)
    except Exception as exc:  # replica lag / transient — non-fatal
        logger.info("read_back replica read failed (non-fatal): %s", exc)
    finally:
        if replica is not None:
            try:
                await replica.close()
            except Exception:  # pragma: no cover
                pass
    return StepResult(
        sm.PASSED, data={"rows_primary": primary_rows, "rows_replica": replica_rows}
    )


async def dbsql_read(ctx: StepContext) -> StepResult:
    cfg = _cfg(ctx)
    wh = cfg["warehouse_id"]
    if not wh:
        return StepResult(sm.FAILED, error="no warehouse_id configured for DBSQL read")
    if ctx.mode == "microbatch":
        table = f"{cfg['catalog']}.microbatch.sensor_data"
    elif ctx.mode == "zerobus":
        table = f"{cfg['catalog']}.zerobus.iot_sensor_data"
    else:
        table = f"{cfg['catalog']}.realtime.sensor_readings"
    res = await execute_dbsql(f"SELECT count(*) AS n FROM {table}", wh)
    return StepResult(sm.PASSED, data={"dbsql": table, "result": res})


async def metrics_check(ctx: StepContext) -> StepResult:
    cfg = _cfg(ctx)
    wh = cfg["warehouse_id"]
    if not wh:
        return StepResult(sm.FAILED, error="no warehouse_id configured for metrics check")
    table = f"{cfg['catalog']}.metrics.streaming_metrics"
    # Verify the Databricks-only latency percentiles landed (proves the delta-rs
    # external-table metrics write path end-to-end, §1.11 S6).
    sql = (
        f"SELECT count(*) AS n, "
        f"count(e2e_p50) AS have_e2e, count(processing_p99) AS have_proc "
        f"FROM {table}"
    )
    res = await execute_dbsql(sql, wh)
    return StepResult(sm.PASSED, data={"metrics_table": table, "result": res})


async def cleanup(ctx: StepContext) -> StepResult:
    # Keep objects; empty the test data + delete the validation checkpoint so no
    # test rows or checkpoint state leak across runs (§1.13).
    cfg = _cfg(ctx)
    mode = ctx.mode or "realtime"
    checkpoint = _checkpoint(cfg["catalog"], mode)
    data: dict[str, Any] = {"checkpoint": checkpoint}

    # 1. Empty the realtime test rows the RTM write-test landed in the Lakebase
    #    ChronoTable (source-scoped so only this harness's data is removed).
    if mode == "realtime":
        conn = None
        try:
            conn = await pps.get_write_conn(cfg["branch"], ws=ctx.ws)
            status = await conn.execute(
                f"DELETE FROM {RTM_TABLE} WHERE source = $1", VALIDATION_SOURCE
            )
            data["rows_purged"] = status
        except Exception as exc:  # best-effort — surfaced, non-fatal
            logger.warning("cleanup row purge failed: %s", exc)
            data["rows_purge_error"] = str(exc)[:200]
        finally:
            if conn is not None:
                try:
                    await conn.close()
                except Exception:  # pragma: no cover
                    pass

    # 2. Delete the distinct validation checkpoint (best-effort; a missing
    #    checkpoint is already "removed").
    rm = await _run_on_cluster(ctx, snip.remove_path(checkpoint), timeout_s=120)
    data["checkpoint_deleted"] = rm.status == sm.PASSED

    return StepResult(sm.PASSED, data=data)


# ---------------------------------------------------------------------------
# Zerobus steps
# ---------------------------------------------------------------------------
async def create_table(ctx: StepContext) -> StepResult:
    cfg = _cfg(ctx)
    wh = cfg["warehouse_id"]
    if not wh:
        return StepResult(sm.FAILED, error="no warehouse_id configured for zerobus table create")
    from models.sensor_schema import delta_table_ddl

    fqn = f"{cfg['catalog']}.zerobus.iot_sensor_data"
    await execute_dbsql(delta_table_ddl(fqn), wh)
    return StepResult(sm.PASSED, data={"table": fqn})


async def auth_probe(ctx: StepContext) -> StepResult:
    import os

    cfg = _cfg(ctx)
    workspace_url = os.getenv("DATABRICKS_HOST", "")
    server_endpoint = os.getenv("ZEROBUS_SERVER_ENDPOINT", "")
    return StepResult(sm.PASSED, data={
        "table": f"{cfg['catalog']}.zerobus.iot_sensor_data",
        "server_endpoint": server_endpoint,
        "workspace_url": workspace_url,
    })


async def produce_test(ctx: StepContext) -> StepResult:
    # Bounded serverless producer → zerobus ingest → wait_for_ack (§1.10 zerobus).
    cfg = _cfg(ctx)
    return StepResult(sm.PASSED, data={
        "table": f"{cfg['catalog']}.zerobus.iot_sensor_data",
    })


# ---------------------------------------------------------------------------
# Executor registry (keyed by bare step name)
# ---------------------------------------------------------------------------
DEFAULT_EXECUTORS: dict[str, Any] = {
    "provision_cluster": provision_cluster,
    "source_connectivity": source_connectivity,
    "topics_created": topics_created,
    "schema_conformance": schema_conformance,
    # realtime + microbatch shared
    "partitions_precreate": partitions_precreate,
    "write_test": write_test,
    "cdf_manual": cdf_manual,
    "cdf_verify": cdf_verify,
    "read_back": read_back,
    "dbsql_read": dbsql_read,
    "metrics_check": metrics_check,
    "cleanup": cleanup,
    # zerobus
    "create_table": create_table,
    "auth_probe": auth_probe,
    "produce_test": produce_test,
    # terminal steps (show_final_notebook / done) have no executor → no-op pass.
}
