"""Per-mode pipeline deploy service (spec §2.6, §1.10, §1.15; plan Phase 5).

Turns a *validated* industry pipeline setup into the running Databricks Job(s)
for a single mode.  Of the 7 jobs a fully-enabled industry runs (spec §2.6),
this module owns **only the per-mode consumer/producer job(s)**:

- **realtime**   — one **Continuous** job, DBR 18.2 single-user *classic* job
  cluster, ``realTimeMode.enabled=true``, Photon/autoscaling/spot OFF, sized per
  M1 (``clamp(ceil(partitions/cores), 1, 8)``).  Two tasks: a Kafka **producer**
  (single-node) + an **RTM consumer** (Kafka → Lakebase LakeTS + delta-rs
  metrics) on the sized cluster.
- **microbatch** — one **Continuous** job, same classic single-user cluster but
  **no** real-time mode.  Producer + micro-batch consumer (``processingTime`` is
  baked into the notebook) → UC Delta.
- **zerobus**    — a **serverless**, producer-only job.

The **4 LakeTS operational jobs** (partition manager / rollup / tiering /
retention) are deployed by the provisioning engine
(``services.lakets_deploy_service``) — this module does **not** duplicate them.

Every job created is recorded in the ``provisioned_resource`` ledger
(``resource_type="job"``) so teardown deletes it in reverse dependency order,
and the ``pipeline_deployment`` row is updated with ``status`` / ``job_id`` /
``notebook_path``.  Teardown deletes topics is *skipped* (DeleteTopic denied,
spec §1.8) — jobs are deletable via :func:`make_job_deleter`.

Deploy is **hard-gated** on validation (spec §1.12): :func:`deploy_pipeline`
refuses to create any job until every required validation step for the mode is
``passed`` (or the caller explicitly guarantees the gate with
``gate_override=True``).

Additive only — this module does not touch ``app.yaml`` / ``backend.py`` /
``iot-lakets-demo``.
"""

from __future__ import annotations

import base64
import logging
import math
import time
from typing import Any

from services import deployment_repo, pipeline_setup_repo
from services.app_repo_common import DBExecutor
from services.databricks_sdk import detect_cloud
from services.resource_ledger import record_resource

# Validation hard-gate (spec §1.12) lives in its own module; re-exported here so
# callers can use ``deployment_service.assert_gate_passed`` / ``REQUIRED_STEPS``.
from services.deployment_gate import (  # noqa: F401  (re-export)
    COMMON_STEPS,
    DeployGateError,
    REQUIRED_STEPS,
    VALID_MODES,
    assert_gate_passed,
    gate_passed,
    gate_status,
    outstanding_steps,
)

logger = logging.getLogger("iot_digital_twin.services.deployment")

# --- Compute constants (spec §1.3 RTM cluster, verified 2026-07-05) ----------
# DBR 18.2 (Spark 4.1.0; latest 18.x GA) — service credentials verified on
# 18.2 + single-user.  Shared by the RTM job cluster and the micro-batch cluster.
RTM_DBR = "18.2.x-scala2.13"

# Smallest suitable general-purpose node (AWS us-east-1 / fe-vm-shared-interop).
# Must have >= CORES_PER_WORKER vCPUs: RTM's concurrent scheduler needs one slot
# per Kafka partition (+ overhead), and ``compute_workers`` assumes 4 cores/worker.
# ``m5d.large`` is only 2 vCPUs — with the industry deploy's default 1 worker that
# gives just 2 slots for a 3-partition topic → [CONCURRENT_SCHEDULER_INSUFFICIENT_SLOT].
# ``m5d.xlarge`` = 4 vCPUs matches CORES_PER_WORKER and the (proven) validation
# harness, which picks the smallest ~4-vCPU node (see validation_steps._pick_gp_node).
DEFAULT_NODE_TYPE = "m5d.xlarge"
CORES_PER_WORKER = 4

# Real-time mode Spark conf (verified).  Only the base flag is needed for the
# single-source (no stream-stream join) Kafka→Lakebase query.
RTM_SPARK_CONF: dict[str, str] = {
    "spark.databricks.streaming.realTimeMode.enabled": "true",
}

# 18.2 cluster libraries for the Spark consumer task (spec §1.10).  boto3 is
# deliberately absent — upgrading it breaks the runtime kernel.
CLASSIC_CONSUMER_LIBRARIES: tuple[str, ...] = (
    "databricks-sdk==0.120.0",
    "kafka-python>=3",
    "aws-msk-iam-sasl-signer-python",
    "psycopg2-binary",
    "deltalake==1.6.1",  # pinned: newer versions break write_deltalake (metrics listener)
)

# Producer task only needs to reach MSK (kafka-python + MSK-IAM signer) and the
# service credential (databricks-sdk).  No delta/psycopg needed.
PRODUCER_LIBRARIES: tuple[str, ...] = (
    "databricks-sdk==0.120.0",
    "kafka-python>=3",
    "aws-msk-iam-sasl-signer-python",
)

# Generation/trigger cadences (informational; the real values are baked into
# the generated notebooks — spec §1.15).
REALTIME_TRIGGER = "15 seconds"
MICROBATCH_PROCESSING_TIME = "10 seconds"

# Ledger teardown_order for the per-mode job.  Higher = deleted earlier (reverse
# dependency order); jobs must be deleted before their Lakebase/UC sinks
# (branch=20, replica=30 in pipelines_project_service), so jobs sit above them.
TEARDOWN_ORDER_JOB = 40

_WS_SUBDIR = ".iot-digital-twin"


# --- Worker sizing (M1) ------------------------------------------------------
def compute_workers(
    partition_count: int | None, cores_per_worker: int = CORES_PER_WORKER
) -> int:
    """Return worker count per M1: ``clamp(ceil(partitions/cores), 1, 8)``.

    Guarantees ``workers × cores ≥ partitions`` so every Kafka partition has a
    reader.  Unknown/zero partitions fall back to a single worker.
    """
    if not partition_count or partition_count <= 0:
        return 1
    workers = math.ceil(partition_count / max(cores_per_worker, 1))
    return max(1, min(workers, 8))


def worker_sizing_warning(
    partition_count: int | None, cores_per_worker: int = CORES_PER_WORKER
) -> str | None:
    """Warn (M1) when partitions exceed the 8-worker cap (``8 × cores``)."""
    if partition_count and partition_count > 8 * max(cores_per_worker, 1):
        return (
            f"{partition_count} partitions exceed the 8-worker cap "
            f"({8 * cores_per_worker} readers); some partitions share a reader."
        )
    return None


# --- WorkspaceClient helpers -------------------------------------------------
def _new_ws() -> Any:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _current_user(w: Any) -> str:
    me = w.current_user.me()
    return getattr(me, "user_name", None) or "unknown"


def _notebook_path(username: str, industry: str, mode: str, kind: str) -> str:
    ts = int(time.time())
    return (
        f"/Workspace/Users/{username}/{_WS_SUBDIR}/"
        f"{industry}_{mode}_{kind}_{ts}"
    )


def _upload_notebook(w: Any, path: str, content: str) -> None:
    from databricks.sdk.service.workspace import ImportFormat, Language

    parent = path.rsplit("/", 1)[0]
    try:
        w.workspace.mkdirs(parent)
    except Exception as exc:  # pragma: no cover - best-effort
        logger.debug("mkdirs(%s) skipped: %s", parent, exc)
    w.workspace.import_(
        path=path,
        content=base64.b64encode(content.encode()).decode(),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )
    logger.info("Uploaded notebook to %s (%d bytes)", path, len(content))


def _no_spot_attrs(cloud: str, num_workers: int) -> dict:
    """Return cloud attrs that force **on-demand** (spot OFF) for the cluster."""
    from databricks.sdk.service.compute import (
        AwsAttributes,
        AzureAttributes,
        EbsVolumeType,
    )

    if cloud == "aws":
        return {
            "aws_attributes": AwsAttributes(
                first_on_demand=num_workers + 1,
                ebs_volume_count=1,
                ebs_volume_size=100,
                ebs_volume_type=EbsVolumeType.GENERAL_PURPOSE_SSD,
            )
        }
    if cloud == "azure":
        return {"azure_attributes": AzureAttributes(first_on_demand=num_workers + 1)}
    return {}


# --- Job-spec builders (pure — no SDK calls beyond object construction) -------
def build_cluster_libraries(packages: tuple[str, ...] = CLASSIC_CONSUMER_LIBRARIES) -> list:
    """Return SDK ``Library`` PyPI entries for the given packages."""
    from databricks.sdk.service.compute import Library, PythonPyPiLibrary

    return [Library(pypi=PythonPyPiLibrary(package=p)) for p in packages]


def build_job_cluster_spec(
    *,
    node_type: str,
    num_workers: int,
    single_user_name: str,
    spark_version: str = RTM_DBR,
    realtime: bool = True,
    cloud: str | None = None,
):
    """Build the classic single-user consumer job cluster (spec §1.3).

    DBR 18.2, DEDICATED/single-user, **Photon OFF** (STANDARD engine),
    **autoscaling OFF** (fixed ``num_workers``), **spot OFF** (on-demand attrs).
    ``realTimeMode.enabled=true`` only for the real-time mode.
    """
    from databricks.sdk.service.compute import (
        ClusterSpec,
        DataSecurityMode,
        RuntimeEngine,
    )

    cloud = cloud if cloud is not None else detect_cloud()
    spark_conf = dict(RTM_SPARK_CONF) if realtime else {}
    return ClusterSpec(
        spark_version=spark_version,
        node_type_id=node_type,
        driver_node_type_id=node_type,
        num_workers=num_workers,
        spark_conf=spark_conf or None,
        data_security_mode=DataSecurityMode.SINGLE_USER,
        single_user_name=single_user_name,
        runtime_engine=RuntimeEngine.STANDARD,
        **_no_spot_attrs(cloud, num_workers),
    )


def build_producer_cluster_spec(
    *,
    node_type: str,
    single_user_name: str,
    spark_version: str = RTM_DBR,
    cloud: str | None = None,
):
    """Build a single-node classic cluster for the Kafka producer task."""
    from databricks.sdk.service.compute import (
        ClusterSpec,
        DataSecurityMode,
        RuntimeEngine,
    )

    cloud = cloud if cloud is not None else detect_cloud()
    return ClusterSpec(
        spark_version=spark_version,
        node_type_id=node_type,
        driver_node_type_id=node_type,
        num_workers=0,
        spark_conf={
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]",
        },
        data_security_mode=DataSecurityMode.SINGLE_USER,
        single_user_name=single_user_name,
        custom_tags={"ResourceClass": "SingleNode"},
        runtime_engine=RuntimeEngine.STANDARD,
        **_no_spot_attrs(cloud, 0),
    )


def build_spark_tasks(
    *,
    mode: str,
    consumer_notebook_path: str,
    producer_notebook_path: str | None,
    node_type: str,
    num_workers: int,
    single_user_name: str,
    cloud: str | None = None,
) -> list:
    """Build the producer + consumer tasks for a Spark (realtime/microbatch) job."""
    from databricks.sdk.service.jobs import NotebookTask, Task

    realtime = mode == "realtime"
    consumer_key = "rtm_consumer" if realtime else "microbatch_consumer"
    tasks = [
        Task(
            task_key=consumer_key,
            new_cluster=build_job_cluster_spec(
                node_type=node_type,
                num_workers=num_workers,
                single_user_name=single_user_name,
                realtime=realtime,
                cloud=cloud,
            ),
            notebook_task=NotebookTask(notebook_path=consumer_notebook_path),
            libraries=build_cluster_libraries(CLASSIC_CONSUMER_LIBRARIES),
        )
    ]
    if producer_notebook_path:
        tasks.append(
            Task(
                task_key="data_producer",
                new_cluster=build_producer_cluster_spec(
                    node_type=node_type,
                    single_user_name=single_user_name,
                    cloud=cloud,
                ),
                notebook_task=NotebookTask(notebook_path=producer_notebook_path),
                libraries=build_cluster_libraries(PRODUCER_LIBRARIES),
            )
        )
    return tasks


def build_zerobus_task(
    *, producer_notebook_path: str, environment_key: str, base_parameters: dict | None = None
) -> Any:
    """Build the single serverless producer task for a zerobus job."""
    from databricks.sdk.service.jobs import NotebookTask, Source, Task

    return Task(
        task_key="zerobus_producer",
        notebook_task=NotebookTask(
            notebook_path=producer_notebook_path,
            source=Source.WORKSPACE,
            base_parameters=base_parameters or {},
        ),
        environment_key=environment_key,
    )


def job_display_name(industry: str, mode: str) -> str:
    """Canonical per-mode job name (spec §2.6)."""
    return f"iot-{industry}-{mode}"


# --- Job create/reset (idempotent by name) -----------------------------------
def _find_job_id(w: Any, job_name: str) -> int | None:
    for j in w.jobs.list(name=job_name):
        if j.settings and j.settings.name == job_name:
            return j.job_id
    return None


def _job_url(w: Any, job_id: Any) -> str | None:
    host = getattr(getattr(w, "config", None), "host", None)
    if not host:
        return None
    return f"{host.rstrip('/')}/jobs/{job_id}"


def _create_or_reset_continuous_job(w: Any, job_name: str, tasks: list) -> tuple[int, str]:
    """Create/reset an **unpaused Continuous** job with the given tasks."""
    from databricks.sdk.service.jobs import (
        Continuous,
        JobSettings,
        PauseStatus,
    )

    continuous = Continuous(pause_status=PauseStatus.UNPAUSED)
    tags = {"app": "iot-digital-twin", "component": "pipeline", "job": job_name}
    existing = _find_job_id(w, job_name)
    if existing is not None:
        try:
            w.jobs.cancel_all_runs(job_id=existing)
        except Exception as exc:  # pragma: no cover - best-effort
            logger.warning("cancel_all_runs(%s) skipped: %s", existing, exc)
        w.jobs.reset(
            job_id=existing,
            new_settings=JobSettings(
                name=job_name, continuous=continuous, tasks=tasks, tags=tags
            ),
        )
        return existing, "updated"
    created = w.jobs.create(
        name=job_name, continuous=continuous, tasks=tasks, tags=tags
    )
    return created.job_id, "created"


def _create_or_reset_serverless_job(
    w: Any, job_name: str, task: Any, environments: list
) -> tuple[int, str]:
    """Create/reset a serverless (producer-only) job — run-triggered, no schedule."""
    from databricks.sdk.service.jobs import JobSettings

    tags = {"app": "iot-digital-twin", "component": "zerobus", "job": job_name}
    existing = _find_job_id(w, job_name)
    if existing is not None:
        w.jobs.reset(
            job_id=existing,
            new_settings=JobSettings(
                name=job_name, tasks=[task], environments=environments, tags=tags
            ),
        )
        return existing, "updated"
    created = w.jobs.create(
        name=job_name, tasks=[task], environments=environments, tags=tags
    )
    return created.job_id, "created"


# --- Ledger + deployment persistence -----------------------------------------
async def _record_job(
    db: DBExecutor,
    *,
    setup_id: Any,
    deployment_id: Any,
    job_id: Any,
    job_name: str,
    mode: str,
    task_keys: list[str],
    notebook_path: str,
    producer_notebook_path: str | None,
) -> None:
    await record_resource(
        db,
        setup_id=setup_id,
        deployment_id=deployment_id,
        resource_type="job",
        identifier=str(job_id),
        teardown_order=TEARDOWN_ORDER_JOB,
        metadata={
            "mode": mode,
            "job_name": job_name,
            "tasks": task_keys,
            "notebook_path": notebook_path,
            "producer_notebook_path": producer_notebook_path,
        },
    )


async def _ensure_deployment(db: DBExecutor, setup_id: Any, mode: str) -> dict:
    deployment = await deployment_repo.get_deployment_by_mode(db, setup_id, mode)
    if deployment is None:
        deployment = await deployment_repo.create_deployment(
            db, setup_id=setup_id, mode=mode, status="deploying"
        )
    return deployment


# --- Per-mode deploy ---------------------------------------------------------
async def _deploy_spark_pipeline(
    db: DBExecutor,
    *,
    setup_id: Any,
    deployment_id: Any,
    industry: str,
    mode: str,
    consumer_notebook: str,
    producer_notebook: str,
    node_type: str,
    num_workers: int,
    single_user_name: str,
    w: Any,
    cloud: str | None = None,
) -> dict:
    if not consumer_notebook:
        raise ValueError(f"consumer_notebook is required for mode '{mode}'")

    consumer_path = _notebook_path(single_user_name, industry, mode, "consumer")
    _upload_notebook(w, consumer_path, consumer_notebook)
    producer_path = None
    if producer_notebook:
        producer_path = _notebook_path(single_user_name, industry, mode, "producer")
        _upload_notebook(w, producer_path, producer_notebook)

    tasks = build_spark_tasks(
        mode=mode,
        consumer_notebook_path=consumer_path,
        producer_notebook_path=producer_path,
        node_type=node_type,
        num_workers=num_workers,
        single_user_name=single_user_name,
        cloud=cloud,
    )
    job_name = job_display_name(industry, mode)
    job_id, action = _create_or_reset_continuous_job(w, job_name, tasks)
    task_keys = [t.task_key for t in tasks]

    await _record_job(
        db,
        setup_id=setup_id,
        deployment_id=deployment_id,
        job_id=job_id,
        job_name=job_name,
        mode=mode,
        task_keys=task_keys,
        notebook_path=consumer_path,
        producer_notebook_path=producer_path,
    )
    await deployment_repo.update_deployment(
        db,
        deployment_id,
        status="deployed",
        job_id=str(job_id),
        notebook_path=consumer_path,
    )

    return {
        "mode": mode,
        "job_id": job_id,
        "job_name": job_name,
        "action": action,
        "num_workers": num_workers,
        "spark_version": RTM_DBR,
        "tasks": task_keys,
        "notebook_path": consumer_path,
        "producer_notebook_path": producer_path,
        "url": _job_url(w, job_id),
    }


async def _deploy_zerobus_pipeline(
    db: DBExecutor,
    *,
    setup_id: Any,
    deployment_id: Any,
    industry: str,
    producer_notebook: str,
    single_user_name: str,
    w: Any,
    base_parameters: dict | None = None,
) -> dict:
    from databricks.sdk.service.compute import Environment
    from databricks.sdk.service.jobs import JobEnvironment

    if not producer_notebook:
        raise ValueError("producer_notebook is required for mode 'zerobus'")

    producer_path = _notebook_path(single_user_name, industry, "zerobus", "producer")
    _upload_notebook(w, producer_path, producer_notebook)

    env_key = "zerobus_serverless"
    task = build_zerobus_task(
        producer_notebook_path=producer_path,
        environment_key=env_key,
        base_parameters=base_parameters,
    )
    # client="4": Unity-Catalog secrets via `dbutils.secrets.get(catalog=…,
    # schema=…, key=…)` require serverless environment version ≥ 4.
    # The zerobus producer reads `app_sp_oauth` from the UC secret at runtime.
    environments = [JobEnvironment(environment_key=env_key, spec=Environment(client="4"))]
    job_name = job_display_name(industry, "zerobus")
    job_id, action = _create_or_reset_serverless_job(w, job_name, task, environments)

    await _record_job(
        db,
        setup_id=setup_id,
        deployment_id=deployment_id,
        job_id=job_id,
        job_name=job_name,
        mode="zerobus",
        task_keys=[task.task_key],
        notebook_path=producer_path,
        producer_notebook_path=producer_path,
    )
    await deployment_repo.update_deployment(
        db,
        deployment_id,
        status="deployed",
        job_id=str(job_id),
        notebook_path=producer_path,
    )

    return {
        "mode": "zerobus",
        "job_id": job_id,
        "job_name": job_name,
        "action": action,
        "serverless": True,
        "tasks": [task.task_key],
        "notebook_path": producer_path,
        "url": _job_url(w, job_id),
    }


async def deploy_pipeline(
    db: DBExecutor,
    setup_id: Any,
    mode: str,
    *,
    consumer_notebook: str = "",
    producer_notebook: str = "",
    node_type: str | None = None,
    num_workers: int | None = None,
    single_user_name: str | None = None,
    base_parameters: dict | None = None,
    w: Any | None = None,
    cloud: str | None = None,
    gate_override: bool = False,
) -> dict:
    """Deploy the per-mode job(s) for ``setup_id`` — **hard-gated on validation**.

    Loads the setup, enforces the validation gate for ``mode`` (spec §1.12)
    unless ``gate_override`` is set (caller guarantees the gate), then builds
    and creates/updates the mode's job.  Records the job in the ledger and
    updates the ``pipeline_deployment`` row.

    ``consumer_notebook`` / ``producer_notebook`` are the P5a-generated notebook
    sources (Kafka producer + RTM/micro-batch consumer, or the zerobus producer).

    Returns the per-mode deploy result dict.
    """
    if mode not in VALID_MODES:
        raise ValueError(f"unknown mode: {mode!r} (expected one of {VALID_MODES})")

    setup = await pipeline_setup_repo.get_setup(db, setup_id)
    if setup is None:
        raise ValueError(f"pipeline_setup {setup_id!r} not found")

    if not gate_override:
        assert_gate_passed(setup.get("validation_state"), mode)

    industry = setup.get("industry") or "industry"
    deployment = await _ensure_deployment(db, setup_id, mode)
    deployment_id = deployment.get("id")

    w = w or _new_ws()
    single_user_name = single_user_name or _current_user(w)

    if mode == "zerobus":
        return await _deploy_zerobus_pipeline(
            db,
            setup_id=setup_id,
            deployment_id=deployment_id,
            industry=industry,
            producer_notebook=producer_notebook,
            single_user_name=single_user_name,
            w=w,
            base_parameters=base_parameters,
        )

    # realtime / microbatch — sized classic job cluster.
    partition_count = deployment.get("partition_count")
    if partition_count is None:
        partition_count = (setup.get("derived") or {}).get("partitions")
    resolved_workers = (
        num_workers if num_workers is not None else compute_workers(partition_count)
    )
    resolved_node = (
        node_type
        or (setup.get("cluster_spec") or {}).get("node_type")
        or DEFAULT_NODE_TYPE
    )
    return await _deploy_spark_pipeline(
        db,
        setup_id=setup_id,
        deployment_id=deployment_id,
        industry=industry,
        mode=mode,
        consumer_notebook=consumer_notebook,
        producer_notebook=producer_notebook,
        node_type=resolved_node,
        num_workers=resolved_workers,
        single_user_name=single_user_name,
        w=w,
        cloud=cloud,
    )


# --- Lifecycle: start / stop / status ----------------------------------------
def start_pipeline(job_id: Any, *, w: Any | None = None) -> dict:
    """Trigger a run of the pipeline job.  Returns ``{job_id, run_id, state}``."""
    w = w or _new_ws()
    run = w.jobs.run_now(job_id=int(job_id))
    run_id = getattr(run, "run_id", None)
    logger.info("Started job %s (run_id=%s)", job_id, run_id)
    return {"job_id": job_id, "run_id": run_id, "state": "STARTING"}


def stop_pipeline(job_id: Any, *, w: Any | None = None) -> dict:
    """Cancel all active runs of the pipeline job.  Returns ``{job_id, state}``."""
    w = w or _new_ws()
    w.jobs.cancel_all_runs(job_id=int(job_id))
    logger.info("Stopped job %s (cancelled active runs)", job_id)
    return {"job_id": job_id, "state": "STOPPED"}


# Task life-cycle / result states that mean a task has BROKEN (not merely
# pending/running). A continuous job keeps its RUN life-cycle at RUNNING even when
# one of its tasks has crashed, so we must inspect the tasks to catch e.g. a dead
# producer while the consumer is still up.
_TASK_BROKEN_LIFECYCLES = ("INTERNAL_ERROR",)
_TASK_BROKEN_RESULTS = ("FAILED", "TIMEDOUT", "ERROR", "MAXIMUM_CONCURRENT_RUNS_REACHED")


def pipeline_status(job_id: Any, *, w: Any | None = None) -> dict:
    """Return the latest run state for the pipeline job, INCLUDING task-level health.

    ``{job_id, life_cycle_state, result_state, state, degraded, detail, tasks}``:

    - ``state`` — coarse ``RUNNING`` / ``IDLE`` / ``FAILED`` / ``STARTING`` rollup
      of the run's life-cycle.
    - ``degraded`` — ``True`` when the run is up (RUNNING/STARTING) but ≥1 task has
      crashed (``INTERNAL_ERROR`` / a failed result). This is the "job is RUNNING
      but the producer died, so data isn't flowing" case that the run-level state
      alone hides.
    - ``detail`` — a short human reason naming the broken task(s), or ``None``.
    - ``tasks`` — per-task ``{task, life_cycle, result, message}`` for the UI.
    """
    w = w or _new_ws()
    runs = list(w.jobs.list_runs(job_id=int(job_id), active_only=False, limit=1))
    if not runs:
        return {"job_id": job_id, "state": "IDLE", "life_cycle_state": None,
                "degraded": False, "detail": None, "tasks": []}
    run = runs[0]
    lcs = None
    rs = None
    if run.state:
        lcs = run.state.life_cycle_state.value if run.state.life_cycle_state else None
        rs = run.state.result_state.value if run.state.result_state else None

    if lcs == "RUNNING":
        coarse = "RUNNING"
    elif lcs in ("PENDING", "QUEUED", "BLOCKED"):
        coarse = "STARTING"
    elif lcs in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
        coarse = "IDLE" if rs == "SUCCESS" else "FAILED"
    elif lcs == "TERMINATING":
        coarse = "STOPPING"
    else:
        coarse = "IDLE"

    # Per-task states (for degraded detection). list_runs may omit tasks, so fetch
    # the full run — but only trust a real list, so a mocked/absent value degrades
    # cleanly to "no task info" (run-level state stands).
    task_list = None
    try:
        full = w.jobs.get_run(run_id=run.run_id)
        if isinstance(getattr(full, "tasks", None), list):
            task_list = full.tasks
    except Exception:  # pragma: no cover - best-effort
        pass
    if task_list is None and isinstance(getattr(run, "tasks", None), list):
        task_list = run.tasks

    tasks: list[dict] = []
    broken: list[str] = []
    for tr in (task_list or []):
        tlc = tr.state.life_cycle_state.value if tr.state and tr.state.life_cycle_state else None
        trs = tr.state.result_state.value if tr.state and tr.state.result_state else None
        tmsg = (tr.state.state_message if tr.state else None) or None
        tasks.append({"task": tr.task_key, "life_cycle": tlc, "result": trs, "message": tmsg})
        if tlc in _TASK_BROKEN_LIFECYCLES or trs in _TASK_BROKEN_RESULTS:
            reason = tmsg or trs or tlc or "failed"
            broken.append(f"{tr.task_key}: {str(reason)[:80]}")

    # Degraded = the run is up but a task broke (data likely not flowing).
    degraded = coarse in ("RUNNING", "STARTING") and bool(broken)
    detail = "; ".join(broken) if broken else None

    return {
        "job_id": job_id,
        "life_cycle_state": lcs,
        "result_state": rs,
        "state": coarse,
        "degraded": degraded,
        "detail": detail,
        "tasks": tasks,
    }


# --- Teardown deleter --------------------------------------------------------
def make_job_deleter(w: Any | None = None):
    """Return an async deleter for ``resource_type='job'`` ledger rows.

    Register it with :func:`services.teardown_service.teardown_setup` so jobs
    are deleted in reverse dependency order.  (Kafka topic delete stays skipped
    — DeleteTopic is denied, spec §1.8.)
    """
    ws = w or _new_ws()

    async def _delete(resource_row: dict) -> None:
        job_id = resource_row.get("identifier")
        if not job_id:
            return
        try:
            ws.jobs.cancel_all_runs(job_id=int(job_id))
        except Exception as exc:  # pragma: no cover - best-effort
            logger.warning("cancel_all_runs(%s) skipped during teardown: %s", job_id, exc)
        ws.jobs.delete(job_id=int(job_id))
        logger.info("Deleted job %s during teardown", job_id)

    return _delete
