"""Deploy the LakeTS retention notebook as a scheduled Databricks Job.

Uploads ``databricks/notebooks/lakets_retention.py`` to the authenticated
user's workspace and creates (or updates) a Databricks Job that runs it
daily against the supplied Lakebase project/branch.
"""

from __future__ import annotations

import base64
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger("iot_digital_twin.services.lakets_deploy")

# Location of the notebook source relative to the repo root.
_NOTEBOOK_REL_PATH = Path("databricks") / "notebooks" / "lakets_retention.py"

# Workspace path where the notebook is uploaded (under the running user's home).
_WS_SUBDIR = "iot-digital-twin/lakets"
_WS_FILENAME = "retention"  # no extension — Databricks infers from import format

# Daily 03:00 UTC.  Quartz syntax required by the Jobs API (seconds + ? for DOW).
_DEFAULT_CRON = "0 0 3 * * ?"


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _notebook_source_bytes() -> bytes:
    path = _repo_root() / _NOTEBOOK_REL_PATH
    if not path.exists():
        raise FileNotFoundError(f"Retention notebook not found at {path}")
    return path.read_bytes()


def _job_display_name(project_id: str, branch_id: str) -> str:
    return f"LakeTS Retention — {project_id}/{branch_id}"


def deploy_retention_job(
    project_id: str,
    branch_id: str = "production",
    database: str = "databricks_postgres",
    *,
    schedule_cron: str = _DEFAULT_CRON,
    timezone: str = "UTC",
) -> dict:
    """Upload the retention notebook and create / update the scheduled Job.

    Returns a dict with ``{job_id, notebook_path, schedule, created_or_updated}``.
    """
    if not project_id:
        raise ValueError("project_id is required")

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import jobs as dj
    from databricks.sdk.service import workspace as dw
    from databricks.sdk.service.compute import (
        DataSecurityMode,
        ClusterSpec,
    )

    w = WorkspaceClient()
    user = w.current_user.me()
    user_email = user.user_name
    workspace_path = f"/Workspace/Users/{user_email}/{_WS_SUBDIR}/{_WS_FILENAME}"

    # ── 1. Ensure parent folders exist ────────────────────────────────────
    parent = f"/Workspace/Users/{user_email}/{_WS_SUBDIR}"
    try:
        w.workspace.mkdirs(parent)
    except Exception as exc:
        logger.debug("mkdirs(%s) skipped: %s", parent, exc)

    # ── 2. Upload notebook (overwrite on every deploy) ────────────────────
    content = _notebook_source_bytes()
    w.workspace.import_(
        path=workspace_path,
        format=dw.ImportFormat.SOURCE,
        language=dw.Language.PYTHON,
        overwrite=True,
        content=base64.b64encode(content).decode("ascii"),
    )
    logger.info("Uploaded retention notebook to %s (%d bytes)", workspace_path, len(content))

    # ── 3. Find existing job by exact name ────────────────────────────────
    job_name = _job_display_name(project_id, branch_id)
    existing_job_id: int | None = None
    for j in w.jobs.list(name=job_name):
        if j.settings and j.settings.name == job_name:
            existing_job_id = j.job_id
            break

    # ── 4. Build job settings (shared between create + reset) ─────────────
    task = dj.Task(
        task_key="lakets_retention",
        notebook_task=dj.NotebookTask(
            notebook_path=workspace_path,
            source=dj.Source.WORKSPACE,
            base_parameters={
                "project_id": project_id,
                "branch_id": branch_id,
                "database": database,
            },
        ),
        new_cluster=ClusterSpec(
            spark_version="15.4.x-scala2.12",
            node_type_id="m5d.xlarge",
            num_workers=0,
            spark_conf={
                "spark.databricks.cluster.profile": "singleNode",
                "spark.master": "local[*]",
            },
            custom_tags={"ResourceClass": "SingleNode"},
            data_security_mode=DataSecurityMode.SINGLE_USER,
            single_user_name=user_email,
        ),
        timeout_seconds=600,
    )

    job_settings = dj.JobSettings(
        name=job_name,
        tasks=[task],
        schedule=dj.CronSchedule(
            quartz_cron_expression=schedule_cron,
            timezone_id=timezone,
            pause_status=dj.PauseStatus.UNPAUSED,
        ),
        max_concurrent_runs=1,
        tags={
            "app": "iot-digital-twin",
            "component": "lakets-retention",
            "project": project_id,
        },
    )

    # ── 5. Create or reset in place ───────────────────────────────────────
    if existing_job_id is not None:
        w.jobs.reset(job_id=existing_job_id, new_settings=job_settings)
        job_id = existing_job_id
        action = "updated"
    else:
        created = w.jobs.create(
            name=job_settings.name,
            tasks=job_settings.tasks,
            schedule=job_settings.schedule,
            max_concurrent_runs=job_settings.max_concurrent_runs,
            tags=job_settings.tags,
        )
        job_id = created.job_id
        action = "created"

    logger.info("Retention job %s (id=%s) for %s", action, job_id, job_name)

    host = w.config.host.rstrip("/") if w.config.host else ""
    return {
        "job_id": job_id,
        "job_name": job_name,
        "notebook_path": workspace_path,
        "schedule": {"cron": schedule_cron, "timezone": timezone},
        "action": action,
        "url": f"{host}/jobs/{job_id}" if host else None,
    }
