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

# Notebook source is inlined rather than read from disk.  `databricks sync`
# treats any `.py` file starting with ``# Databricks notebook source`` as a
# workspace notebook and strips the ``.py`` extension during upload — which
# makes it unreachable via the deployed app's filesystem.  Keeping the source
# inline here sidesteps that round-trip entirely.
_RETENTION_NOTEBOOK_SOURCE = b"""# Databricks notebook source
# MAGIC %md
# MAGIC # LakeTS Retention
# MAGIC
# MAGIC Scheduled daily job.  Invokes `lakets.execute_retention(<table>)` for every
# MAGIC ChronoTable that has an enabled retention or tiered_retention policy on the
# MAGIC target Lakebase.  Prints a JSON summary of dropped-chunk counts per table.

# COMMAND ----------
# MAGIC %pip install psycopg2-binary databricks-sdk --quiet

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json
import os
import psycopg2

dbutils.widgets.text("project_id", "", "Lakebase project")
dbutils.widgets.text("branch_id", "production", "Lakebase branch")
dbutils.widgets.text("database", "databricks_postgres", "Postgres database")

project_id = dbutils.widgets.get("project_id").strip()
branch_id = dbutils.widgets.get("branch_id").strip() or "production"
database = dbutils.widgets.get("database").strip() or "databricks_postgres"

if not project_id:
    raise ValueError("project_id widget is empty")

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
parent = f"projects/{project_id}/branches/{branch_id}"
endpoints = list(w.postgres.list_endpoints(parent=parent))
if not endpoints:
    raise RuntimeError(f"No endpoints on {parent}")

endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)
pg_user = os.getenv("DATABRICKS_CLIENT_ID") or w.current_user.me().user_name

conn = psycopg2.connect(
    host=endpoint.status.hosts.host,
    port=5432,
    dbname=database,
    user=pg_user,
    password=cred.token,
    sslmode="require",
)
conn.autocommit = True

results: dict = {}
with conn.cursor() as cur:
    cur.execute(
        "SELECT DISTINCT c.schema_name, c.table_name "
        "FROM lakets._policy_registry p "
        "JOIN lakets._chronotable_registry c ON p.chronotable_id = c.id "
        "WHERE p.policy_type IN ('retention', 'tiered_retention') "
        "  AND p.enabled = TRUE"
    )
    tables = cur.fetchall()

for schema, table in tables:
    key = f"{schema}.{table}"
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT lakets.execute_retention(%s, %s)", (table, schema))
            dropped = cur.fetchone()[0]
        results[key] = dropped
        print(f"Retention on {key}: dropped {dropped} chunk(s)")
    except Exception as exc:
        results[key] = f"error: {exc}"
        print(f"Retention on {key} FAILED: {exc}")

conn.close()

print("\\n==== Summary ====")
print(json.dumps(results, indent=2, default=str))

if any(isinstance(v, str) and v.startswith("error:") for v in results.values()):
    dbutils.notebook.exit(json.dumps({"status": "partial_failure", "results": results}))
else:
    dbutils.notebook.exit(json.dumps({"status": "ok", "results": results}))
"""

# Workspace path where the notebook is uploaded (under the running user's home).
_WS_SUBDIR = "iot-digital-twin/lakets"
_WS_FILENAME = "retention"  # no extension — Databricks infers from import format

# Daily 03:00 UTC.  Quartz syntax required by the Jobs API (seconds + ? for DOW).
_DEFAULT_CRON = "0 0 3 * * ?"


def _notebook_source_bytes() -> bytes:
    """Return the inlined retention-notebook source as raw bytes."""
    return _RETENTION_NOTEBOOK_SOURCE


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
    from databricks.sdk.service.compute import Environment

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

    # ── 4. Build job settings (serverless + performance-optimized) ───────
    # Notebook task references the job-level serverless environment via
    # environment_key.  No new_cluster — compute is provisioned by the
    # serverless job runtime, and the job-level performance_target pins it
    # to PERFORMANCE_OPTIMIZED for fast startup (daily cron, short runs).
    env_key = "lakets_serverless"
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
        environment_key=env_key,
        timeout_seconds=600,
    )

    job_settings = dj.JobSettings(
        name=job_name,
        tasks=[task],
        environments=[
            dj.JobEnvironment(
                environment_key=env_key,
                spec=Environment(
                    client="2",
                    dependencies=["psycopg2-binary", "databricks-sdk"],
                ),
            ),
        ],
        performance_target=dj.PerformanceTarget.PERFORMANCE_OPTIMIZED,
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
            environments=job_settings.environments,
            performance_target=job_settings.performance_target,
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
