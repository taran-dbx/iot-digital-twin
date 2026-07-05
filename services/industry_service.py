"""Industry catalog status resolution.

Joins the curated industry/mode templates (``services.industry_templates``)
against real pipeline rows in the Lakebase ``pipelines`` table to report, per
curated industry, which of its three streaming modes are
provisioned/running. This is the data the gallery + setup screens consume.

Naming is fully deterministic — a mode's pipeline/catalog/topic name is
derived from ``(industry, mode)`` alone (see ``pipeline_name``,
``catalog_name``, ``topic_name``), so a pipeline row can be joined back to
its template slot with no extra lookup table.
"""

from __future__ import annotations

import inspect
import json
import logging
from typing import Any

from core.state import AppState
from services.industry_templates import (
    CURATED_INDUSTRIES,
    MODES,
    get_template,
    mode_config,
)
from services.provisioning_service import ensure_shared_streaming_sp

logger = logging.getLogger("iot_digital_twin.services.industry_service")

# Pipeline lifecycle values (row "status", falling back to "state") that map
# to a "running" / "starting" mode status. Anything else — including no
# matching pipeline row at all — reports as "idle".
_RUNNING_STATES = {"running"}
_STARTING_STATES = {"starting", "pending", "queued", "provisioning", "deploying", "blocked"}


def pipeline_name(industry: str, mode: str) -> str:
    """Deterministic pipeline name for a curated industry's streaming mode."""
    return f"iot-{industry}-{mode}"


def catalog_name(industry: str) -> str:
    """Deterministic Unity Catalog name for a curated industry."""
    return f"iot_{industry}"


def topic_name(industry: str, mode: str) -> str:
    """Deterministic Kafka topic name for a curated industry's streaming mode."""
    return f"iot.{industry}.{mode}"


async def _list_pipelines(state: AppState) -> list[dict[str, Any]]:
    """Return non-deleted pipeline rows (``name`` + lifecycle state).

    Thin wrapper around the same ``pipelines`` table access
    ``routes.pipelines.list_pipelines`` uses (``state.db.fetch`` against the
    Lakebase-backed ``pipelines`` table, excluding ``status = 'deleted'``).
    The gallery/setup catalog is not user-scoped — it needs the lifecycle of
    every curated-industry pipeline, not just one user's — so this omits
    that route's ``user_email`` filter.
    """
    if not state or not getattr(state, "db_available", False) or not getattr(state, "db", None):
        return []
    try:
        rows = await state.db.fetch(
            "SELECT name, status FROM pipelines WHERE status != 'deleted'"
        )
    except Exception as exc:
        logger.warning("Failed to list pipelines for industry catalog: %s", exc)
        return []
    return [dict(row) for row in rows]


def _mode_status(row: dict[str, Any] | None) -> str:
    """Map a pipeline row's lifecycle state to a mode status; absent -> idle."""
    if row is None:
        return "idle"
    state_value = str(row.get("status") or row.get("state") or "").lower()
    if state_value in _RUNNING_STATES:
        return "running"
    if state_value in _STARTING_STATES:
        return "starting"
    return "idle"


async def get_catalog(state: AppState) -> list[dict[str, Any]]:
    """Return one gallery entry per curated industry, with per-mode status.

    Each entry: ``{id, name, accent, icon, twin_type, tag, modes, running_count}``
    where ``modes`` is ``[{id, name, to, desc, status}]`` and ``status`` is
    one of ``"idle" | "starting" | "running"``.
    """
    rows = await _list_pipelines(state)
    rows_by_name = {row["name"]: row for row in rows if row.get("name")}

    catalog: list[dict[str, Any]] = []
    for industry in CURATED_INDUSTRIES:
        template = get_template(industry)
        if template is None:
            continue

        modes: list[dict[str, Any]] = []
        running_count = 0
        for mode in MODES:
            cfg = mode_config(mode)
            status = _mode_status(rows_by_name.get(pipeline_name(industry, mode)))
            if status == "running":
                running_count += 1
            modes.append(
                {
                    "id": mode,
                    "name": cfg["name"],
                    "to": cfg["to"],
                    "desc": cfg["desc"],
                    "status": status,
                }
            )

        catalog.append(
            {
                "id": template.id,
                "name": template.name,
                "accent": template.accent,
                "icon": template.icon,
                "twin_type": template.twin_type,
                "tag": template.tag,
                "modes": modes,
                "running_count": running_count,
            }
        )

    return catalog


# ---------------------------------------------------------------------------
# Start / stop a mode's pipeline
# ---------------------------------------------------------------------------

# Curated-industry pipelines are app-wide (see ``get_catalog``'s docstring) —
# they are not created through the per-user wizard
# (``routes/pipelines.py::create_pipeline``), so there is no end-user
# ``user_email`` to attribute the row to. ``pipelines.user_email`` is
# NOT NULL (see ``backend.py::ensure_pipelines_table``), so a fixed system
# owner is used for every curated-industry row.
_INDUSTRY_PIPELINE_OWNER = "industries@iot-digital-twin.system"

# Status ``stop_mode`` writes. Reuses the exact value
# ``routes/pipelines.py::reset_pipeline`` already writes for "provisioned but
# not currently running" — deliberately NOT in ``_RUNNING_STATES`` or
# ``_STARTING_STATES`` above, so ``_mode_status`` reports it as "idle",
# which is what the gallery/setup screens should show once a mode is stopped.
_STOPPED_STATUS = "step2_complete"


async def _call_maybe_async(fn: Any, *args: Any, **kwargs: Any) -> Any:
    """Call ``fn`` and await its result if ``fn`` returned an awaitable.

    The provisioning seams below (``_provision_catalog_schema``,
    ``_provision_lakebase_branch``, ``_deploy_and_start``) and
    ``ensure_shared_streaming_sp`` are real ``async def`` functions in
    production, but ``tests/test_industry_service_start.py`` (per the task
    brief) monkeypatches them with plain synchronous lambdas that return a
    value directly instead of a coroutine. This lets ``start_mode`` /
    ``stop_mode`` call every seam the same way regardless of which shape the
    caller installed.
    """
    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        result = await result
    return result


def _parse_resources(row: dict[str, Any] | None) -> dict[str, Any]:
    """Best-effort JSON-decode a pipeline row's ``resources`` column."""
    if not row:
        return {}
    resources = row.get("resources")
    if isinstance(resources, str):
        try:
            return json.loads(resources) or {}
        except (json.JSONDecodeError, TypeError):
            return {}
    return resources or {}


async def _ensure_pipeline_row(
    state: AppState, name: str, industry: str, status: str,
) -> str | None:
    """Ensure a pipeline row named ``name`` exists, set to ``status``.

    Idempotent lookup-by-name — deterministic naming means no other lookup
    key is needed. Returns the row's id, or ``None`` when no DB is
    configured; the caller (``start_mode``) degrades gracefully in that
    case, same as every other DB-touching helper in this module.
    """
    if not state or not getattr(state, "db_available", False) or not getattr(state, "db", None):
        return None
    try:
        row = await state.db.fetchrow(
            "SELECT id FROM pipelines WHERE name = $1 AND status != 'deleted'", name,
        )
        if row is None:
            row = await state.db.fetchrow(
                """
                INSERT INTO pipelines (user_email, name, industry, status)
                VALUES ($1, $2, $3, $4)
                RETURNING id
                """,
                _INDUSTRY_PIPELINE_OWNER, name, industry, status,
            )
        else:
            await state.db.execute(
                "UPDATE pipelines SET status = $2, updated_at = NOW() WHERE id = $1",
                row["id"], status,
            )
    except Exception as exc:
        logger.warning("Failed to ensure pipeline row for %s: %s", name, exc)
        return None
    return str(row["id"]) if row else None


async def _set_pipeline_status(state: AppState, name: str, status: str) -> None:
    """Write ``status`` onto the pipeline row named ``name`` (non-fatal)."""
    if not state or not getattr(state, "db_available", False) or not getattr(state, "db", None):
        return
    try:
        await state.db.execute(
            "UPDATE pipelines SET status = $2, updated_at = NOW() WHERE name = $1",
            name, status,
        )
    except Exception as exc:
        logger.warning("Failed to set status for pipeline %s: %s", name, exc)


async def _get_job_id(state: AppState, pipeline_id: str | None) -> str | None:
    """Resolve the tracked ``job_id`` (if any) for a pipeline row by id."""
    if not pipeline_id or not state or not getattr(state, "db_available", False) or not getattr(state, "db", None):
        return None
    try:
        row = await state.db.fetchrow(
            "SELECT resources FROM pipelines WHERE id = $1::uuid", pipeline_id,
        )
    except Exception as exc:
        logger.warning("Failed to look up job_id for pipeline %s: %s", pipeline_id, exc)
        return None
    return _parse_resources(row).get("job_id")


async def _get_pipeline_id_and_job(
    state: AppState, name: str,
) -> tuple[str | None, str | None]:
    """Resolve ``(pipeline_id, job_id)`` for the mode's pipeline row by name."""
    if not state or not getattr(state, "db_available", False) or not getattr(state, "db", None):
        return None, None
    try:
        row = await state.db.fetchrow(
            "SELECT id, resources FROM pipelines WHERE name = $1 AND status != 'deleted'", name,
        )
    except Exception as exc:
        logger.warning("Failed to look up pipeline %s: %s", name, exc)
        return None, None
    if not row:
        return None, None
    return str(row["id"]), _parse_resources(row).get("job_id")


async def _provision_catalog_schema(industry: str, mode: str) -> list[dict]:
    """Idempotently create catalog ``iot_<industry>`` with schema ``<mode>``.

    Thin wrapper around ``provisioning_service.provision_uc_catalog`` — the
    real UC catalog/schema provisioning already used by the settings page
    and the pipeline wizard. Passes the exact catalog name (no ``{name}``
    templating) and a single schema (the mode name), so no new provisioning
    logic is introduced here.
    """
    from services.provisioning_service import provision_uc_catalog

    return await provision_uc_catalog({
        "catalog_pattern": catalog_name(industry),
        "schemas": [mode],
    })


async def _provision_lakebase_branch(
    state: AppState, industry: str, mode: str, pipeline_id: str | None,
) -> dict:
    """Provision the mode's Lakebase branch + LakeTS (realtime only).

    Thin wrapper around the existing one-click orchestrator
    ``pipeline_setup_service.setup_pipeline_lakebase`` — reuses its full
    Lakebase project/branch/role/table/rollup/ops-job chain rather than
    reimplementing any of it. Each mode gets its own dedicated Lakebase
    project, named after the mode's own deterministic pipeline name, so
    ``create_new_project=True`` is always safe to pass — idempotent, since
    ``provision_lakebase_project_and_branch`` no-ops on an already-existing
    project/branch.
    """
    from services.pipeline_setup_service import setup_pipeline_lakebase

    params = {
        "pipeline_id": pipeline_id,
        "create_new_project": True,
        "lb_project_id": pipeline_name(industry, mode),
        "lb_branch_id": "production",
        "lb_database": "databricks_postgres",
        "table_name": "sensor_readings",
        "secret_scope": "iot-secrets",
        "industry": industry,
    }
    return await setup_pipeline_lakebase(state, params)


async def _deploy_and_start(
    state: AppState, industry: str, mode: str, pipeline_id: str | None,
) -> dict:
    """Deploy (idempotently) and start the mode's streaming job.

    Zerobus writes straight to Delta via the SDK — there is no Spark job for
    that mode (``mode_config("zerobus")["uses_kafka"] is False``), so once
    provisioning above finishes the mode is already live:
    returns ``{"status": "running"}`` directly.

    Realtime/microbatch run as a ``Continuous`` Databricks Job — the same
    trigger type ``routes/deployment.py::deploy_job`` creates, which
    self-starts on create/unpause (no separate ``run_now`` call needed).
    Creating that job from nothing requires the SSS consumer notebook's
    *content*, which is generated client-side
    (``client/src/lib/notebook-template.ts``) and supplied through
    ``POST /api/cluster/deploy-job`` — outside this backend-only service's
    scope (wiring that route is a later task). So:
      - if a job is already deployed for this pipeline
        (``resources.job_id``, set by that same deploy route on a previous
        run), (re)start it idempotently by unpausing it — exactly like
        ``routes/jobs.py::restart_job``;
      - otherwise, report ``"starting"`` truthfully (no job has actually
        been created/started yet) rather than fabricating a job this
        service has no real notebook content to give.
    """
    if not mode_config(mode).get("uses_kafka"):
        return {"status": "running", "note": "no Spark job for this mode"}

    job_id = await _get_job_id(state, pipeline_id)
    if not job_id:
        return {
            "status": "starting",
            "note": (
                "No job deployed yet for this pipeline — job creation needs "
                "the SSS consumer notebook content (generated client-side) "
                "and is wired by the deploy route, not this service."
            ),
        }

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import Continuous, JobSettings, PauseStatus

    w = getattr(state, "workspace_client", None) or WorkspaceClient()
    try:
        w.jobs.update(
            job_id=int(job_id),
            new_settings=JobSettings(continuous=Continuous(pause_status=PauseStatus.UNPAUSED)),
        )
    except Exception as exc:
        logger.warning(
            "Failed to unpause job %s for %s: %s", job_id, pipeline_name(industry, mode), exc,
        )
        return {"status": "starting", "job_id": job_id, "error": str(exc)[:200]}
    return {"status": "running", "job_id": job_id}


def _stop_job(job_id: str, state: AppState | None = None) -> dict:
    """Cancel active runs and pause the continuous job (keeps every asset).

    Mirrors ``routes/jobs.py::stop_job`` exactly — the canonical
    "stop but keep resources" pair with ``_deploy_and_start``'s unpause path.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import Continuous, JobSettings, PauseStatus

    w = getattr(state, "workspace_client", None) or WorkspaceClient()
    try:
        w.jobs.cancel_all_runs(job_id=int(job_id))
    except Exception as exc:  # non-fatal — pausing below still stops future runs
        logger.warning("cancel_all_runs failed for job %s (non-fatal): %s", job_id, exc)
    try:
        w.jobs.update(
            job_id=int(job_id),
            new_settings=JobSettings(continuous=Continuous(pause_status=PauseStatus.PAUSED)),
        )
        return {"status": "stopped", "job_id": job_id}
    except Exception as exc:
        return {"status": "failed", "job_id": job_id, "error": str(exc)[:200]}


async def start_mode(state: AppState, industry: str, mode: str) -> dict:
    """Provision (idempotently) and start one curated industry's mode.

    Owns the pipeline row's status transitions so the gallery/setup screens
    reflect reality: writes ``'starting'`` before any provisioning begins,
    and ``'running'`` once the job is confirmed started (per
    ``_deploy_and_start``'s result). Order: catalog+schema -> shared SP ->
    (realtime only) Lakebase branch+LakeTS -> topic (streaming modes) ->
    deploy+start job.
    """
    name = pipeline_name(industry, mode)
    cfg = mode_config(mode)
    steps: list[dict[str, Any]] = []

    pipeline_id = await _ensure_pipeline_row(state, name, industry, status="starting")

    catalog_result = await _call_maybe_async(_provision_catalog_schema, industry, mode)
    steps.append({"step": "catalog_schema", "result": catalog_result})

    sp_result = await _call_maybe_async(ensure_shared_streaming_sp, state)
    steps.append({"step": "shared_sp", "result": sp_result})

    if cfg.get("uses_lakebase"):
        lakebase_result = await _call_maybe_async(
            _provision_lakebase_branch, state, industry, mode, pipeline_id,
        )
        steps.append({"step": "lakebase_branch", "result": lakebase_result})

    if cfg.get("uses_kafka"):
        steps.append({"step": "topic", "topic": topic_name(industry, mode)})

    deploy_result = await _call_maybe_async(_deploy_and_start, state, industry, mode, pipeline_id)
    steps.append({"step": "deploy_and_start", "result": deploy_result})

    final_status = "starting"
    if isinstance(deploy_result, dict) and deploy_result.get("status") == "running":
        final_status = "running"
        await _set_pipeline_status(state, name, "running")

    return {"pipeline": name, "status": final_status, "steps": steps}


async def stop_mode(state: AppState, industry: str, mode: str) -> dict:
    """Stop the mode's job, keeping all provisioned assets (branch kept forever).

    Sets the pipeline row's status to ``_STOPPED_STATUS``
    (``"step2_complete"``) — see that constant's docstring for why this maps
    to "idle" (via ``_mode_status``) rather than "running"/"starting".
    """
    name = pipeline_name(industry, mode)
    steps: list[dict[str, Any]] = []

    _pipeline_id, job_id = await _get_pipeline_id_and_job(state, name)

    if job_id:
        stop_result = _stop_job(job_id, state)
        steps.append({"step": "stop_job", "result": stop_result})
    else:
        steps.append(
            {"step": "stop_job", "result": {"status": "skipped", "note": "no job deployed"}}
        )

    await _set_pipeline_status(state, name, _STOPPED_STATUS)

    return {
        "pipeline": name,
        "status": _mode_status({"status": _STOPPED_STATUS}),
        "steps": steps,
    }
