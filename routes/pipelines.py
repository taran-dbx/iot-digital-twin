"""Pipeline CRUD endpoints."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any

from fastapi import APIRouter, Query, Request, Depends
from fastapi.responses import JSONResponse

from core.auth import get_current_user
from core.state import AppState, get_app_state
from services.pipeline_service import pipeline_row_to_dict
from services.producer_service import stop_data_producer
from services.source_registry import check_source_available, claim_source, release_source

logger = logging.getLogger("iot_digital_twin.routes.pipelines")
router = APIRouter()


@router.get("/api/pipelines")
async def list_pipelines(
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    rows = await state.db.fetch(
        "SELECT * FROM pipelines WHERE user_email = $1 AND status != 'deleted' ORDER BY created_at DESC",
        user_email,
    )
    return [pipeline_row_to_dict(r) for r in rows]


@router.post("/api/pipelines")
async def create_pipeline(
    request: Request,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    name = (body.get("name") or "").strip()
    if not name:
        return JSONResponse(status_code=422, content={"error": "'name' is required"})

    dup = await state.db.fetchrow(
        "SELECT id FROM pipelines WHERE user_email = $1 AND LOWER(name) = LOWER($2) AND status != 'deleted'",
        user_email, name,
    )
    if dup:
        return JSONResponse(status_code=409, content={"error": f"A pipeline named '{name}' already exists"})

    source_type = body.get("source_type")
    source_config = body.get("source_config")
    if source_type and source_config:
        try:
            availability = await check_source_available(
                db=state.db, source_type=source_type, source_config=source_config
            )
        except (ValueError, KeyError) as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})
        if not availability["available"]:
            return JSONResponse(
                status_code=409,
                content={
                    "error": "Source already claimed by another pipeline",
                    "claimed_by_pipeline": availability.get("claimed_by_pipeline"),
                    "claimed_by": availability.get("claimed_by"),
                },
            )

    row = await state.db.fetchrow(
        """
        INSERT INTO pipelines (user_email, name, industry)
        VALUES ($1, $2, $3)
        RETURNING *
        """,
        user_email,
        name,
        body.get("industry"),
    )

    if source_type and source_config and row:
        try:
            await claim_source(
                db=state.db,
                pipeline_id=str(row["id"]),
                source_type=source_type,
                source_config=source_config,
                user_email=user_email,
                partition_count=body.get("partition_count"),
            )
        except Exception as exc:
            logger.warning("claim_source failed after pipeline insert: %s", exc)

    return pipeline_row_to_dict(row)


@router.post("/api/pipelines/check-source")
async def check_source(
    request: Request,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Check whether a source is available (not claimed by any active pipeline)."""
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    body = await request.json()
    source_type = body.get("source_type")
    source_config = body.get("source_config")
    if not source_type or not source_config:
        return JSONResponse(
            status_code=400,
            content={"detail": "source_type and source_config required"},
        )
    try:
        result = await check_source_available(
            db=state.db, source_type=source_type, source_config=source_config
        )
        return JSONResponse(content=result)
    except (ValueError, KeyError) as exc:
        return JSONResponse(status_code=400, content={"detail": str(exc)})


@router.get("/api/pipelines/{pipeline_id}")
async def get_pipeline(
    pipeline_id: str,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    row = await state.db.fetchrow(
        "SELECT * FROM pipelines WHERE id = $1::uuid AND user_email = $2",
        pipeline_id, user_email,
    )
    if not row:
        return JSONResponse(status_code=404, content={"error": "Pipeline not found"})
    return pipeline_row_to_dict(row)


@router.put("/api/pipelines/{pipeline_id}")
async def update_pipeline(
    pipeline_id: str,
    request: Request,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    existing = await state.db.fetchrow(
        "SELECT id FROM pipelines WHERE id = $1::uuid AND user_email = $2",
        pipeline_id, user_email,
    )
    if not existing:
        return JSONResponse(status_code=404, content={"error": "Pipeline not found"})

    if "name" in body and body["name"]:
        new_name = body["name"].strip()
        dup = await state.db.fetchrow(
            "SELECT id FROM pipelines WHERE user_email = $1 AND LOWER(name) = LOWER($2) AND id != $3::uuid AND status != 'deleted'",
            user_email, new_name, pipeline_id,
        )
        if dup:
            return JSONResponse(status_code=409, content={"error": f"A pipeline named '{new_name}' already exists"})

    allowed = {
        "name", "status", "industry", "sensor_config",
        "source_type", "source_config", "sink_config", "resources",
    }
    merge_jsonb = {"source_config", "sink_config", "resources"}
    sets: list[str] = ["updated_at = NOW()"]
    params: list[object] = []
    idx = 1
    for key in allowed:
        if key in body:
            idx += 1
            val = body[key]
            if key in ("sensor_config", "source_config", "sink_config", "resources"):
                val = json.dumps(val)
            if key in merge_jsonb:
                sets.append(f"{key} = COALESCE({key}, '{{}}'::jsonb) || ${idx}::jsonb")
            else:
                sets.append(f"{key} = ${idx}")
            params.append(val)

    query = f"UPDATE pipelines SET {', '.join(sets)} WHERE id = $1::uuid RETURNING *"
    row = await state.db.fetchrow(query, pipeline_id, *params)
    return pipeline_row_to_dict(row)


@router.delete("/api/pipelines/{pipeline_id}")
async def delete_pipeline(
    pipeline_id: str,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Delete a pipeline and clean up all provisioned resources except notebooks."""
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})

    row = await state.db.fetchrow(
        "SELECT * FROM pipelines WHERE id = $1::uuid AND user_email = $2",
        pipeline_id, user_email,
    )
    if not row:
        return JSONResponse(status_code=404, content={"error": "Pipeline not found"})

    resources = json.loads(row["resources"]) if row["resources"] else {}

    if pipeline_id in state.producer_states:
        try:
            await stop_data_producer(state.producer_states, pipeline_id)
        except Exception:
            pass

    try:
        await release_source(db=state.db, pipeline_id=pipeline_id)
    except Exception as exc:
        logger.warning("release_source failed during pipeline delete: %s", exc)

    await state.db.execute(
        "UPDATE pipelines SET status = 'deleted', updated_at = NOW() WHERE id = $1::uuid",
        pipeline_id,
    )

    cleanup_log: list[str] = []

    def _sdk_cleanup() -> list[str]:
        log: list[str] = []
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()

            if resources.get("job_id"):
                try:
                    w.jobs.delete(job_id=int(resources["job_id"]))
                    log.append(f"Deleted job {resources['job_id']}")
                except Exception as exc:
                    log.append(f"Job delete failed: {exc}")

            if resources.get("cluster_id"):
                try:
                    w.clusters.permanent_delete(cluster_id=resources["cluster_id"])
                    log.append(f"Deleted cluster {resources['cluster_id']}")
                except Exception as exc:
                    log.append(f"Cluster delete failed: {exc}")

            if resources.get("uc_schema") and resources.get("uc_catalog"):
                full_schema = f"{resources['uc_catalog']}.{resources['uc_schema']}"
                try:
                    w.schemas.delete(full_name=full_schema)
                    log.append(f"Deleted schema {full_schema}")
                except Exception as exc:
                    log.append(f"Schema delete failed: {exc}")
        except Exception as exc:
            log.append(f"SDK init failed: {exc}")
        return log

    try:
        cleanup_log = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, _sdk_cleanup),
            timeout=15,
        )
    except asyncio.TimeoutError:
        cleanup_log.append("SDK cleanup timed out after 15s; resources may be orphaned")

    if resources.get("lakebase_table"):
        import re
        table_name = resources["lakebase_table"]
        if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_.]*", table_name):
            cleanup_log.append(f"Skipped table drop — invalid name: {table_name!r}")
        else:
            try:
                await state.db.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                cleanup_log.append(f"Dropped table {table_name}")
            except Exception as exc:
                cleanup_log.append(f"Table drop failed: {exc}")

    return {"deleted": True, "cleanup_log": cleanup_log}


@router.post("/api/pipelines/{pipeline_id}/reset")
async def reset_pipeline(
    pipeline_id: str,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Stop the running job/cluster and reset pipeline to step2_complete for re-deployment.

    Preserves UC catalog, secrets, and Lakebase tables.
    """
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})

    row = await state.db.fetchrow(
        "SELECT * FROM pipelines WHERE id = $1::uuid AND user_email = $2 AND status != 'deleted'",
        pipeline_id, user_email,
    )
    if not row:
        return JSONResponse(status_code=404, content={"error": "Pipeline not found"})

    resources = json.loads(row["resources"]) if isinstance(row["resources"], str) else (row["resources"] or {})
    cleanup_log: list[str] = []

    # Stop job and cluster in background
    def _cleanup() -> list[str]:
        log: list[str] = []
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()

            job_id = resources.get("job_id")
            if job_id:
                try:
                    w.jobs.cancel_all_runs(job_id=int(job_id))
                    log.append(f"Cancelled runs for job {job_id}")
                except Exception as exc:
                    log.append(f"Job cancel failed: {exc}")

            # Note: do NOT delete the validation cluster here — it's the user's
            # all-purpose cluster for testing. Job clusters are ephemeral and
            # auto-deleted when the job stops.
        except Exception as exc:
            log.append(f"SDK init failed: {exc}")
        return log

    try:
        cleanup_log = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, _cleanup),
            timeout=15,
        )
    except asyncio.TimeoutError:
        cleanup_log.append("Cleanup timed out after 15s")

    # Clear job/cluster/notebook from resources, keep catalog/secrets/tables
    keep_keys = {"uc_catalog", "uc_schema", "secret_scope", "checkpoint_volume", "catalog_schemas", "lakebase_table", "job_id"}
    cleaned_resources = {k: v for k, v in resources.items() if k in keep_keys}

    updated = await state.db.fetchrow(
        """UPDATE pipelines
           SET status = 'step2_complete', resources = $2::jsonb, updated_at = NOW()
           WHERE id = $1::uuid
           RETURNING *""",
        pipeline_id, json.dumps(cleaned_resources),
    )

    # Stop any in-memory producer
    if pipeline_id in state.producer_states:
        try:
            await stop_data_producer(state.producer_states, pipeline_id)
        except Exception:
            pass

    return {
        "pipeline": pipeline_row_to_dict(updated),
        "cleanup_log": cleanup_log,
    }


@router.post("/api/pipelines/{pipeline_id}/clone")
async def clone_pipeline(
    pipeline_id: str,
    request: Request,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Clone an existing pipeline's configuration into a new draft pipeline."""
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})

    source = await state.db.fetchrow(
        "SELECT * FROM pipelines WHERE id = $1::uuid AND user_email = $2 AND status != 'deleted'",
        pipeline_id, user_email,
    )
    if not source:
        return JSONResponse(status_code=404, content={"error": "Pipeline not found"})

    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    new_name = (body.get("name") or "").strip()
    if not new_name:
        return JSONResponse(status_code=422, content={"error": "'name' is required"})

    dup = await state.db.fetchrow(
        "SELECT id FROM pipelines WHERE user_email = $1 AND LOWER(name) = LOWER($2) AND status != 'deleted'",
        user_email, new_name,
    )
    if dup:
        return JSONResponse(status_code=409, content={"error": f"A pipeline named '{new_name}' already exists"})

    # Copy config fields, reset resources and status
    sensor_config = source["sensor_config"] if source["sensor_config"] else "[]"
    source_config = source["source_config"] if source["source_config"] else "{}"
    sink_config = source["sink_config"] if source["sink_config"] else "{}"

    row = await state.db.fetchrow(
        """
        INSERT INTO pipelines (user_email, name, industry, sensor_config, source_type, source_config, sink_config)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6::jsonb, $7::jsonb)
        RETURNING *
        """,
        user_email,
        new_name,
        source["industry"],
        sensor_config,
        source["source_type"],
        source_config,
        sink_config,
    )
    return pipeline_row_to_dict(row)


@router.get("/api/pipeline/{pipeline_id}/history")
async def pipeline_history(
    pipeline_id: str,
    days: int = Query(90, ge=1, le=365, description="Lookback window in days"),
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    """Query historical sensor data from the pipeline's UC catalog via DBSQL."""
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})

    row = await state.db.fetchrow(
        "SELECT resources FROM pipelines WHERE id = $1::uuid AND user_email = $2",
        pipeline_id, user_email,
    )
    if not row:
        return JSONResponse(status_code=404, content={"error": "Pipeline not found"})

    resources = row["resources"] if isinstance(row["resources"], dict) else json.loads(row["resources"] or "{}")
    catalog = resources.get("uc_catalog")
    if not catalog:
        return JSONResponse(status_code=400, content={"error": "Pipeline has no UC catalog"})

    import re
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", catalog):
        return JSONResponse(status_code=400, content={"error": "Invalid catalog name"})
    table = f"{catalog}.raw.sensor_data"

    try:
        import httpx
        host = os.getenv("DATABRICKS_HOST", "")
        token = os.getenv("DATABRICKS_TOKEN", "")
        if not token:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            token = w.config.token

        sql = f"""
            SELECT sensor_name, sensor_type, industry,
                   DATE_TRUNC('HOUR', timestamp) AS hour,
                   AVG(value) AS avg_value,
                   MIN(value) AS min_value,
                   MAX(value) AS max_value,
                   COUNT(*) AS cnt
            FROM {table}
            WHERE timestamp >= DATE_SUB(CURRENT_TIMESTAMP(), {days})
            GROUP BY 1, 2, 3, 4
            ORDER BY hour DESC
            LIMIT 5000
        """

        warehouse_id = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "")
        if state.db_available and state.db:
            try:
                settings_row = await state.db.fetchrow(
                    "SELECT settings FROM user_settings WHERE user_email = $1", user_email
                )
                if settings_row:
                    user_settings = json.loads(settings_row["settings"]) if isinstance(settings_row["settings"], str) else settings_row["settings"]
                    saved_wh = user_settings.get("warehouse", {}).get("warehouse_id", "")
                    if saved_wh:
                        warehouse_id = saved_wh
            except Exception:
                pass
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                f"{host}/api/2.0/sql/statements",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "statement": sql,
                    "warehouse_id": warehouse_id,
                    "wait_timeout": "50s",
                    "disposition": "INLINE",
                },
            )
            resp.raise_for_status()
            data = resp.json()

        result = data.get("result", {})
        columns = [c["name"] for c in result.get("manifest", {}).get("schema", {}).get("columns", [])]
        rows_data = []
        for chunk in result.get("data_array", []):
            rows_data.append(dict(zip(columns, chunk)))

        return {"columns": columns, "data": rows_data, "row_count": len(rows_data)}

    except Exception as exc:
        logger.error("Pipeline history query failed: %s", exc)
        return JSONResponse(status_code=500, content={"error": "Internal server error"})
