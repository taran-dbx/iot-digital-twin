"""Deployment endpoints — connectivity test, cluster validate, deploy-job."""

from __future__ import annotations

import json
import logging
import os
import time
import traceback
from datetime import timedelta
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from core.auth import get_current_user
from core.state import AppState, get_app_state
from services.databricks_sdk import detect_cloud
from services.pipeline_service import track_resource

logger = logging.getLogger("iot_digital_twin.routes.deployment")

router = APIRouter(tags=["deployment"])

WHEEL_FILENAME = "iot_data_simulator-0.1.0-py3-none-any.whl"


@router.post("/api/test-connectivity/upload-wheel")
async def upload_wheel(user_email: str = Depends(get_current_user)):
    """Upload the bundled iot-data-simulator wheel to the Databricks workspace."""
    try:
        import base64
        from pathlib import Path
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.workspace import ImportFormat

        local_wheel = Path(__file__).parent.parent / "lib" / WHEEL_FILENAME
        if not local_wheel.exists():
            return JSONResponse(status_code=404, content={
                "error": f"Wheel not found at {local_wheel}. Ensure lib/{WHEEL_FILENAME} is packaged with the app.",
            })

        w = WorkspaceClient()
        me = w.current_user.me()
        username = me.user_name or "unknown"
        ws_dir = f"/Workspace/Users/{username}/.iot-digital-twin/lib"
        ws_path = f"{ws_dir}/{WHEEL_FILENAME}"

        try:
            w.workspace.mkdirs(ws_dir)
        except Exception:
            pass

        raw = local_wheel.read_bytes()
        w.workspace.import_(
            path=ws_path,
            content=base64.b64encode(raw).decode(),
            format=ImportFormat.AUTO,
            overwrite=True,
        )
        logger.info("Uploaded wheel to %s (%d bytes)", ws_path, len(raw))
        return {"wheel_path": ws_path}

    except Exception as exc:
        logger.error("Wheel upload failed: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/test-connectivity/submit")
async def submit_connectivity_test(request: Request, user_email: str = Depends(get_current_user)):
    """Upload a test notebook and submit a one-time serverless run."""
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    notebook_content = body.get("notebook", "")
    if not notebook_content:
        return JSONResponse(status_code=422, content={"error": "'notebook' content is required"})

    try:
        import base64
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.workspace import ImportFormat, Language
        from databricks.sdk.service.jobs import SubmitTask, NotebookTask, Source, JobEnvironment
        from databricks.sdk.service.compute import Environment

        w = WorkspaceClient()
        me = w.current_user.me()
        username = me.user_name or "unknown"
        ts = int(time.time())
        notebook_path = f"/Workspace/Users/{username}/.iot-digital-twin/connectivity_test_{ts}"
        parent_folder = notebook_path.rsplit("/", 1)[0]

        try:
            w.workspace.mkdirs(parent_folder)
        except Exception:
            pass

        w.workspace.import_(
            path=notebook_path,
            content=base64.b64encode(notebook_content.encode()).decode(),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        logger.info("Uploaded connectivity test notebook to %s", notebook_path)

        waiter = w.jobs.submit(
            run_name=f"iot-connectivity-test-{ts}",
            tasks=[
                SubmitTask(
                    task_key="connectivity_test",
                    notebook_task=NotebookTask(
                        notebook_path=notebook_path,
                        source=Source.WORKSPACE,
                    ),
                    environment_key="default",
                )
            ],
            environments=[
                JobEnvironment(
                    environment_key="default",
                    spec=Environment(client="1"),
                )
            ],
        )
        run_id = waiter.response.run_id
        logger.info("Submitted serverless connectivity test run_id=%s", run_id)

        return {"run_id": run_id, "notebook_path": notebook_path}

    except Exception as exc:
        logger.error("Connectivity test submit failed: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/test-connectivity/{run_id}/status")
async def get_connectivity_test_status(run_id: int, user_email: str = Depends(get_current_user)):
    """Poll a serverless connectivity test run for its current state and result."""
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        run = w.jobs.get_run(run_id=run_id)

        life_cycle = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else "UNKNOWN"
        result_state = run.state.result_state.value if run.state and run.state.result_state else None
        state_message = run.state.state_message if run.state else ""

        if life_cycle in ("PENDING", "QUEUED", "RUNNING", "BLOCKED"):
            return {"state": life_cycle, "state_message": state_message}

        if life_cycle in ("TERMINATING",):
            return {"state": "RUNNING", "state_message": "Finishing up..."}

        if life_cycle == "TERMINATED" and result_state == "SUCCESS":
            nb_result = None

            # For multi-task runs, get output from the individual task run
            task_run_id = None
            if run.tasks:
                for task in run.tasks:
                    if task.run_id:
                        task_run_id = task.run_id
                        break

            try:
                output = w.jobs.get_run_output(run_id=task_run_id or run_id)
                if output.notebook_output and output.notebook_output.result:
                    try:
                        nb_result = json.loads(output.notebook_output.result)
                    except (json.JSONDecodeError, TypeError):
                        nb_result = {"raw": output.notebook_output.result}
            except Exception as out_err:
                logger.warning("Could not get run output: %s", out_err)
                nb_result = {"success": True, "message": "Run completed but output could not be retrieved."}

            # Clean up the temp notebook
            if run.tasks and run.tasks[0].notebook_task:
                try:
                    w.workspace.delete(run.tasks[0].notebook_task.notebook_path)
                except Exception:
                    pass

            return {"state": "COMPLETED", "result": nb_result}

        error_msg = state_message
        if run.state and run.state.state_message:
            error_msg = run.state.state_message

        return {"state": "FAILED", "error": error_msg, "result_state": result_state}

    except Exception as exc:
        logger.error("Connectivity test status check failed: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/cluster/{cluster_id}/validate")
async def validate_on_cluster(cluster_id: str, request: Request, user_email: str = Depends(get_current_user)):
    """Upload a notebook and execute it on the given cluster for validation.

    The notebook is written to a temporary workspace path, then executed
    via the command execution API.  The response includes stdout/stderr
    so the caller can inspect results.
    """
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    notebook_content = body.get("notebook", "")
    if not notebook_content:
        return JSONResponse(status_code=422, content={"error": "'notebook' content is required"})



    try:
        import base64
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.workspace import ImportFormat, Language

        w = WorkspaceClient()

        me = w.current_user.me()
        username = me.user_name or "unknown"
        notebook_path = f"/Workspace/Users/{username}/.iot-digital-twin/validate_{int(time.time())}"

        parent_folder = notebook_path.rsplit("/", 1)[0]
        try:
            w.workspace.mkdirs(parent_folder)
        except Exception:
            pass

        # For validation, replace the entire try/awaitTermination/except block
        # with a time-bounded check so the command execution API returns
        # instead of blocking forever.
        _await_block = (
            'try:\n'
            '    print("[SSS] Streaming query started \u2014 awaiting termination \u2026")\n'
            '    query.awaitTermination()\n'
            'except Exception as e:\n'
            '    print(f"[SSS] Streaming query failed: {e}")\n'
            '    import traceback\n'
            '    traceback.print_exc()\n'
            '    raise'
        )
        _validation_block = (
            'import time as _t\n'
            '_deadline = _t.time() + 20\n'
            '_batch_seen = False\n'
            'while _t.time() < _deadline:\n'
            '    for _q in spark.streams.active:\n'
            '        if _q.lastProgress and _q.lastProgress.get("numInputRows", 0) > 0:\n'
            '            _batch_seen = True\n'
            '    if _batch_seen:\n'
            '        break\n'
            '    _t.sleep(2)\n'
            'for _q in spark.streams.active:\n'
            '    _status = "active" if _q.isActive else "DEAD"\n'
            '    _rows = _q.lastProgress.get("numInputRows", 0) if _q.lastProgress else 0\n'
            '    print(f"Stream {_q.name}: {_status}, rows_processed={_rows}")\n'
            '    if not _q.isActive:\n'
            '        raise RuntimeError(f"Stream {_q.name} died: {_q.exception()}")\n'
            '    _q.stop()\n'
            'if not spark.streams.active and not _batch_seen:\n'
            '    print("Warning: No streaming queries found after timeout")\n'
            'print("Validation OK \\u2014 streaming queries started and processed data successfully")'
        )
        validation_content = notebook_content.replace(_await_block, _validation_block)

        w.workspace.import_(
            path=notebook_path,
            content=base64.b64encode(validation_content.encode()).decode(),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        logger.info("Uploaded validation notebook to %s", notebook_path)

        from databricks.sdk.service.compute import Language as CmdLanguage

        ctx = w.command_execution.create(
            cluster_id=cluster_id,
            language=CmdLanguage.PYTHON,
        ).result()
        context_id = ctx.id

        # Kafka/Kinesis setup + 20 s batch wait inside the validation block
        # regularly runs past 60 s.  Allow a generous 5 min default and
        # accept a caller-provided ``validation_timeout_sec`` for tuning.
        validation_timeout_sec = int(body.get("validation_timeout_sec", 300))
        result = w.command_execution.execute(
            cluster_id=cluster_id,
            context_id=context_id,
            language=CmdLanguage.PYTHON,
            command=validation_content,
        ).result(timeout=timedelta(seconds=validation_timeout_sec))

        status = result.status.value if result.status else "UNKNOWN"
        output = ""
        error = None

        if result.results:
            output = getattr(result.results, "data", "") or ""
            if getattr(result.results, "cause", None):
                error = result.results.cause

        if error:
            logger.error("validate_on_cluster error:\n%s", error)
        logger.info(
            "validate_on_cluster status=%s has_error=%s output_len=%s notebook=%s",
            status, error is not None, len(str(output)), notebook_path,
        )

        try:
            w.command_execution.destroy(cluster_id=cluster_id, context_id=context_id)
        except Exception:
            pass

        success = status == "Finished" and error is None
        return {
            "success": success,
            "status": status,
            "output": str(output),
            "error": error,
            "notebook_path": notebook_path,
        }

    except Exception as exc:
        logger.error("Validation failed: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/cluster/deploy-job")
async def deploy_job(request: Request, user_email: str = Depends(get_current_user), state: AppState = Depends(get_app_state)):
    """Create a Databricks job with a job cluster and optionally start it.

    The notebook is uploaded to the workspace, then a job is created with
    a ``new_cluster`` spec so it runs on its own ephemeral job cluster
    (not the all-purpose cluster used for validation).
    """
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    notebook_content = body.get("notebook", "")
    producer_notebook_content = body.get("producer_notebook")
    cluster_config = body.get("cluster_config", {})

    if not notebook_content:
        return JSONResponse(status_code=422, content={"error": "'notebook' content is required"})
    if not cluster_config.get("spark_version") or not cluster_config.get("worker_node_type"):
        return JSONResponse(
            status_code=422,
            content={"error": "'cluster_config' must include 'spark_version' and 'worker_node_type'"},
        )

    try:
        import base64
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.jobs import (
            Task as JobTaskSettings,
            NotebookTask,
            Continuous,
            PauseStatus,
            JobSettings,
        )
        from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode

        w = WorkspaceClient()

        me = w.current_user.me()
        username = me.user_name or "unknown"
        notebook_path = f"/Workspace/Users/{username}/.iot-digital-twin/sss_pipeline_{int(time.time())}"

        from databricks.sdk.service.workspace import ImportFormat, Language

        parent_folder = notebook_path.rsplit("/", 1)[0]
        try:
            w.workspace.mkdirs(parent_folder)
        except Exception:
            pass

        w.workspace.import_(
            path=notebook_path,
            content=base64.b64encode(notebook_content.encode()).decode(),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        logger.info("Uploaded job notebook to %s", notebook_path)

        # Upload producer notebook if provided
        producer_notebook_path = None
        if producer_notebook_content:
            producer_notebook_path = f"/Workspace/Users/{username}/.iot-digital-twin/data_producer_{int(time.time())}"
            w.workspace.import_(
                path=producer_notebook_path,
                content=base64.b64encode(producer_notebook_content.encode()).decode(),
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                overwrite=True,
            )
            logger.info("Uploaded producer notebook to %s", producer_notebook_path)

        spark_version = cluster_config["spark_version"]
        worker_node_type = cluster_config["worker_node_type"]
        num_workers = int(cluster_config.get("num_workers", 2))
        spark_conf = cluster_config.get("spark_conf", {})
        spark_conf["spark.databricks.streaming.realTimeMode.enabled"] = "true"

        from databricks.sdk.service.compute import AwsAttributes, AzureAttributes, EbsVolumeType
        cloud = detect_cloud()
        no_spot_kwargs: dict[str, Any] = {}
        if cloud == "aws":
            no_spot_kwargs["aws_attributes"] = AwsAttributes(
                first_on_demand=num_workers + 1,
                ebs_volume_count=1,
                ebs_volume_size=100,
                ebs_volume_type=EbsVolumeType.GENERAL_PURPOSE_SSD,
            )
        elif cloud == "azure":
            no_spot_kwargs["azure_attributes"] = AzureAttributes(first_on_demand=num_workers + 1)

        from databricks.sdk.service.jobs import Source, JobEnvironment as JE
        from databricks.sdk.service.compute import Environment

        tasks = [
            JobTaskSettings(
                task_key="sss_consumer",
                new_cluster=ClusterSpec(
                    spark_version=spark_version,
                    node_type_id=worker_node_type,
                    driver_node_type_id=worker_node_type,
                    num_workers=num_workers,
                    spark_conf=spark_conf,
                    data_security_mode=DataSecurityMode.SINGLE_USER,
                    single_user_name=username,
                    **no_spot_kwargs,
                ),
                notebook_task=NotebookTask(
                    notebook_path=notebook_path,
                ),
            ),
        ]

        environments = None
        if producer_notebook_path:
            tasks.append(
                JobTaskSettings(
                    task_key="data_producer",
                    new_cluster=ClusterSpec(
                        spark_version=spark_version,
                        node_type_id=worker_node_type,
                        driver_node_type_id=worker_node_type,
                        num_workers=0,
                        spark_conf={
                            "spark.databricks.cluster.profile": "singleNode",
                            "spark.master": "local[*]",
                        },
                        data_security_mode=DataSecurityMode.SINGLE_USER,
                        single_user_name=username,
                        custom_tags={"ResourceClass": "SingleNode"},
                        **no_spot_kwargs,
                    ),
                    notebook_task=NotebookTask(
                        notebook_path=producer_notebook_path,
                    ),
                ),
            )

        # Check for existing job_id — reuse instead of creating a new one
        existing_job_id = body.get("job_id")
        pipeline_id = body.get("pipeline_id")

        # Also check the pipeline's stored resources if no job_id in request
        if not existing_job_id and pipeline_id and state.db_available and state.db:
            try:
                import json as _json
                row = await state.db.fetchrow(
                    "SELECT resources FROM pipelines WHERE id = $1::uuid", pipeline_id
                )
                if row:
                    res = _json.loads(row["resources"]) if isinstance(row["resources"], str) else (row["resources"] or {})
                    existing_job_id = res.get("job_id")
            except Exception:
                pass

        host = os.getenv("DATABRICKS_HOST", "").rstrip("/")

        if existing_job_id:
            # Update the existing job with new notebook and config
            jid = int(existing_job_id)
            try:
                w.jobs.cancel_all_runs(job_id=jid)
            except Exception as cancel_err:
                logger.warning("Could not cancel runs for job %s: %s", jid, cancel_err)

            w.jobs.reset(
                job_id=jid,
                new_settings=JobSettings(
                    name=f"IoT-SSS-Pipeline-{jid}",
                    continuous=Continuous(pause_status=PauseStatus.UNPAUSED),
                    tasks=tasks,
                    environments=environments,
                ),
            )
            job_id = jid
            job_url = f"{host}/#job/{job_id}" if host else ""
            logger.info("Job updated (reused): id=%s, notebook=%s", job_id, notebook_path)
        else:
            # Create a brand new job
            job = w.jobs.create(
                name=f"IoT-SSS-Pipeline-{int(time.time())}",
                continuous=Continuous(pause_status=PauseStatus.UNPAUSED),
                tasks=tasks,
                environments=environments,
            )
            job_id = job.job_id
            job_url = f"{host}/#job/{job_id}" if host else ""
            logger.info("Job created (new): id=%s, notebook=%s", job_id, notebook_path)

        if pipeline_id:
            await track_resource(state.db, state.db_available, pipeline_id, "job_id", str(job_id))
            await track_resource(state.db, state.db_available, pipeline_id, "notebook_path", notebook_path)

        return {
            "job_id": job_id,
            "job_url": job_url,
            "notebook_path": notebook_path,
        }

    except Exception as exc:
        logger.error("Job deployment failed: %s\n%s", exc, traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": str(exc)})
