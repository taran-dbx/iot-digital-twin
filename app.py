"""IoT Digital Twin — FastAPI application.

Slim factory: lifespan, routers, and static file mounts.
All route handlers and business logic live in routes/ and services/.
"""

from __future__ import annotations

import asyncio
import logging
import os
import traceback
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from core.config import INDUSTRIES, UPDATE_RATE_HZ
from core.state import AppState
from core.shutdown import install_sigterm_handler, shutdown
from routes import create_api_router
from routes.setup import router as setup_router
from routes.industries_admin import router as industries_admin_router
from routes.validation_admin import router as validation_admin_router
from routes.static_files import router as static_files_router
from services.simulator_service import run_simulator
from services.retention_service import run_retention_loop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("iot_digital_twin")


# ---------------------------------------------------------------------------
# Lifespan — startup and shutdown
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(application: FastAPI):
    state = AppState(industries=INDUSTRIES, update_rate_hz=UPDATE_RATE_HZ)
    application.state.app_state = state

    install_sigterm_handler(state)

    logger.info("=== IoT Digital Twin starting ===")
    logger.info("PGHOST=%s", os.getenv("PGHOST", "(not set)"))
    logger.info("LAKEBASE_ENDPOINT=%s", os.getenv("LAKEBASE_ENDPOINT", "(not set)"))
    logger.info("ZEROBUS_SERVER_ENDPOINT=%s", os.getenv("ZEROBUS_SERVER_ENDPOINT", "(not set)"))
    logger.info("DATABRICKS_CLIENT_ID=%s", os.getenv("DATABRICKS_CLIENT_ID", "(not set)"))
    logger.info("INDUSTRIES=%s", INDUSTRIES)

    # --- Database ---
    if os.getenv("PGHOST"):
        try:
            from backend import LakebaseConnectionManager

            db = LakebaseConnectionManager()
            await db.initialize()
            state.db = db
            state.db_available = True
            state.db_error = None
            logger.info("Lakebase connection pool ready")
            await db.ensure_pipelines_table()
            await db.ensure_user_settings_table()
            await db.ensure_source_bindings_table()
        except Exception as exc:
            err_msg = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "Lakebase connection failed (app will run without DB): %s\n%s",
                exc,
                traceback.format_exc(),
            )
            state.db_available = False
            state.db_error = err_msg
            state.db = None
    else:
        logger.info("PGHOST not set — Lakebase features disabled, WebSocket-only mode")
        state.db_available = False
        state.db_error = "PGHOST not set"

    # --- App-backend metadata DB (additive; separate iot-app-backend project) ---
    # Guarded like the DB block above: any failure is swallowed/logged so it can
    # never break boot. Does NOT touch PGHOST / LAKEBASE_ENDPOINT / the existing
    # iot-lakets-demo connection above.
    try:
        from services.app_backend_db import build_app_backend_manager
        from models import app_schema

        app_db = build_app_backend_manager()
        await app_db.initialize()
        state.app_backend_db = app_db
        state.app_backend_available = True
        state.app_backend_error = None
        await app_schema.ensure_app_schema(app_db)
        logger.info("app-backend metadata DB ready — schema ensured")
    except Exception as exc:
        err_msg = f"{type(exc).__name__}: {exc}"
        logger.warning(
            "app-backend DB init failed (additive; app continues): %s\n%s",
            exc,
            traceback.format_exc(),
        )
        state.app_backend_db = None
        state.app_backend_available = False
        state.app_backend_error = err_msg

    # --- Simulator background task ---
    state.simulator_task = asyncio.create_task(run_simulator(state))

    # --- Materialized view refresh task ---
    if state.db_available and state.db:
        db_ref = state.db

        async def refresh_materialized_views():
            while True:
                await asyncio.sleep(30)
                try:
                    exists = await db_ref.fetch(
                        "SELECT matviewname FROM pg_matviews "
                        "WHERE matviewname IN ('sensor_1m', 'sensor_5m')"
                    )
                    view_names = {r["matviewname"] for r in exists}
                    if "sensor_1m" in view_names:
                        await db_ref.execute(
                            "REFRESH MATERIALIZED VIEW CONCURRENTLY sensor_1m"
                        )
                    if "sensor_5m" in view_names:
                        await db_ref.execute(
                            "REFRESH MATERIALIZED VIEW CONCURRENTLY sensor_5m"
                        )
                    if view_names:
                        logger.debug("Materialized views refreshed: %s", view_names)
                except Exception as exc:
                    logger.warning("Materialized view refresh failed: %s", exc)

        state.refresh_task = asyncio.create_task(refresh_materialized_views())

    # --- Retention auto-purge task ---
    if state.db_available and state.db:
        state.retention_task = asyncio.create_task(run_retention_loop(state.db))

    logger.info("=== IoT Digital Twin started (db=%s) ===", state.db_available)
    yield
    logger.info("=== IoT Digital Twin shutting down ===")
    await shutdown(state)
    logger.info("=== IoT Digital Twin stopped ===")


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------
app = FastAPI(title="IoT Digital Twin", lifespan=lifespan)

# API routes (health, websocket, sources, sensor_data, simulator, producers,
# provisioning, cluster, deployment, jobs, industries, user, settings,
# warehouses, pipelines)
app.include_router(create_api_router())

# One-click Lakebase + LakeTS pipeline-setup router (POST /api/setup/...)
app.include_router(setup_router)

# Admin pipeline management router (POST /api/admin/industries/...,
# GET/DELETE /api/admin/pipelines/...) — app-backend data model + provisioning.
app.include_router(industries_admin_router)
app.include_router(validation_admin_router)

# Static file routes (/, /legacy, /legacy-settings)
app.include_router(static_files_router)

# Static file mounts — must come after route registration
app.mount("/assets", StaticFiles(directory="static/dist/assets"), name="spa-assets")
app.mount("/static", StaticFiles(directory="static"), name="static")
# Vite copies client/public/* to the dist root; serve brand SVGs from there so
# the SPA catch-all below doesn't swallow /brand/*.svg and return index.html.
if os.path.isdir("static/dist/brand"):
    app.mount("/brand", StaticFiles(directory="static/dist/brand"), name="brand")

# SPA catch-all — must be the very last route
SPA_INDEX = "static/dist/index.html"


@app.get("/{path:path}")
async def spa_fallback(path: str):
    """Catch-all for client-side routes — serve the React SPA entry point."""
    return FileResponse(SPA_INDEX)
