"""Health, metrics, and app-defaults endpoints."""

from __future__ import annotations

import os

from fastapi import APIRouter, Depends

from core.state import AppState, get_app_state

router = APIRouter()


@router.get("/api/health")
async def health(state: AppState = Depends(get_app_state)):
    return {
        "status": "ok",
        "db_available": state.db_available,
        "db_error": state.db_error,
        "industries": state.industries,
        "databricks_client_id": os.getenv("DATABRICKS_CLIENT_ID", "(not set)"),
        "databricks_host": os.getenv("DATABRICKS_HOST", "(not set)"),
        "zerobus_client_id": os.getenv("ZEROBUS_CLIENT_ID", "(not set)")[:8] + "...",
        "zerobus_endpoint": os.getenv("ZEROBUS_SERVER_ENDPOINT", "(not set)"),
    }


@router.get("/api/app-defaults")
async def app_defaults():
    """Return workspace-level defaults the frontend can auto-populate.

    NOTE: Only non-secret values are returned. The client_secret is indicated
    as present/absent but never exposed.
    """
    return {
        "zerobus_endpoint": os.getenv("ZEROBUS_SERVER_ENDPOINT", ""),
        "zerobus_client_id": os.getenv("ZEROBUS_CLIENT_ID", ""),
        "zerobus_client_secret_set": bool(
            os.getenv("ZEROBUS_CLIENT_SECRET")
            or (os.getenv("ZEROBUS_SECRET_SCOPE") and os.getenv("ZEROBUS_SECRET_KEY"))
        ),
        "workspace_url": os.getenv("DATABRICKS_HOST", ""),
    }


@router.get("/metrics")
async def metrics():
    return {"status": "healthy"}
