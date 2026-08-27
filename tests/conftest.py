"""Shared fixtures for the IoT Digital Twin test suite."""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from pathlib import Path
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

# Ensure the project root is on sys.path so imports resolve.
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ---------------------------------------------------------------------------
# Pre-mock databricks.sdk to prevent architecture-mismatch ImportError.
#
# On this machine the cryptography/cffi packages in the SYSTEM site-packages
# are arm64 binaries, but the Python interpreter runs as x86_64 (Rosetta).
# Any import that transitively reaches `databricks.sdk` (e.g. `routes.pipelines`
# via `routes/__init__.py`) triggers the broken chain:
#   databricks.sdk → google.auth.impersonated_credentials → cryptography → ❌
#
# Mocking the SDK at sys.modules level before any route imports happen lets
# the unit tests (which mock every SDK call anyway) run cleanly. The
# integration fixture `app_backend_conn` wraps its SDK import in try/except
# and calls `pytest.skip(...)` on failure, so the mock is transparent there.
# ---------------------------------------------------------------------------
def _pre_mock_sdk() -> None:
    """Inject lightweight mocks for modules that fail to import on this host."""
    _stub = MagicMock()
    for _name in (
        "_cffi_backend",
        "cffi",
        "cryptography",
        "cryptography.exceptions",
        "cryptography.hazmat",
        "cryptography.hazmat.bindings",
        "cryptography.hazmat.bindings._rust",
        "google.auth.crypt.es",
        "google.auth.crypt",
        "google.auth.iam",
        "google.auth.impersonated_credentials",
        "google.auth",
        "google",
        "databricks.sdk.credentials_provider",
        "databricks.sdk.config",
        "databricks.sdk.core",
        "databricks.sdk",
        "databricks",
    ):
        sys.modules.setdefault(_name, _stub)


_pre_mock_sdk()

from core.state import AppState


# ---------------------------------------------------------------------------
# AppState with a mocked database
# ---------------------------------------------------------------------------
@pytest.fixture()
def mock_db() -> MagicMock:
    """Return a mock LakebaseConnectionManager with async helpers."""
    db = MagicMock()
    db.fetch = AsyncMock(return_value=[])
    db.execute = AsyncMock(return_value=None)
    db.fetchrow = AsyncMock(return_value=None)
    db.fetchval = AsyncMock(return_value=None)
    return db


@pytest.fixture()
def mock_app_state(mock_db: MagicMock) -> AppState:
    """Return an AppState with a mocked database layer."""
    state = AppState()
    state.db = mock_db
    state.db_available = True
    state.db_error = None
    state.industries = "pharmaceutical"
    return state


# ---------------------------------------------------------------------------
# FastAPI test client (httpx AsyncClient)
# ---------------------------------------------------------------------------
def _build_test_app(state: AppState):
    """Create a minimal FastAPI app wired to the given AppState."""
    from fastapi import FastAPI
    from routes.health import router as health_router

    test_app = FastAPI()
    test_app.state.app_state = state
    test_app.include_router(health_router)
    return test_app


@pytest_asyncio.fixture()
async def test_client(mock_app_state: AppState) -> AsyncGenerator[AsyncClient, None]:
    """Yield an httpx AsyncClient bound to the test app."""
    app = _build_test_app(mock_app_state)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# Databricks WorkspaceClient mock
# ---------------------------------------------------------------------------
@pytest.fixture()
def mock_ws_client() -> MagicMock:
    """Return a mocked Databricks WorkspaceClient."""
    ws = MagicMock()
    ws.clusters = MagicMock()
    ws.clusters.list = MagicMock(return_value=[])
    ws.jobs = MagicMock()
    ws.jobs.list = MagicMock(return_value=[])
    ws.warehouses = MagicMock()
    ws.warehouses.list = MagicMock(return_value=[])
    ws.current_user = MagicMock()
    ws.current_user.me = MagicMock(return_value=MagicMock(user_name="test@example.com"))
    ws.postgres = MagicMock()
    ws.postgres.generate_database_credential = MagicMock(
        return_value=MagicMock(token="mock-token")
    )
    return ws


# ---------------------------------------------------------------------------
# Integration fixture — live iot-app-backend Lakebase project
# ---------------------------------------------------------------------------
# The app-backend data layer (models/app_schema.py + services repos) is
# validated against the real ``iot-app-backend`` Lakebase project.  Each test
# runs inside a throwaway schema (``test_phase1_<hex>``) that is dropped on
# teardown, so nothing pollutes ``production``.  The fixture ``pytest.skip``s
# when the project is unreachable (no creds / offline CI) so the mocked unit
# tests still run everywhere.
APP_BACKEND_ENDPOINT = (
    "projects/iot-app-backend/branches/production/endpoints/primary"
)


@pytest_asyncio.fixture()
async def app_backend_conn():
    """Yield an asyncpg connection to iot-app-backend in a throwaway schema."""
    try:
        import asyncpg
        from databricks.sdk import WorkspaceClient
    except Exception as exc:  # pragma: no cover - deps missing
        pytest.skip(f"integration deps unavailable: {exc}")

    profile = os.getenv("DATABRICKS_CONFIG_PROFILE", "fe-vm-shared-interop")
    try:
        w = WorkspaceClient(profile=profile)
        endpoint = w.postgres.get_endpoint(name=APP_BACKEND_ENDPOINT)
        host = endpoint.status.hosts.host
        user = w.current_user.me().user_name
        cred = w.postgres.generate_database_credential(endpoint=APP_BACKEND_ENDPOINT)
        conn = await asyncpg.connect(
            host=host,
            database="databricks_postgres",
            user=user,
            password=cred.token,
            port=5432,
            ssl="require",
        )
    except Exception as exc:  # pragma: no cover - offline / no creds
        pytest.skip(f"iot-app-backend not reachable: {exc}")

    schema = f"test_phase1_{uuid.uuid4().hex[:8]}"
    await conn.execute(f'CREATE SCHEMA "{schema}"')
    await conn.execute(f'SET search_path TO "{schema}"')
    try:
        yield conn
    finally:
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await conn.close()
