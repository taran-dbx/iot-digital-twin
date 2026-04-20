"""Shared fixtures for the IoT Digital Twin test suite."""

from __future__ import annotations

import asyncio
import sys
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
