"""Centralised mutable application state.

All former module-level globals live here.  ``get_app_state()`` is a FastAPI
dependency that retrieves the singleton from ``request.app.state.app_state``.
"""

from __future__ import annotations

import asyncio
import dataclasses
from dataclasses import field
from typing import Any, TYPE_CHECKING

from fastapi import Request, WebSocket

if TYPE_CHECKING:
    from backend import LakebaseConnectionManager
    from models.source_metrics import SourceMetrics
    from services.pipeline_db import PipelineLakebaseHandle


def _default_sources() -> dict[str, SourceMetrics]:
    from models.source_metrics import create_default_sources
    return create_default_sources()


@dataclasses.dataclass
class AppState:
    """Holds every piece of mutable state the application needs."""

    # Database
    db: LakebaseConnectionManager | None = None
    db_available: bool = False
    db_error: str | None = None

    # App-backend metadata DB (additive; separate iot-app-backend project).
    # Backs pipeline_setup / pipeline_deployment / provisioned_resource.
    app_backend_db: LakebaseConnectionManager | None = None
    app_backend_available: bool = False
    app_backend_error: str | None = None

    # WebSocket clients
    connected_clients: list[WebSocket] = field(default_factory=list)

    # SSE clients (each is an asyncio.Queue that the SSE endpoint drains)
    sse_clients: list[asyncio.Queue] = field(default_factory=list)

    # Background tasks
    simulator_task: asyncio.Task | None = None
    refresh_task: asyncio.Task | None = None
    retention_task: asyncio.Task | None = None

    # Shutdown coordination
    shutdown_event: asyncio.Event = field(default_factory=asyncio.Event)

    # Source metrics registry
    sources: dict[str, SourceMetrics] = field(default_factory=_default_sources)

    # Per-pipeline producer state
    producer_states: dict[str, dict[str, Any]] = field(default_factory=dict)

    # Per-pipeline Lakebase connection pools (resolved from source_config)
    pipeline_dbs: dict[str, PipelineLakebaseHandle] = field(default_factory=dict)

    # Runtime-mutable config (set via /api/simulator/restart)
    industries: str = ""
    update_rate_hz: float = 2.0


def get_app_state(request: Request) -> AppState:
    """FastAPI dependency — retrieve the shared ``AppState``."""
    return request.app.state.app_state
