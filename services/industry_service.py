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

import logging
from typing import Any

from core.state import AppState
from services.industry_templates import (
    CURATED_INDUSTRIES,
    MODES,
    get_template,
    mode_config,
)

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
