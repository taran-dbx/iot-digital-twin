"""Per-pipeline Genie space lifecycle for the manufacturing dashboard.

One Genie space per ``(setup_id, mode)`` for the **manufacturing** industry,
auto-created (or refreshed) after a successful deploy.  The space contains:

- the mode's telemetry table ``{catalog}.{schema}.manufacturing_{mode}``,
- the shared streaming metrics table ``manufacturing_metrics``,
- the dedicated ``intelligence_*`` reference tables.

Manufacturing KPIs (downtime / availability / OEE / anomalies) are answered
through Genie text instructions, example SQL and SQL snippets — no new tables.

**Persistence**: the space id lives in the ``provisioned_resource`` ledger
(``resource_type='genie_space'``, ``metadata.mode``).  The ledger is already
the per-setup source of truth for teardown, so no schema migration is needed;
:func:`find_space_id` is the ``(setup_id, mode)`` lookup.

``serialized_space`` follows the Genie v2 format: every id is a 32-char
lowercase hex string, text fields are string arrays, id-keyed arrays are sorted
by ``id`` and tables by ``identifier``.  Ids are derived deterministically so a
redeploy produces an identical payload.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from typing import Any

from databricks.sdk.errors import NotFound

from iot_twin.services import resource_ledger
from iot_twin.services.db.app_repo_common import DBExecutor
from iot_twin.services.genie.genie_space_content import (
    column_configs,
    example_sqls,
    sql_snippets,
    starter_questions,
    text_instructions,
)
from iot_twin.services.industry.industry_catalog_service import industry_table
from iot_twin.services.intelligence_catalog_service import reference_tables

logger = logging.getLogger("iot_digital_twin.services.genie_space")

RESOURCE_TYPE = "genie_space"
# Higher teardown_order is deleted first: the space references the tables
# (20-35) and is independent of the jobs (40), so trash it before both.
TEARDOWN_ORDER_GENIE_SPACE = 45
PARENT_PATH_ENV = "GENIE_SPACE_PARENT_PATH"
GENIE_INDUSTRY = "manufacturing"
PIPELINE_MODES: tuple[str, ...] = ("realtime", "microbatch", "zerobus")
MAX_RESULT_ROWS = 500
SERIALIZED_VERSION = 2

__all__ = [
    "GenieSpaceError",
    "MAX_RESULT_ROWS",
    "PARENT_PATH_ENV",
    "RESOURCE_TYPE",
    "build_serialized_space",
    "ensure_pipeline_space",
    "expected_tables",
    "find_pipeline_space",
    "find_space_id",
    "make_genie_space_deleter",
    "query_result_payload",
    "starter_questions",
    "supports_industry",
]


class GenieSpaceError(RuntimeError):
    """Raised when a per-pipeline Genie space cannot be provisioned."""


# ---------------------------------------------------------------------------
# Scope helpers
# ---------------------------------------------------------------------------
def supports_industry(industry: str | None) -> bool:
    """Return True when per-pipeline Genie spaces exist for ``industry``."""
    return (industry or "").strip().lower() == GENIE_INDUSTRY


def _require_mode(mode: str) -> str:
    if mode not in PIPELINE_MODES:
        raise ValueError(f"unknown mode {mode!r}; expected one of {PIPELINE_MODES}")
    return mode


def expected_tables(industry: str, mode: str) -> tuple[str, ...]:
    """Return the sorted FQN set a pipeline space must contain — no more, no less."""
    _require_mode(mode)
    tables = {
        industry_table(industry, mode),
        industry_table(industry, "metrics"),
        *reference_tables().values(),
    }
    return tuple(sorted(tables))


def _stable_id(*parts: str) -> str:
    """Deterministic 32-hex id (Genie requires lowercase hex without hyphens)."""
    return hashlib.sha256(":".join(parts).encode("utf-8")).hexdigest()[:32]


def _parent_path() -> str | None:
    value = (os.getenv(PARENT_PATH_ENV) or "").strip()
    return value or None


def space_title(mode: str, setup_id: Any) -> str:
    return f"Manufacturing {mode} pipeline ({setup_id})"


def space_description(mode: str) -> str:
    return (
        f"Manufacturing {mode} telemetry, streaming metrics and intelligence "
        "reference tables. Answers OEE, downtime, anomaly, fault and maintenance "
        "questions for this pipeline."
    )


# ---------------------------------------------------------------------------
# serialized_space builder
# ---------------------------------------------------------------------------
def _table_entries(industry: str, mode: str) -> list[dict[str, Any]]:
    telemetry = industry_table(industry, mode)
    entries = []
    for fqn in expected_tables(industry, mode):
        entry: dict[str, Any] = {"identifier": fqn}
        if fqn == telemetry:
            entry["column_configs"] = sorted(column_configs(), key=lambda c: c["column_name"])
        entries.append(entry)
    return sorted(entries, key=lambda e: e["identifier"])


def _with_ids(kind: str, mode: str, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keyed = [{"id": _stable_id(kind, mode, str(i)), **item} for i, item in enumerate(items)]
    return sorted(keyed, key=lambda item: item["id"])


def _sample_questions(mode: str) -> list[dict[str, Any]]:
    return _with_ids("sample", mode, [{"question": [q]} for q in starter_questions(mode)])


def _instructions(industry: str, mode: str) -> dict[str, Any]:
    telemetry = industry_table(industry, mode)
    metrics = industry_table(industry, "metrics")
    refs = reference_tables()
    snippets = sql_snippets(telemetry)
    return {
        "text_instructions": _with_ids(
            "text", mode, [{"content": text_instructions(telemetry, metrics, mode)}]
        ),
        "example_question_sqls": _with_ids(
            "example",
            mode,
            [
                {"question": [question], "sql": [sql]}
                for question, sql in example_sqls(telemetry, metrics, refs, mode)
            ],
        ),
        "sql_snippets": {
            key: _with_ids(f"snippet-{key}", mode, list(items)) for key, items in snippets.items()
        },
    }


def build_serialized_space(industry: str, mode: str) -> str:
    """Return the Genie v2 ``serialized_space`` JSON string for a pipeline."""
    _require_mode(mode)
    payload = {
        "version": SERIALIZED_VERSION,
        "config": {"sample_questions": _sample_questions(mode)},
        "data_sources": {"tables": _table_entries(industry, mode)},
        "instructions": _instructions(industry, mode),
    }
    return json.dumps(payload, sort_keys=True)


# ---------------------------------------------------------------------------
# Ledger lookup
# ---------------------------------------------------------------------------
def _is_active_space(row: dict, mode: str) -> bool:
    metadata = row.get("metadata") or {}
    return (
        row.get("resource_type") == RESOURCE_TYPE
        and (row.get("status") or "active") == "active"
        and metadata.get("mode") == mode
        and bool(row.get("identifier"))
    )


async def find_pipeline_space(db: DBExecutor, setup_id: Any, mode: str) -> dict | None:
    """Return the newest active ``genie_space`` ledger row for ``(setup_id, mode)``."""
    rows = await resource_ledger.list_resources(db, setup_id)
    matches = [row for row in rows if row and _is_active_space(row, mode)]
    return matches[-1] if matches else None


async def find_space_id(db: DBExecutor, setup_id: Any, mode: str) -> str | None:
    row = await find_pipeline_space(db, setup_id, mode)
    return str(row["identifier"]) if row else None


# ---------------------------------------------------------------------------
# Create / update
# ---------------------------------------------------------------------------
def _new_ws() -> Any:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _space_kwargs(mode: str, setup_id: Any, industry: str, warehouse_id: str) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "warehouse_id": warehouse_id,
        "serialized_space": build_serialized_space(industry, mode),
        "title": space_title(mode, setup_id),
        "description": space_description(mode),
    }
    parent = _parent_path()
    if parent:
        kwargs["parent_path"] = parent
    return kwargs


async def _create_space(w: Any, kwargs: dict[str, Any]) -> str:
    space = await asyncio.to_thread(lambda: w.genie.create_space(**kwargs))
    space_id = getattr(space, "space_id", None)
    if not space_id:
        raise GenieSpaceError("Genie create_space returned no space_id")
    return str(space_id)


async def _update_space(w: Any, space_id: str, kwargs: dict[str, Any]) -> bool:
    """Update in place; return False when the recorded space no longer exists."""
    update = {key: value for key, value in kwargs.items() if key != "parent_path"}
    try:
        await asyncio.to_thread(lambda: w.genie.update_space(space_id, **update))
    except NotFound:
        logger.warning("Recorded Genie space %s is gone — recreating", space_id)
        return False
    return True


async def _record_space(
    db: DBExecutor,
    *,
    setup_id: Any,
    deployment_id: Any,
    space_id: str,
    industry: str,
    mode: str,
    kwargs: dict[str, Any],
) -> None:
    await resource_ledger.record_resource(
        db,
        setup_id=setup_id,
        deployment_id=deployment_id,
        resource_type=RESOURCE_TYPE,
        identifier=space_id,
        teardown_order=TEARDOWN_ORDER_GENIE_SPACE,
        metadata={
            "mode": mode,
            "industry": industry,
            "title": kwargs["title"],
            "parent_path": kwargs.get("parent_path"),
            "tables": list(expected_tables(industry, mode)),
        },
    )


def _validate_inputs(industry: str, mode: str, warehouse_id: str | None) -> None:
    if not supports_industry(industry):
        raise GenieSpaceError(f"per-pipeline Genie spaces are not supported for {industry!r}")
    _require_mode(mode)
    if not (warehouse_id or "").strip():
        raise GenieSpaceError("no SQL warehouse configured for the Genie space")


async def ensure_pipeline_space(
    db: DBExecutor,
    *,
    setup_id: Any,
    deployment_id: Any,
    industry: str,
    mode: str,
    warehouse_id: str | None,
    w: Any | None = None,
) -> dict[str, Any]:
    """Create the pipeline's Genie space, or update it in place on redeploy.

    Returns ``{"space_id", "action": created|updated|recreated, "tables"}``.
    Raises :class:`GenieSpaceError` (or the SDK error) on failure — the deploy
    hook turns any failure into a warning.
    """
    _validate_inputs(industry, mode, warehouse_id)
    industry_key = industry.strip().lower()
    client = w or _new_ws()
    kwargs = _space_kwargs(mode, setup_id, industry_key, str(warehouse_id).strip())
    tables = list(expected_tables(industry_key, mode))

    existing = await find_pipeline_space(db, setup_id, mode)
    if existing is not None:
        space_id = str(existing["identifier"])
        if await _update_space(client, space_id, kwargs):
            return {"space_id": space_id, "action": "updated", "tables": tables}
        await resource_ledger.mark_resource_status(db, existing.get("id"), "deleted")

    space_id = await _create_space(client, kwargs)
    await _record_space(
        db,
        setup_id=setup_id,
        deployment_id=deployment_id,
        space_id=space_id,
        industry=industry_key,
        mode=mode,
        kwargs=kwargs,
    )
    action = "recreated" if existing is not None else "created"
    logger.info("Genie space %s %s for setup %s mode %s", space_id, action, setup_id, mode)
    return {"space_id": space_id, "action": action, "tables": tables}


# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
def make_genie_space_deleter(w: Any | None = None):
    """Return an async teardown deleter for ``resource_type='genie_space'`` rows."""
    box: dict[str, Any] = {"ws": w}

    async def _delete(resource_row: dict) -> None:
        space_id = resource_row.get("identifier")
        if not space_id:
            return
        if box["ws"] is None:
            box["ws"] = _new_ws()
        client = box["ws"]
        try:
            await asyncio.to_thread(client.genie.trash_space, str(space_id))
        except NotFound:
            logger.info("Genie space %s already absent during teardown", space_id)
            return
        logger.info("Trashed Genie space %s during teardown", space_id)

    return _delete


# ---------------------------------------------------------------------------
# Attachment query-result shaping
# ---------------------------------------------------------------------------
_EMPTY_RESULT: dict[str, Any] = {"columns": [], "rows": [], "truncated": False}


def _column_type(column: Any) -> str | None:
    type_text = getattr(column, "type_text", None)
    if type_text:
        return str(type_text)
    type_name = getattr(column, "type_name", None)
    value = getattr(type_name, "value", None)
    if value:
        return str(value)
    return str(type_name) if type_name is not None else None


def _columns(manifest: Any) -> list[dict[str, Any]]:
    schema = getattr(manifest, "schema", None)
    columns = getattr(schema, "columns", None) or []
    return [{"name": getattr(c, "name", None), "type": _column_type(c)} for c in columns]


def _is_truncated(manifest: Any, total_rows: int, max_rows: int) -> bool:
    if total_rows > max_rows or bool(getattr(manifest, "truncated", False)):
        return True
    reported = getattr(manifest, "total_row_count", None)
    return isinstance(reported, int) and reported > min(total_rows, max_rows)


def query_result_payload(response: Any, max_rows: int = MAX_RESULT_ROWS) -> dict[str, Any]:
    """Shape ``get_message_attachment_query_result`` into ``{columns, rows, truncated}``."""
    statement = getattr(response, "statement_response", None)
    if statement is None:
        return dict(_EMPTY_RESULT)
    manifest = getattr(statement, "manifest", None)
    data = getattr(getattr(statement, "result", None), "data_array", None) or []
    rows = [list(row) for row in data[:max_rows]]
    return {
        "columns": _columns(manifest),
        "rows": rows,
        "truncated": _is_truncated(manifest, len(data), max_rows),
    }
