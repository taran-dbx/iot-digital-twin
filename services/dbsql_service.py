"""Databricks SQL Statement Execution helper for querying UC tables."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

logger = logging.getLogger("iot_digital_twin.services.dbsql")

_CATALOG_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def validate_catalog(catalog: str) -> bool:
    """Return True if the catalog name is safe for SQL interpolation."""
    return bool(_CATALOG_RE.fullmatch(catalog))


async def resolve_warehouse_id(db: Any, db_available: bool, user_email: str) -> str:
    """Resolve the SQL warehouse ID from user settings or environment."""
    warehouse_id = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "")
    if db_available and db:
        try:
            row = await db.fetchrow(
                "SELECT settings FROM user_settings WHERE user_email = $1", user_email
            )
            if row:
                settings = json.loads(row["settings"]) if isinstance(row["settings"], str) else row["settings"]
                saved = settings.get("warehouse", {}).get("warehouse_id", "")
                if saved:
                    warehouse_id = saved
        except Exception:
            pass
    return warehouse_id


async def execute_dbsql(
    sql: str,
    warehouse_id: str,
    timeout: str = "50s",
) -> dict:
    """Execute a SQL statement via the Databricks SQL Statement Execution API.

    Returns {"columns": [...], "data": [...], "row_count": int}.
    Raises on HTTP or API errors.
    """
    import httpx

    host = os.getenv("DATABRICKS_HOST", "")
    token = os.getenv("DATABRICKS_TOKEN", "")
    if not token:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        token = w.config.token
        if not host:
            host = f"https://{w.config.host}" if w.config.host and not w.config.host.startswith("https") else w.config.host

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            f"{host}/api/2.0/sql/statements",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "statement": sql,
                "warehouse_id": warehouse_id,
                "wait_timeout": timeout,
                "disposition": "INLINE",
            },
        )
        resp.raise_for_status()
        data = resp.json()

    result = data.get("result", {})
    columns = [c["name"] for c in result.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows: list[dict] = [dict(zip(columns, chunk)) for chunk in result.get("data_array", [])]
    return {"columns": columns, "data": rows, "row_count": len(rows)}


async def get_pipeline_catalog(
    db: Any, db_available: bool, pipeline_id: str, user_email: str,
) -> str | None:
    """Resolve UC catalog from a pipeline's resources. Returns None if unavailable."""
    if not db_available or not db:
        return None
    row = await db.fetchrow(
        "SELECT resources FROM pipelines WHERE id = $1::uuid AND user_email = $2",
        pipeline_id, user_email,
    )
    if not row:
        return None
    resources = row["resources"] if isinstance(row["resources"], dict) else json.loads(row["resources"] or "{}")
    return resources.get("uc_catalog")
