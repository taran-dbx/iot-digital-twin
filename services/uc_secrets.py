"""Unity Catalog secret read/write helper (Task 3).

Unity Catalog secrets are catalog-scoped (``{catalog}.{schema}.{name}``) and
distinct from workspace secret scopes.  The app SP stores its own OAuth
client-secret here (``{catalog}.{schema}.app_sp_oauth``) so the Zerobus
producer notebooks can read it at runtime with only ``READ SECRET`` on the
schema — no static credential is baked into notebook source.

Both helpers hit the ``/api/2.1/unity-catalog/secrets`` REST surface via the
SDK api-client (which carries the app SP's OAuth auth automatically).  The
blocking api-client call is run in a worker thread so the event loop is never
blocked during provisioning.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger("iot_digital_twin.services.uc_secrets")

_SECRETS_PATH = "/api/2.1/unity-catalog/secrets"


def _new_ws() -> Any:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _fqn(catalog: str, schema: str, name: str) -> str:
    """Return the fully-qualified UC secret name ``{catalog}.{schema}.{name}``."""
    return f"{catalog}.{schema}.{name}"


def _is_already_exists(exc: Exception) -> bool:
    return "already exists" in str(exc).lower()


def _put_sync(ws: Any, catalog: str, schema: str, name: str, value: str) -> dict:
    """Create-or-update a UC secret via the api-client (blocking)."""
    fqn = _fqn(catalog, schema, name)
    body = {
        "secret_info": {
            "catalog_name": catalog,
            "schema_name": schema,
            "name": name,
            "value": value,
        }
    }
    try:
        ws.api_client.do("POST", _SECRETS_PATH, body=body)
        return {"resource": f"UC secret '{fqn}'", "status": "created"}
    except Exception as exc:
        if not _is_already_exists(exc):
            return {"resource": f"UC secret '{fqn}'", "status": "failed", "error": str(exc)[:200]}
        # Already present — update the value in place.
        ws.api_client.do(
            "PATCH",
            f"{_SECRETS_PATH}/{fqn}",
            query={"update_mask": "value"},
            body={"secret_info": {"value": value}},
        )
        return {"resource": f"UC secret '{fqn}'", "status": "updated"}


def _get_sync(ws: Any, catalog: str, schema: str, name: str) -> str:
    """Read a UC secret value via the api-client (blocking)."""
    fqn = _fqn(catalog, schema, name)
    resp = ws.api_client.do("GET", f"{_SECRETS_PATH}/{fqn}") or {}
    info = resp.get("secret_info") if isinstance(resp, dict) else None
    if isinstance(info, dict) and "value" in info:
        return info["value"]
    if isinstance(resp, dict) and "value" in resp:
        return resp["value"]
    return ""


async def put_secret(
    catalog: str, schema: str, name: str, value: str, *, ws: Any | None = None,
) -> dict:
    """Create or update the UC secret ``{catalog}.{schema}.{name}`` (idempotent).

    Returns a ``{resource, status}`` dict (``created`` / ``updated`` / ``failed``).
    """
    ws = ws or _new_ws()
    return await asyncio.to_thread(_put_sync, ws, catalog, schema, name, value)


async def get_secret(
    catalog: str, schema: str, name: str, *, ws: Any | None = None,
) -> str:
    """Return the value of the UC secret ``{catalog}.{schema}.{name}``."""
    ws = ws or _new_ws()
    return await asyncio.to_thread(_get_sync, ws, catalog, schema, name)
