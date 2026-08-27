"""Central workspace configuration module.

Every workspace-specific value the app needs lives in a single place:
``app.yaml`` env vars → this module → callsites.  No workspace-specific
literals may appear elsewhere in source.

Env var → WorkspaceConfig field mapping
---------------------------------------
UC_CATALOG                  → uc_catalog
UC_SCHEMA                   → uc_schema
LAKEBASE_PROJECT            → lakebase_project
LAKEBASE_ENDPOINT           → lakebase_endpoint
PGHOST                      → pg_host          (optional — blank until Task 2)
DATABRICKS_SQL_WAREHOUSE_ID → warehouse_id
ZEROBUS_SERVER_ENDPOINT     → zerobus_endpoint
DATABRICKS_CLIENT_ID        → app_sp_client_id
INDUSTRIES                  → industries       (comma-separated → list)

All fields except ``pg_host`` are required.  ``get_config()`` raises
``RuntimeError`` naming every missing variable when any required var is unset.
The config is a frozen dataclass so callers cannot mutate it accidentally.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

_REQUIRED_ENV: tuple[str, ...] = (
    "UC_CATALOG",
    "UC_SCHEMA",
    "LAKEBASE_PROJECT",
    "LAKEBASE_ENDPOINT",
    "DATABRICKS_SQL_WAREHOUSE_ID",
    "ZEROBUS_SERVER_ENDPOINT",
    "DATABRICKS_CLIENT_ID",
    "INDUSTRIES",
)


@dataclass(frozen=True)
class WorkspaceConfig:
    """Immutable workspace-level configuration resolved from environment variables."""

    uc_catalog: str
    uc_schema: str
    lakebase_project: str
    lakebase_endpoint: str
    pg_host: str  # may be empty until the Lakebase project is provisioned (Task 2)
    warehouse_id: str
    zerobus_endpoint: str
    app_sp_client_id: str
    industries: list[str]


def get_config() -> WorkspaceConfig:
    """Return the workspace configuration read from environment variables.

    Raises ``RuntimeError`` with a clear message listing every missing
    required env var.  ``PGHOST`` is optional and may be empty.
    """
    missing = [env for env in _REQUIRED_ENV if not os.getenv(env)]
    if missing:
        raise RuntimeError(
            f"Missing required workspace env var(s): {', '.join(missing)}. "
            "Configure these in app.yaml (or as environment variables) before starting."
        )

    industries_raw = os.environ["INDUSTRIES"]
    industries = [i.strip() for i in industries_raw.split(",") if i.strip()]

    return WorkspaceConfig(
        uc_catalog=os.environ["UC_CATALOG"],
        uc_schema=os.environ["UC_SCHEMA"],
        lakebase_project=os.environ["LAKEBASE_PROJECT"],
        lakebase_endpoint=os.environ["LAKEBASE_ENDPOINT"],
        pg_host=os.getenv("PGHOST", ""),
        warehouse_id=os.environ["DATABRICKS_SQL_WAREHOUSE_ID"],
        zerobus_endpoint=os.environ["ZEROBUS_SERVER_ENDPOINT"],
        app_sp_client_id=os.environ["DATABRICKS_CLIENT_ID"],
        industries=industries,
    )
