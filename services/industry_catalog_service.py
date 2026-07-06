"""Canonical per-industry UC provisioning + grants + secrets (spec §2.5–2.7, §1.11).

Creates the canonical ``iot_<industry>`` Unity Catalog structure and the shared
identity/secret plumbing for one industry pipeline, recording every resource in
the ``provisioned_resource`` ledger with a sensible ``teardown_order`` +
``parent_ref`` so delete can walk them in reverse-dependency order.

Layout (spec §2.5)::

    iot_<industry>/
    ├── realtime/     sensor_readings              Delta   (CDF-synced historical mirror)
    ├── microbatch/   sensor_data                  Delta   (micro-batch bronze)
    ├── zerobus/      iot_sensor_data              Delta   (zerobus ingest target)
    ├── metrics/      streaming_metrics_realtime   Delta   EXTERNAL, delta-rs writes
    │                 streaming_metrics_microbatch Delta   EXTERNAL, delta-rs writes
    │                 streaming_metrics_zerobus    Delta   EXTERNAL, delta-rs writes
    └── checkpoints/  stream_checkpoints           Volume  (SS checkpoints)

Provisioning notes (spike-verified 2026-07-05):
- This metastore has **no default storage root**, so ``CREATE CATALOG`` must
  supply an explicit ``MANAGED LOCATION`` — ``s3://taran-shared-interop/<catalog>``
  (external location ``taran-shared-interop``, ``us-east-1``).
- Each mode gets its own EXTERNAL ``metrics.streaming_metrics_<mode>`` table
  (delta-rs writes via UC temp-table-credentials) → any writer SP (streaming SP
  for the streaming query listener, app SP for the app-hosted listener) needs
  ``EXTERNAL USE SCHEMA`` + ``MODIFY`` on the metrics schema.

This module is **additive** — it does not touch ``app.yaml``, ``backend.py`` live
wiring, or the legacy ``iot-lakets-demo`` project.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from models.metrics_schema import (
    ALL_METRIC_MODES,
    METRICS_SCHEMA,
    metrics_external_table_ddl,
    metrics_table_fqn,
    metrics_table_location,
)
from services.dbsql_service import execute_dbsql
from services.provisioning_service import (
    SHARED_STREAMING_SP_DISPLAY_NAME,
    SHARED_STREAMING_SP_SECRET_SCOPE,
    _find_sp_by_display_name,
)
from services.resource_ledger import record_resource

logger = logging.getLogger("iot_digital_twin.services.industry_catalog")

# --- Constants -------------------------------------------------------------
# Shared external location the metastore is bound to (us-east-1).
SHARED_STORAGE_BASE = "s3://taran-shared-interop"
EXTERNAL_LOCATION_NAME = "taran-shared-interop"

# Canonical schema set + checkpoint volume (spec §2.5).
CANONICAL_SCHEMAS = ("realtime", "microbatch", "zerobus", "metrics", "checkpoints")
CHECKPOINT_SCHEMA = "checkpoints"
CHECKPOINT_VOLUME = "stream_checkpoints"

# Fallback SP application id (spec §2.5) if the SP can't be resolved by name.
FALLBACK_SP_APP_ID = "9927a7fa-2427-4b6a-97b6-cd41b878d405"

# Ledger teardown_order.  Higher = deleted earlier (reverse-dependency order).
# ``DROP CATALOG ... CASCADE`` removes schemas/tables/volume, so the per-object
# rows are recorded for inventory/health; the catalog drop does the real work.
TEARDOWN_ORDER_SP = 1              # shared — never torn down per-industry
TEARDOWN_ORDER_SECRET_SCOPE = 2    # shared — never torn down per-industry
TEARDOWN_ORDER_SECRET = 3          # shared — never torn down per-industry
TEARDOWN_ORDER_CATALOG = 10
TEARDOWN_ORDER_SCHEMA = 15
TEARDOWN_ORDER_VOLUME = 18
TEARDOWN_ORDER_METRICS_TABLE = 20
TEARDOWN_ORDER_GRANT = 45          # revoke first (harmless once objects dropped)

_CATALOG_SAFE_RE = re.compile(r"[^a-z0-9_]+")


# --- Name helpers ----------------------------------------------------------
def catalog_name(industry: str) -> str:
    """Return the canonical catalog name ``iot_<industry>`` for ``industry``.

    Lower-cases and collapses any non ``[a-z0-9_]`` run to a single underscore
    so an arbitrary industry label produces a valid UC identifier.
    """
    safe = _CATALOG_SAFE_RE.sub("_", (industry or "").strip().lower()).strip("_")
    if not safe:
        raise ValueError(f"industry {industry!r} has no valid characters for a catalog name")
    return f"iot_{safe}"


def catalog_managed_location(catalog: str) -> str:
    """Return the MANAGED LOCATION S3 URI for ``catalog``."""
    return f"{SHARED_STORAGE_BASE}/{catalog}"


def _new_ws() -> Any:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _is_already_exists(exc: Exception) -> bool:
    return "already exists" in str(exc).lower()


def _is_not_found(exc: Exception) -> bool:
    s = str(exc).lower()
    return "not_found" in s or "not found" in s or "does not exist" in s


# --- Warehouse resolution --------------------------------------------------
def resolve_warehouse_id(ws: Any, warehouse_id: str | None = None) -> str:
    """Return a usable SQL warehouse id (explicit arg wins, else first listed)."""
    if warehouse_id:
        return warehouse_id
    warehouses = list(ws.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouse available for catalog SQL/DDL")
    return warehouses[0].id


# --- Pre-flight ------------------------------------------------------------
def preflight_checks(*, ws: Any | None = None, warehouse_id: str | None = None) -> list[dict]:
    """Run pre-flight checks for industry catalog provisioning.

    Returns one ``{check, status, detail}`` per gate (``status`` ∈
    ``pass``/``fail``).  The orchestrator raises with a clear message when any
    check fails.  Never raises itself.
    """
    ws = ws or _new_ws()
    checks: list[dict] = []

    # 1. External location the MANAGED LOCATION lives under must exist.
    try:
        ws.external_locations.get(name=EXTERNAL_LOCATION_NAME)
        checks.append({
            "check": f"External location '{EXTERNAL_LOCATION_NAME}'",
            "status": "pass",
            "detail": "present",
        })
    except Exception as exc:
        checks.append({
            "check": f"External location '{EXTERNAL_LOCATION_NAME}'",
            "status": "fail",
            "detail": (
                f"Not found ({str(exc)[:120]}). CREATE CATALOG needs an explicit "
                f"MANAGED LOCATION under {SHARED_STORAGE_BASE}; the external "
                f"location '{EXTERNAL_LOCATION_NAME}' must exist and be writable."
            ),
        })

    # 2. A SQL warehouse must be available for external-table DDL + grants.
    try:
        wh = resolve_warehouse_id(ws, warehouse_id)
        checks.append({
            "check": "SQL warehouse",
            "status": "pass",
            "detail": f"using warehouse {wh}",
        })
    except Exception as exc:
        checks.append({
            "check": "SQL warehouse",
            "status": "fail",
            "detail": str(exc)[:160],
        })

    return checks


def raise_on_failed_preflight(checks: list[dict]) -> None:
    """Raise ``RuntimeError`` with a clear message if any pre-flight check failed."""
    failed = [c for c in checks if c.get("status") == "fail"]
    if failed:
        detail = "; ".join(f"{c['check']}: {c['detail']}" for c in failed)
        raise RuntimeError(f"Pre-flight checks failed — {detail}")


# --- SQL helper ------------------------------------------------------------
async def _run_sql(sql: str, warehouse_id: str) -> dict:
    """Execute one SQL statement via DBSQL, returning a result/status dict."""
    try:
        await execute_dbsql(sql, warehouse_id)
        return {"status": "ok"}
    except Exception as exc:
        if _is_already_exists(exc):
            return {"status": "already_exists"}
        return {"status": "failed", "error": str(exc)[:200]}


# --- Catalog / schemas / volume / metrics table ----------------------------
async def provision_industry_catalog(
    industry: str,
    *,
    ws: Any | None = None,
    warehouse_id: str | None = None,
    db: Any | None = None,
    setup_id: Any | None = None,
    deployment_id: Any | None = None,
) -> dict:
    """Create the canonical ``iot_<industry>`` catalog + schemas + volume + the
    three per-mode external metrics tables (idempotent), recording each in the
    ledger.

    Returns ``{catalog, results, metrics_table, metrics_tables, metrics_locations}``
    — ``metrics_table``/``metrics_tables`` are ``{mode: fqn}`` dicts (kept as two
    keys for readability at call sites) and ``metrics_locations`` is ``{mode: s3_uri}``.
    """
    ws = ws or _new_ws()
    catalog = catalog_name(industry)
    managed_location = catalog_managed_location(catalog)
    results: list[dict] = []

    # 1. Catalog with explicit MANAGED LOCATION (metastore has no storage root).
    try:
        ws.catalogs.create(name=catalog, storage_root=managed_location)
        results.append({"resource": f"UC catalog '{catalog}'", "status": "created"})
    except Exception as exc:
        if _is_already_exists(exc):
            results.append({"resource": f"UC catalog '{catalog}'", "status": "already_exists"})
        else:
            results.append({
                "resource": f"UC catalog '{catalog}'",
                "status": "failed",
                "error": str(exc)[:200],
            })
            # Catalog is the root dependency — abort if it can't be created.
            return {
                "catalog": catalog,
                "results": results,
                "metrics_table": None,
                "metrics_tables": None,
                "metrics_locations": None,
            }
    await _record(
        db, setup_id, deployment_id,
        resource_type="uc_catalog", identifier=catalog,
        teardown_order=TEARDOWN_ORDER_CATALOG,
        metadata={"managed_location": managed_location},
    )

    # 2. Schemas.
    for schema in CANONICAL_SCHEMAS:
        try:
            ws.schemas.create(name=schema, catalog_name=catalog)
            results.append({"resource": f"Schema '{catalog}.{schema}'", "status": "created"})
        except Exception as exc:
            if _is_already_exists(exc):
                results.append({"resource": f"Schema '{catalog}.{schema}'", "status": "already_exists"})
            else:
                results.append({
                    "resource": f"Schema '{catalog}.{schema}'",
                    "status": "failed",
                    "error": str(exc)[:200],
                })
                continue
        await _record(
            db, setup_id, deployment_id,
            resource_type="uc_schema", identifier=f"{catalog}.{schema}",
            parent_ref=catalog, teardown_order=TEARDOWN_ORDER_SCHEMA,
        )

    # 3. Managed checkpoint volume.
    from databricks.sdk.service.catalog import VolumeType

    vol_fqn = f"{catalog}.{CHECKPOINT_SCHEMA}.{CHECKPOINT_VOLUME}"
    try:
        ws.volumes.create(
            catalog_name=catalog,
            schema_name=CHECKPOINT_SCHEMA,
            name=CHECKPOINT_VOLUME,
            volume_type=VolumeType.MANAGED,
        )
        results.append({"resource": f"Volume '{vol_fqn}'", "status": "created"})
    except Exception as exc:
        if _is_already_exists(exc):
            results.append({"resource": f"Volume '{vol_fqn}'", "status": "already_exists"})
        else:
            results.append({
                "resource": f"Volume '{vol_fqn}'",
                "status": "failed",
                "error": str(exc)[:200],
            })
    await _record(
        db, setup_id, deployment_id,
        resource_type="uc_volume", identifier=vol_fqn,
        parent_ref=f"{catalog}.{CHECKPOINT_SCHEMA}",
        teardown_order=TEARDOWN_ORDER_VOLUME,
    )

    # 4. External metrics Delta tables (delta-rs write targets) — one per mode
    # so realtime/microbatch/zerobus metrics never share a table.
    wh = resolve_warehouse_id(ws, warehouse_id)
    metrics_fqns: dict[str, str] = {}
    metrics_locs: dict[str, str] = {}
    for mode in ALL_METRIC_MODES:
        metrics_fqn = metrics_table_fqn(catalog, mode)
        metrics_loc = metrics_table_location(catalog, SHARED_STORAGE_BASE, mode)
        ddl = metrics_external_table_ddl(catalog, SHARED_STORAGE_BASE, mode)
        res = await _run_sql(ddl, wh)
        if res["status"] == "failed":
            results.append({
                "resource": f"Metrics table '{metrics_fqn}'",
                "status": "failed",
                "error": res.get("error"),
            })
            continue
        results.append({
            "resource": f"Metrics table '{metrics_fqn}'",
            "status": "created" if res["status"] == "ok" else res["status"],
        })
        metrics_fqns[mode] = metrics_fqn
        metrics_locs[mode] = metrics_loc
        await _record(
            db, setup_id, deployment_id,
            resource_type="metrics_table", identifier=metrics_fqn,
            parent_ref=f"{catalog}.{METRICS_SCHEMA}",
            teardown_order=TEARDOWN_ORDER_METRICS_TABLE,
            metadata={"external": True, "location": metrics_loc, "mode": mode},
        )

    return {
        "catalog": catalog,
        "results": results,
        "metrics_table": metrics_fqns,
        "metrics_tables": metrics_fqns,
        "metrics_locations": metrics_locs,
    }


# --- Grants ----------------------------------------------------------------
def resolve_streaming_sp_principal(ws: Any) -> str:
    """Return the streaming SP's grant principal (application id).

    Grants to a service principal in UC SQL use the SP's *application id*.
    Falls back to the known shared SP app id if lookup fails.
    """
    try:
        sp = _find_sp_by_display_name(ws, SHARED_STREAMING_SP_DISPLAY_NAME)
        if sp is not None and getattr(sp, "application_id", None):
            return sp.application_id
    except Exception as exc:
        logger.warning("Could not resolve streaming SP by name (non-fatal): %s", exc)
    return FALLBACK_SP_APP_ID


def _grant_statements(catalog: str, principal: str) -> list[tuple[str, str]]:
    """Return ``(label, sql)`` GRANT statements for the shared SP on ``catalog``.

    Covers streaming-writer needs on realtime/microbatch/metrics + zerobus
    CREATE TABLE + the ``EXTERNAL USE SCHEMA`` required for delta-rs temp-creds
    on the metrics schema (spec §1.9–1.11, §1.10 zerobus, §2.7).
    """
    q = f"`{principal}`"
    stmts: list[tuple[str, str]] = [
        ("USE CATALOG", f"GRANT USE CATALOG ON CATALOG {catalog} TO {q}"),
    ]
    # Writer schemas (streaming writes land here + metrics).
    for schema in ("realtime", "microbatch", METRICS_SCHEMA):
        fqn = f"{catalog}.{schema}"
        stmts.append((
            f"USE/MODIFY/SELECT on {schema}",
            f"GRANT USE SCHEMA, MODIFY, SELECT ON SCHEMA {fqn} TO {q}",
        ))
    # EXTERNAL USE SCHEMA on metrics — required for GenerateTemporaryTableCredentials.
    stmts.append((
        "EXTERNAL USE SCHEMA on metrics",
        f"GRANT EXTERNAL USE SCHEMA ON SCHEMA {catalog}.{METRICS_SCHEMA} TO {q}",
    ))
    # Zerobus schema — needs CREATE TABLE (create_stream requires the table to
    # pre-exist) plus the standard read/write privileges.
    stmts.append((
        "USE/MODIFY/SELECT/CREATE on zerobus",
        f"GRANT USE SCHEMA, MODIFY, SELECT, CREATE TABLE ON SCHEMA {catalog}.zerobus TO {q}",
    ))
    return stmts


async def grant_streaming_sp(
    industry: str,
    *,
    ws: Any | None = None,
    warehouse_id: str | None = None,
    principal: str | None = None,
    db: Any | None = None,
    setup_id: Any | None = None,
    deployment_id: Any | None = None,
) -> dict:
    """Grant the shared streaming SP the privileges it needs on ``iot_<industry>``.

    Records each grant in the ledger.  Returns ``{catalog, principal, results}``.
    """
    ws = ws or _new_ws()
    catalog = catalog_name(industry)
    principal = principal or resolve_streaming_sp_principal(ws)
    wh = resolve_warehouse_id(ws, warehouse_id)
    results: list[dict] = []

    for label, sql in _grant_statements(catalog, principal):
        res = await _run_sql(sql, wh)
        results.append({"resource": f"GRANT {label} → {principal}", "status": res["status"], **({"error": res["error"]} if res.get("error") else {})})
        if res["status"] != "failed":
            await _record(
                db, setup_id, deployment_id,
                resource_type="grant",
                identifier=f"{catalog}:{label}:{principal}",
                parent_ref=catalog,
                teardown_order=TEARDOWN_ORDER_GRANT,
                metadata={"sql": sql},
            )

    return {"catalog": catalog, "principal": principal, "results": results}


def resolve_app_principal(ws: Any) -> str | None:
    """Return the APP BACKEND's own SP application id — the identity that runs the
    app's DBSQL reads + Tier-1 health checks (distinct from the streaming SP that
    runs the jobs). In the deployed app this is ``DATABRICKS_CLIENT_ID``; falls
    back to the current principal. ``None`` if it can't be resolved (grant then
    skipped, non-fatal)."""
    import os

    cid = os.getenv("DATABRICKS_CLIENT_ID")
    if cid:
        return cid
    try:
        return ws.current_user.me().user_name
    except Exception as exc:  # pragma: no cover - offline / no creds
        logger.warning("Could not resolve app principal (non-fatal): %s", exc)
        return None


def _app_read_grant_statements(catalog: str, principal: str) -> list[tuple[str, str]]:
    """Read-only, catalog-wide grants for the app backend SP. Catalog-level SELECT
    cascades to ALL current + future tables in every schema — critically the CDF
    ``lb_<shadow>_history`` Delta table the managed sync creates later, owned by
    whoever set up the sync (so the app SP would otherwise lack SELECT on it)."""
    q = f"`{principal}`"
    return [
        ("app USE CATALOG", f"GRANT USE CATALOG ON CATALOG {catalog} TO {q}"),
        ("app USE SCHEMA", f"GRANT USE SCHEMA ON CATALOG {catalog} TO {q}"),
        ("app SELECT", f"GRANT SELECT ON CATALOG {catalog} TO {q}"),
    ]


def _app_metrics_write_grant_statements(catalog: str, principal: str) -> list[tuple[str, str]]:
    """Metrics-WRITE grants for the app backend SP.

    The per-mode metrics listeners run **as the app SP** (not the streaming SP)
    on the validation/interactive cluster, so delta-rs's UC temp-table-credentials
    write path needs ``EXTERNAL USE SCHEMA`` + ``MODIFY`` on the metrics schema
    for that principal too — mirroring the streaming SP's grant in
    ``_grant_statements``. Without this the per-mode metrics tables stay empty."""
    q = f"`{principal}`"
    fqn = f"{catalog}.{METRICS_SCHEMA}"
    return [
        ("app EXTERNAL USE SCHEMA on metrics", f"GRANT EXTERNAL USE SCHEMA ON SCHEMA {fqn} TO {q}"),
        ("app MODIFY on metrics", f"GRANT MODIFY ON SCHEMA {fqn} TO {q}"),
    ]


async def grant_app_backend_read(
    industry: str,
    *,
    ws: Any | None = None,
    warehouse_id: str | None = None,
    principal: str | None = None,
    db: Any | None = None,
    setup_id: Any | None = None,
    deployment_id: Any | None = None,
) -> dict:
    """Grant the app backend SP read-only access to ``iot_<industry>`` so its DBSQL
    reads (validation ``dbsql_read``) + Tier-1 health checks can query every table,
    including the CDF-synced Delta table created later by the managed sync — plus
    the metrics-WRITE grants (``EXTERNAL USE SCHEMA`` + ``MODIFY`` on the metrics
    schema) the app-hosted streaming-metrics listener needs to write the per-mode
    external Delta tables via delta-rs temp credentials.

    Records each grant in the ledger. Returns ``{catalog, principal, results}``.
    """
    ws = ws or _new_ws()
    catalog = catalog_name(industry)
    principal = principal or resolve_app_principal(ws)
    if not principal:
        return {"catalog": catalog, "principal": None,
                "results": [{"resource": "app read grant", "status": "skipped",
                             "error": "app principal unresolved"}]}
    wh = resolve_warehouse_id(ws, warehouse_id)
    results: list[dict] = []
    grant_statements = (
        _app_read_grant_statements(catalog, principal)
        + _app_metrics_write_grant_statements(catalog, principal)
    )
    for label, sql in grant_statements:
        res = await _run_sql(sql, wh)
        results.append({"resource": f"GRANT {label} → {principal}", "status": res["status"],
                        **({"error": res["error"]} if res.get("error") else {})})
        if res["status"] != "failed":
            await _record(
                db, setup_id, deployment_id,
                resource_type="grant",
                identifier=f"{catalog}:{label}:{principal}",
                parent_ref=catalog,
                teardown_order=TEARDOWN_ORDER_GRANT,
                metadata={"sql": sql},
            )
    return {"catalog": catalog, "principal": principal, "results": results}


# --- Secrets + SP ----------------------------------------------------------
async def ensure_secret_scope(
    scope: str = SHARED_STREAMING_SP_SECRET_SCOPE,
    *,
    ws: Any | None = None,
    db: Any | None = None,
    setup_id: Any | None = None,
) -> dict:
    """Ensure the shared secret scope exists (idempotent), recording it (shared).

    The scope + SP are shared across all industries, so the ledger row is
    flagged ``shared`` and must never be torn down per-industry.
    """
    ws = ws or _new_ws()
    status: str
    try:
        ws.secrets.create_scope(scope=scope)
        status = "created"
    except Exception as exc:
        if _is_already_exists(exc):
            status = "already_exists"
        else:
            return {"resource": f"Secret scope '{scope}'", "status": "failed", "error": str(exc)[:200]}

    await _record(
        db, setup_id, None,
        resource_type="secret_scope", identifier=scope,
        teardown_order=TEARDOWN_ORDER_SECRET_SCOPE,
        metadata={"shared": True},
    )
    return {"resource": f"Secret scope '{scope}'", "status": status}


# --- Ledger helper ---------------------------------------------------------
async def _record(
    db: Any | None,
    setup_id: Any | None,
    deployment_id: Any | None,
    *,
    resource_type: str,
    identifier: str,
    parent_ref: str | None = None,
    teardown_order: int = 0,
    metadata: dict | None = None,
) -> None:
    """Record a resource in the ledger when a db + setup_id are provided."""
    if db is None or setup_id is None:
        return
    await record_resource(
        db,
        setup_id=setup_id,
        deployment_id=deployment_id,
        resource_type=resource_type,
        identifier=identifier,
        parent_ref=parent_ref,
        teardown_order=teardown_order,
        metadata=metadata,
    )
