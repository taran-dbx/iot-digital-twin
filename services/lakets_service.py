"""LakeTS bootstrap — install toolkit into a Lakebase instance and probe version.

LakeTS is a PL/pgSQL-only toolkit; no Postgres extension is required. Installing
it is a single `psql -f dist/lakets.sql` equivalent against the Lakebase pool.
The SQL registers its own version in ``lakets._version``.

Source resolution precedence (in order):
1. explicit ``sql_path`` argument
2. ``LAKETS_SQL_PATH`` env var — absolute path to ``lakets.sql``
3. ``LAKETS_GIT_URL`` env var — git repo to shallow-clone at provision time
"""

from __future__ import annotations

import datetime as _dt
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Optional

from models.sql_ddl import LAKETS_VERSION_CHECK_SQL

logger = logging.getLogger("iot_digital_twin.services.lakets")


_CLONE_TIMEOUT_SEC = 60
_DIST_RELPATH = Path("dist") / "lakets.sql"


async def get_installed_version(pool: Any) -> Optional[str]:
    """Return the latest installed LakeTS version, or None if not installed."""
    try:
        return await pool.fetchval(LAKETS_VERSION_CHECK_SQL)
    except Exception as exc:
        logger.debug("LakeTS version probe failed (not installed?): %s", exc)
        return None


def resolve_lakets_source(explicit_path: Optional[str] = None) -> Path:
    """Resolve a path to ``lakets.sql`` from arg, env, or git clone.

    Raises:
        FileNotFoundError: explicit path was supplied but does not exist.
        RuntimeError: no source is configured at all.
    """
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            raise FileNotFoundError(f"LakeTS SQL not found at {path}")
        return path

    env_path = os.getenv("LAKETS_SQL_PATH")
    if env_path:
        path = Path(env_path)
        if not path.exists():
            raise FileNotFoundError(f"LakeTS SQL not found at {path} (LAKETS_SQL_PATH)")
        return path

    git_url = os.getenv("LAKETS_GIT_URL")
    if git_url:
        clone_dir = Path(tempfile.mkdtemp(prefix="lakets-"))
        try:
            _git_clone(git_url, clone_dir)
        except Exception:
            shutil.rmtree(clone_dir, ignore_errors=True)
            raise
        resolved = clone_dir / _DIST_RELPATH
        if not resolved.exists():
            raise FileNotFoundError(
                f"Cloned repo {git_url} does not contain {_DIST_RELPATH}"
            )
        return resolved

    raise RuntimeError(
        "LakeTS source not configured — set LAKETS_SQL_PATH or LAKETS_GIT_URL"
    )


def _git_clone(url: str, dest: Path) -> None:
    """Shallow-clone ``url`` into ``dest``. Kept as a separate function for mocking."""
    subprocess.run(
        ["git", "clone", "--depth", "1", url, str(dest)],
        check=True,
        timeout=_CLONE_TIMEOUT_SEC,
        capture_output=True,
    )


async def install_lakets(
    pool: Any,
    sql_path: Path,
    *,
    skip_if_installed: bool = False,
) -> str:
    """Execute ``lakets.sql`` against the pool and return the installed version.

    Args:
        pool: asyncpg pool or anything exposing ``.execute`` / ``.fetchval`` coroutines.
        sql_path: path to ``dist/lakets.sql``.
        skip_if_installed: if True and LakeTS is already present, skip execution
            and just return the existing version (idempotent fast path).

    Raises:
        FileNotFoundError: sql_path does not exist.
    """
    if not sql_path.exists():
        raise FileNotFoundError(f"LakeTS SQL not found at {sql_path}")

    if skip_if_installed:
        existing = await get_installed_version(pool)
        if existing:
            logger.info("LakeTS %s already installed — skipping execution", existing)
            return existing

    sql_text = sql_path.read_text(encoding="utf-8")
    logger.info("Installing LakeTS from %s (%d bytes)", sql_path, len(sql_text))
    await pool.execute(sql_text)

    version = await get_installed_version(pool)
    if not version:
        raise RuntimeError(
            "LakeTS install completed but lakets._version is empty — check for SQL errors"
        )
    logger.info("LakeTS %s installed", version)
    return version


# ---------------------------------------------------------------------------
# ChronoTable + tier-policy helpers (Phase 2)
# ---------------------------------------------------------------------------
_CHRONOTABLE_PROBE_SQL = (
    "SELECT id FROM lakets._chronotable_registry "
    "WHERE schema_name = $1 AND table_name = $2"
)
_CHRONOTABLE_CREATE_SQL = (
    "SELECT lakets.create_chronotable($1, $2, $3::interval, $4)"
)
_POLICY_PROBE_SQL = (
    "SELECT p.id FROM lakets._policy_registry p "
    "JOIN lakets._chronotable_registry c ON p.chronotable_id = c.id "
    "WHERE c.schema_name = $1 AND c.table_name = $2 "
    "  AND p.policy_type = $3"
)
_TIERED_RETENTION_APPLY_SQL = (
    "SELECT lakets.add_tiered_retention_policy($1, $2::interval, $3::interval, $4)"
)


async def create_chronotable(
    pool: Any,
    *,
    table: str,
    time_col: str,
    chunk: str,
    schema: str = "public",
) -> bool:
    """Register ``table`` as a LakeTS ChronoTable. Idempotent.

    Returns ``True`` if the table was already a ChronoTable (no-op),
    ``False`` if it was created.
    """
    existing_id = await pool.fetchval(_CHRONOTABLE_PROBE_SQL, schema, table)
    if existing_id is not None:
        logger.info("ChronoTable %s.%s already registered (id=%s)", schema, table, existing_id)
        return True

    new_id = await pool.fetchval(
        _CHRONOTABLE_CREATE_SQL, table, time_col, _interval_to_timedelta(chunk), schema,
    )
    logger.info(
        "ChronoTable %s.%s created (id=%s, time=%s, chunk=%s)",
        schema, table, new_id, time_col, chunk,
    )
    return False


async def apply_tier_policy(
    pool: Any,
    *,
    table: str,
    hot: str,
    drop: str,
    schema: str = "public",
) -> bool:
    """Apply a tiered-retention policy. Data tiers from hot (Postgres) to
    cold (Delta) after ``hot`` elapses; Postgres copy is dropped after ``drop``.

    Both ``hot`` and ``drop`` are Postgres interval strings (e.g. ``"1 hour"``,
    ``"7 days"``).  Raises ``ValueError`` if ``hot >= drop`` (redundant with
    LakeTS' own server-side check, surfaced early with a clearer message).

    Returns ``True`` if a policy already existed (no-op), ``False`` if applied.
    """
    if hot == drop or not _interval_lt(hot, drop):
        raise ValueError(f"hot ({hot}) must be less than drop ({drop})")

    existing_id = await pool.fetchval(
        _POLICY_PROBE_SQL, schema, table, "tiered_retention",
    )
    if existing_id is not None:
        logger.info(
            "Tiered retention policy on %s.%s already exists (id=%s)",
            schema, table, existing_id,
        )
        return True

    new_id = await pool.fetchval(
        _TIERED_RETENTION_APPLY_SQL, table,
        _interval_to_timedelta(hot), _interval_to_timedelta(drop), schema,
    )
    logger.info(
        "Tiered retention policy applied on %s.%s (id=%s, hot=%s, drop=%s)",
        schema, table, new_id, hot, drop,
    )
    return False


# Approximate ordering of Postgres interval strings for client-side validation.
# LakeTS enforces the same rule server-side — this just fails fast with a clearer error.
_UNIT_SECONDS = {
    "second": 1, "seconds": 1, "sec": 1,
    "minute": 60, "minutes": 60, "min": 60,
    "hour": 3600, "hours": 3600,
    "day": 86400, "days": 86400,
    "week": 604800, "weeks": 604800,
    "month": 2592000, "months": 2592000,  # approximate — 30 days
    "year": 31536000, "years": 31536000,  # approximate — 365 days
}


def _interval_lt(a: str, b: str) -> bool:
    """Return True iff Postgres interval string ``a`` is strictly less than ``b``."""
    return _interval_seconds(a) < _interval_seconds(b)


def _interval_seconds(s: str) -> float:
    """Rough conversion of a Postgres interval string to seconds."""
    parts = s.strip().split()
    if len(parts) != 2:
        raise ValueError(f"expected '<n> <unit>', got {s!r}")
    n, unit = parts
    seconds_per_unit = _UNIT_SECONDS.get(unit.lower())
    if seconds_per_unit is None:
        raise ValueError(f"unknown interval unit: {unit!r}")
    return float(n) * seconds_per_unit


# ---------------------------------------------------------------------------
# Lakehouse Sync readiness (Phase 5 preflight, surfaced to the UI)
# ---------------------------------------------------------------------------
_REQUIRED_DATABASE = "databricks_postgres"

_CURRENT_DATABASE_SQL = "SELECT current_database()"
_WAL2DELTA_AVAILABLE_SQL = (
    "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'wal2delta')"
)
_WAL2DELTA_INSTALLED_SQL = (
    "SELECT extversion FROM pg_extension WHERE extname = 'wal2delta'"
)
_LAKETS_SYNC_PRESENT_SQL = (
    "SELECT EXISTS ("
    " SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid "
    " WHERE n.nspname = 'lakets' AND p.proname = 'enable_sync'"
    ")"
)


async def check_lakehouse_sync_readiness(pool: Any) -> dict:
    """Inspect the connected database for Lakehouse Sync prerequisites.

    Returns a dict with booleans for each gate plus a human-readable ``reason``
    when ``ready_to_enable`` is False.  Never raises — all pool errors are
    caught and surfaced as ``error``.
    """
    try:
        current_db = await pool.fetchval(_CURRENT_DATABASE_SQL)
        wal2delta_available = bool(await pool.fetchval(_WAL2DELTA_AVAILABLE_SQL))
        wal2delta_version = await pool.fetchval(_WAL2DELTA_INSTALLED_SQL)
        lakets_sync = bool(await pool.fetchval(_LAKETS_SYNC_PRESENT_SQL))
    except Exception as exc:
        logger.warning("Lakehouse Sync readiness probe failed: %s", exc)
        return {
            "database_compatible": False,
            "wal2delta_available": False,
            "wal2delta_installed": False,
            "lakets_sync_functions_present": False,
            "ready_to_enable": False,
            "error": str(exc)[:200],
        }

    database_compatible = (current_db == _REQUIRED_DATABASE)
    wal2delta_installed = wal2delta_version is not None

    reasons: list[str] = []
    if not database_compatible:
        reasons.append(
            f"Lakehouse Sync currently requires the `{_REQUIRED_DATABASE}` database "
            f"(connected to `{current_db}`)."
        )
    if not lakets_sync:
        reasons.append("LakeTS shadow-sync functions not found — install LakeTS first.")
    if not wal2delta_available:
        reasons.append("`wal2delta` extension is not available on this Lakebase instance.")
    elif not wal2delta_installed:
        reasons.append(
            "`wal2delta` extension is available but not yet installed "
            "(run `CREATE EXTENSION wal2delta`)."
        )

    return {
        "database_compatible": database_compatible,
        "current_database": current_db,
        "wal2delta_available": wal2delta_available,
        "wal2delta_installed": wal2delta_installed,
        "wal2delta_version": wal2delta_version,
        "lakets_sync_functions_present": lakets_sync,
        "ready_to_enable": not reasons,
        "reason": "  ".join(reasons) if reasons else None,
    }


# ---------------------------------------------------------------------------
# Retention runner (Phase 4.1a — scheduled job entry point)
# ---------------------------------------------------------------------------
_RETENTION_TABLES_SQL = (
    "SELECT DISTINCT c.schema_name, c.table_name "
    "FROM lakets._policy_registry p "
    "JOIN lakets._chronotable_registry c ON p.chronotable_id = c.id "
    "WHERE p.policy_type IN ('retention', 'tiered_retention') "
    "  AND p.enabled = TRUE"
)
_EXECUTE_RETENTION_SQL = "SELECT lakets.execute_retention($1, $2)"


async def run_retention(pool: Any) -> dict:
    """Run LakeTS retention for every ChronoTable that has a retention policy.

    Returns ``{f"{schema}.{table}": rows_dropped_or_error_message}``.  A failure
    on one table does not abort the rest — errors are captured per-table.
    """
    rows = await pool.fetch(_RETENTION_TABLES_SQL)
    results: dict = {}
    for row in rows:
        schema = row["schema_name"]
        table = row["table_name"]
        key = f"{schema}.{table}"
        try:
            dropped = await pool.fetchval(_EXECUTE_RETENTION_SQL, table, schema)
            results[key] = dropped
            logger.info("Retention on %s dropped %s chunk(s)", key, dropped)
        except Exception as exc:
            logger.warning("Retention on %s failed: %s", key, exc)
            results[key] = f"error: {exc}"
    return results


def _interval_to_timedelta(s: str) -> _dt.timedelta:
    """Convert a Postgres interval string to a Python timedelta.

    asyncpg encodes timedeltas as INTERVAL on the wire; raw strings fail with
    ``'str' object has no attribute 'days'``.  Month/year are approximated as
    30 / 365 days — callers wanting calendar-aware semantics should inline the
    interval literal in SQL instead.
    """
    return _dt.timedelta(seconds=_interval_seconds(s))
