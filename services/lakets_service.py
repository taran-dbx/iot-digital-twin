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
# Per-endpoint idempotent install (used by provisioning + heal endpoints)
# ---------------------------------------------------------------------------


async def install_lakets_if_missing(
    endpoint_name: str,
    database: str = "databricks_postgres",
) -> dict:
    """Install LakeTS on the given Lakebase endpoint if not already present.

    Generates an OAuth credential for ``endpoint_name`` via the Databricks
    SDK, opens an asyncpg connection to ``database``, and checks
    ``lakets._version``.  If the toolkit is already installed, returns
    ``{"status": "already_exists", "version": "..."}``.  Otherwise reads
    the SQL from ``LAKETS_SQL_PATH`` (or ``LAKETS_GIT_URL``), executes it,
    and re-verifies the version.

    Returns a single result dict compatible with the other provisioning
    helpers:
        {"resource": "lakets extension", "status": "...", "version": "...", "error": "..."}

    Never raises — all failures are surfaced in the ``error`` field.  Safe
    to call multiple times.
    """
    resource = f"LakeTS on {endpoint_name}"
    result: dict = {"resource": resource, "status": "failed"}

    try:
        import asyncpg
        from databricks.sdk import WorkspaceClient
    except Exception as exc:
        result["error"] = f"import failure: {exc}"[:200]
        return result

    try:
        w = WorkspaceClient()
        # Host resolution with REST fallback for older SDKs.
        try:
            endpoint = w.postgres.get_endpoint(name=endpoint_name)
            pg_host = endpoint.status.hosts.host
        except AttributeError:
            resp = w.api_client.do("GET", f"/api/2.0/{endpoint_name}")
            pg_host = resp["status"]["hosts"]["host"]

        # Credential with REST fallback.
        try:
            cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
            pg_token = cred.token
        except AttributeError:
            cred_resp = w.api_client.do(
                "POST",
                "/api/2.0/postgres/credentials",
                body={"endpoint": endpoint_name},
            )
            pg_token = cred_resp["token"]

        pg_user = w.current_user.me().user_name
    except Exception as exc:
        result["error"] = f"credential resolution failed: {exc}"[:200]
        return result

    try:
        conn = await asyncpg.connect(
            host=pg_host, database=database,
            user=pg_user, password=pg_token, port=5432, ssl="require",
        )
    except Exception as exc:
        result["error"] = f"connect failed: {exc}"[:200]
        return result

    try:
        # Step 1: probe the version.  ``get_installed_version`` returns None
        # if the table doesn't exist yet.
        existing = await get_installed_version(conn)
        if existing:
            return {
                "resource": resource,
                "status": "already_exists",
                "version": existing,
            }

        # Step 2: resolve the source SQL — prefers LAKETS_SQL_PATH, then
        # LAKETS_GIT_URL.  Errors bubble up as a "failed" result.
        try:
            sql_path = resolve_lakets_source(None)
        except Exception as exc:
            return {
                "resource": resource,
                "status": "failed",
                "error": f"cannot resolve LakeTS SQL source: {exc}"[:200],
            }

        # Step 3: execute the SQL.
        try:
            sql_text = sql_path.read_text(encoding="utf-8")
            await conn.execute(sql_text)
        except Exception as exc:
            return {
                "resource": resource,
                "status": "failed",
                "error": f"SQL execution failed: {exc}"[:200],
            }

        # Step 4: re-verify.
        version = await get_installed_version(conn)
        if not version:
            return {
                "resource": resource,
                "status": "failed",
                "error": "LakeTS install completed but _version is empty",
            }
        return {
            "resource": resource,
            "status": "created",
            "version": version,
        }
    finally:
        try:
            await conn.close()
        except Exception:
            pass


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


# ---------------------------------------------------------------------------
# RollUp creation (Phase 3) — incrementally-maintained time-bucketed aggregates
# over a ChronoTable.  Wraps the vendored SQL:
#   lakets.create_rollup(p_name, p_query, p_bucket_interval,
#                        p_source_table, p_source_schema, p_depends_on)
# The toolkit takes a full SELECT in ``p_query``; this helper builds that query
# from a high-level ``aggregates`` spec so callers don't hand-roll SQL.
# ---------------------------------------------------------------------------
_ROLLUP_PROBE_SQL = "SELECT id FROM lakets._rollup_registry WHERE name = $1"
_ROLLUP_CREATE_SQL = (
    "SELECT lakets.create_rollup($1, $2, $3::interval, $4, $5)"
)


# Aggregate function names the rollup editor offers as bare strings. A bare
# name (e.g. "avg") means "apply this function to the metric column", which the
# UI does not spell out — it sends just ["avg", "min", "max"]. Without this
# mapping those land in the SELECT verbatim ("…, avg, min, max …") and Postgres
# fails with `column "avg" does not exist`.
_KNOWN_AGG_FNS = {"avg", "min", "max", "sum", "count"}


def _aggregate_to_sql(agg: Any, value_column: str = "value") -> str:
    """Map one aggregate definition to a SQL select-list fragment.

    Accepts:
      * a dict ``{"fn": "avg", "column": "value", "as": "avg_v"}`` (the ``as``
        alias is optional and defaults to ``<fn>_<column>``);
      * a bare aggregate function name from the UI (``"avg"``, ``"min"``,
        ``"max"``, ``"sum"``, ``"count"``), expanded to
        ``<fn>(<value_column>) AS <fn>_<value_column>`` (``count`` →
        ``count(*) AS count_<value_column>``);
      * any other raw SQL string, passed through verbatim.
    """
    if isinstance(agg, str):
        token = agg.strip()
        fn = token.lower()
        if fn in _KNOWN_AGG_FNS:
            if fn == "count":
                return f"count(*) AS count_{value_column}"
            return f"{fn}({value_column}) AS {fn}_{value_column}"
        # Not a recognized bare function — assume a raw SQL fragment.
        return token
    if not isinstance(agg, dict):
        raise ValueError(f"unsupported aggregate spec: {agg!r}")
    fn = agg.get("fn")
    column = agg.get("column")
    if not fn or not column:
        raise ValueError(f"aggregate spec needs 'fn' and 'column': {agg!r}")
    alias = agg.get("as") or f"{fn}_{column}"
    return f"{fn}({column}) AS {alias}"


def _build_rollup_query(
    *,
    table: str,
    schema: str,
    bucket: str,
    time_col: str,
    aggregates: Any,
    value_column: str = "value",
) -> str:
    """Compose the time-bucketed aggregation SELECT for ``create_rollup``.

    ``aggregates`` is either a list of aggregate specs (dict/bare-fn/raw-str)
    or a single raw SQL select-list string.  The first column is always the
    time bucket.
    """
    if isinstance(aggregates, str):
        agg_sql = aggregates
    else:
        agg_sql = ", ".join(_aggregate_to_sql(a, value_column) for a in aggregates)

    bucket_expr = f"lakets.time_bucket('{bucket}'::interval, {time_col}) AS bucket"
    return (
        f"SELECT {bucket_expr}, {agg_sql} "
        f"FROM {schema}.{table} "
        f"GROUP BY bucket"
    )


async def create_rollup(
    pool: Any,
    *,
    table: str,
    name: str,
    bucket: str,
    aggregates: Any,
    schema: str = "public",
    time_col: str = "event_time",
    value_column: str = "value",
) -> dict:
    """Create a LakeTS RollUp over ``schema.table``. Idempotent. Never raises.

    Args:
        table: source ChronoTable name.
        name: rollup name (also the registry key).
        bucket: Postgres interval string for the time bucket (e.g. ``"1 hour"``).
        aggregates: either a list of aggregate specs — each a dict like
            ``{"fn": "avg", "column": "value", "as": "avg_v"}`` (``as`` optional)
            — or a raw SQL select-list string (e.g. ``"avg(value) AS v"``).  This
            is mapped into the ``p_query`` SELECT that ``lakets.create_rollup``
            stores and incrementally refreshes.
        schema: source schema (default ``public``); passed as ``p_source_schema``.
        time_col: time column on the source table used for bucketing.

    Returns ``{"resource", "status"}`` where status is
    ``created`` | ``already_exists`` | ``failed`` (with ``error`` on failure).
    """
    resource = f"LakeTS rollup {name}"
    try:
        existing = await pool.fetchval(_ROLLUP_PROBE_SQL, name)
        if existing is not None:
            logger.info("RollUp %s already exists (id=%s)", name, existing)
            return {"resource": resource, "status": "already_exists"}

        query = _build_rollup_query(
            table=table, schema=schema, bucket=bucket,
            time_col=time_col, aggregates=aggregates, value_column=value_column,
        )
        rollup_id = await pool.fetchval(
            _ROLLUP_CREATE_SQL,
            name,
            query,
            _interval_to_timedelta(bucket),
            table,
            schema,
        )
        logger.info("RollUp %s created (id=%s)", name, rollup_id)
        return {"resource": resource, "status": "created"}
    except Exception as exc:
        logger.warning("create_rollup %s failed: %s", name, exc)
        return {"resource": resource, "status": "failed", "error": str(exc)[:200]}


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
# Shadow-sync enablement (enable_sync) — prepares the unpartitioned shadow in
# the lakets_cdf schema (REPLICA IDENTITY FULL) + mirror trigger, REQUIRED
# before Lakebase CDF / cold-tiering can stream.  The managed sync itself is
# still enabled once via the Databricks UI on the lakets_cdf schema; this only
# prepares the shadow.  Signature mirrors the vendored SQL:
#   lakets.enable_sync(p_table_name TEXT, p_schema_name TEXT DEFAULT 'public')
# ---------------------------------------------------------------------------
_SYNC_STATE_PROBE_SQL = (
    "SELECT id, sync_enabled FROM lakets._chronotable_registry "
    "WHERE schema_name = $2 AND table_name = $1"
)
_ENABLE_SYNC_SQL = "SELECT lakets.enable_sync($1, $2)"

# Substrings that mean the shadow / sync is already in place — treated as
# success (already_exists), never as a failure.
_ALREADY_SYNCED_HINTS = ("already", "exists", "duplicate")


async def enable_sync(pool: Any, table: str, schema: str = "public") -> dict:
    """Prepare the LakeTS shadow table + mirror trigger for ``schema.table``.

    Runs ``SELECT lakets.enable_sync(table, schema)``, which creates the
    unpartitioned shadow in the ``lakets_cdf`` schema (REPLICA IDENTITY FULL)
    and the mirror trigger on the source.  Idempotent: if the ChronoTable's
    registry row already has ``sync_enabled = TRUE`` the call is skipped, and
    any "already exists" race from the SQL is swallowed.

    Never raises — all outcomes are returned as a result dict:
        {"resource": "...", "status": "created|already_exists|failed", "error": ...}
    """
    resource = f"LakeTS sync on {schema}.{table}"

    # Idempotency fast-path: if the registry already records sync_enabled,
    # there's nothing to do.
    try:
        row = await pool.fetchrow(_SYNC_STATE_PROBE_SQL, table, schema)
    except Exception as exc:
        logger.debug("enable_sync registry probe failed for %s.%s: %s", schema, table, exc)
        row = None

    if row is not None and row.get("sync_enabled"):
        logger.info("LakeTS sync on %s.%s already enabled", schema, table)
        return {"resource": resource, "status": "already_exists"}

    try:
        await pool.fetchval(_ENABLE_SYNC_SQL, table, schema)
    except Exception as exc:
        msg = str(exc)
        if any(hint in msg.lower() for hint in _ALREADY_SYNCED_HINTS):
            logger.info("LakeTS sync on %s.%s already present (%s)", schema, table, msg[:80])
            return {"resource": resource, "status": "already_exists"}
        logger.warning("LakeTS enable_sync on %s.%s failed: %s", schema, table, exc)
        return {"resource": resource, "status": "failed", "error": msg[:200]}

    logger.info("LakeTS sync enabled on %s.%s", schema, table)
    return {"resource": resource, "status": "created"}


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

    # Lakehouse Sync is a Databricks-managed feature — the wal2delta extension
    # is wired up by the platform, not by the user.  Keep the probe for info
    # only; do NOT gate readiness on it.
    # See https://docs.databricks.com/aws/en/oltp/projects/lakehouse-sync.
    reasons: list[str] = []
    if not database_compatible:
        reasons.append(
            f"Lakehouse Sync currently requires the `{_REQUIRED_DATABASE}` database "
            f"(connected to `{current_db}`)."
        )
    if not lakets_sync:
        reasons.append("LakeTS shadow-sync functions not found — install LakeTS first.")

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
