"""One-shot diagnostic for the Lakebase sensor_readings sink.

Usage::

    DATABRICKS_CONFIG_PROFILE=e2-demo-field-eng .venv/bin/python \
        -m scripts.diagnose_lakebase_sink <project_id> [<branch_id>]

Prints a readable report covering total row count, recent write volume, and
the 5 most recent rows.  Exits 0 on success; exits 1 if the
``sensor_readings`` table does not exist.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logger = logging.getLogger("diagnose_lakebase_sink")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(message)s")

DEFAULT_BRANCH = "production"
DEFAULT_DATABASE = os.getenv("PGDATABASE", "databricks_postgres")


def _print_header(title: str) -> None:
    print()
    print(title)
    print("-" * len(title))


def _print_kv(label: str, value: Any) -> None:
    print(f"  {label:<28}{value}")


async def _run(project_id: str, branch_id: str, database: str) -> int:
    try:
        from databricks.sdk import WorkspaceClient
        import asyncpg
    except ImportError as exc:  # pragma: no cover — import guard
        print(f"Missing dependency: {exc}")
        return 1

    w = WorkspaceClient()
    parent = f"projects/{project_id}/branches/{branch_id}"
    endpoints = list(w.postgres.list_endpoints(parent=parent))
    if not endpoints:
        print(f"No Lakebase endpoints found for {parent}")
        return 1

    endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
    cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)
    host = endpoint.status.hosts.host
    user = w.current_user.me().user_name

    print("=" * 72)
    print("Lakebase sensor_readings diagnostic")
    print("=" * 72)
    _print_kv("Project", project_id)
    _print_kv("Branch", branch_id)
    _print_kv("Database", database)
    _print_kv("Host", host)
    _print_kv("User", user)

    conn = await asyncpg.connect(
        host=host,
        database=database,
        user=user,
        password=cred.token,
        port=5432,
        ssl="require",
    )

    try:
        table_exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'sensor_readings'
            )
            """
        )
        if not table_exists:
            print()
            print("ERROR: public.sensor_readings does not exist in this database.")
            return 1

        total = await conn.fetchval("SELECT COUNT(*) FROM sensor_readings")
        rows_5m = await conn.fetchval(
            "SELECT COUNT(*) FROM sensor_readings "
            "WHERE event_time > NOW() - INTERVAL '5 minutes'"
        )
        rows_1h = await conn.fetchval(
            "SELECT COUNT(*) FROM sensor_readings "
            "WHERE event_time > NOW() - INTERVAL '1 hour'"
        )
        distinct_sensors_5m = await conn.fetchval(
            "SELECT COUNT(DISTINCT sensor_name) FROM sensor_readings "
            "WHERE event_time > NOW() - INTERVAL '5 minutes'"
        )

        _print_header("Row counts")
        _print_kv("Total", total)
        _print_kv("Last 5 minutes", rows_5m)
        _print_kv("Last 1 hour", rows_1h)
        _print_kv("Distinct sensors (5 min)", distinct_sensors_5m)

        recent = await conn.fetch(
            """
            SELECT event_time, sensor_name, value
            FROM sensor_readings
            ORDER BY event_time DESC
            LIMIT 5
            """
        )

        _print_header("Top 5 most recent rows")
        if not recent:
            print("  (no rows)")
        else:
            for row in recent:
                event_time = row["event_time"]
                sensor_name = row["sensor_name"]
                value = row["value"]
                ts = (
                    event_time.isoformat()
                    if hasattr(event_time, "isoformat")
                    else str(event_time)
                )
                print(f"  {ts}  {sensor_name:<30}  {value}")

        print()
        print("Diagnostic complete.")
        return 0
    finally:
        await conn.close()


def _parse_args(argv: list[str]) -> tuple[str, str, str]:
    if len(argv) < 2:
        print(
            "Usage: python -m scripts.diagnose_lakebase_sink <project_id> [<branch_id>]",
            file=sys.stderr,
        )
        sys.exit(2)
    project_id = argv[1]
    branch_id = argv[2] if len(argv) > 2 else DEFAULT_BRANCH
    database = os.getenv("PGDATABASE", DEFAULT_DATABASE)
    return project_id, branch_id, database


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(list(argv if argv is not None else sys.argv))
    return asyncio.run(_run(*args))


if __name__ == "__main__":
    sys.exit(main())
