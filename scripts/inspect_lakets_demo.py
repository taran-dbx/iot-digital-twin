"""Peek at iot-lakets-demo Lakebase state to debug the smoke-test run."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

PROJECT_ID = os.getenv("LAKETS_PROJECT_ID", "iot-lakets-demo")
BRANCH_ID = os.getenv("LAKETS_BRANCH_ID", "production")
DATABASE = os.getenv("PGDATABASE", "databricks_postgres")


async def main() -> int:
    from databricks.sdk import WorkspaceClient
    import asyncpg

    w = WorkspaceClient()
    parent = f"projects/{PROJECT_ID}/branches/{BRANCH_ID}"
    endpoints = list(w.postgres.list_endpoints(parent=parent))
    endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
    cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)

    conn = await asyncpg.connect(
        host=endpoint.status.hosts.host, database=DATABASE,
        user=w.current_user.me().user_name, password=cred.token,
        port=5432, ssl="require",
    )
    try:
        tables = await conn.fetch(
            "SELECT tablename, schemaname FROM pg_tables "
            "WHERE schemaname = 'public' ORDER BY tablename"
        )
        print("public.* tables:")
        for t in tables:
            print(f"  {t['tablename']}")

        matviews = await conn.fetch(
            "SELECT matviewname FROM pg_matviews WHERE schemaname = 'public'"
        )
        print(f"\npublic matviews: {[m['matviewname'] for m in matviews]}")

        registry = await conn.fetch(
            "SELECT id, schema_name, table_name, time_column, chunk_interval "
            "FROM lakets._chronotable_registry"
        )
        print(f"\nlakets._chronotable_registry:")
        for r in registry:
            print(f"  id={r['id']}  {r['schema_name']}.{r['table_name']}"
                  f"  time={r['time_column']}  chunk={r['chunk_interval']}")

        policies = await conn.fetch(
            "SELECT id, chronotable_id, policy_type, config, last_run_at "
            "FROM lakets._policy_registry"
        )
        print(f"\nlakets._policy_registry:")
        for p in policies:
            print(f"  id={p['id']}  ct_id={p['chronotable_id']}"
                  f"  type={p['policy_type']}  config={p['config']}"
                  f"  last_run={p['last_run_at']}")
        return 0
    finally:
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
