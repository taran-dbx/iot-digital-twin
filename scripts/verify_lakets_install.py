"""Connect to the smoke-test Lakebase and verify LakeTS is live."""

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
        host=endpoint.status.hosts.host,
        database=DATABASE,
        user=w.current_user.me().user_name,
        password=cred.token,
        port=5432,
        ssl="require",
    )
    try:
        version = await conn.fetchval(
            "SELECT version FROM lakets._version ORDER BY installed_at DESC LIMIT 1"
        )
        print(f"lakets._version latest: {version}")

        function_count = await conn.fetchval(
            "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid "
            "WHERE n.nspname = 'lakets'"
        )
        print(f"lakets.* functions: {function_count}")

        tables = await conn.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'lakets' ORDER BY tablename"
        )
        print(f"lakets.* metadata tables: {[t['tablename'] for t in tables]}")

        sensor_readings_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_name = 'sensor_readings')"
        )
        print(f"sensor_readings table present: {sensor_readings_exists}")
        return 0
    finally:
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
