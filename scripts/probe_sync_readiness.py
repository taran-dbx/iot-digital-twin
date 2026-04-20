"""Probe iot-lakets-demo for Lakehouse Sync prerequisites."""

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
        db = await conn.fetchval("SELECT current_database()")
        print(f"current_database(): {db}")

        avail = await conn.fetch(
            "SELECT name, default_version, installed_version, comment "
            "FROM pg_available_extensions "
            "WHERE name IN ('wal2delta', 'wal2json', 'pgoutput', 'pglogical') "
            "ORDER BY name"
        )
        print("\nReplication extensions:")
        for r in avail:
            installed = r["installed_version"] or "—"
            print(f"  {r['name']:12s}  default={r['default_version']}  "
                  f"installed={installed}  ({(r['comment'] or '')[:60]})")

        # Check if LakeTS shadow-sync module is present
        shadow_funcs = await conn.fetch(
            "SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid "
            "WHERE n.nspname = 'lakets' AND (p.proname LIKE '%sync%' OR p.proname LIKE '%shadow%') "
            "ORDER BY p.proname"
        )
        print(f"\nLakeTS sync functions: {[f['proname'] for f in shadow_funcs]}")

        # Replication slots (if any) — indicates active CDC
        try:
            slots = await conn.fetch(
                "SELECT slot_name, plugin, database FROM pg_replication_slots"
            )
            print("\n" f"pg_replication_slots: {len(slots)} slot(s)")
            for s in slots:
                print(f"  {s['slot_name']}  plugin={s['plugin']}  db={s['database']}")
        except Exception as e:
            print(f"\n(pg_replication_slots not accessible: {e})")
        return 0
    finally:
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
