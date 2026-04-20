"""Smoke-test LakeTS install against a real Lakebase instance.

Runs the same code path as the `/api/settings/provision` endpoint
(`provision_lakebase_table` → `install_lakets`) and prints the outcome.

Usage — autoscale (SDK OAuth):
    LAKEBASE_PROJECT_ID=my-project \\
    LAKEBASE_BRANCH_ID=production \\
    LAKETS_SQL_PATH=/path/to/dist/lakets.sql \\
    .venv/bin/python -m scripts.smoke_lakets_install

Usage — direct (plain PG creds):
    LAKEBASE_MODE=direct \\
    PGHOST=... PGUSER=... PGPASSWORD=... \\
    LAKETS_SQL_PATH=/path/to/dist/lakets.sql \\
    .venv/bin/python -m scripts.smoke_lakets_install

Requires DATABRICKS_HOST + DATABRICKS_TOKEN (or DATABRICKS_CONFIG_PROFILE)
for the autoscale path — same as the app.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

# Ensure project root is importable when run as `python scripts/...`
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from services.provisioning_service import provision_lakebase_table

DEFAULT_LAKETS_SQL = (
    "/Users/taranpreetsingh.grover/Documents/Claude Projects/"
    "Lakebase/timeseries/dist/lakets.sql"
)


async def main() -> int:
    mode = os.getenv("LAKEBASE_MODE", "autoscale")
    lb = {
        "mode": mode,
        "project_id": os.getenv("LAKEBASE_PROJECT_ID", ""),
        "branch_id": os.getenv("LAKEBASE_BRANCH_ID", "production"),
        "host": os.getenv("PGHOST", ""),
        "user": os.getenv("PGUSER", ""),
        "password": os.getenv("PGPASSWORD", ""),
        "port": int(os.getenv("PGPORT", "5432")),
        "database": os.getenv("PGDATABASE", "databricks_postgres"),
        "pg_table": os.getenv("PG_TABLE", "sensor_readings"),
        "lakets_sql_path": os.getenv("LAKETS_SQL_PATH", DEFAULT_LAKETS_SQL),
    }

    print(f"→ provision_lakebase_table (mode={mode}, db={lb['database']}, table={lb['pg_table']})")
    print(f"  LakeTS source: {lb['lakets_sql_path']}")
    print()

    result = await provision_lakebase_table(lb)

    print("Result:")
    for key, value in result.items():
        print(f"  {key}: {value}")
    print()

    if result.get("status") == "failed":
        print(f"❌ Base table provisioning failed: {result.get('error')}")
        return 1

    if "lakets_version" in result:
        print(f"✓ LakeTS {result['lakets_version']} installed on {lb['database']}")
        return 0

    if "lakets_error" in result:
        print(f"❌ LakeTS install warning: {result['lakets_error']}")
        return 1

    print("⚠ No LakeTS outcome reported — is install_lakets flag on?")
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
