"""Smoke-test check_lakehouse_sync_readiness against iot-lakets-demo."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from routes.lakets import _open_lakebase_conn
from services import lakets_service


async def main() -> int:
    lb = {
        "mode": "autoscale",
        "project_id": os.getenv("LAKETS_PROJECT_ID", "iot-lakets-demo"),
        "branch_id": os.getenv("LAKETS_BRANCH_ID", "production"),
        "database": os.getenv("PGDATABASE", "databricks_postgres"),
    }
    conn = await _open_lakebase_conn(lb)
    try:
        result = await lakets_service.check_lakehouse_sync_readiness(conn)
    finally:
        await conn.close()
    print(json.dumps(result, indent=2, default=str))
    return 0 if result["ready_to_enable"] else 2


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
