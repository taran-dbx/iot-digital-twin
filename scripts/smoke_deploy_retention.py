"""Smoke-deploy the retention job against iot-lakets-demo on E2DEMO."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from services.lakets_deploy_service import deploy_retention_job


def main() -> int:
    result = deploy_retention_job(
        project_id=os.getenv("LAKETS_PROJECT_ID", "iot-lakets-demo"),
        branch_id=os.getenv("LAKETS_BRANCH_ID", "production"),
        database=os.getenv("PGDATABASE", "databricks_postgres"),
    )
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
