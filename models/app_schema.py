"""App-backend data-model DDL for the ``iot-app-backend`` Lakebase project.

Implements the parent/child + normalized-ledger model from the admin pipeline
design spec §2.3 (Option B):

    pipeline_setup       — one row per industry (UNIQUE industry)
    pipeline_deployment  — one row per (setup, mode); FK setup_id CASCADE,
                           UNIQUE(setup_id, mode)
    provisioned_resource — normalized teardown ledger; FK setup_id CASCADE +
                           nullable deployment_id, ordered by teardown_order

These tables live ONLY in the new ``iot-app-backend`` project — they are
additive and do not touch the legacy ``pipelines`` / ``user_settings`` /
``source_bindings`` wiring on ``iot-lakets-demo``.

The ``ensure_*`` creators are idempotent (``CREATE TABLE IF NOT EXISTS`` +
``CREATE INDEX IF NOT EXISTS``) and accept any async DB executor exposing an
``execute(query, *args)`` coroutine — the ``LakebaseConnectionManager``, a
``PipelineLakebaseHandle``, or a raw ``asyncpg.Connection``.
"""

from __future__ import annotations

from typing import Protocol


class _Executor(Protocol):
    async def execute(self, query: str, *args): ...


# ---------------------------------------------------------------------------
# DDL constants
# ---------------------------------------------------------------------------
PIPELINE_SETUP_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_setup (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    industry TEXT NOT NULL UNIQUE,
    flavor TEXT,
    source_type TEXT,
    bootstrap_servers TEXT,
    derived JSONB DEFAULT '{}'::jsonb,
    service_credential TEXT,
    enabled_modes JSONB DEFAULT '[]'::jsonb,
    cluster_spec JSONB DEFAULT '{}'::jsonb,
    warehouse_id TEXT,
    validation_state JSONB DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'draft',
    user_email TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

PIPELINE_DEPLOYMENT_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_deployment (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    setup_id UUID NOT NULL
        REFERENCES pipeline_setup(id) ON DELETE CASCADE,
    mode TEXT NOT NULL
        CHECK (mode IN ('realtime', 'microbatch', 'zerobus')),
    status TEXT NOT NULL DEFAULT 'pending',
    partition_count INT,
    cdf_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    metrics_config JSONB DEFAULT '{}'::jsonb,
    lakebase_branch TEXT,
    lakebase_endpoints JSONB DEFAULT '{}'::jsonb,
    delta_table TEXT,
    checkpoint_path TEXT,
    job_id TEXT,
    notebook_path TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_deployment_setup_mode UNIQUE (setup_id, mode)
)
"""

PROVISIONED_RESOURCE_DDL = """
CREATE TABLE IF NOT EXISTS provisioned_resource (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    setup_id UUID NOT NULL
        REFERENCES pipeline_setup(id) ON DELETE CASCADE,
    deployment_id UUID
        REFERENCES pipeline_deployment(id) ON DELETE CASCADE,
    resource_type TEXT NOT NULL,
    identifier TEXT NOT NULL,
    parent_ref TEXT,
    teardown_order INT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

PROVISIONED_RESOURCE_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_provisioned_resource_setup "
    "ON provisioned_resource (setup_id, teardown_order DESC)",
    "CREATE INDEX IF NOT EXISTS idx_provisioned_resource_deployment "
    "ON provisioned_resource (deployment_id)",
)


# ---------------------------------------------------------------------------
# Idempotent creators
# ---------------------------------------------------------------------------
async def ensure_pipeline_setup_table(db: _Executor) -> None:
    """Create the ``pipeline_setup`` table if it does not exist."""
    await db.execute(PIPELINE_SETUP_DDL)


async def ensure_pipeline_deployment_table(db: _Executor) -> None:
    """Create the ``pipeline_deployment`` table if it does not exist."""
    await db.execute(PIPELINE_DEPLOYMENT_DDL)


async def ensure_provisioned_resource_table(db: _Executor) -> None:
    """Create the ``provisioned_resource`` ledger table + indexes."""
    await db.execute(PROVISIONED_RESOURCE_DDL)
    for index_ddl in PROVISIONED_RESOURCE_INDEXES:
        await db.execute(index_ddl)


async def ensure_app_schema(db: _Executor) -> None:
    """Create all three app-backend tables in FK-dependency order.

    Order matters: ``pipeline_deployment`` and ``provisioned_resource``
    reference ``pipeline_setup``; ``provisioned_resource`` also references
    ``pipeline_deployment``.
    """
    await ensure_pipeline_setup_table(db)
    await ensure_pipeline_deployment_table(db)
    await ensure_provisioned_resource_table(db)
