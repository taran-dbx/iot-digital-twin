# Phase 1: Stateful Streaming + Source Registry — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a source registry to prevent pipeline source conflicts, and enhance the SSS notebook with sliding windows, session windows, and stateful per-sensor operations — all driven by a JSON config from the pipeline wizard.

**Architecture:** Two parallel tracks. Track A adds a `source_bindings` table in Lakebase with a partial unique index enforcing one-pipeline-per-source, integrated into pipeline CRUD. Track B enhances `sss_rtm_job.py` with 3 new streaming queries (sliding windows, session windows, mapGroupsWithState) parameterized by a `streaming_config` JSON. The frontend gets a hybrid visual builder / JSON code editor for composing streaming configs.

**Tech Stack:** Python 3.12, FastAPI, asyncpg, PySpark Structured Streaming, React 18 + TypeScript, Tailwind CSS, Radix UI

**Spec:** `docs/superpowers/specs/2026-04-05-iot-twin-improvements-design.md` — Phase 1

---

## File Structure

### New Files

| File | Responsibility |
|------|---------------|
| `services/source_registry.py` | Source key derivation, claim/release/check logic |
| `tests/test_source_registry.py` | Unit tests for source registry |
| `tests/test_routes_pipelines_source.py` | Integration tests for source validation in pipeline CRUD |
| `client/src/lib/streaming-config.ts` | StreamingConfig types + validation helpers |
| `client/src/components/streaming-ops-builder.tsx` | Hybrid visual builder / JSON editor component |
| `client/src/components/source-availability.tsx` | Real-time source availability indicator |

### Modified Files

| File | Changes |
|------|---------|
| `models/sql_ddl.py` | Add DDL for `source_bindings`, `sliding_aggregates`, `session_events`, `state_snapshots` |
| `services/pipeline_service.py` | Import source registry; add streaming_config helpers |
| `routes/pipelines.py` | Call source registry on create/delete; accept streaming_config |
| `routes/websocket.py` | Add `/api/sss/alerts` ingest endpoint |
| `notebooks/sss_rtm_job.py` | Add Queries 3-5, config parsing, ForeachWriters for new tables |
| `client/src/lib/types.ts` | Add StreamingConfig, AlertEvent, SessionEvent types |
| `client/src/lib/api.ts` | Add `checkSourceAvailability()`, `getSessionEvents()`, `getSlidingAggregates()` |

---

## Track A: Source Registry

### Task 1: Source Bindings DDL

**Files:**
- Modify: `models/sql_ddl.py:48-55`
- Test: `tests/test_models.py`

- [ ] **Step 1: Add source_bindings DDL constant**

Add after `MATVIEW_INDEX_DDL` in `models/sql_ddl.py`:

```python
SOURCE_BINDINGS_DDL = """
CREATE TABLE IF NOT EXISTS source_bindings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id UUID NOT NULL,
    source_type TEXT NOT NULL,
    source_key TEXT NOT NULL,
    partition_count INT,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_by TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    CONSTRAINT fk_pipeline FOREIGN KEY (pipeline_id)
        REFERENCES pipelines(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_active_source
    ON source_bindings (source_type, source_key)
    WHERE status = 'active';
"""
```

- [ ] **Step 2: Verify DDL parses correctly**

Run: `python -c "from models.sql_ddl import SOURCE_BINDINGS_DDL; print('OK:', len(SOURCE_BINDINGS_DDL), 'chars')"`
Expected: `OK: <N> chars`

- [ ] **Step 3: Commit**

```bash
git add models/sql_ddl.py
git commit -m "feat: add source_bindings DDL with partial unique index"
```

---

### Task 2: Source Registry Service — Key Derivation

**Files:**
- Create: `services/source_registry.py`
- Create: `tests/test_source_registry.py`

- [ ] **Step 1: Write failing tests for source key derivation**

Create `tests/test_source_registry.py`:

```python
import pytest
from services.source_registry import derive_source_key


class TestDeriveSourceKey:
    def test_kafka_key_sorts_bootstrap_servers(self):
        config = {
            "bootstrap_servers": "broker-b:9092,broker-a:9092",
            "topic": "sensor-data",
        }
        key = derive_source_key("kafka", config)
        assert key == "broker-a:9092,broker-b:9092::sensor-data"

    def test_kafka_key_lowercased(self):
        config = {
            "bootstrap_servers": "Broker-A:9092",
            "topic": "Sensor-Data",
        }
        key = derive_source_key("kafka", config)
        assert key == "broker-a:9092::sensor-data"

    def test_kinesis_key(self):
        config = {"region": "us-east-1", "stream_name": "iot-stream"}
        key = derive_source_key("kinesis", config)
        assert key == "us-east-1::iot-stream"

    def test_eventhub_key(self):
        config = {
            "namespace": "my-ns",
            "eventhub_name": "my-hub",
            "consumer_group": "$Default",
        }
        key = derive_source_key("eventhub", config)
        assert key == "my-ns::my-hub::$default"

    def test_rabbitmq_key(self):
        config = {"host": "rabbit.local", "port": 5672, "queue_name": "sensors"}
        key = derive_source_key("rabbitmq", config)
        assert key == "rabbit.local:5672::sensors"

    def test_pubsub_key(self):
        config = {"project_id": "my-proj", "subscription_id": "my-sub"}
        key = derive_source_key("pubsub", config)
        assert key == "my-proj::my-sub"

    def test_zerobus_key(self):
        config = {
            "catalog": "iot_catalog",
            "schema": "raw",
            "table_name": "sensor_data",
        }
        key = derive_source_key("zerobus", config)
        assert key == "iot_catalog::raw::sensor_data"

    def test_unknown_source_type_raises(self):
        with pytest.raises(ValueError, match="Unknown source type"):
            derive_source_key("mqtt", {"host": "x"})

    def test_missing_required_field_raises(self):
        with pytest.raises(KeyError):
            derive_source_key("kafka", {"bootstrap_servers": "x"})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/taranpreetsingh.grover/Documents/Claude\ Projects/Apps/iot-digital-twin && python -m pytest tests/test_source_registry.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'services.source_registry'`

- [ ] **Step 3: Implement derive_source_key**

Create `services/source_registry.py`:

```python
"""Source Registry — prevents pipeline source config conflicts.

Maintains a binding ledger in Lakebase. Each streaming source (topic, stream,
queue, etc.) can be claimed by exactly one active pipeline.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Key derivation
# ---------------------------------------------------------------------------

_KEY_EXTRACTORS: dict[str, callable] = {}


def _extractor(source_type: str):
    """Decorator to register a key extractor for a source type."""
    def decorator(fn):
        _KEY_EXTRACTORS[source_type] = fn
        return fn
    return decorator


@_extractor("kafka")
def _kafka_key(config: dict[str, Any]) -> str:
    servers = config["bootstrap_servers"]
    sorted_servers = ",".join(sorted(servers.split(",")))
    topic = config["topic"]
    return f"{sorted_servers}::{topic}"


@_extractor("kinesis")
def _kinesis_key(config: dict[str, Any]) -> str:
    return f"{config['region']}::{config['stream_name']}"


@_extractor("eventhub")
def _eventhub_key(config: dict[str, Any]) -> str:
    return f"{config['namespace']}::{config['eventhub_name']}::{config['consumer_group']}"


@_extractor("rabbitmq")
def _rabbitmq_key(config: dict[str, Any]) -> str:
    return f"{config['host']}:{config['port']}::{config['queue_name']}"


@_extractor("pubsub")
def _pubsub_key(config: dict[str, Any]) -> str:
    return f"{config['project_id']}::{config['subscription_id']}"


@_extractor("zerobus")
def _zerobus_key(config: dict[str, Any]) -> str:
    return f"{config['catalog']}::{config['schema']}::{config['table_name']}"


def derive_source_key(source_type: str, source_config: dict[str, Any]) -> str:
    """Derive a unique, normalized key for a streaming source.

    Keys are lowercased and trimmed. Kafka bootstrap servers are sorted.

    Raises:
        ValueError: If source_type is not recognized.
        KeyError: If required config fields are missing.
    """
    extractor = _KEY_EXTRACTORS.get(source_type)
    if extractor is None:
        raise ValueError(f"Unknown source type: {source_type}")
    raw_key = extractor(source_config)
    return raw_key.strip().lower()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_source_registry.py -v`
Expected: All 9 tests PASS

- [ ] **Step 5: Commit**

```bash
git add services/source_registry.py tests/test_source_registry.py
git commit -m "feat: add source key derivation for all 6 streaming sources"
```

---

### Task 3: Source Registry Service — Claim/Release/Check

**Files:**
- Modify: `services/source_registry.py`
- Modify: `tests/test_source_registry.py`

- [ ] **Step 1: Write failing tests for claim, release, and check**

Append to `tests/test_source_registry.py`:

```python
from unittest.mock import AsyncMock, MagicMock
from services.source_registry import claim_source, release_source, check_source_available


class TestClaimSource:
    @pytest.mark.asyncio
    async def test_claim_inserts_binding(self):
        db = AsyncMock()
        db.fetchrow.return_value = None  # no existing binding
        db.execute.return_value = None

        result = await claim_source(
            db=db,
            pipeline_id="pipe-123",
            source_type="kafka",
            source_config={"bootstrap_servers": "b:9092", "topic": "t1"},
            user_email="user@test.com",
        )

        assert result["claimed"] is True
        assert db.execute.called
        call_sql = db.execute.call_args[0][0]
        assert "INSERT INTO source_bindings" in call_sql

    @pytest.mark.asyncio
    async def test_claim_rejects_when_already_claimed(self):
        db = AsyncMock()
        db.fetchrow.return_value = {
            "pipeline_id": "other-pipe",
            "claimed_by": "other@test.com",
        }

        result = await claim_source(
            db=db,
            pipeline_id="pipe-123",
            source_type="kafka",
            source_config={"bootstrap_servers": "b:9092", "topic": "t1"},
            user_email="user@test.com",
        )

        assert result["claimed"] is False
        assert "other-pipe" in result["claimed_by_pipeline"]
        assert not db.execute.called


class TestReleaseSource:
    @pytest.mark.asyncio
    async def test_release_sets_status_released(self):
        db = AsyncMock()
        db.execute.return_value = None

        await release_source(db=db, pipeline_id="pipe-123")

        call_sql = db.execute.call_args[0][0]
        assert "status = 'released'" in call_sql
        assert "pipe-123" in str(db.execute.call_args[0][1])


class TestCheckSourceAvailable:
    @pytest.mark.asyncio
    async def test_available_when_no_binding(self):
        db = AsyncMock()
        db.fetchrow.return_value = None

        result = await check_source_available(
            db=db, source_type="kafka",
            source_config={"bootstrap_servers": "b:9092", "topic": "t1"},
        )

        assert result["available"] is True

    @pytest.mark.asyncio
    async def test_unavailable_when_binding_exists(self):
        db = AsyncMock()
        db.fetchrow.return_value = {
            "pipeline_id": "other-pipe",
            "claimed_by": "other@test.com",
        }

        result = await check_source_available(
            db=db, source_type="kafka",
            source_config={"bootstrap_servers": "b:9092", "topic": "t1"},
        )

        assert result["available"] is False
        assert result["claimed_by_pipeline"] == "other-pipe"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_source_registry.py::TestClaimSource -v`
Expected: FAIL — `ImportError: cannot import name 'claim_source'`

- [ ] **Step 3: Implement claim, release, and check**

Append to `services/source_registry.py`:

```python
# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

async def check_source_available(
    db,
    source_type: str,
    source_config: dict[str, Any],
) -> dict[str, Any]:
    """Check if a source is available (not claimed by another active pipeline).

    Returns:
        {"available": True} or
        {"available": False, "claimed_by_pipeline": str, "claimed_by": str}
    """
    source_key = derive_source_key(source_type, source_config)
    row = await db.fetchrow(
        """
        SELECT pipeline_id, claimed_by
        FROM source_bindings
        WHERE source_type = $1 AND source_key = $2 AND status = 'active'
        """,
        source_type,
        source_key,
    )
    if row is None:
        return {"available": True}
    return {
        "available": False,
        "claimed_by_pipeline": str(row["pipeline_id"]),
        "claimed_by": row["claimed_by"],
    }


async def claim_source(
    db,
    pipeline_id: str,
    source_type: str,
    source_config: dict[str, Any],
    user_email: str,
    partition_count: int | None = None,
) -> dict[str, Any]:
    """Claim a source for a pipeline. Returns {"claimed": True} on success.

    If already claimed, returns {"claimed": False, "claimed_by_pipeline": str}.
    The partial unique index on source_bindings prevents race conditions.
    """
    source_key = derive_source_key(source_type, source_config)

    # Check if already claimed
    existing = await db.fetchrow(
        """
        SELECT pipeline_id, claimed_by
        FROM source_bindings
        WHERE source_type = $1 AND source_key = $2 AND status = 'active'
        """,
        source_type,
        source_key,
    )
    if existing is not None:
        return {
            "claimed": False,
            "claimed_by_pipeline": str(existing["pipeline_id"]),
            "claimed_by": existing["claimed_by"],
        }

    await db.execute(
        """
        INSERT INTO source_bindings
            (pipeline_id, source_type, source_key, partition_count, claimed_by)
        VALUES ($1, $2, $3, $4, $5)
        """,
        pipeline_id,
        source_type,
        source_key,
        partition_count,
        user_email,
    )
    logger.info(
        "Source claimed: type=%s key=%s pipeline=%s",
        source_type, source_key, pipeline_id,
    )
    return {"claimed": True}


async def release_source(db, pipeline_id: str) -> None:
    """Release all source bindings for a pipeline (soft delete)."""
    await db.execute(
        """
        UPDATE source_bindings
        SET status = 'released'
        WHERE pipeline_id = $1 AND status = 'active'
        """,
        pipeline_id,
    )
    logger.info("Source bindings released for pipeline=%s", pipeline_id)
```

- [ ] **Step 4: Run all source registry tests**

Run: `python -m pytest tests/test_source_registry.py -v`
Expected: All 14 tests PASS

- [ ] **Step 5: Commit**

```bash
git add services/source_registry.py tests/test_source_registry.py
git commit -m "feat: add claim/release/check operations to source registry"
```

---

### Task 4: Integrate Source Registry into Pipeline Routes

**Files:**
- Modify: `routes/pipelines.py:37-71` (create endpoint), `routes/pipelines.py:146-229` (delete endpoint)
- Create: `tests/test_routes_pipelines_source.py`

- [ ] **Step 1: Write failing integration test for source validation on create**

Create `tests/test_routes_pipelines_source.py`:

```python
import pytest
from unittest.mock import AsyncMock, patch, MagicMock


@pytest.mark.asyncio
async def test_create_pipeline_rejects_claimed_source(test_client):
    """POST /api/pipelines should reject if source is already claimed."""
    with patch("routes.pipelines.check_source_available") as mock_check:
        mock_check.return_value = {
            "available": False,
            "claimed_by_pipeline": "other-pipe-id",
            "claimed_by": "other@test.com",
        }

        response = await test_client.post(
            "/api/pipelines",
            json={
                "name": "test-pipeline",
                "source_type": "kafka",
                "source_config": {
                    "bootstrap_servers": "b:9092",
                    "topic": "claimed-topic",
                },
            },
        )

        assert response.status_code == 409
        body = response.json()
        assert "already claimed" in body["detail"].lower()


@pytest.mark.asyncio
async def test_create_pipeline_claims_source_on_success(test_client):
    """POST /api/pipelines should claim source when available."""
    with patch("routes.pipelines.check_source_available") as mock_check, \
         patch("routes.pipelines.claim_source") as mock_claim:
        mock_check.return_value = {"available": True}
        mock_claim.return_value = {"claimed": True}

        response = await test_client.post(
            "/api/pipelines",
            json={
                "name": "test-pipeline",
                "source_type": "kafka",
                "source_config": {
                    "bootstrap_servers": "b:9092",
                    "topic": "free-topic",
                },
            },
        )

        # Pipeline creation may fail on DB mock, but claim should have been called
        assert mock_claim.called
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_routes_pipelines_source.py -v`
Expected: FAIL — `check_source_available` not imported in routes.pipelines

- [ ] **Step 3: Add source validation to pipeline create endpoint**

In `routes/pipelines.py`, add imports at the top:

```python
from services.source_registry import check_source_available, claim_source, release_source
```

In the `POST /api/pipelines` handler (around line 37), add source validation after the name dedup check but before the INSERT:

```python
    # Source conflict check (if source_config provided)
    source_type = body.get("source_type")
    source_config = body.get("source_config")
    if source_type and source_config:
        availability = await check_source_available(
            db=state.db, source_type=source_type, source_config=source_config,
        )
        if not availability["available"]:
            return JSONResponse(
                status_code=409,
                content={
                    "detail": f"Source already claimed by pipeline {availability['claimed_by_pipeline']}",
                    "claimed_by": availability.get("claimed_by"),
                },
            )
```

After the pipeline INSERT succeeds, claim the source:

```python
    # Claim source binding
    if source_type and source_config:
        await claim_source(
            db=state.db,
            pipeline_id=str(pipeline_id),
            source_type=source_type,
            source_config=source_config,
            user_email=user_email,
        )
```

- [ ] **Step 4: Add source release to pipeline delete endpoint**

In the `DELETE /api/pipelines/{pipeline_id}` handler (around line 146), add before the DELETE query:

```python
    # Release source bindings
    await release_source(db=state.db, pipeline_id=pipeline_id)
```

- [ ] **Step 5: Run integration tests**

Run: `python -m pytest tests/test_routes_pipelines_source.py -v`
Expected: PASS (or adjust test fixtures to match route structure)

- [ ] **Step 6: Commit**

```bash
git add routes/pipelines.py tests/test_routes_pipelines_source.py
git commit -m "feat: integrate source registry into pipeline create/delete"
```

---

### Task 5: Source Availability API Endpoint

**Files:**
- Modify: `routes/pipelines.py`
- Modify: `client/src/lib/api.ts`
- Modify: `client/src/lib/types.ts`

- [ ] **Step 1: Add check-source endpoint**

In `routes/pipelines.py`, add a new endpoint:

```python
@router.post("/api/pipelines/check-source")
async def check_source(request: Request):
    """Check if a streaming source is available for a new pipeline."""
    state: AppState = request.app.state.app_state
    body = await request.json()

    source_type = body.get("source_type")
    source_config = body.get("source_config")
    if not source_type or not source_config:
        return JSONResponse(status_code=400, content={"detail": "source_type and source_config required"})

    try:
        result = await check_source_available(
            db=state.db, source_type=source_type, source_config=source_config,
        )
        return JSONResponse(content=result)
    except (ValueError, KeyError) as e:
        return JSONResponse(status_code=400, content={"detail": str(e)})
```

- [ ] **Step 2: Add TypeScript types**

In `client/src/lib/types.ts`, add:

```typescript
export interface SourceAvailabilityResult {
  available: boolean;
  claimed_by_pipeline?: string;
  claimed_by?: string;
}
```

- [ ] **Step 3: Add API call**

In `client/src/lib/api.ts`, add to the `api` object:

```typescript
  checkSourceAvailability: (sourceType: string, sourceConfig: Record<string, unknown>): Promise<SourceAvailabilityResult> =>
    request('/api/pipelines/check-source', {
      method: 'POST',
      body: JSON.stringify({ source_type: sourceType, source_config: sourceConfig }),
    }),
```

- [ ] **Step 4: Commit**

```bash
git add routes/pipelines.py client/src/lib/types.ts client/src/lib/api.ts
git commit -m "feat: add source availability check endpoint + client API"
```

---

## Track B: Stateful Streaming

### Task 6: New Table DDLs for Streaming Operations

**Files:**
- Modify: `models/sql_ddl.py`

- [ ] **Step 1: Add sliding_aggregates, session_events, state_snapshots DDLs**

Append to `models/sql_ddl.py`:

```python
SLIDING_AGGREGATES_DDL = """
CREATE TABLE IF NOT EXISTS sliding_aggregates (
    pipeline_id UUID NOT NULL,
    sensor_id TEXT NOT NULL,
    sensor_type TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    slide_start TIMESTAMPTZ NOT NULL,
    avg_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    count INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sliding_agg_pipeline_time
    ON sliding_aggregates (pipeline_id, window_start DESC);
"""

SESSION_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS session_events (
    pipeline_id UUID NOT NULL,
    sensor_id TEXT NOT NULL,
    sensor_type TEXT NOT NULL,
    session_id TEXT NOT NULL,
    session_start TIMESTAMPTZ NOT NULL,
    session_end TIMESTAMPTZ NOT NULL,
    duration_sec DOUBLE PRECISION,
    reading_count INT,
    avg_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_session_events_pipeline_time
    ON session_events (pipeline_id, session_start DESC);
"""

STATE_SNAPSHOTS_DDL = """
CREATE TABLE IF NOT EXISTS state_snapshots (
    pipeline_id UUID NOT NULL,
    sensor_id TEXT NOT NULL,
    sensor_type TEXT NOT NULL,
    running_avg DOUBLE PRECISION,
    running_min DOUBLE PRECISION,
    running_max DOUBLE PRECISION,
    readings_since_reset INT DEFAULT 0,
    last_value DOUBLE PRECISION,
    prev_value DOUBLE PRECISION,
    threshold_breached BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_id, sensor_id)
);
"""
```

- [ ] **Step 2: Verify DDLs parse**

Run: `python -c "from models.sql_ddl import SLIDING_AGGREGATES_DDL, SESSION_EVENTS_DDL, STATE_SNAPSHOTS_DDL; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add models/sql_ddl.py
git commit -m "feat: add DDLs for sliding_aggregates, session_events, state_snapshots"
```

---

### Task 7: Streaming Config Types (Frontend)

**Files:**
- Create: `client/src/lib/streaming-config.ts`

- [ ] **Step 1: Create streaming config types and validation**

Create `client/src/lib/streaming-config.ts`:

```typescript
// Streaming configuration types for the State Operations Builder.
// These types map 1:1 to the JSON parameter passed to sss_rtm_job.py.

export interface TumblingWindowConfig {
  type: "tumbling";
  duration: string; // e.g. "30s", "5m"
}

export interface SlidingWindowConfig {
  type: "sliding";
  duration: string; // e.g. "5m"
  slide: string;    // e.g. "1m"
}

export type WindowConfig = TumblingWindowConfig | SlidingWindowConfig;

export interface SessionConfig {
  enabled: boolean;
  gap: string;     // e.g. "2m"
  timeout: string; // e.g. "30m"
}

export interface ThresholdRule {
  min: number;
  max: number;
}

export interface StateOpsConfig {
  running_avg: boolean;
  min_max_tracking: boolean;
  change_detection: boolean;
  threshold_alerts: Record<string, ThresholdRule>;
}

export interface StreamingConfig {
  windows: WindowConfig[];
  session: SessionConfig;
  state_ops: StateOpsConfig;
}

export const DEFAULT_STREAMING_CONFIG: StreamingConfig = {
  windows: [
    { type: "tumbling", duration: "30s" },
  ],
  session: {
    enabled: false,
    gap: "2m",
    timeout: "30m",
  },
  state_ops: {
    running_avg: false,
    min_max_tracking: false,
    change_detection: false,
    threshold_alerts: {},
  },
};

/** Validate a streaming config object. Returns error messages or empty array. */
export function validateStreamingConfig(config: StreamingConfig): string[] {
  const errors: string[] = [];

  if (!config.windows || config.windows.length === 0) {
    errors.push("At least one window configuration is required");
  }

  for (const w of config.windows) {
    if (!w.duration || !/^\d+[smh]$/.test(w.duration)) {
      errors.push(`Invalid window duration: "${w.duration}". Use format like "30s", "5m", "1h"`);
    }
    if (w.type === "sliding" && (!w.slide || !/^\d+[smh]$/.test(w.slide))) {
      errors.push(`Sliding window requires a valid slide interval: "${w.slide}"`);
    }
  }

  if (config.session.enabled) {
    if (!/^\d+[smh]$/.test(config.session.gap)) {
      errors.push(`Invalid session gap: "${config.session.gap}"`);
    }
    if (!/^\d+[smh]$/.test(config.session.timeout)) {
      errors.push(`Invalid session timeout: "${config.session.timeout}"`);
    }
  }

  for (const [sensor, rule] of Object.entries(config.state_ops.threshold_alerts)) {
    if (rule.min >= rule.max) {
      errors.push(`Threshold for "${sensor}": min (${rule.min}) must be less than max (${rule.max})`);
    }
  }

  return errors;
}
```

- [ ] **Step 2: Commit**

```bash
git add client/src/lib/streaming-config.ts
git commit -m "feat: add StreamingConfig types and validation for state ops builder"
```

---

### Task 8: SSS Notebook — Config Parsing + Sliding Windows (Query 3)

**Files:**
- Modify: `notebooks/sss_rtm_job.py`

- [ ] **Step 1: Add config parsing at the top of the notebook**

After the existing parameter parsing (around line 30), add streaming config parsing:

```python
# Streaming configuration (JSON parameter from pipeline wizard)
import json as _json

streaming_config_raw = dbutils.widgets.get("streaming_config") if "streaming_config" in [w.name for w in dbutils.widgets.getAll()] else "{}"
streaming_config = _json.loads(streaming_config_raw) if streaming_config_raw else {}

# Extract config sections
window_configs = streaming_config.get("windows", [{"type": "tumbling", "duration": "30s"}])
session_config = streaming_config.get("session", {"enabled": False})
state_ops_config = streaming_config.get("state_ops", {})
```

- [ ] **Step 2: Add SlidingAggregateForeachWriter class**

Add after the existing `AppForeachWriter` class (around line 292):

```python
class SlidingAggregateForeachWriter(ForeachWriter):
    """Writes sliding window aggregates to Lakebase."""

    MAX_BUFFER_SIZE = 20

    def open(self, partition_id, epoch_id):
        self.buffer = []
        self.conn = None
        return True

    def process(self, row):
        self.buffer.append(row)
        if len(self.buffer) >= self.MAX_BUFFER_SIZE:
            self._flush()

    def close(self, error):
        if self.buffer:
            self._flush()
        if self.conn:
            self.conn.close()

    def _flush(self):
        if not self.buffer:
            return
        try:
            if not self.conn:
                self.conn = psycopg2.connect(
                    host=pg_host_bc.value, port=pg_port_bc.value,
                    dbname=pg_database_bc.value,
                    user=pg_user_bc.value, password=pg_password_bc.value,
                    sslmode="require",
                )
            with self.conn.cursor() as cur:
                values = [
                    (
                        pipeline_id_bc.value,
                        r["sensor_id"], r["sensor_type"],
                        r["window_start"], r["window_end"], r["slide_start"],
                        r["avg_value"], r["min_value"], r["max_value"],
                        r["count"],
                    )
                    for r in self.buffer
                ]
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO sliding_aggregates
                       (pipeline_id, sensor_id, sensor_type,
                        window_start, window_end, slide_start,
                        avg_value, min_value, max_value, count)
                       VALUES %s""",
                    values,
                )
                self.conn.commit()
        except Exception as e:
            print(f"SlidingAggregateForeachWriter flush error: {e}")
        finally:
            self.buffer = []
```

- [ ] **Step 3: Add Query 3 — Sliding windows**

After existing Query 2 (around line 387), add:

```python
# Query 3: Sliding window aggregates (if configured)
sliding_queries = []
for wc in window_configs:
    if wc.get("type") == "sliding":
        duration = wc["duration"]
        slide = wc["slide"]
        print(f"Starting sliding window query: duration={duration}, slide={slide}")

        sliding_agg = (
            stream_df
            .withWatermark("timestamp", "1 minute")
            .groupBy(
                F.window("timestamp", duration, slide),
                "sensor_id", "sensor_type",
            )
            .agg(
                F.avg("value").alias("avg_value"),
                F.min("value").alias("min_value"),
                F.max("value").alias("max_value"),
                F.count("*").alias("count"),
            )
            .select(
                "sensor_id", "sensor_type",
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.col("window.start").alias("slide_start"),
                "avg_value", "min_value", "max_value", "count",
            )
        )

        query = (
            sliding_agg.writeStream
            .foreach(SlidingAggregateForeachWriter())
            .outputMode("update")
            .option("checkpointLocation", f"{checkpoint_base}/sliding_{duration}_{slide}")
            .start()
        )
        sliding_queries.append(query)
        print(f"Sliding window query started: {duration}/{slide}")
```

- [ ] **Step 4: Add sliding queries to the monitoring loop**

In the monitoring loop (around line 412), add the sliding queries to the list being monitored:

```python
all_queries = [query_raw, query_agg] + sliding_queries
```

- [ ] **Step 5: Commit**

```bash
git add notebooks/sss_rtm_job.py
git commit -m "feat: add streaming config parsing and sliding window query to SSS notebook"
```

---

### Task 9: SSS Notebook — Session Windows (Query 4)

**Files:**
- Modify: `notebooks/sss_rtm_job.py`

- [ ] **Step 1: Add SessionEventForeachWriter class**

Add after `SlidingAggregateForeachWriter`:

```python
class SessionEventForeachWriter(ForeachWriter):
    """Writes session window events to Lakebase."""

    MAX_BUFFER_SIZE = 10

    def open(self, partition_id, epoch_id):
        self.buffer = []
        self.conn = None
        return True

    def process(self, row):
        self.buffer.append(row)
        if len(self.buffer) >= self.MAX_BUFFER_SIZE:
            self._flush()

    def close(self, error):
        if self.buffer:
            self._flush()
        if self.conn:
            self.conn.close()

    def _flush(self):
        if not self.buffer:
            return
        try:
            if not self.conn:
                self.conn = psycopg2.connect(
                    host=pg_host_bc.value, port=pg_port_bc.value,
                    dbname=pg_database_bc.value,
                    user=pg_user_bc.value, password=pg_password_bc.value,
                    sslmode="require",
                )
            with self.conn.cursor() as cur:
                values = [
                    (
                        pipeline_id_bc.value,
                        r["sensor_id"], r["sensor_type"],
                        r["session_id"],
                        r["session_start"], r["session_end"],
                        r["duration_sec"], r["reading_count"],
                        r["avg_value"], r["min_value"], r["max_value"],
                    )
                    for r in self.buffer
                ]
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO session_events
                       (pipeline_id, sensor_id, sensor_type,
                        session_id, session_start, session_end,
                        duration_sec, reading_count,
                        avg_value, min_value, max_value)
                       VALUES %s""",
                    values,
                )
                self.conn.commit()
        except Exception as e:
            print(f"SessionEventForeachWriter flush error: {e}")
        finally:
            self.buffer = []
```

- [ ] **Step 2: Add Query 4 — Session windows**

After the sliding window queries section:

```python
# Query 4: Session windows (if enabled)
session_query = None
if session_config.get("enabled", False):
    gap = session_config["gap"]
    print(f"Starting session window query: gap={gap}")

    session_agg = (
        stream_df
        .withWatermark("timestamp", "2 minutes")
        .groupBy(
            F.session_window("timestamp", gap),
            "sensor_id", "sensor_type",
        )
        .agg(
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.count("*").alias("reading_count"),
        )
        .select(
            "sensor_id", "sensor_type",
            F.md5(
                F.concat("sensor_id", F.col("session_window.start").cast("string"))
            ).alias("session_id"),
            F.col("session_window.start").alias("session_start"),
            F.col("session_window.end").alias("session_end"),
            (
                F.col("session_window.end").cast("long")
                - F.col("session_window.start").cast("long")
            ).alias("duration_sec"),
            "reading_count",
            "avg_value", "min_value", "max_value",
        )
    )

    session_query = (
        session_agg.writeStream
        .foreach(SessionEventForeachWriter())
        .outputMode("update")
        .option("checkpointLocation", f"{checkpoint_base}/sessions")
        .start()
    )
    print("Session window query started")
```

- [ ] **Step 3: Add session query to monitoring loop**

Update the all_queries line:

```python
all_queries = [query_raw, query_agg] + sliding_queries + ([session_query] if session_query else [])
```

- [ ] **Step 4: Commit**

```bash
git add notebooks/sss_rtm_job.py
git commit -m "feat: add session window query to SSS notebook"
```

---

### Task 10: SSS Notebook — mapGroupsWithState (Query 5)

**Files:**
- Modify: `notebooks/sss_rtm_job.py`

- [ ] **Step 1: Add state update function and schema**

Add after the session window section:

```python
# Query 5: Stateful per-sensor operations via mapGroupsWithState
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType, IntegerType

state_output_schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("sensor_type", StringType()),
    StructField("running_avg", DoubleType()),
    StructField("running_min", DoubleType()),
    StructField("running_max", DoubleType()),
    StructField("readings_since_reset", IntegerType()),
    StructField("last_value", DoubleType()),
    StructField("prev_value", DoubleType()),
    StructField("threshold_breached", BooleanType()),
    StructField("updated_at", TimestampType()),
])

# Broadcast state ops config
state_ops_bc = sc.broadcast(state_ops_config)
threshold_alerts_bc = sc.broadcast(state_ops_config.get("threshold_alerts", {}))


def update_sensor_state(key, rows, state: GroupState):
    """Per-sensor stateful processing.

    Maintains running average (EWMA), min/max, and checks threshold breaches.
    """
    import datetime

    # Load or initialize state
    if state.exists:
        s = state.get
    else:
        s = {
            "running_avg": 0.0,
            "running_min": float("inf"),
            "running_max": float("-inf"),
            "readings_since_reset": 0,
            "last_value": 0.0,
            "prev_value": 0.0,
        }

    sensor_id = key[0]
    sensor_type = key[1]
    ops = state_ops_bc.value
    thresholds = threshold_alerts_bc.value
    ewma_span = 20
    alpha = 2.0 / (ewma_span + 1)

    for row in rows:
        value = row["value"]
        s["prev_value"] = s["last_value"]
        s["last_value"] = value
        s["readings_since_reset"] += 1

        # EWMA running average
        if ops.get("running_avg", False):
            if s["readings_since_reset"] == 1:
                s["running_avg"] = value
            else:
                s["running_avg"] = alpha * value + (1 - alpha) * s["running_avg"]

        # Min/Max tracking
        if ops.get("min_max_tracking", False):
            s["running_min"] = min(s["running_min"], value)
            s["running_max"] = max(s["running_max"], value)

    # Threshold check
    breached = False
    if sensor_type in thresholds:
        t = thresholds[sensor_type]
        if s["last_value"] < t.get("min", float("-inf")) or s["last_value"] > t.get("max", float("inf")):
            breached = True

    state.update(s)

    yield (
        sensor_id,
        sensor_type,
        s["running_avg"],
        s.get("running_min") if s.get("running_min") != float("inf") else None,
        s.get("running_max") if s.get("running_max") != float("-inf") else None,
        s["readings_since_reset"],
        s["last_value"],
        s["prev_value"],
        breached,
        datetime.datetime.utcnow(),
    )
```

- [ ] **Step 2: Add StateForeachWriter and Query 5**

```python
class StateForeachWriter(ForeachWriter):
    """Writes state snapshots to Lakebase via upsert."""

    MAX_BUFFER_SIZE = 20

    def open(self, partition_id, epoch_id):
        self.buffer = []
        self.conn = None
        return True

    def process(self, row):
        self.buffer.append(row)
        if len(self.buffer) >= self.MAX_BUFFER_SIZE:
            self._flush()

    def close(self, error):
        if self.buffer:
            self._flush()
        if self.conn:
            self.conn.close()

    def _flush(self):
        if not self.buffer:
            return
        try:
            if not self.conn:
                self.conn = psycopg2.connect(
                    host=pg_host_bc.value, port=pg_port_bc.value,
                    dbname=pg_database_bc.value,
                    user=pg_user_bc.value, password=pg_password_bc.value,
                    sslmode="require",
                )
            with self.conn.cursor() as cur:
                for r in self.buffer:
                    cur.execute(
                        """INSERT INTO state_snapshots
                           (pipeline_id, sensor_id, sensor_type,
                            running_avg, running_min, running_max,
                            readings_since_reset, last_value, prev_value,
                            threshold_breached, updated_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (pipeline_id, sensor_id) DO UPDATE SET
                            running_avg = EXCLUDED.running_avg,
                            running_min = EXCLUDED.running_min,
                            running_max = EXCLUDED.running_max,
                            readings_since_reset = EXCLUDED.readings_since_reset,
                            last_value = EXCLUDED.last_value,
                            prev_value = EXCLUDED.prev_value,
                            threshold_breached = EXCLUDED.threshold_breached,
                            updated_at = EXCLUDED.updated_at
                        """,
                        (
                            pipeline_id_bc.value,
                            r["sensor_id"], r["sensor_type"],
                            r["running_avg"], r["running_min"], r["running_max"],
                            r["readings_since_reset"],
                            r["last_value"], r["prev_value"],
                            r["threshold_breached"], r["updated_at"],
                        ),
                    )
                self.conn.commit()
        except Exception as e:
            print(f"StateForeachWriter flush error: {e}")
        finally:
            self.buffer = []


# Start Query 5 if any state ops are enabled
has_state_ops = any(state_ops_config.get(k, False) for k in ["running_avg", "min_max_tracking", "change_detection"])
has_thresholds = bool(state_ops_config.get("threshold_alerts"))
state_query = None

if has_state_ops or has_thresholds:
    print("Starting stateful per-sensor query (mapGroupsWithState)")

    state_stream = (
        stream_df
        .withWatermark("timestamp", "1 minute")
        .groupBy("sensor_id", "sensor_type")
        .applyInPandasWithState(
            update_sensor_state,
            outputStructType=state_output_schema,
            stateStructType=StructType([
                StructField("running_avg", DoubleType()),
                StructField("running_min", DoubleType()),
                StructField("running_max", DoubleType()),
                StructField("readings_since_reset", IntegerType()),
                StructField("last_value", DoubleType()),
                StructField("prev_value", DoubleType()),
            ]),
            outputMode="update",
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
        )
    )

    state_query = (
        state_stream.writeStream
        .foreach(StateForeachWriter())
        .outputMode("update")
        .option("checkpointLocation", f"{checkpoint_base}/state")
        .start()
    )
    print("Stateful query started")
```

- [ ] **Step 3: Update monitoring loop**

```python
all_queries = (
    [query_raw, query_agg]
    + sliding_queries
    + ([session_query] if session_query else [])
    + ([state_query] if state_query else [])
)
```

- [ ] **Step 4: Commit**

```bash
git add notebooks/sss_rtm_job.py
git commit -m "feat: add mapGroupsWithState for per-sensor EWMA, min/max, threshold alerts"
```

---

### Task 11: Alerts Ingest Endpoint

**Files:**
- Modify: `routes/websocket.py`

- [ ] **Step 1: Add /api/sss/alerts endpoint**

In `routes/websocket.py`, add a new endpoint for receiving threshold breach alerts from Query 5:

```python
@router.post("/api/sss/alerts")
async def sss_alerts_ingest(request: Request):
    """Receive threshold breach alerts from SSS notebook and broadcast via SSE."""
    state: AppState = request.app.state.app_state
    body = await request.json()

    alerts = body.get("alerts", [])
    if not alerts:
        return JSONResponse(content={"status": "ok", "count": 0})

    # Broadcast each alert to SSE clients
    for alert in alerts:
        event_data = {
            "type": "threshold_alert",
            "pipeline_id": alert.get("pipeline_id"),
            "sensor_id": alert.get("sensor_id"),
            "sensor_type": alert.get("sensor_type"),
            "value": alert.get("value"),
            "threshold_min": alert.get("threshold_min"),
            "threshold_max": alert.get("threshold_max"),
            "timestamp": alert.get("timestamp"),
        }
        for client_queue in state.sse_clients:
            try:
                client_queue.put_nowait(event_data)
            except Exception:
                pass

    return JSONResponse(content={"status": "ok", "count": len(alerts)})
```

- [ ] **Step 2: Add AlertEvent type to frontend**

In `client/src/lib/types.ts`, add:

```typescript
export interface ThresholdAlertEvent {
  type: "threshold_alert";
  pipeline_id: string;
  sensor_id: string;
  sensor_type: string;
  value: number;
  threshold_min?: number;
  threshold_max?: number;
  timestamp: string;
}

export interface SessionEvent {
  pipeline_id: string;
  sensor_id: string;
  sensor_type: string;
  session_id: string;
  session_start: string;
  session_end: string;
  duration_sec: number;
  reading_count: number;
  avg_value: number;
  min_value: number;
  max_value: number;
}
```

- [ ] **Step 3: Commit**

```bash
git add routes/websocket.py client/src/lib/types.ts
git commit -m "feat: add SSE alerts ingest endpoint + ThresholdAlertEvent types"
```

---

### Task 12: Source Availability UI Component

**Files:**
- Create: `client/src/components/source-availability.tsx`

- [ ] **Step 1: Create the component**

```tsx
import { useState, useEffect } from "react";
import { api } from "@/lib/api";
import type { SourceAvailabilityResult } from "@/lib/types";

interface SourceAvailabilityProps {
  sourceType: string | null;
  sourceConfig: Record<string, unknown> | null;
  debounceMs?: number;
}

export function SourceAvailability({
  sourceType,
  sourceConfig,
  debounceMs = 500,
}: SourceAvailabilityProps) {
  const [result, setResult] = useState<SourceAvailabilityResult | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!sourceType || !sourceConfig) {
      setResult(null);
      return;
    }

    setLoading(true);
    const timeout = setTimeout(async () => {
      try {
        const res = await api.checkSourceAvailability(sourceType, sourceConfig);
        setResult(res);
      } catch {
        setResult(null);
      } finally {
        setLoading(false);
      }
    }, debounceMs);

    return () => clearTimeout(timeout);
  }, [sourceType, JSON.stringify(sourceConfig), debounceMs]);

  if (!sourceType || !sourceConfig) return null;

  if (loading) {
    return (
      <div className="flex items-center gap-2 text-xs text-muted-foreground font-ui">
        <span className="inline-block w-2 h-2 rounded-full bg-muted animate-pulse" />
        Checking source availability...
      </div>
    );
  }

  if (!result) return null;

  if (result.available) {
    return (
      <div className="flex items-center gap-2 text-xs font-ui text-success">
        <span className="inline-block w-2 h-2 rounded-full bg-success" />
        Source available
      </div>
    );
  }

  return (
    <div className="flex items-center gap-2 text-xs font-ui text-destructive">
      <span className="inline-block w-2 h-2 rounded-full bg-destructive" />
      Source claimed by another pipeline
      {result.claimed_by && (
        <span className="text-muted-foreground">({result.claimed_by})</span>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add client/src/components/source-availability.tsx
git commit -m "feat: add SourceAvailability component with debounced check"
```

---

### Task 13: Streaming Operations Builder Component (Skeleton)

**Files:**
- Create: `client/src/components/streaming-ops-builder.tsx`

- [ ] **Step 1: Create the hybrid builder component**

```tsx
import { useState, useCallback } from "react";
import type { StreamingConfig } from "@/lib/streaming-config";
import { DEFAULT_STREAMING_CONFIG, validateStreamingConfig } from "@/lib/streaming-config";

interface StreamingOpsBuilderProps {
  value: StreamingConfig;
  onChange: (config: StreamingConfig) => void;
}

type Mode = "visual" | "code";

export function StreamingOpsBuilder({ value, onChange }: StreamingOpsBuilderProps) {
  const [mode, setMode] = useState<Mode>("visual");
  const [codeText, setCodeText] = useState(JSON.stringify(value, null, 2));
  const [codeError, setCodeError] = useState<string | null>(null);
  const errors = validateStreamingConfig(value);

  const handleCodeChange = useCallback(
    (text: string) => {
      setCodeText(text);
      try {
        const parsed = JSON.parse(text) as StreamingConfig;
        const errs = validateStreamingConfig(parsed);
        if (errs.length > 0) {
          setCodeError(errs[0]);
        } else {
          setCodeError(null);
          onChange(parsed);
        }
      } catch {
        setCodeError("Invalid JSON");
      }
    },
    [onChange],
  );

  const handleVisualChange = useCallback(
    (updated: StreamingConfig) => {
      onChange(updated);
      setCodeText(JSON.stringify(updated, null, 2));
      setCodeError(null);
    },
    [onChange],
  );

  return (
    <div className="space-y-4">
      {/* Mode Toggle */}
      <div className="flex items-center gap-1 p-1 bg-surface-low rounded-lg w-fit">
        <button
          className={`px-3 py-1.5 rounded-md text-xs font-ui font-medium transition-all ${
            mode === "visual"
              ? "bg-surface-high text-foreground"
              : "text-muted-foreground hover:text-foreground"
          }`}
          onClick={() => setMode("visual")}
        >
          Visual Builder
        </button>
        <button
          className={`px-3 py-1.5 rounded-md text-xs font-ui font-medium transition-all ${
            mode === "code"
              ? "bg-surface-high text-foreground"
              : "text-muted-foreground hover:text-foreground"
          }`}
          onClick={() => setMode("code")}
        >
          JSON Editor
        </button>
      </div>

      {mode === "visual" ? (
        <VisualBuilder value={value} onChange={handleVisualChange} />
      ) : (
        <div className="space-y-2">
          <textarea
            className="w-full h-80 p-4 bg-surface-low border border-border rounded-lg font-mono text-xs text-foreground resize-none focus:outline-none focus:ring-2 focus:ring-primary/20 focus:border-primary/40"
            value={codeText}
            onChange={(e) => handleCodeChange(e.target.value)}
            spellCheck={false}
          />
          {codeError && (
            <p className="text-xs text-destructive font-ui">{codeError}</p>
          )}
        </div>
      )}

      {errors.length > 0 && mode === "visual" && (
        <div className="text-xs text-destructive space-y-1 font-ui">
          {errors.map((e, i) => (
            <p key={i}>{e}</p>
          ))}
        </div>
      )}
    </div>
  );
}

function VisualBuilder({
  value,
  onChange,
}: {
  value: StreamingConfig;
  onChange: (c: StreamingConfig) => void;
}) {
  const toggleStateOp = (key: keyof StreamingConfig["state_ops"]) => {
    if (key === "threshold_alerts") return;
    onChange({
      ...value,
      state_ops: {
        ...value.state_ops,
        [key]: !value.state_ops[key],
      },
    });
  };

  const toggleSession = () => {
    onChange({
      ...value,
      session: { ...value.session, enabled: !value.session.enabled },
    });
  };

  const addWindow = (type: "tumbling" | "sliding") => {
    const newWindow =
      type === "tumbling"
        ? { type: "tumbling" as const, duration: "5m" }
        : { type: "sliding" as const, duration: "5m", slide: "1m" };
    onChange({ ...value, windows: [...value.windows, newWindow] });
  };

  const removeWindow = (index: number) => {
    onChange({ ...value, windows: value.windows.filter((_, i) => i !== index) });
  };

  return (
    <div className="space-y-6">
      {/* Windows */}
      <div>
        <h4 className="text-xs font-ui font-semibold text-muted-foreground uppercase tracking-wider mb-3">
          Window Operations
        </h4>
        <div className="space-y-2">
          {value.windows.map((w, i) => (
            <div
              key={i}
              className="flex items-center gap-3 p-3 bg-surface-low rounded-lg border border-border"
            >
              <span className="text-xs font-mono text-secondary px-2 py-0.5 bg-secondary/10 rounded">
                {w.type}
              </span>
              <label className="text-xs text-muted-foreground">Duration:</label>
              <input
                className="w-16 px-2 py-1 bg-surface-high border border-border rounded text-xs font-mono text-foreground"
                value={w.duration}
                onChange={(e) => {
                  const updated = [...value.windows];
                  updated[i] = { ...updated[i], duration: e.target.value };
                  onChange({ ...value, windows: updated });
                }}
              />
              {w.type === "sliding" && (
                <>
                  <label className="text-xs text-muted-foreground">Slide:</label>
                  <input
                    className="w-16 px-2 py-1 bg-surface-high border border-border rounded text-xs font-mono text-foreground"
                    value={w.slide}
                    onChange={(e) => {
                      const updated = [...value.windows];
                      updated[i] = { ...updated[i], slide: e.target.value };
                      onChange({ ...value, windows: updated });
                    }}
                  />
                </>
              )}
              <button
                className="ml-auto text-muted-foreground hover:text-destructive text-xs"
                onClick={() => removeWindow(i)}
              >
                Remove
              </button>
            </div>
          ))}
          <div className="flex gap-2">
            <button
              className="px-3 py-1.5 text-xs font-ui bg-surface-low border border-border rounded-md hover:border-border-hover text-muted-foreground hover:text-foreground transition-all"
              onClick={() => addWindow("tumbling")}
            >
              + Tumbling
            </button>
            <button
              className="px-3 py-1.5 text-xs font-ui bg-surface-low border border-border rounded-md hover:border-border-hover text-muted-foreground hover:text-foreground transition-all"
              onClick={() => addWindow("sliding")}
            >
              + Sliding
            </button>
          </div>
        </div>
      </div>

      {/* Session Windows */}
      <div>
        <h4 className="text-xs font-ui font-semibold text-muted-foreground uppercase tracking-wider mb-3">
          Session Windows
        </h4>
        <label className="flex items-center gap-3 p-3 bg-surface-low rounded-lg border border-border cursor-pointer">
          <input
            type="checkbox"
            checked={value.session.enabled}
            onChange={toggleSession}
            className="accent-primary"
          />
          <div>
            <span className="text-sm text-foreground">Enable Session Tracking</span>
            <p className="text-xs text-muted-foreground">
              Groups sensor readings into activity sessions based on inactivity gaps
            </p>
          </div>
        </label>
        {value.session.enabled && (
          <div className="flex gap-4 mt-2 ml-8">
            <div>
              <label className="text-xs text-muted-foreground block mb-1">Gap</label>
              <input
                className="w-16 px-2 py-1 bg-surface-high border border-border rounded text-xs font-mono"
                value={value.session.gap}
                onChange={(e) =>
                  onChange({
                    ...value,
                    session: { ...value.session, gap: e.target.value },
                  })
                }
              />
            </div>
            <div>
              <label className="text-xs text-muted-foreground block mb-1">Timeout</label>
              <input
                className="w-16 px-2 py-1 bg-surface-high border border-border rounded text-xs font-mono"
                value={value.session.timeout}
                onChange={(e) =>
                  onChange({
                    ...value,
                    session: { ...value.session, timeout: e.target.value },
                  })
                }
              />
            </div>
          </div>
        )}
      </div>

      {/* State Operations */}
      <div>
        <h4 className="text-xs font-ui font-semibold text-muted-foreground uppercase tracking-wider mb-3">
          Per-Sensor State Operations
        </h4>
        <div className="space-y-2">
          {[
            { key: "running_avg" as const, label: "Running Average (EWMA)", desc: "Exponentially weighted moving average with span=20" },
            { key: "min_max_tracking" as const, label: "Min/Max Tracking", desc: "Track minimum and maximum values since last reset" },
            { key: "change_detection" as const, label: "Change Detection", desc: "Trigger when value delta exceeds threshold" },
          ].map(({ key, label, desc }) => (
            <label
              key={key}
              className="flex items-center gap-3 p-3 bg-surface-low rounded-lg border border-border cursor-pointer"
            >
              <input
                type="checkbox"
                checked={value.state_ops[key] as boolean}
                onChange={() => toggleStateOp(key)}
                className="accent-primary"
              />
              <div>
                <span className="text-sm text-foreground">{label}</span>
                <p className="text-xs text-muted-foreground">{desc}</p>
              </div>
            </label>
          ))}
        </div>
      </div>

      {/* Threshold Alerts */}
      <div>
        <h4 className="text-xs font-ui font-semibold text-muted-foreground uppercase tracking-wider mb-3">
          Threshold Alerts
        </h4>
        <div className="space-y-2">
          {Object.entries(value.state_ops.threshold_alerts).map(
            ([sensorType, rule]) => (
              <div
                key={sensorType}
                className="flex items-center gap-3 p-3 bg-surface-low rounded-lg border border-border"
              >
                <span className="text-xs font-mono text-primary-soft px-2 py-0.5 bg-primary/10 rounded">
                  {sensorType}
                </span>
                <label className="text-xs text-muted-foreground">Min:</label>
                <input
                  className="w-16 px-2 py-1 bg-surface-high border border-border rounded text-xs font-mono"
                  type="number"
                  value={rule.min}
                  onChange={(e) => {
                    const updated = {
                      ...value.state_ops.threshold_alerts,
                      [sensorType]: { ...rule, min: Number(e.target.value) },
                    };
                    onChange({
                      ...value,
                      state_ops: { ...value.state_ops, threshold_alerts: updated },
                    });
                  }}
                />
                <label className="text-xs text-muted-foreground">Max:</label>
                <input
                  className="w-16 px-2 py-1 bg-surface-high border border-border rounded text-xs font-mono"
                  type="number"
                  value={rule.max}
                  onChange={(e) => {
                    const updated = {
                      ...value.state_ops.threshold_alerts,
                      [sensorType]: { ...rule, max: Number(e.target.value) },
                    };
                    onChange({
                      ...value,
                      state_ops: { ...value.state_ops, threshold_alerts: updated },
                    });
                  }}
                />
                <button
                  className="ml-auto text-muted-foreground hover:text-destructive text-xs"
                  onClick={() => {
                    const { [sensorType]: _, ...rest } =
                      value.state_ops.threshold_alerts;
                    onChange({
                      ...value,
                      state_ops: { ...value.state_ops, threshold_alerts: rest },
                    });
                  }}
                >
                  Remove
                </button>
              </div>
            ),
          )}
          <button
            className="px-3 py-1.5 text-xs font-ui bg-surface-low border border-border rounded-md hover:border-border-hover text-muted-foreground hover:text-foreground transition-all"
            onClick={() => {
              const updated = {
                ...value.state_ops.threshold_alerts,
                new_sensor: { min: 0, max: 100 },
              };
              onChange({
                ...value,
                state_ops: { ...value.state_ops, threshold_alerts: updated },
              });
            }}
          >
            + Add Threshold Rule
          </button>
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add client/src/components/streaming-ops-builder.tsx
git commit -m "feat: add StreamingOpsBuilder with visual builder and JSON code editor"
```

---

### Task 14: Run Full Test Suite

- [ ] **Step 1: Run all Python tests**

Run: `cd /Users/taranpreetsingh.grover/Documents/Claude\ Projects/Apps/iot-digital-twin && python -m pytest tests/ -v --tb=short`
Expected: All tests pass (existing + new source registry tests)

- [ ] **Step 2: Run frontend type check**

Run: `cd client && npx tsc --noEmit`
Expected: No type errors

- [ ] **Step 3: Final commit with all Phase 1 changes**

```bash
git add -A
git commit -m "feat: Phase 1 complete — source registry + stateful streaming + ops builder"
```
