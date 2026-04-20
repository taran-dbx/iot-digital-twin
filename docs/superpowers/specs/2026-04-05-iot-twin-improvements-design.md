# IoT Digital Twin — Comprehensive Improvements Design

**Date:** 2026-04-05
**Approach:** Feature-First with Guards (Approach B)
**Audience:** Field Engineering demos with expanding capabilities

---

## Overview

Three-phase improvement plan for the IoT Digital Twin Databricks App. Prioritizes demo impact while building architectural guardrails alongside new features.

- **Phase 1:** Stateful Streaming + Source Registry (parallel tracks)
- **Phase 2:** Collaboration + Pipeline Templates
- **Phase 3:** Observability + Resilience + Cost Tracking

---

## Phase 1: Stateful Streaming + Source Registry

### 1.1 Source Registry

Prevents source config conflicts by maintaining a central binding ledger in Lakebase.

**New table: `source_bindings`**

| Column | Type | Description |
|--------|------|-------------|
| id | UUID PK | Binding identifier |
| pipeline_id | UUID FK → pipelines(id) ON DELETE CASCADE | Owning pipeline |
| source_type | TEXT | kafka, kinesis, eventhub, rabbitmq, pubsub, zerobus |
| source_key | TEXT | Unique source identifier (see derivation below) |
| partition_count | INT | For scaling visibility |
| claimed_at | TIMESTAMPTZ | When the binding was created |
| claimed_by | TEXT | User email |
| status | TEXT | active, released |

**Constraint:** `UNIQUE(source_type, source_key) WHERE status = 'active'` — partial unique index enforces one active pipeline per source at the DB level.

**Source key derivation:**

| Source Type | Key Format |
|-------------|------------|
| Kafka | `{bootstrap_servers_sorted}::{topic}` |
| Kinesis | `{region}::{stream_name}` |
| EventHub | `{namespace}::{eventhub_name}::{consumer_group}` |
| RabbitMQ | `{host}:{port}::{queue_name}` |
| Pub/Sub | `{project_id}::{subscription_id}` |
| Zerobus | `{catalog}::{schema}::{table_name}` |

**Key behaviors:**
- Keys are lowercased and trimmed before comparison
- Bootstrap servers are sorted alphabetically to avoid order-dependent mismatches
- Zerobus binds on the destination Delta table (catalog::schema::table) since it's a sink-side service
- Zerobus validation also checks that the upstream source feeding Zerobus isn't claimed by an SSS pipeline
- Pipeline creation derives key → checks availability → inserts binding atomically
- Pipeline deletion soft-releases binding (status → released) for audit history
- Partition count fetched at claim time via admin APIs where available

**Frontend integration:**
- Pipeline wizard checks source availability in real-time before form submission
- "Source available" / "Source claimed by Pipeline X" indicator on source config step

**Code impact:**
- `models/sql_ddl.py` — Add `source_bindings` DDL
- `services/pipeline_service.py` — Add claim/release/check methods
- `routes/pipelines.py` — Validate source before creating pipeline
- `client/src/lib/api.ts` — Add `checkSourceAvailability()` call
- Client pipeline wizard — Real-time availability indicator

### 1.2 Stateful Streaming Operations

Enhances the SSS notebook from 2 queries to 5, all driven by a JSON config parameter.

**Current state (sss_rtm_job.py):**
- Query 1: Raw row passthrough → Lakebase `sensor_readings`
- Query 2: 30-sec tumbling window aggregates → Lakebase `sensor_aggregates`

**New streaming operations:**

| Query | Type | Description |
|-------|------|-------------|
| Query 3 | Sliding windows | Overlapping windows for smooth trend lines (e.g., 5m window, 1m slide) |
| Query 4 | Session windows | Groups sensor bursts by activity gap (e.g., 2 min gap). "Device active 14:01–14:23" |
| Query 5 | mapGroupsWithState | Per-sensor running state: EWMA, min/max, change detection, threshold alerts |

**Streaming config (JSON notebook parameter):**

```json
{
  "windows": [
    {"type": "tumbling", "duration": "30s"},
    {"type": "tumbling", "duration": "5m"},
    {"type": "sliding", "duration": "5m", "slide": "1m"}
  ],
  "session": {
    "enabled": true,
    "gap": "2m",
    "timeout": "30m"
  },
  "state_ops": {
    "running_avg": true,
    "min_max_tracking": true,
    "change_detection": true,
    "threshold_alerts": {
      "temperature": {"min": -10, "max": 85},
      "pressure": {"min": 0, "max": 150}
    }
  }
}
```

**New Lakebase tables:**

**`sliding_aggregates`:**
pipeline_id, sensor_id, sensor_type, window_start, window_end, slide_start, avg_value, min_value, max_value, count, created_at

**`session_events`:**
pipeline_id, sensor_id, sensor_type, session_id, session_start, session_end, duration_sec, reading_count, avg_value, min_value, max_value, created_at

**`state_snapshots`:**
pipeline_id, sensor_id, sensor_type, running_avg, running_min, running_max, readings_since_reset, last_value, prev_value, threshold_breached (BOOLEAN), updated_at

**Key behaviors:**
- All windowed queries use `withWatermark("timestamp", "1 minute")` for late data handling
- State is checkpointed to UC managed volume (already provisioned per pipeline)
- Threshold breach alerts POST to `/api/sss/alerts` → SSE broadcast to UI
- Config-driven: no code changes needed to add new window sizes or thresholds

### 1.3 State Operations Builder (Hybrid UI)

Users define state logic via a **Visual Builder** (default) with a **JSON Code Editor** toggle.

**Visual Builder mode:**
- Checkbox-driven operation selection with configurable parameters
- Available operations: Running Average (EWMA with span), Min/Max Tracking (with reset interval), Change Detection (sigma threshold), Threshold Alerts (per sensor type min/max), Custom Derived Metric (name + formula + window), Anomaly Score (Z-score), Rate of Change (derivative), Cumulative Sum (CUSUM)
- "Preview Output Schema" and "Test with Sample Data" buttons

**Code Editor mode:**
- JSON editor showing the streaming_config that the notebook consumes directly
- Syntax validation and schema checking
- Visual ↔ Code stay in sync (edits in either mode reflect in the other)

**Key behaviors:**
- Visual mode is the demo hero — interactive, approachable
- Code mode lets power users (data engineers) see and edit the raw config
- Custom expressions use a safe expression parser with an explicit allowlist: arithmetic operators (+, -, *, /), aggregation functions (avg, min, max, sum, count, stddev), and field references (sensor_type.value). No eval(), no imports, no function calls outside the allowlist. Parsed to AST and validated before submission.
- Templates become shareable JSON snippets

**Code impact:**
- `notebooks/sss_rtm_job.py` — Major enhancement: add Queries 3-5, config parsing, mapGroupsWithState
- `models/sql_ddl.py` — Add 3 new table DDLs
- `routes/websocket.py` — Add `/api/sss/alerts` ingest endpoint + SSE broadcast
- `client/src/lib/types.ts` — Add streaming config types, alert types
- Client pipeline wizard — New "Streaming Operations" step with visual builder + code editor
- Client dashboard — New charts for sliding trends, session timeline, alert feed

---

## Phase 2: Collaboration & Templates

### 2.1 Teams

Lightweight team abstraction for sharing pipelines and templates.

**New table: `teams`:**
id (UUID PK), name, description, created_by (email), created_at

**New table: `team_members`:**
team_id (FK), user_email, role (owner | editor | viewer), joined_at

**Visibility & access rules:**

| Visibility | Who can see | Who can edit |
|-----------|-------------|--------------|
| private | Creator only | Creator only |
| team | Team members | owner + editor roles |
| public | All workspace users | Creator only (read-only for others) |

**Key behaviors:**
- Teams are ad-hoc — created quickly for demos, no org hierarchy
- Existing pipelines default to `visibility=private, team_id=NULL` — zero migration risk
- Activity is logged for audit (see activity_log table)

### 2.2 Pipeline Templates

Reusable pipeline configurations that can be shared across teams.

**New table: `pipeline_templates`:**
id (UUID PK), name, description, industry, source_type, source_config_template (JSON, no secrets), streaming_config (JSON), created_by, team_id (NULL = personal), is_featured (BOOLEAN), version, created_at

**Template lifecycle:**

1. **Create from existing pipeline:** "Save as Template" on pipeline detail page. Strips secrets from source config, preserves streaming_config as-is. User names it and picks an industry tag.

2. **Use template:** In pipeline wizard Step 1, toggle "Start from template". Template gallery filterable by industry and source type. Three sections: Featured (pre-built, shipped with app), Team Templates, My Templates. Pre-fills source type + streaming config. User only needs to add credentials + source-specific details.

3. **Featured templates ship with the app:** "Pharma RTM with Kafka + EWMA + Thresholds", "Factory Floor with EventHub + Sessions", "Thermal Anomaly Detector with Kinesis + Z-Score".

### 2.3 Activity Log

**New table: `activity_log`:**
id, pipeline_id, team_id, user_email, action (created | started | stopped | shared | cloned | template_saved), details (JSON), created_at

Append-only audit trail. Feeds into Phase 3 observability and the activity feed sidebar in the sharing dialog.

### 2.4 Pipeline Updates

**Existing `pipelines` table — new columns:**
- `team_id` UUID REFERENCES teams(id) NULL
- `visibility` TEXT DEFAULT 'private'

**Code impact:**
- `models/sql_ddl.py` — Add teams, team_members, pipeline_templates, activity_log DDL; alter pipelines
- `services/pipeline_service.py` — Add visibility filtering, team-scoped queries
- `routes/pipelines.py` — Add sharing, template CRUD endpoints
- NEW: `routes/teams.py` — Team CRUD + membership management
- NEW: `routes/templates.py` — Template gallery + clone endpoints
- NEW: `services/team_service.py` — Team + permission logic
- Client pipeline wizard — "Start from template" flow with gallery
- Client dashboard — Team pipeline view, activity feed sidebar
- Client sharing dialog — Visibility selector, member management, copy link

---

## Phase 3: Observability & Resilience

### 3.1 Pipeline Health Monitor

**New table: `pipeline_metrics`:**
pipeline_id, timestamp, throughput_rps, consumer_lag, processing_latency_ms, error_count, state_store_size_bytes, checkpoint_lag_sec, watermark_delay_sec

**Metric sources:**
- SSS notebook POSTs to `/api/sss/metrics` every 30 seconds
- Simulator writes directly via AppState
- Zerobus: poll Delta table row count delta

**Health status rules:**
- Healthy: lag < 60s AND error_rate < 5%
- Degraded: lag > 60s OR error_rate > 5%
- Unhealthy: lag > 300s OR error_rate > 20%

**Dashboard widget:** Per-pipeline health card with throughput sparkline, lag gauge, latency percentiles (p50/p95/p99), error rate, and traffic light status indicator.

### 3.2 Cost Tracking

**New table: `pipeline_costs`:**
pipeline_id, date, compute_dbu, storage_gb, network_egress_gb, estimated_cost_usd

**Sources:**
- Databricks Jobs API → DBU usage per run
- Cluster API → node hours × DBU rate
- UC API → table size metadata
- Computed daily via a lightweight scheduled background task

**Dashboard widget:** Cost breakdown by pipeline (bar chart), daily/weekly trend (line chart), "cost per 1M records" efficiency metric, configurable budget alert threshold.

### 3.3 Alerting Engine

**New table: `alert_rules`:**
id, pipeline_id, name, condition (JSON), severity (info | warning | critical), channels (JSON array: ["sse", "slack", "email"]), cooldown_sec, enabled, created_by

**New table: `alert_events`:**
id, rule_id, pipeline_id, triggered_at, resolved_at, current_value, message

**Condition format example:**
```json
{"metric": "consumer_lag", "op": ">", "value": 120}
```

**Evaluation loop:**
- Background task checks rules against pipeline_metrics every 30 seconds
- Fires alert → inserts alert_event + broadcasts via SSE
- Respects cooldown to avoid alert storms
- Auto-resolves when condition clears (sets resolved_at)

**Channels (phased):**
- Phase 3a: SSE only (real-time toast in UI)
- Phase 3b: Slack webhook integration
- Future: Email via Databricks notification API

### 3.4 Resilience & Error Recovery

**Auto-retry logic (in producer_service.py):**
1. Pipeline job fails (cluster lost, OOM, network error)
2. Record failure in pipeline_metrics (error_count++)
3. If retry_count < max_retries (default: 3): exponential backoff (30s, 60s, 120s), restart from last checkpoint, fire "info" alert
4. If retries exhausted: mark pipeline "failed", fire "critical" alert, require manual restart

**Provisioning rollback (in provisioning_service.py):**
- Track created resources in pipeline metadata as they're provisioned
- On failure mid-provisioning: delete in reverse order (volumes → schemas → catalogs)
- Release source binding on rollback
- Solves the existing "broken catalogs on partial failure" issue from the deployment playbook

### 3.5 Phase 3 Sub-phases

| Sub-phase | Scope |
|-----------|-------|
| 3a | Pipeline health metrics + SSE alerts + auto-retry |
| 3b | Cost tracking + configurable alert rules + Slack |
| 3c | Provisioning rollback + advanced alerting (anomaly, composite) |

**Code impact:**
- `models/sql_ddl.py` — 4 new table DDLs
- `notebooks/sss_rtm_job.py` — Add metrics POST every 30s
- NEW: `services/metrics_service.py` — Metrics collection, health computation
- NEW: `services/alerting_service.py` — Rule evaluation loop, alert firing, cooldown
- NEW: `services/cost_service.py` — Daily cost aggregation from Jobs/Cluster APIs
- `services/producer_service.py` — Add retry logic with exponential backoff
- `services/provisioning_service.py` — Add rollback on partial failure
- `routes/websocket.py` — Add `/api/sss/metrics` ingest endpoint
- NEW: `routes/alerts.py` — Alert rule CRUD + history endpoints
- Client dashboard — Health cards, cost charts, alert feed
- Client settings — Alert rule configuration UI

---

## New Lakebase Tables Summary

| Table | Phase | Purpose |
|-------|-------|---------|
| source_bindings | 1 | Source-to-pipeline binding ledger |
| sliding_aggregates | 1 | Sliding window aggregation results |
| session_events | 1 | Sessionized sensor activity |
| state_snapshots | 1 | Per-sensor running state |
| teams | 2 | Team definitions |
| team_members | 2 | Team membership + roles |
| pipeline_templates | 2 | Reusable pipeline configs |
| activity_log | 2 | Audit trail of pipeline actions |
| pipeline_metrics | 3 | Time-series health data |
| pipeline_costs | 3 | Daily cost rollup |
| alert_rules | 3 | User-configurable alert conditions |
| alert_events | 3 | Triggered alert history |

**Existing table modifications:**
- `pipelines` — Add `team_id` (UUID, nullable) and `visibility` (TEXT, default 'private')

---

## New Backend Files

| File | Phase | Purpose |
|------|-------|---------|
| routes/teams.py | 2 | Team CRUD + membership |
| routes/templates.py | 2 | Template gallery + clone |
| routes/alerts.py | 3 | Alert rule CRUD + history |
| services/team_service.py | 2 | Team + permission logic |
| services/metrics_service.py | 3 | Health metrics collection |
| services/alerting_service.py | 3 | Alert evaluation + firing |
| services/cost_service.py | 3 | Cost aggregation |

---

## Frontend Changes Summary

| Component | Phase | Changes |
|-----------|-------|---------|
| Pipeline Wizard | 1 | Source availability check, "Streaming Operations" step with visual builder + code editor, "Start from template" toggle |
| Dashboard | 1, 3 | Sliding trend charts, session timeline, alert feed, health cards, cost charts |
| Pipeline Detail | 2 | "Save as Template" button, "Share" button |
| Template Gallery | 2 | New page: featured/team/personal templates, search + filter |
| Sharing Dialog | 2 | Visibility selector, team dropdown, member management, activity feed, copy link |
| Settings | 3 | Alert rule configuration, budget thresholds |
| Types | 1, 2, 3 | StreamingConfig, AlertRule, Team, Template, PipelineMetrics types |

---

## Testing Strategy

Each phase includes:
- **Unit tests** for new services (source registry claim/release, alert rule evaluation, cost computation)
- **Integration tests** for new API endpoints (pipeline creation with source validation, template CRUD, alert CRUD)
- **E2E tests** for critical flows (create pipeline from template, share pipeline with team, alert fires on threshold breach)
- Target: 80%+ coverage on new code

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Source registry race conditions | Partial unique index enforces at DB level |
| mapGroupsWithState complexity | Checkpoint to UC volume; watermarks handle late data |
| Expression parser security | Allowlist arithmetic + aggregation functions; no eval() |
| Migration risk on pipelines table | New columns are nullable with defaults; zero breaking change |
| Alert storm on flapping metrics | Cooldown period per rule; auto-resolve on clear |
| Cost API rate limits | Daily batch aggregation, not real-time polling |
