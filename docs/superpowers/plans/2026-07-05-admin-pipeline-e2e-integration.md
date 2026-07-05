# Admin Pipeline E2E Integration & Deploy — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take the design in `docs/superpowers/specs/2026-07-05-admin-pipeline-setup-design.md` from prototype to a working, deployed end-to-end pipeline (starting with **Pharmaceutical**) driven by the real admin config + validation flow, backed by the two-project Lakebase model and the resource-ledger lifecycle.

**Architecture:** FastAPI (Python) backend + React/Vite/TS frontend, deployed as a Databricks App on `fe-vm-shared-interop`. App metadata in a dedicated `iot-app-backend` Lakebase project; real-time streaming data in a shared `iot-pipelines` project (one never-expiring branch per real-time industry pipeline, primary RW + read-replica endpoints). Kafka = Amazon MSK cluster `iotdemo` (us-east-1). UC catalogs `iot_<industry>`. All provisioned resources recorded in a normalized `provisioned_resource` ledger for ordered teardown.

**Tech Stack:** Python (FastAPI, asyncpg, boto3 MSK, databricks-sdk, deltalake/delta-rs, kafka), React 18 + Vite + Tailwind + Radix, Databricks (Lakebase/Postgres, Unity Catalog, Structured Streaming real-time mode, Zerobus SDK, DBSQL, Jobs).

## Global Constraints

- **Workspace:** `fe-vm-shared-interop` (id `332040261549026`), region `us-east-1`.
- **MSK cluster:** `iotdemo`. Public bootstrap `:9198`, private bootstrap `:9098` (IAM). **Test connectivity against BOTH.**
- **UC service credential:** `iot-rtm-sc` (mapped to the AWS IAM role for MSK).
- **Lakebase DB name must be `databricks_postgres`** (Lakehouse Sync constraint).
- **Classic flavor only** (Serverless deferred). **Kafka source only** (Kinesis deferred/"coming soon").
- **One pipeline setup per industry.** Modes: `realtime`, `microbatch`, `zerobus`.
- **Streaming metrics always on** (no toggle) for real-time + micro-batch, via delta-rs.
- **Schema is industry-invariant**, generated from the SSOT field-map (`SensorRecord`).
- No AI attribution in commits; conventional-commit messages.

---

## Phase 0 — Blocker-discovery spikes (RUN FIRST; fan out in parallel)

Purpose: empirically confirm the risky Databricks operations work with our identity/creds **before** building on them. Each spike is a throwaway script/notebook that provisions a tiny resource, verifies, and tears down. **Any failure is a blocker to report to the user, not to silently work around.** Use profile `fe-vm-shared-interop`.

### Task 0.1: MSK topic-create + round-trip via `iot-rtm-sc` (both endpoints)
**Files:** Create `scripts/spikes/spike_msk.py` (Databricks notebook/job, single-node cluster in the workspace VPC).
- [ ] Build a boto3 MSK client from the UC service credential: `boto3.Session(botocore_session=dbutils.credentials.getServiceCredentialsProvider("iot-rtm-sc"), region_name="us-east-1").client("kafka")` (needs `%pip install --upgrade boto3 botocore`).
- [ ] `msk.list_clusters()` → resolve the `iotdemo` cluster ARN by name derived from bootstrap host.
- [ ] `msk.create_topic(ClusterArn=..., TopicName="iot.spike.test", PartitionCount=3, ReplicationFactor=min(3, broker_count))`; then confirm via `list_topics`/describe.
- [ ] Produce + consume a round-trip test message, once against the **private** bootstrap (`:9098`, IAM/OAUTHBEARER) and once against the **public** bootstrap (`:9198`). Record DNS/TCP/TLS reachability + egress IP for each.
- [ ] Delete the test topic.
- [ ] **Report:** which endpoint(s) are reachable from the cluster, whether create_topic succeeded, and the IAM perms present. FAIL → blocker (IAM perms and/or SG/VPC network path).

### Task 0.2: Lakebase project + branch + read-replica endpoint creation
**Files:** Create `scripts/spikes/spike_lakebase.py`.
- [ ] Via databricks-sdk, create a throwaway Lakebase project `iot-spike`, a never-expiring branch `spike`, a `primary` READ_WRITE endpoint, and a **read-replica** endpoint. Wait for ACTIVE.
- [ ] Connect (OAuth) to the branch's `databricks_postgres`, create + drop a table.
- [ ] Delete the project.
- [ ] **Report:** can our identity create projects/branches/replicas? FAIL → blocker (Lakebase provisioning permission).

### Task 0.3: UC catalog + schemas + external location + volume
**Files:** Create `scripts/spikes/spike_uc.py`.
- [ ] Create catalog `iot_spike`, schemas `realtime/microbatch/zerobus/metrics/checkpoints`, a managed volume, and an **external Delta table** in `metrics` (needs an external location / storage credential). Grant to the SP. Drop all.
- [ ] **Report:** metastore `CREATE CATALOG` + external-location availability. FAIL → blocker (metastore perms / external location).

### Task 0.4: delta-rs write via SP OAuth + UC temporary-table-credentials (+ VARIANT probe)
**Files:** Create `scripts/spikes/spike_deltars.py`.
- [ ] `pip install deltalake`; mint SP OAuth token → UC `GenerateTemporaryTableCredentials` (READ_WRITE) → `write_deltalake(table_uri, df, mode="append", storage_options=...)` to the external table from 0.3.
- [ ] Probe writing a **VARIANT** column; if unsupported, confirm the STRING + `parse_json` fallback works.
- [ ] **Report:** delta-rs external-table write works; VARIANT supported yes/no. FAIL → fallback path confirmed.

### Task 0.5: Zerobus create_stream + ingest
**Files:** Create `scripts/spikes/spike_zerobus.py`.
- [ ] Pre-create a UC Delta target table; open `ZerobusSdk(server_endpoint=<ws-id>.zerobus.us-east-1.cloud.databricks.com, workspace_url=https://...)`; `create_stream`; ingest a few records; `wait_for_ack()`; confirm rows via DBSQL.
- [ ] **Report:** zerobus reachable + ingest works (watch the https-prefix gotcha). FAIL → blocker.

**Gate:** Compile a Phase-0 blocker report for the user. Proceed to Phase 1 only for the paths that passed; surface any failures for a fix decision.

---

## Phase 1 — App-backend DB + data model + resource ledger

### Task 1.1: Provision the `iot-app-backend` Lakebase project
- [ ] Create project `iot-app-backend` + branch `production` + `primary` endpoint (no replica). Repoint the app's `PGHOST`/`LAKEBASE_ENDPOINT` (in `app.yaml`) to it. Leave `iot-lakets-demo` as legacy/demo.

### Task 1.2: Data-model DDL + migration (TDD)
**Files:** Create `models/app_schema.py` (DDL for `pipeline_setup`, `pipeline_deployment`, `provisioned_resource`); modify `backend.py` (ensure-tables at startup); Test: `tests/test_app_schema.py`.
- [ ] Write tests asserting the three tables + FKs (`ON DELETE CASCADE`) + unique(`industry`), unique(`setup_id, mode`) exist after `ensure_*`.
- [ ] Implement DDL constants + `ensure_*` idempotent creators; remove dead `sliding_aggregates`/`session_events`/`state_snapshots` DDL.
- [ ] Run tests (against a local/branch Postgres) → pass → commit.

### Task 1.3: Repositories + CRUD (TDD)
**Files:** Create `services/pipeline_setup_repo.py`, `services/deployment_repo.py`, `services/resource_ledger.py`; Tests alongside.
- [ ] TDD: create/read/update setup + deployments; `record_resource(setup_id, deployment_id, type, identifier, parent_ref, teardown_order)`; `list_resources(setup_id)`; `teardown(setup_id)` returns resources in **descending teardown_order**.
- [ ] Implement; tests pass; commit.

### Task 1.4: Teardown orchestrator (TDD + integration)
**Files:** Create `services/teardown_service.py`; Test: `tests/test_teardown.py`.
- [ ] TDD the ordering + per-type delete dispatch (mockable); each delete marks the ledger row `deleted`, then FK cascade removes metadata. Verify nothing orphaned.

---

## Phase 2 — Provisioning engine (records every resource in the ledger)

### Task 2.1: Topic provisioning (boto3 MSK)
**Files:** Create `services/msk_service.py`; Test: `tests/test_msk_service.py`.
- [ ] Resolve ARN via `list_clusters`; `create_topic` for `iot.<industry>.realtime` + `.microbatch`; `RF=min(3,brokers)`; partition-increase-only on edit; idempotent (skip existing). Record each topic in the ledger.

### Task 2.2: UC catalog/schema/volume/metrics-table provisioning
**Files:** Modify `services/provisioning_service.py` (unify to canonical `iot_<industry>` + schemas `realtime/microbatch/zerobus/metrics/checkpoints`; external metrics table); record in ledger.

### Task 2.3: Lakebase pipelines project + per-industry branch + endpoints
**Files:** Modify `services/provisioning_service.py` / `services/pipeline_setup_service.py`: ensure shared `iot-pipelines` project; create per-industry never-expiring branch + `primary` RW + 1 read replica; record in ledger. `services/pipeline_db.py`: resolve reads to the **replica** endpoint, writes to primary.

### Task 2.4: Grants, SP, secrets
- [ ] Ensure shared SP `iot-sss-industries` + scope `iot-secrets`; UC grants (catalog/schema); metrics-table grants to writer SP + DBSQL reader; pre-flight checks. Record in ledger.

---

## Phase 3 — Schema contract (SSOT field-map)
**Files:** Create `models/sensor_schema.py` (the single field-map); refactor DDL + Kafka parse (`notebook-template.ts`) + INSERT + zerobus DDL + metrics schema to derive from it. Test: `tests/test_sensor_schema.py`.
- [ ] TDD: generating PG DDL, Delta DDL, Kafka `from_json` schema, and INSERT column list from one map; assert all 11 `SensorRecord` fields present (fixes the `min_value/…/metadata` drift). `metadata`→JSONB/VARIANT; `timestamp`→`event_time TIMESTAMPTZ`.

---

## Phase 4 — Validation harness + persisted state machine (Section 1)
**Files:** Create `services/validation_service.py`, `routes/validation.py`; Tests.
- [ ] Interactive cluster provisioning (90-min auto-term, smallest GP node, latest DBR, real-time mode, M1 worker sizing, same IAM/subnet/SG).
- [ ] Steps as a persisted state machine in `pipeline_setup.validation_state`: `provision_cluster → source_connectivity(both endpoints) → topics_created → schema_conformance`, then per-mode lanes (realtime incl. `partitions_precreate` 24h/96-chunk + Partition Manager kick + hard-gate → write_test → cdf_manual(waiting_admin) → **cdf_verify** (poll STREAMING+LSN) → read_back(Lakebase) → dbsql_read → metrics_check → cleanup; microbatch; zerobus).
- [ ] **Per-step retry** from failure point; **resume on reload**; **hard deploy gate**; test-data isolation (latest offsets + separate validation checkpoint); full cleanup.
- [ ] delta-rs metrics writer (SP OAuth → temp-table-creds), always-on.

---

## Phase 5 — Consumer/producer notebooks + 7-job deploy
**Files:** `client/src/lib/notebook-template.ts`, `data-producer-notebook.ts`, `services/deployment_service.py` / `routes/deployment.py`.
- [ ] Real-time consumer (Kafka→Lakebase/LakeTS via psycopg foreach, real-time trigger, unbatched producer). Micro-batch consumer (Kafka→Delta, `processingTime=10s`, batched). Zerobus producer (serverless, per-ack). 4 LakeTS ops jobs. Record all 7 jobs + checkpoints in the ledger. Deploy identities per mode (SP / user / zerobus SP).

---

## Phase 6 — Frontend wiring (replace prototype mocks with real endpoints)
**Files:** `client/src/pages/pipeline-setup-validation-page.tsx` + `setup-validation/*`, `pipeline-management-page.tsx`, `pipeline-health-page.tsx`, `industry-gallery-page.tsx`, contexts/api.
- [ ] Wire `/setup-validate` steps to the Phase-4 validation endpoints (SSE/poll for live state, retry, save-draft). Management list + health detail read real resource/health data. Role-gated admin controls; end-user simplified view + runtime health gate; draft "coming soon"; Delete on list + config pages only.

---

## Phase 7 — App deploy + end-to-end test-case suite

### Deploy
- [ ] Build client (`npm run build`), deploy the Databricks App; verify boot (DB connect, tables ensured, routes live).

### E2E test-case catalog (drafted — expand into executable checks)
**Backend/unit (pytest):**
1. Schema SSOT generates matching PG/Delta/Kafka/INSERT (no drift).
2. Source-key derivation + one-pipeline-per-industry uniqueness.
3. Resource-ledger CRUD + descending teardown order.
4. Teardown removes all resources + cascades metadata (no orphans).
5. Worker-sizing `clamp(ceil(p/cores),1,8)`; RF=min(3,brokers); partition-increase-only.
6. Validation state machine transitions incl. per-step retry + resume + hard gate.
7. Health rollup (worst-resource → overall; degraded on one failure).

**Integration (against Databricks):**
8. MSK topic create (both endpoints) + connectivity (DNS/TCP/TLS/auth).
9. Lakebase project/branch/replica create; reads hit replica, writes hit primary.
10. UC catalog/schemas/volume/metrics-external-table create + grants.
11. LakeTS ChronoTable + 24h partition pre-create + Partition Manager one-shot; insert outside window fails before, succeeds after.
12. delta-rs metrics write + DBSQL read; Databricks latency percentiles present.
13. CDF manual-enable → `cdf_verify` detects STREAMING + LSN advance; negative case fails.
14. Zerobus create_stream + ingest + DBSQL read.

**End-to-end (Pharmaceutical):**
15. Admin creates Pharmaceutical setup → validation runs all lanes green → deploy → real-time job streams Kafka→Lakebase→(CDF)→Delta; dashboard reads replica; metrics flowing.
16. Micro-batch: Kafka→Delta; DBSQL read.
17. Zerobus: serverless producer→Delta; DBSQL read.
18. End-user runtime health gate: healthy → starts; force a resource down → start blocked + "contact admin".
19. Save-draft → resume; end user sees draft as "coming soon".
20. Delete pipeline → all resources torn down (verify none remain in UC/MSK/Lakebase/Jobs) + metadata gone.

---

## Self-review notes
- Every spec section maps to a phase: §1.3–1.15 → Phases 2–5; §2.1–2.9 → Phases 1,2,6; validation state/retry/gate → Phase 4; health/management → Phase 6; deploy/tests → Phase 7.
- Risk front-loaded in Phase 0 (network/perms) per the user's "surface blockers" directive.
- Later phases are outlined at task granularity rather than micro-stepped because they depend on Phase-0 outcomes; each will be expanded into TDD steps by its implementing subagent at execution time.
