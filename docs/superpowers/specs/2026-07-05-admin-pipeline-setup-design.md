# Admin Pipeline Setup & Validation — Design Spec

**Date:** 2026-07-05
**Status:** In progress — Section 1 CLOSED; Section 2+ in progress
**Related:** `2026-07-05-industry-first-ui-design.md`, memory `project_industry_first_ui`

This spec defines the admin experience for creating, validating, and preparing
an industry pipeline for deployment. It is built section by section; each
section is locked before the next begins. The final document is the
implementation brief for the build agent.

---

## Glossary

- **Industry pipeline setup** — a single per-industry configuration created by an
  admin. One setup fans out to up to **three** deployable pipelines by *mode*.
- **Mode** — one of `realtime`, `microbatch`, `zerobus`. Selected by the
  end-user at start time (home page); each selected mode deploys its own
  end-to-end pipeline.
- **Flavor** — compute style for the Kafka-based Spark modes: **Classic** (built
  now) or **Serverless** (future). Applies to `realtime` + `microbatch` only.
  **Zerobus is always serverless** (producer-only; no flavor toggle).
- **Interactive (classic) cluster** — the validation harness. Everything is
  proven here first; it is the proxy for the deploy-time job cluster.
- **SSOT** — single source of truth. A declarative field-map that generates
  every artifact so schemas cannot drift.
- **LakeTS / ChronoTable** — the Lakebase time-series layer; native PG range
  partitioning (15-min chunks) over `sensor_readings`.

---

## Section 1 — Classic Pipeline Creation & Validation (CLOSED)

### 1.1 Goal

Before any pipeline is deployed, the admin validates the entire integration on
an interactive cluster (the proxy for the job cluster). Passing validation =
confidence that the deployed job cluster / serverless producer will work. All
three modes an industry could later start (`realtime`, `microbatch`, `zerobus`)
are validated up front, and test data is purged before finishing.

### 1.2 Flavor & uniqueness

- Pipeline preview asks **Classic vs Serverless**; **Classic only** is built now
  (Serverless deferred). The flavor choice applies to `realtime` + `microbatch`
  (they run a Spark consumer on a cluster). **Zerobus is always serverless — no
  toggle** (producer-only).
- **One pipeline setup per industry.** Creation is blocked if the industry
  already has a setup (edit of the existing one is allowed).

### 1.3 Interactive validation cluster (harness)

- Provisioned with `spark.databricks.streaming.realTimeMode.enabled=true`
  (reuses `routes/cluster.py`). Used for `realtime` + `microbatch` validation.
- **Spec:** smallest suitable general-purpose node (auto-selected via
  `list_node_types`, ~4 vCPU class); driver = worker node type; **latest stable
  DBR** that supports real-time mode; **90-minute auto-termination** (default &
  floor; admin may raise, not lower — inactivity-based, survives the active
  tests and tolerates an idle `cdf_manual` wait; re-provisions on resume if it
  terminates).
- **Same IAM role / instance profile, subnet, and security group** as the
  deploy-time job cluster, so the test is representative.
- **Worker count (M1):**
  `workers = clamp(ceil(num_partitions / cores_per_worker), min=1, max=8)`,
  guaranteeing `workers × cores_per_worker ≥ num_partitions` so every Kafka
  partition has a reader. If `num_partitions > 8 × cores_per_worker`, **warn**
  the admin rather than under-provision. Worker count is **shown to the admin**.
- Idempotent retry on failure; cluster is **explicitly stopped** on
  success/abandon.
- **Zerobus does not use this cluster** — its validation runs on serverless.

### 1.4 Source config (minimal input)

- Admin selects **Kafka or Kinesis** and provides **only `bootstrap_servers`**
  (Kafka).
- Derived/pulled in code (previously hard-coded in the reference notebook):
  **region** (from bootstrap hostname), MSK **cluster ARN** (via
  `msk.list_clusters()` matched on cluster name), **replication factor**
  (`RF = min(3, broker_count)`), and the **UC service credential** already mapped
  to the AWS IAM role.

### 1.5 Pre-flight checks

- IAM role backing the service credential has MSK permissions
  (`kafka:ListClusters/DescribeCluster`, `kafka-cluster:*` incl. topic create).
- Running identity has **UC `ACCESS`** on the service credential.
- Per-mode write-identity grants exist (see §1.9).
- Failures produce a clear message naming the missing grant/permission.

### 1.6 Setup Preview screen

A read-only preview before validation runs, showing exactly what will be built:
compute (node type, **# workers + why**, driver, DBR, 90-min auto-terminate),
source (type, `bootstrap_servers`, derived region/ARN/RF, service credential),
topics (`iot.<industry>.realtime` + `.microbatch`, partition counts), schema
(the SSOT columns), real-time targets (Lakebase + LakeTS in `databricks_postgres`,
24h partition horizon, CDF plan, DBSQL warehouse). The live validation view
mirrors this layout with per-step status.

### 1.7 Source connectivity test (on the cluster, UC service credential)

DNS resolution → TCP port reachability → SSL/TLS handshake → auth. On failure,
**surface the cluster egress IP** (per the Boto3 MSK notebook pattern) as the
diagnostic. Security-group / VPC provisioning is **out of scope** (assumed
pre-configured).

### 1.8 Topic provisioning (Boto3 MSK API)

- Create **two Kafka topics** — `iot.<industry>.realtime` and
  `iot.<industry>.microbatch` — via `boto3` MSK `kafka.create_topic(ClusterArn,
  TopicName, PartitionCount, ReplicationFactor)`, authenticated from the UC
  service credential (`boto3.Session(botocore_session=dbutils.credentials
  .getServiceCredentialsProvider(<cred>), region_name=<region>).client("kafka")`).
  Requires `%pip install --upgrade boto3 botocore` (MSK `create_topic` is newer
  than DBR-bundled boto3).
- Zerobus is non-Kafka → **no topic**.
- **Partition count is an admin input** and drives worker sizing (M1).
- On edit, allow partition **increase only** (Kafka cannot decrease); `RF =
  min(3, broker_count)`.
- Validate both topics exist before proceeding.

### 1.9 Schema contract (SSOT field-map)

A single declarative field-map (`SensorRecord field → column, PG/Delta type,
transform`) generates **five** artifacts so nothing can drift:

1. The real-time ChronoTable DDL,
2. The real-time INSERT column list,
3. **Both** Kafka JSON parse schemas (realtime *and* microbatch),
4. The zerobus target Delta table DDL,
5. The zerobus record dict.

- Schema is **industry-invariant** — industry only changes row values
  (`sensor_name`, `value`, `unit`, `sensor_type`), never columns.
- **Fixes today's drift bug:** the streaming writer inserts
  `min_value/max_value/nominal_value/metadata`, which the current
  `SENSOR_READINGS_DDL` (9 cols) lacks, and the micro-batch parse schema drops
  the same fields → silent data loss. The SSOT reconciles all of them.
- Exact column types (e.g. `metadata` as `JSONB`, epoch `timestamp` →
  `event_time TIMESTAMPTZ`) are resolved at implementation (E2).
- All per-pipeline PG objects live in the **`databricks_postgres`** database (the
  only DB name Lakehouse Sync supports).

### 1.10 Per-mode validation (validate all three — F1)

A **schema-conformance gate** runs first: produce sample records for the selected
industry → write into the freshly-created table → assert columns line up and the
write succeeds. Then, per mode:

**Real-time** — Kafka → Spark real-time → Lakebase + LakeTS → CDF → Delta.
Write identity = **streaming SP** (via OAuth).
1. **Partition pre-create guarantee (Approach A):** before any write,
   synchronously `lakets._ensure_partitions` a **24h / 96-chunk** forward window
   (configurable); **deploy the Partition Manager job and trigger one run
   immediately**; **hard-gate** on partitions covering `[now, now+horizon]`. The
   maintained forward horizon is widened from ~2h (`p_future_count := 8`) to ~24h.
   *(Optional defensive net C — writer catches "no partition" → ensure → retry —
   deferred/optional.)*
2. Simulator produces to `.realtime`; Spark real-time SS writes to Lakebase + LakeTS.
3. **CDF admin option shown before running:** the app preps the shadow table +
   trigger in the `lakets_cdf` schema; the admin **manually enables** the managed
   Lakebase→Delta sync in the Databricks UI; we then verify `STREAMING` status.
4. **Read-back validation:** read from **Lakebase directly** for the immediate
   round-trip (CDF sync is async); validate the Delta path by **polling
   `committed_lsn` / `STREAMING`** with a timeout, then **DBSQL-read** the synced
   Delta.
5. **Cleanup:** empty test data (keep objects); delete validation checkpoints.

**Micro-batch** — Kafka → Spark micro-batch → Delta (UC), no Lakebase.
Write identity = **deploying user** (`single_user_name`).
1. Provision catalog `iot_<industry>` + schema `microbatch` + checkpoint UC
   Volume.
2. Bounded consumer run on the interactive cluster (reuse
   `POST /api/cluster/{id}/validate` 20s bounded-batch harness) → auto-create +
   write Delta `iot_<industry>.microbatch.<table>` via a **separate validation
   checkpoint**; trigger = `.trigger(processingTime="10 seconds")`.
3. Simulator produces to `.microbatch`; assert rows land → **DBSQL-read** the
   Delta table.
4. **Cleanup:** empty rows (keep table); delete validation checkpoint.

**Zerobus** — simulator → `ZerobusSink` → UC Delta via SDK. Serverless,
producer-only; **no cluster to proxy**. Write identity = **zerobus SP**.
1. Provision catalog `iot_<industry>` + schema `raw`; ensure secrets
   `zerobus-client-id`/`-secret` in `iot-secrets`; ensure `_grant_zerobus_sp`
   grants (`USE_CATALOG`; `USE_SCHEMA/MODIFY/SELECT/CREATE_TABLE`).
2. **Create the target Delta table** `iot_<industry>.raw.iot_sensor_data` from the
   SSOT (**new — not auto-created today**; Zerobus `create_stream` requires the
   table to pre-exist).
3. Auth/endpoint probe: mint OAuth against `https://<workspace_url>/oidc/v1/token`
   (**https:// prefix** is the known gotcha) and open a `ZerobusSdk` stream to
   `<ws-id>.zerobus.<region>.cloud.databricks.com` + `create_stream`.
4. Bounded serverless producer run → ingest sample records → `wait_for_ack()`
   confirms durability → assert rows → **DBSQL-read**.
5. **Cleanup:** cancel the serverless run; empty rows but **keep the table +
   grants + secrets**.

### 1.11 Streaming metrics (real-time + micro-batch only)

- **Capture:** `StreamingQueryListener.onQueryProgress` → **delta-rs
  `write_deltalake` (append)**, decoupled from the Spark sink; identical for both
  Spark modes. Replaces the Spark `saveAsTable` path and the zerobus-metrics
  branch (standardize on delta-rs). Zerobus has no metrics (`has_metrics=False`).
- **Auth (H1/H3):** metrics-writer **SP OAuth → UC temporary-table-credentials
  (READ_WRITE) → S3 creds** into delta-rs `storage_options`.
- **Table (H1):** UC **external** Delta table `iot_<industry>.metrics
  .streaming_metrics` (per-industry catalog; rows tagged by `query_name`/
  `query_id` to distinguish modes — per-mode split revisited under G11/Section 2).
  External (not managed) so delta-rs commits are clean (managed tables may use
  coordinated commits delta-rs can't do).
- **Schema (H2 — metrics SSOT):** extracted typed columns (`batch_id, timestamp,
  num_input_rows, input_rps, processed_rps, trigger_ms, e2e_p50/p99,
  processing_p50/p99, queuing_p50/p99`) **+ raw `progress` as a VARIANT column**.
  Fixes the `metrics_json` vs `progress` column-name inconsistency between the
  generated notebook and the DDL/read endpoint.
  - ⚠️ **Verify at implementation:** delta-rs may not yet write the VARIANT
    logical type. Fallback: write raw as JSON **STRING** and expose VARIANT via
    `parse_json(...)` in a view/computed column. Reads are unaffected because the
    typed columns carry the metrics.
- **Grants (H3):** `MODIFY, SELECT` + `USE CATALOG/SCHEMA` to the metrics-writer SP
  and the DBSQL reader; pre-flight check (fixes today's zerobus-only grant).
- **Validation (S6):** after each bounded real-time/micro-batch run, assert
  metrics rows landed, **verify the Databricks-runtime latency percentiles
  appear** (`e2eLatencyMs`/`processingLatencyMs`/`sourceQueuingLatencyMs` are
  Databricks-only — proves the delta-rs + OAuth-SP + grants path end-to-end),
  **DBSQL-read** via `GET /api/pipeline/{id}/streaming-metrics`; cleanup empties
  rows (keep table).

### 1.12 Finish & resumability

- **Show the final consumer notebook** — exactly what the job cluster will run
  (the same generated artifact deploy uses, not a separate preview).
- **Gate:** proceed to the next step only when all enabled modes pass and test
  data is purged.
- **Resumable validation state (M3):** a per-pipeline state machine (survives
  browser reload) with per-step status (`pending | running | passed | failed |
  waiting_admin`) + last error + `cluster_id` + `run_ids`:
  ```
  provision_cluster → source_connectivity → topics_created → schema_conformance
    ├─ realtime:  partitions_precreate → write_test → cdf_manual(waiting_admin)
    │             → read_back → dbsql_read → metrics_check → cleanup
    ├─ microbatch: write_test → dbsql_read → metrics_check → cleanup
    └─ zerobus:   create_table → auth_probe → produce_test → dbsql_read → cleanup
  → show_final_notebook → done
  ```
  The `cdf_manual` step parks in `waiting_admin` and survives reload.

### 1.13 Cleanup rule (all modes)

Keep objects/tables/grants/secrets; **empty data**; delete validation
checkpoints. The deploy-time job/producer performs real ingestion into the same
objects.

### 1.14 Topic naming

`iot.<industry>.<mode>` (dot notation). No `.`/`_` metric-name collision because
only dotted variants are ever created.

---

## Decisions log (Section 1)

| ID | Decision |
|----|----------|
| F1 | Validate all three modes; admin may toggle a mode off to skip (then it isn't startable). |
| E1 | Field-map SSOT generates DDL + parse schemas + INSERT + zerobus record (prevents drift). |
| E2 | Exact column types deferred to implementation. |
| M1 | Worker sizing `clamp(ceil(partitions/cores_per_worker),1,8)`; warn if over cap. |
| M2 | Keep dot topic naming. |
| M3 | Resumable per-pipeline validation state machine. |
| A  | Partition guarantee: proactive pre-create + guaranteed manager run + pre-flight gate; 24h/96-chunk horizon (configurable); optional writer-side retry (C). |
| G1 | Per-pipeline objects in `databricks_postgres` (Lakehouse Sync constraint). |
| G2 | Validation uses the same IAM/subnet/SG and **per-mode write identity** as deploy. |
| G3 | Pre-flight IAM MSK perms + UC `ACCESS` on the service credential. |
| G4/G10 | Test-data isolation via latest-offset semantics + a distinct validation checkpoint discarded at cleanup. |
| G5 | Real-time read-back from Lakebase directly; Delta validated by polling committed LSN/STREAMING. |
| G6 | Cleanup covers all sink tables + checkpoints. |
| G7 | Idempotent-retry on failure; stop cluster on finish; 90-min auto-terminate. |
| G8 | Partition-increase-only on edit; `RF=min(3,brokers)`. |
| G9 | **Struck** — trigger intervals were correct (`realTime="5 minutes"` vs `processingTime="10 seconds"`). |
| H1 | Metrics table = UC external Delta; creds via UC temporary-table-credentials. |
| H2 | Metrics schema = typed columns + raw `progress` VARIANT (STRING+parse_json fallback). |
| H3 | Metrics grants to writer SP + DBSQL reader; pre-flight. |
| S1–S6 | Streaming-metrics capture via delta-rs for real-time + micro-batch; validated on the harness. |

## Parked for later sections

- **G11** — Data model: one industry *setup* fans out to up to three deployed
  *pipelines* (realtime/microbatch/zerobus). One `pipelines` row or several?
  Also whether the metrics table splits per-mode. → Section 2.
- **G12** — Serverless code coexistence: the current connectivity path is
  serverless; do we hide the serverless option in the UI and leave the code
  dormant, or deprecate/remove it? → Section 2/later.

---

## Section 2 — Pipeline Catalog Structure & Application Database (IN PROGRESS)

_To be written._
