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
  `list_node_types`, ~4 vCPU class); driver = worker node type; **90-minute
  auto-termination** (default & floor; admin may raise, not lower —
  inactivity-based, survives the active tests and tolerates an idle `cdf_manual`
  wait; re-provisions on resume if it terminates).
- **Runtime (RTM) — CONFIRMED 2026-07-05:** DBR **`18.2.x-scala2.13`** (Spark
  4.1.0; latest 18.x GA — 19.x is beta), **DEDICATED / single-user** access mode,
  **Photon OFF, autoscaling OFF, spot OFF**, node sized so task slots ≥ total
  query tasks. Applies to **both** the interactive validation cluster **and** the
  deploy-time RTM **job cluster** (must match — §G2). **Service credentials verified
  working on 18.2 + single-user** (the earlier failure was the 17.3 *beta*), so RTM
  streaming and service-credential MSK access share one classic job cluster.
- **RTM Spark conf (verified):** `spark.databricks.streaming.realTimeMode.enabled=true`
  — plus, **only if the query does stream-stream joins**:
  `spark.databricks.streaming.realTimeMode.streamStreamJoin.enabled=true`,
  `spark.sql.streaming.join.stateFormatVersion=4`,
  `spark.sql.streaming.join.stateFormatV4.enabled=true`,
  `spark.sql.streaming.stateStore.rocksdb.mergeOperatorVersion=2`,
  `spark.databricks.streaming.realTimeMode.controlMessage.enabled=true`.
- **RTM trigger (verified):** `.outputMode("update").trigger(realTime="<interval>")`
  — output mode **must** be `update`; the interval is the commit/checkpoint
  interval, not batch latency.
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

- Source type: **Kafka only** is built now. **Kinesis is deferred — shown as
  "coming soon"** (disabled), same as the Serverless flavor. Kinesis has a
  different config field set; it will be designed when we build it.
- For Kafka, the admin provides **only `bootstrap_servers`**.
- Derived/pulled in code (previously hard-coded in the reference notebook):
  **region** (from bootstrap hostname), MSK **cluster ARN** (via
  `msk.list_clusters()` matched on cluster name), **replication factor**
  (`RF = min(3, broker_count)`), and the **UC service credential** already mapped
  to the AWS IAM role (concrete value: `iot-rtm-sc`, MSK cluster `iotdemo`,
  us-east-1).

### 1.5 Pre-flight checks

- IAM role backing the service credential has MSK permissions
  (`kafka:ListClusters/DescribeCluster`, `kafka-cluster:*` incl. topic create).
- Running identity has **UC `ACCESS`** on the service credential.
- **Service-credentials compute requirement (verified via docs 2026-07-05):**
  `dbutils.credentials.getServiceCredentialsProvider(...)` needs **DBR ≥ 16.2
  (GA)** on an **all-purpose/jobs classic cluster** in **single-user / dedicated**
  access mode — NOT serverless, NOT SQL warehouses, and **not a 17.x beta
  runtime** (the beta build returned "Service credentials are not enabled in this
  workspace"). The validation cluster (§1.3) must use a GA/LTS runtime ≥16.2.
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
pre-configured). **Verified 2026-07-05:** on `iotdemo`, only the **public
`:9198`** endpoint is reachable (DNS/TCP/TLS OK, egress IP already allowed on the
MSK SG); the **private `:9098`** brokers **time out** (no VPC peering). So the
pipeline uses the **public `:9198`** bootstrap. The connectivity test still probes
both and reports which is reachable.

### 1.8 Topic provisioning (Kafka AdminClient via MSK-IAM)

> **Revised 2026-07-05 (spike-confirmed): use the Kafka `KafkaAdminClient`, NOT
> the boto3 MSK `create_topic` API.** The boto3 MSK API needs the cluster ARN, and
> `kafka:ListClusters`/`DescribeCluster` are **denied** on the `taran-msk` role, so
> the ARN can't be auto-resolved. The AdminClient path needs only bootstrap +
> OAUTHBEARER and was proven end-to-end (create + produce + consume). *(Alt: add
> `kafka:ListClusters`+`DescribeCluster` to the role to keep the boto3 API.)*

- Create **two Kafka topics** — `iot.<industry>.realtime` and
  `iot.<industry>.microbatch` — via **`kafka.admin.KafkaAdminClient`** (kafka-python)
  with `security_protocol=SASL_SSL`, `sasl_mechanism=OAUTHBEARER`, and an
  **MSK-IAM token provider** built from the UC service credential `iot-rtm-sc`.
  kafka-python 3.0.7: the token provider **must subclass
  `kafka.net.sasl.oauth.AbstractTokenProvider`**.
- **Bootstrap = public `:9198`** (the private `:9098` brokers are unreachable from
  this workspace — no VPC peering; see §1.7).
- **boto3:** use the **stock runtime boto3** — do **NOT** `pip install --upgrade
  boto3 botocore` on DBR 16.4 LTS (it breaks the kernel with a `botocore.httpchecksum`
  import error). Install only `kafka-python` + `aws-msk-iam-sasl-signer-python`
  with `--no-deps`.
- Zerobus is non-Kafka → **no topic**.
- **Partition count is an admin input** and drives worker sizing (M1). `RF =
  min(3, broker_count)`. On edit, allow partition **increase only**.
- **Teardown caveat:** deleting a topic needs `kafka-cluster:DeleteTopic` on the
  `taran-msk` role — **currently denied**. Until it's added, the ledger records
  the topic but topic-delete on teardown is **skipped/flagged** (manual cleanup)
  rather than failing the teardown.
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

> **E2E write path VERIFIED 2026-07-05 (spike, DBR 18.2 RTM):** RTM → psycopg
> `ForeachWriter` → Lakebase **LakeTS ChronoTable** landed **375 rows**,
> partition-routed, no "no partition" errors; the buffered **delta-rs metrics**
> listener landed batches (DBSQL `parse_json` reads OK). **Build constraints
> found:** (1) **RTM cannot read the `iot_simulator` PySpark data source**
> (`STREAMING_REAL_TIME_MODE.INPUT_STREAM_NOT_SUPPORTED`) — RTM accepts only
> **Kafka/MSK or rate**, so the real-time consumer **reads the Kafka topic** (the
> producer writes sensor data to the topic; the old `format("iot_simulator")`
> reference notebook is dead for RTM). (2) The `sensor_readings` PK must use an
> **independent sequence, not `BIGSERIAL`** (BIGSERIAL blocks `create_chronotable`).
> (3) Install **`databricks-sdk==0.120.0`** as a cluster library on 18.2 (bundled
> SDK lacks `w.postgres`). (4) RTM `ForeachWriter.process()` receives a
> **positional tuple**, not a named Row. (5) MSK is reached via `iotdemo` **public
> `:9198`** from `fe-vm-shared-interop` — not a separate sandbox workspace.
>
> **Full MSK→RTM→Lakebase loop VERIFIED 2026-07-05 (200 rows + metrics landed).**
> Exact RTM Kafka read config (no `kafka.sasl.*`/`security.protocol` — the service
> credential handles MSK-IAM): `spark.readStream.format("kafka")` +
> `.option("kafka.bootstrap.servers", "<b-1/2/3-public.iotdemo…:9198>")` +
> `.option("subscribe", topic)` + `.option("startingOffsets","earliest")` +
> `.option("databricks.serviceCredential", "iot-rtm-sc").load()`; writer
> `.outputMode("update").foreach(LakebaseWriter).trigger(realTime="15 seconds")`.
> Producer (kafka-python): `SASL_SSL`/`OAUTHBEARER`, token provider subclassing
> `AbstractTokenProvider` → `MSKAuthTokenProvider.generate_auth_token("us-east-1")`.
> **Gotcha:** `getServiceCredentialsProvider(name)` returns a botocore **Session** —
> call `provider.get_credentials().get_frozen_credentials()` directly (also for
> delta-rs S3 creds). 18.2 cluster libs: `databricks-sdk==0.120.0`,
> `kafka-python>=3`, `aws-msk-iam-sasl-signer-python`, `psycopg2-binary`,
> `deltalake`. Orphan test topics on `iotdemo` (DeleteTopic denied → manual
> cleanup): `iot.spike.338409`, `iot.e2e.a7ff7d`.

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
3. **CDF admin option, then active verification (`cdf_verify`):** the app preps
   the shadow table + trigger in the `lakets_cdf` schema; the admin **manually
   enables** the managed Lakebase→Delta sync in the Databricks UI and clicks
   "I've enabled it". We do **not** trust the click — a `cdf_verify` step
   **actively tests** CDF is live: poll `wal2delta.tables` for `STREAMING` **and**
   confirm `committed_lsn` advances (data actually flowing shadow → Delta), with a
   timeout. If not live, `cdf_verify` **fails** with a clear message ("CDF not
   detected on `lakets_cdf` — enable managed sync and retry") and is **retryable**
   (§1.12).
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

> **Approach (locked 2026-07-05, spike-confirmed): buffered async write via
> delta-rs to a UC EXTERNAL Delta table — NOT `saveAsTable`.** `saveAsTable` from
> the listener launches a Spark job that **contends with / hinders the streaming
> query**, so it is disallowed. delta-rs is pure-Python (no Spark job → no
> streaming interference); the listener buffers rows and a background thread
> flushes batches. Trade-off (accepted): delta-rs can't write VARIANT, so
> `progress` is `STRING` + `parse_json` at read; reads go through DBSQL (delta-rs
> can't read `deletionVectors` tables). The delta-rs external-table write path is
> already spike-confirmed end-to-end.

- **Capture:** `StreamingQueryListener.onQueryProgress` **buffers** each progress
  row into an in-memory queue and returns immediately — **no Spark work in the
  callback**. A **background daemon thread** flushes batches via **delta-rs
  `write_deltalake(..., mode="append")`** to the external metrics table (pure
  Python, no Spark job → never contends with the streaming query). **Always on**
  for both Spark modes — the home-page `metrics_enabled` toggle is **removed**.
  Zerobus has no metrics (`has_metrics=False`).
- **Table:** UC **EXTERNAL** Delta table `iot_<industry>.metrics.streaming_metrics`
  at `s3://taran-shared-interop/iot-digital-twin/<catalog>/metrics/streaming_metrics/`.
  Per-industry catalog; rows tagged by `query_name`/`query_id` to distinguish
  modes (per-mode split revisited under G11/Section 2).
- **Auth:** metrics-writer **SP OAuth → UC temporary-table-credentials
  (READ_WRITE) → S3 creds** into delta-rs `storage_options`, plus
  `AWS_REGION=us-east-1` + `AWS_S3_ALLOW_UNSAFE_RENAME=true`; regenerate temp
  creds **per flush batch** (they expire).
- **Schema (metrics SSOT):** extracted typed columns (`batch_id, timestamp,
  num_input_rows, input_rps, processed_rps, trigger_ms, e2e_p50/p99,
  processing_p50/p99, queuing_p50/p99`) **+ `progress` as a `STRING` column**
  (delta-rs can't write VARIANT); read with `parse_json(progress):field` via
  DBSQL. Fixes the `metrics_json` vs `progress` column-name inconsistency.
- **Grants:** metrics-writer SP needs `MODIFY, SELECT` + `USE CATALOG`/`USE SCHEMA`
  **+ `EXTERNAL USE SCHEMA`** (required for `GenerateTemporaryTableCredentials`);
  DBSQL reader gets `SELECT`/`USE`; pre-flight check.
- **Validation (S6):** after each bounded real-time/micro-batch run, assert
  metrics rows landed, **verify the Databricks-runtime latency percentiles
  appear** (`e2eLatencyMs`/`processingLatencyMs`/`sourceQueuingLatencyMs` are
  Databricks-only — proves the delta-rs external-table metrics-write path end-to-end),
  **DBSQL-read** via `GET /api/pipeline/{id}/streaming-metrics`; cleanup empties
  rows (keep table).

### 1.12 Finish & resumability

- **Show the final consumer notebook** — exactly what the job cluster will run
  (the same generated artifact deploy uses, not a separate preview).
- **Hard deployment gate:** the admin **cannot proceed to deploy** until **every**
  step for **every enabled mode** is `passed` **and** test data is purged. The
  Deploy action stays disabled with a summary of what's still outstanding.
- **Resumable, retryable state machine (M3):** a per-pipeline state machine
  **persisted in `pipeline_setup.validation_state`** (Section 2), so it survives
  browser/tab close. Per-step status (`pending | running | passed | failed |
  waiting_admin`) + last error + `cluster_id` + `run_ids`:
  ```
  provision_cluster → source_connectivity → topics_created → schema_conformance
    ├─ realtime:  partitions_precreate → write_test → cdf_manual(waiting_admin)
    │             → cdf_verify → read_back → dbsql_read → metrics_check → cleanup
    ├─ microbatch: write_test → dbsql_read → metrics_check → cleanup
    └─ zerobus:   create_table → auth_probe → produce_test → dbsql_read → cleanup
  → show_final_notebook → done
  ```
- **Per-step retry from the failure point:** any `failed` step exposes a **Retry**
  action that re-runs **only that step and its downstream** (upstream `passed`
  steps are not re-run). The admin fixes the backend cause first (missing grant,
  MSK SG rule, CDF not enabled, IAM) and retries — no full restart. `cdf_manual`
  parks in `waiting_admin`; `cdf_verify` is the common failure→fix→retry point.
- **Config edits mid-flow:** the admin can return to earlier steps (flavor /
  source / preview) and **edit config at any time**; saving persists to
  `pipeline_setup` and **invalidates** (resets to `pending`) any validation steps
  whose inputs changed, so they re-run before the gate can clear.
- **Resume on reopen:** reopening the pipeline rehydrates `validation_state` and
  resumes exactly where it left off (including a parked `waiting_admin`).

### 1.13 Cleanup rule (all modes)

Keep objects/tables/grants/secrets; **empty data**; delete validation
checkpoints. The deploy-time job/producer performs real ingestion into the same
objects.

### 1.14 Topic naming

`iot.<industry>.<mode>` (dot notation). No `.`/`_` metric-name collision because
only dotted variants are ever created.

### 1.15 Data rate & producer generation semantics

- **Rate units:** the generation rate is configured/shown as **records/sec**
  (NOT Hz), with an **estimated MB/s** displayed alongside (derived from the
  average `SensorRecord` size). This replaces the old `update_rate_hz` Hz field
  everywhere it surfaces (setup preview, home-page config, `pipeline_setup`).
- **Real-time mode:** the producer generates and **flushes each row as fast as
  possible — unbatched** (producer `linger.ms ≈ 0`, no client-side batching), for
  lowest end-to-end latency; the consumer uses the real-time trigger. Rows land
  in Lakebase/LakeTS individually, not in batches.
- **Micro-batch mode:** the producer **batches** records before flushing, and the
  consumer uses `.trigger(processingTime="10 seconds")`. Throughput over latency.
- **Zerobus mode:** producer flushes per `wait_for_ack()` batch to Delta (its own
  cadence; no Kafka).
- Stored on `pipeline_setup` as `records_per_sec` (+ derived `est_mb_per_sec`);
  no metrics toggle is stored (metrics always on — §1.11).

## Decisions log (Section 1)

| ID | Decision |
|----|----------|
| F1 | Validate all three modes; admin may toggle a mode off to skip (then it isn't startable). |
| E1 | Field-map SSOT generates DDL + parse schemas + INSERT + zerobus record (prevents drift). |
| E2 | Exact column types deferred to implementation. |
| M1 | Worker sizing `clamp(ceil(partitions/cores_per_worker),1,8)`; warn if over cap. |
| M2 | Keep dot topic naming. |
| M3 | Resumable per-pipeline validation state machine. |
| Metrics-default | Streaming metrics **always on** for Spark modes; home-page `metrics_enabled` toggle removed. |
| Rate-units | Generation rate in **records/sec** (+ est. MB/s), not Hz; stored as `records_per_sec`. |
| Gen-semantics | Real-time = flush each row ASAP, **unbatched**; micro-batch = **batched**. |
| Kinesis-defer | Kafka only now; **Kinesis "coming soon"** (different field set), like Serverless flavor. |
| M3+ | Active `cdf_verify` (test STREAMING + LSN advance, not just the click); per-step **Retry** from failure point (fix backend → retry that step + downstream); state persisted in `pipeline_setup.validation_state` (survives tab close); config editable mid-flow (invalidates affected steps); **hard gate** — no deploy until all enabled-mode steps pass + data purged. |
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
| H1 | Metrics table = UC **external** Delta on `taran-shared-interop`; creds via UC temp-table-credentials. |
| H2 | Metrics schema = typed columns + `progress` **STRING** (delta-rs can't VARIANT; `parse_json` at read). |
| H3 | Metrics grants: writer needs MODIFY/USE **+ `EXTERNAL USE SCHEMA`**; reader SELECT; pre-flight. |
| S1–S6 | Streaming metrics: listener **buffers → background thread → delta-rs append to an external table** (NOT `saveAsTable` — avoids hindering the streaming query); validated on the harness. |

## Parked for later sections

- **G11** — Data model: one industry *setup* fans out to up to three deployed
  *pipelines* (realtime/microbatch/zerobus). One `pipelines` row or several?
  Also whether the metrics table splits per-mode. → Section 2.
- **G12** — Serverless code coexistence: the current connectivity path is
  serverless; do we hide the serverless option in the UI and leave the code
  dormant, or deprecate/remove it? → Section 2/later.

---

## Section 2 — Pipeline Catalog Structure & Application Database (PARKED — locked)

### 2.1 Two separate Lakebase projects

The application database and the pipeline streaming data live in **two distinct
Lakebase projects** — never the same project.

| Project | Purpose | Endpoints |
|---|---|---|
| **`iot-app-backend`** (brand new, dedicated) | Application metadata store only | Single endpoint; **no read replicas** (low-volume CRUD) |
| **`iot-pipelines`** (shared) | Real-time streaming data for all real-time industry pipelines | Per-branch: **primary `READ_WRITE`** + **1 read replica** |

Micro-batch and zerobus use **neither** — they write to UC Delta. Today's
`iot-lakets-demo` double-duty (app state + demo sink in one instance) is
retired: app state → `iot-app-backend`, streaming data → `iot-pipelines`.

### 2.2 Branch & endpoint model (real-time only)

- **One never-expiring branch per real-time industry pipeline** in
  `iot-pipelines` (branch name = full industry name, e.g. `pharmaceutical`).
- Endpoints per branch: **primary `READ_WRITE`** (the streaming job writes) +
  **1 read replica** (default; the app/dashboard reads route here, never the
  write path).
- The branch's `databricks_postgres` DB holds the LakeTS ChronoTable, rollups,
  the `lakets` registry, and `lakets_cdf` shadow tables.
- Micro-batch/zerobus get **no branch**.

**Verified (2026-07-05 Lakebase spike — no permission errors):** creating a
branch **auto-provisions its own `primary` READ_WRITE endpoint** — do NOT create
it; resize via `update_endpoint` + `FieldMask(["spec.autoscaling_limit_min_cu",
"spec.autoscaling_limit_max_cu"])`. The read replica is a separate
`w.postgres.create_endpoint(parent="projects/{pid}/branches/{b}", endpoint=
Endpoint(spec=EndpointSpec(endpoint_type=ENDPOINT_TYPE_READ_ONLY, ...)),
endpoint_id="replica")` (~2s to ACTIVE, ~6s replication lag, writes correctly
rejected). OAuth token: `w.postgres.generate_database_credential(endpoint=
"projects/.../endpoints/primary")` — pass the **resource name, not the host**.
Limits: 10 unarchived branches/project, 500 branches/project, 20 concurrent
active computes/workspace (→ watch the compute cap as pipelines scale).

### 2.3 App DB data model (Option B — parent/child + normalized ledger)

Tables in `iot-app-backend`:

```
pipeline_setup            -- one row per industry (UNIQUE industry)
  id, industry, flavor, source_type, bootstrap_servers,
  derived{region, cluster_arn, replication_factor}, service_credential,
  enabled_modes[], cluster_spec{}, warehouse_id,
  validation_state{}   (M3 state machine), status, user_email,
  created_at, updated_at

pipeline_deployment       -- one row per (setup, mode); UNIQUE(setup_id, mode)
  id, setup_id → pipeline_setup (FK, ON DELETE CASCADE),
  mode (realtime | microbatch | zerobus), status,
  partition_count, cdf_enabled, metrics_config{},
  lakebase_branch, lakebase_endpoints{primary, replica}   (realtime only),
  delta_table, checkpoint_path                            (microbatch/zerobus),
  job_id, notebook_path, created_at, updated_at

provisioned_resource      -- normalized teardown ledger
  id, setup_id → pipeline_setup (FK, ON DELETE CASCADE),
  deployment_id → pipeline_deployment (FK, nullable),
  resource_type (uc_catalog | uc_schema | uc_table | uc_volume | kafka_topic |
    lakebase_branch | lakebase_endpoint | lakebase_read_replica | job |
    secret_scope | secret | service_principal | grant | cdf_sync |
    metrics_table),
  identifier, parent_ref, teardown_order, status, metadata{}, created_at
```

Retained: `user_settings` (per-user + app-global `__app__` config) and
`source_bindings` (source-claim uniqueness). **Removed** (dead/unused):
`sliding_aggregates`, `session_events`, `state_snapshots`.

### 2.4 CRUD lifecycle

- **Create** → insert `pipeline_setup` → insert a `pipeline_deployment` per
  enabled mode → record each provisioned resource as a `provisioned_resource`
  row (with `teardown_order` + `parent_ref`) as it is created.
- **Edit** → update rows; append new resources, mark obsolete ones.
- **Delete** → read the ledger → tear down resources in **descending
  `teardown_order`** (reverse dependency) → FK `ON DELETE CASCADE`
  (setup → deployment → resource) removes the metadata. Nothing orphaned.
- The ledger is the SSOT used **both** to delete and to (re)create resources.

### 2.5 Canonical UC catalog structure

One catalog per industry — `iot_<industry>` — with a fixed schema set (drops the
competing `iot_<safe_name>_<short_id>` / `iot_shared` / `iot_{name}` variants):

```
iot_<industry>/
├── realtime/     sensor_readings   Delta   ← CDF-synced mirror of the Lakebase ChronoTable (historical)
├── microbatch/   sensor_data       Delta   ← micro-batch bronze output
├── zerobus/      iot_sensor_data   Delta   ← zerobus SDK ingest target (pre-created from SSOT)
├── metrics/      streaming_metrics Delta   ← EXTERNAL, delta-rs writes (realtime + microbatch, mode-tagged)
└── checkpoints/  stream_checkpoints Volume ← SS checkpoints; sub-paths /realtime, /microbatch
```

Real-time **live** data is in the Lakebase branch; `realtime.sensor_readings` is
the CDF-synced **historical** copy for DBSQL.

**Provisioning note (verified 2026-07-05):** this metastore has no storage root
(Default Storage is UI-only), so `CREATE CATALOG` via SQL/API **must supply an
explicit `MANAGED LOCATION`** — use `s3://taran-shared-interop/<catalog>` (external
location `taran-shared-interop`). The `metrics.streaming_metrics` external Delta
table also lives under `taran-shared-interop` (e.g.
`s3://taran-shared-interop/iot-digital-twin/<catalog>/metrics/streaming_metrics/`).
Schemas, managed volumes, external tables, and SP grants (`iot-sss-industries`,
appId `9927a7fa-2427-4b6a-97b6-cd41b878d405`) all confirmed working.

### 2.6 Job inventory (per fully-enabled industry = 7 jobs)

| Mode | Job | Type | Tasks | Schedule |
|---|---|---|---|---|
| Real-time | `iot-<industry>-realtime` | Continuous, job cluster | producer + consumer (→ Lakebase/LakeTS + delta-rs metrics) | unpaused |
| Real-time | `LakeTS Partition Manager — …` | Serverless | ensure partitions | every 10 min |
| Real-time | `LakeTS Rollup Refresh — …` | Serverless | refresh rollups | every 15 min |
| Real-time | `LakeTS Tiering — …` | Serverless | evict cold chunks → Delta | every 30 min |
| Real-time | `LakeTS Retention — …` | Serverless | retention | daily 03:00 |
| Micro-batch | `iot-<industry>-microbatch` | Continuous, job cluster | producer + consumer (→ Delta + delta-rs metrics) | unpaused |
| Zerobus | `iot-<industry>-zerobus` | Serverless (producer-only) | simulator → Zerobus → Delta | run |

**Producer + consumer are two tasks in one job** per Spark mode (7 jobs,
2 job clusters). Counts per industry: **1** catalog · **5** schemas · **3** Delta
tables + **1** volume · **1** Lakebase branch + **2** endpoints · **2** Kafka
topics · **7** jobs · **1** shared SP.

### 2.7 Shared identity & secrets

```
secret scope: iot-secrets
  ├── sss-industries-client-id / -secret   (shared streaming SP)
  └── zerobus-client-id / -secret
service principal: iot-sss-industries      → Lakebase role, metrics writer, zerobus ingest
```

### 2.8 Pipeline management & health page

A dedicated admin surface to see and operate all deployed pipelines. Two views:

**List (`/pipelines`)** — every industry pipeline setup with: enabled modes,
**overall health** (Healthy / Degraded / Down), jobs running (e.g. 2/2),
resources healthy (e.g. 19/19), last updated, and actions (View health, Edit
config, Start/Stop, Delete).

**Health detail (`/pipelines/:id/health`)** — per-resource status for one
pipeline, grouped and each with a health pill + last-checked/last-run:
- **Unity Catalog** — catalog bound, schemas, tables, checkpoint volume.
- **Lakebase (real-time)** — project, branch active, primary RW + read replica
  endpoints running, ChronoTable partition freshness (next chunk / horizon),
  rollups, **CDF sync `STREAMING` + committed_lsn advancing**.
- **Kafka topics** — existence + partition count.
- **Jobs (7)** — consumer/producer + LakeTS ops (partition manager, rollup,
  tiering, retention): status + schedule + last run.
- **Metrics** — `streaming_metrics` receiving rows (freshness).
- **Identity** — SP + secret scope.
- Actions: Modify config (→ setup wizard in edit mode), Re-run validation,
  Restart jobs. **No Delete here** — Delete lives on the **pipeline list page**
  and the **config page** (`/setup-validate`), not the status/health page. Delete
  tears down all resources via the `provisioned_resource` ledger in reverse
  `teardown_order`.

Health is computed live against the provisioned resources; the
`provisioned_resource` registry (§2.3) is the inventory the page walks. Prototype
built as in-app React pages under `TwinShell`.

### 2.9 Role-gated access & simplified end-user start view

**Admin-only controls on the home page.** Each industry card shows, **for admins
only** (gated on the existing admin role / `iot-admins` group), two affordances:
- **⚙ Config** → the pipeline configuration/validation page (`/setup-validate` in
  edit mode for that industry).
- **Status/Health** → the pipeline health page (`/pipelines/:id/health`). The
  config page also links to the health page.
Non-admin users never see these.

**Simplified end-user pipeline view.** When a non-admin opens a pipeline/mode, we
**no longer surface provisioning / starting / validation status** — the admin has
already run those checks (Section 1). Instead show a single verdict:
- **Healthy — ready to start**, or
- **Not healthy — please contact your admin** (no start affordance; directs them
  to the admin).
The verdict derives from the same live health as §2.8. This supersedes the
current end-user provisioning/starting status display.

**End-user start health gate.** The user can **run health checks manually** (a
"Run health check" button) before starting, and **Start runs them automatically
as a pre-flight gate**. These are lightweight **runtime** checks (resources up
*right now* — Lakebase endpoints active, Kafka topics reachable, the mode's
job(s) healthy, CDF `STREAMING` for real-time, partitions current, metrics table
writable), a live rollup of §2.8 — NOT the admin's full provisioning validation
(§1, already done). Behaviour:
- **All checks pass →** the pipeline/mode starts.
- **Any check fails →** the pipeline **does not run**; show which check failed and
  "please contact your admin."
Present as a short live checklist (pending/running/passed/failed) while it runs.

**Draft (partially-configured) pipelines.** The admin can **Save configuration**
at any point in the setup/validation wizard even if incomplete — it persists as a
**draft** (`pipeline_setup.status = 'draft'`, with `validation_state`) and can be
resumed later (§1.12). A draft / not-yet-finished pipeline is **visible to end
users on the home page but marked "Coming soon"** (same treatment as the
Serverless flavor / Kinesis source) — visible, **not startable**. Admins still see
it with the ⚙ Config / Status controls. It becomes startable only once fully
validated and deployed.

## Decisions log (Section 2)

| ID | Decision |
|----|----------|
| S2-projects | Two Lakebase projects: new `iot-app-backend` (metadata) + shared `iot-pipelines` (streaming). |
| S2-branch | One never-expiring branch per real-time pipeline; branch name = industry. |
| S2-endpoints | Per branch: primary RW + 1 read replica (reads only from replica). Replicas only on `iot-pipelines`. |
| G11 | Data model = Option B (parent `pipeline_setup` → per-mode `pipeline_deployment`). |
| S2-ledger | Resources tracked in a normalized `provisioned_resource` table (ordered teardown). |
| S2-catalog | Canonical `iot_<industry>` + schemas `realtime/microbatch/zerobus/metrics/checkpoints`. |
| S2-jobs | 7 jobs per fully-enabled industry; producer+consumer bundled as tasks in one job per Spark mode. |
| S2-cleanup | Remove dead tables `sliding_aggregates`, `session_events`, `state_snapshots`. |

**Parked:** G12 (serverless code coexistence — hide the serverless option in the
UI and leave the code dormant) — revisit during integration.

---

## Next phase (after prototype review)

Per the agreed sequence: **prototype done & reviewed → stitch Sections 1 + 2 +
prototype into the full app → deploy → validate all test cases end-to-end →
only then move forward.** Not started until the prototype is signed off.
