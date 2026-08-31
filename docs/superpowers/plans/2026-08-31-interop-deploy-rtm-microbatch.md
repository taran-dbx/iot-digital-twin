# Interop Workspace Deploy + RTM/Microbatch E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Point the IoT Digital Twin Databricks App at `fe-vm-shared-interop`, provision pipeline assets into the existing UC catalog `taran-interop`, and run **realtime (RTM)** plus **micro-batch** pipelines end-to-end using the existing MSK-IAM service credential `iot-rtm-sc`.

**Architecture:** Reuse the already-centralized `core/workspace_config.py` + `app.yaml` env injection. Do **not** `CREATE CATALOG` — `taran-interop` already exists (isolated, storage on `s3://taran-shared-interop/catalog/taran-interop`). Create only a dedicated app schema inside it. Kafka auth is cluster-level via UC SERVICE credential `iot-rtm-sc` (role `arn:aws:iam::997819012307:role/taran-msk`) and the public MSK `:9198` bootstrap already verified on this workspace. App identity is the single Databricks App service principal (same pattern as the classic-stable deploy).

**Tech Stack:** FastAPI Databricks App, Unity Catalog, Lakebase Autoscale, DBSQL warehouse, Spark DBR `18.2.x-scala2.13` (RTM), MSK-IAM via `databricks.serviceCredential`, pytest.

**Spec:** `docs/app-end-to-end.md` (one-schema layout, single app SP, config-driven workspace values). Operational lessons: `docs/deployment-playbook.md`. Prior sibling plan (different workspace, zerobus-first): `docs/superpowers/plans/2026-08-27-newworkspace-deploy-zerobus-e2e.md`.

## Global Constraints

- **CLI profile:** `fe-vm-shared-interop` → `https://fe-vm-shared-interop.cloud.databricks.com` (workspace id `332040261549026`, metastore `metastore_aws_us_east_1`).
- **Do not deploy until the user confirms.** Platform rule: user consent required for `apps` create/deploy.
- **Installed Databricks CLI is v0.228.0** (below the apps-skill floor). Use `databricks api` REST for app create/deploy (`POST /api/2.0/apps`, `POST /api/2.0/apps/<name>/deployments` with `mode: SNAPSHOT`) plus `databricks sync`. Do not rely on `databricks apps deploy`.
- **UC catalog:** `taran-interop` — **already exists, owned by** `taranpreetsingh.grover@databricks.com`. Isolation mode **ISOLATED**. **Never `CREATE CATALOG`.** Hyphen in the name is load-bearing: Spark SQL / DBSQL identifiers must be quoted (`` `taran-interop` ``).
- **UC schema (proposed default):** `rtm_lakets_zb` — create if missing. Do **not** reuse schema `lakets` (existing LakeTS live-demo cold tier).
- **UC SERVICE credential:** `iot-rtm-sc` (OPEN isolation, owner Taran). Set `UC_SERVICE_CREDENTIAL=iot-rtm-sc`.
- **MSK public bootstrap (verified in `scripts/e2e/pharma_e2e.py`):** `b-1-public.iotdemo.2ol92p.c6.kafka.us-east-1.amazonaws.com:9198,b-2-public.iotdemo.2ol92p.c6.kafka.us-east-1.amazonaws.com:9198,b-3-public.iotdemo.2ol92p.c6.kafka.us-east-1.amazonaws.com:9198` (order does not matter). Private `:9098` is unreachable (no VPC peering). Set `MSK_BOOTSTRAP_SERVERS` to this string.
- **SQL warehouse:** `31df823c0f824066` (`lakets-demo-wh`, serverless PRO, was RUNNING at inventory). Matches `scripts/e2e/pharma_e2e.py`.
- **Zerobus endpoint (this ws):** `332040261549026.zerobus.us-east-1.cloud.databricks.com`. `system.lakeflow.zerobus_ingest` exists. RTM/microbatch e2e does not require a zerobus *pipeline*, but metrics-via-Zerobus still needs the endpoint + app-SP OAuth secret if that path is live.
- **Spark for RTM:** pin `18.2.x-scala2.13`. `m5d.xlarge` is available. No spot, no Photon on job clusters.
- **External location (fallback only):** `taran-shared-interop` → `s3://taran-shared-interop/`. Managed tables in this catalog already sit on that bucket via catalog `storage_root`. Leave `UC_STORAGE_BASE` / `UC_EXTERNAL_LOCATION` blank unless a task proves managed writes fail.
- **App name:** `iot-digital-twin` (free on this workspace as of 2026-08-31). Apps URL host suffix is workspace id `332040261549026`.
- **Admin:** `ADMIN_EMAILS=taranpreetsingh.grover@databricks.com`. `ADMIN_GROUP=iot-admins` (group may not exist here — email allow-list is the gate).
- **Industries:** `pharmaceutical,manufacturing` unless the user says otherwise. First e2e industry: `pharmaceutical`.
- **Git:** conventional commits, no AI attribution. Do not commit secrets. Do not push unless asked.
- **Tests:** repo `.venv`; `python -m pytest -q`. Client: `cd client && npx tsc --noEmit` then `npm run build` before sync.

## Inventory (checked 2026-08-31 after OAuth re-login)

| Resource | Status |
|---|---|
| Catalog `taran-interop` | Exists, MANAGED, ISOLATED, owner Taran, storage `s3://taran-shared-interop/catalog/taran-interop` |
| Schemas | `default`, `information_schema`, `lakets` (leave `lakets` alone) |
| SERVICE cred `iot-rtm-sc` | Exists, AWS IAM `taran-msk` |
| App `iot-digital-twin` | **Does not exist** (other apps: log-console, signal-obs, snowflake-explorer) |
| Warehouses | `lakets-demo-wh` `31df823c0f824066` RUNNING; others stopped |
| Lakebase autoscale | Several exist. Taran-owned: `iot-lakets-demo` (pg-native login **on**, host `ep-fancy-king-d2l9rua6.database.us-east-1.cloud.databricks.com`, currently IDLE), `lakets-demo`, `demo-lakets`, `lakets-tiering-test` |
| DBR 18.2 | Available |
| Zerobus system table | Present |
| Local CLI | v0.228.0 — REST for Apps |

## Open decisions (defaults used below if unanswered)

1. **App-backend Lakebase project** — **Default: create a dedicated `iot-app-backend`** (OAuth, do not share the LakeTS demo project). Fallback if SP OAuth fails on this workspace: native PG user (playbook §2.1); `iot-lakets-demo` already has `enable_pg_native_login: true` as a last resort, not the first choice.
2. **Streaming / LakeTS project (`PIPELINES_LAKEBASE_PROJECT`)** — **Default: create `iot-pipelines`** so RTM ChronoTables do not land on `iot-lakets-demo`. Confirm before create (Lakebase cost).
3. **UC schema name** — **Default: `rtm_lakets_zb`**. Alternative: user-specified name under `taran-interop`.
4. **App compute size** — Medium unless RTM dual-write + SSE needs Large.

---

## File map

| File | Role |
|---|---|
| `services/uc_ident.py` (**create**) | Quote UC names that contain `-` for Spark/DBSQL SQL. |
| `services/industry_catalog_service.py` | Use quoted FQNs in `CREATE SCHEMA` / grants / table DDL. |
| `services/dbsql_service.py` / query builders in `routes/sensor_data.py`, `routes/streaming_latency.py`, `services/pipeline_freshness.py` | Quoted catalog in generated SQL. |
| `client/src/lib/industry-deploy-config.ts` / notebook templates | Backtick-quote catalog in generated Spark SQL if the FQN is interpolated unquoted. |
| `app.yaml` | Interop env values (catalog, schema, warehouse, zerobus, MSK, service cred, Lakebase). |
| `tests/test_uc_ident.py` (**create**) | Hyphenated catalog quoting. |
| `docs/deployment-playbook.md` | Append an “Interop workspace” environment table after e2e. |

Do **not** change catalog *creation* logic to `CREATE CATALOG taran-interop`. Provisioning must stay on the catalog-exists path.

---

### Task 1: Quote hyphenated UC identifiers

**Why:** `taran-interop` is not a valid unquoted Spark SQL identifier. Current SQL is `CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}` (`industry_catalog_service.py`) and `industry_table()` returns an unquoted FQN. Unquoted, DBSQL/Spark parse `taran` minus `interop`.

**Files:** create `services/uc_ident.py`, `tests/test_uc_ident.py`; modify `services/industry_catalog_service.py` and any SQL that interpolates `UC_CATALOG`.

**Interface:** `sql_ident(name: str) -> str` wraps with backticks when the name is not `[A-Za-z_][A-Za-z0-9_]*`. `sql_fqn(*parts) -> str` joins quoted parts with `.`. `industry_table()` can keep returning a *logical* unquoted FQN for Python/SDK table APIs; SQL strings must go through `sql_fqn`.

- [ ] **Step 1:** Write `tests/test_uc_ident.py`: `sql_ident("taran-interop") == "\`taran-interop\`"`; `sql_ident("rtm_lakets_zb") == "rtm_lakets_zb"` (or always-quote — pick one and test it); `sql_fqn("taran-interop", "rtm_lakets_zb", "pharmaceutical_microbatch")` is fully quoted on the catalog part.
- [ ] **Step 2:** Run `python -m pytest tests/test_uc_ident.py -q` → FAIL (module missing).
- [ ] **Step 3:** Implement `services/uc_ident.py`.
- [ ] **Step 4:** Update `industry_catalog_service` schema/table/grant SQL to use `sql_fqn`. Add/adjust `tests/test_provision_catalog_exists.py` (and `test_industry_catalog_service.py`) so expected SQL includes backticks around `taran-interop` **or** keep those tests on underscore catalogs and add one hyphen case.
- [ ] **Step 5:** Grep generated SQL (`routes/sensor_data.py`, `streaming_latency.py`, `pipeline_freshness.py`, notebook templates) for `{uc_catalog}` / `UC_CATALOG` interpolation; quote where it becomes Spark/DBSQL.
- [ ] **Step 6:** `python -m pytest tests/test_uc_ident.py tests/test_provision_catalog_exists.py tests/test_industry_catalog_service.py tests/test_dbsql_table_resolution.py tests/test_pipeline_freshness.py tests/test_routes_streaming_latency.py -q` → PASS.
- [ ] **Step 7:** Commit: `fix: quote hyphenated Unity Catalog names in generated SQL`.

---

### Task 2: `app.yaml` for fe-vm-shared-interop

**Files:** `app.yaml` only (no new hardcoded literals in Python/TS).

Fill after Task 3 creates Lakebase (PGHOST may stay blank until the project is up, then a follow-up edit).

Proposed values:

```yaml
UC_CATALOG: taran-interop
UC_SCHEMA: rtm_lakets_zb
UC_SERVICE_CREDENTIAL: iot-rtm-sc
MSK_BOOTSTRAP_SERVERS: b-1-public.iotdemo.2ol92p.c6.kafka.us-east-1.amazonaws.com:9198,b-2-public.iotdemo.2ol92p.c6.kafka.us-east-1.amazonaws.com:9198,b-3-public.iotdemo.2ol92p.c6.kafka.us-east-1.amazonaws.com:9198
DATABRICKS_SQL_WAREHOUSE_ID: "31df823c0f824066"
ZEROBUS_SERVER_ENDPOINT: 332040261549026.zerobus.us-east-1.cloud.databricks.com
ZEROBUS_TABLE_NAME: ""   # keep legacy in-app sink off
INDUSTRIES: pharmaceutical,manufacturing
ADMIN_EMAILS: taranpreetsingh.grover@databricks.com
LAKETS_SQL_PATH: /app/python/source_code/databricks/vendor/lakets.sql
UC_STORAGE_BASE: ""
UC_EXTERNAL_LOCATION: ""
```

Lakebase fields (`LAKEBASE_PROJECT`, `LAKEBASE_ENDPOINT`, `PGHOST`, `PIPELINES_LAKEBASE_PROJECT`) filled in Task 3.

Leave `ZEROBUS_CLIENT_ID` / secret-scope keys blank or point at the **app SP** after the app exists (classic-stable currently hardcodes a different workspace’s SP). After `POST /api/2.0/apps/iot-digital-twin`, copy `service_principal_client_id` into any zerobus client-id field if still required. Prefer UC secret `app_sp_oauth` written at provision time from `DATABRICKS_CLIENT_SECRET`.

- [ ] **Step 1:** Replace classic-stable catalog/warehouse/zerobus/PGHOST values in `app.yaml` with the interop set above. Clear `ZEROBUS_CLIENT_ID` until the app SP is known.
- [ ] **Step 2:** Confirm `core/workspace_config.py` still reads every field from env (no new literals).
- [ ] **Step 3:** Commit: `chore: point app.yaml at fe-vm-shared-interop and taran-interop`.

---

### Task 3: Create app, Lakebase, grants (ops — user consent)

Do **not** start this task until the user says to provision.

```bash
P=fe-vm-shared-interop
APP=iot-digital-twin
```

- [ ] **Step 1:** Create the app (no source yet): `databricks api post /api/2.0/apps -p $P --json '{"name":"iot-digital-twin","description":"IoT Digital Twin"}'`. Poll compute until `ACTIVE`. Record `service_principal_client_id` and numeric application id. **Do not** use `oauth2_app_client_id` as PGUSER (playbook §2.2).
- [ ] **Step 2:** Create Lakebase autoscale project `iot-app-backend` (pg 17). Read primary endpoint host; set `PGHOST`, `LAKEBASE_PROJECT=iot-app-backend`, `LAKEBASE_ENDPOINT=projects/iot-app-backend/branches/production/endpoints/primary`. Grant the **app SP** a database role on the production branch (OAuth path that worked on this workspace historically — playbook W1). If OAuth fails, stop and switch to native login rather than silently using `iot-lakets-demo`.
- [ ] **Step 3:** Create streaming project `iot-pipelines` (or the user-chosen name). Set `PIPELINES_LAKEBASE_PROJECT`.
- [ ] **Step 4:** Catalog grants as **catalog owner** (you). Bind/grant the app SP on isolated catalog `taran-interop`: `USE CATALOG`, `CREATE SCHEMA`. After schema create (first app boot or a one-off `CREATE SCHEMA`): `USE SCHEMA`, `CREATE TABLE`, `CREATE VOLUME`, `MODIFY`, `SELECT` on `taran-interop.rtm_lakets_zb`. `ACCESS` on SERVICE credential `iot-rtm-sc` so job clusters can `serviceCredential`. `CAN_USE` on warehouse `31df823c0f824066`. `CREATE VOLUME` / `READ_VOLUME`/`WRITE_VOLUME` as needed for checkpoints.
- [ ] **Step 5:** Put the recorded app SP client id into `app.yaml` only if a field still requires it (zerobus). Prefer runtime `DATABRICKS_CLIENT_ID`.
- [ ] **Step 6:** Commit `app.yaml` Lakebase host/project values: `chore: fill interop Lakebase endpoint and app-backend project`.

---

### Task 4: Build, sync, SNAPSHOT deploy

- [ ] **Step 1:** `cd client && npm run build`.
- [ ] **Step 2:**

```bash
P=fe-vm-shared-interop
USER=$(databricks current-user me -p $P -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['userName'])")
TARGET="/Workspace/Users/$USER/iot-digital-twin"
databricks sync . "$TARGET" --watch=false -p $P
# If requirements.txt still references the local wheel and .databricksignore excludes it, import the wheel (playbook §5).
databricks api post /api/2.0/apps/iot-digital-twin/deployments -p $P --json "{\"source_code_path\":\"$TARGET\",\"mode\":\"SNAPSHOT\"}"
```

- [ ] **Step 3:** Poll deployment to `SUCCEEDED`. Open app URL. `GET /api/health` → `db_available: true`, `uc_catalog: taran-interop`. Confirm `GET /api/app-defaults` shows `msk_bootstrap` and service credential.
- [ ] **Step 4:** If health fails on Lakebase auth, follow playbook §2.1 (native PG user) — do not pre-create `pipeline_setup` tables as the human user (playbook §2.3).

---

### Task 5: Pharmaceutical RTM + microbatch e2e

Use the in-app admin flow (or `scripts/e2e/pharma_e2e.py` stages) against the **deployed** app, not a second ad-hoc catalog.

- [ ] **Step 1:** Admin setup + provision for `pharmaceutical`. Expect schema `taran-interop.rtm_lakets_zb` and managed tables `pharmaceutical_realtime`, `pharmaceutical_microbatch`, `pharmaceutical_metrics` (and zerobus table if provision still creates it). No new catalog.
- [ ] **Step 2:** Deploy **microbatch** first (simpler sink: Delta only). Start producer + consumer. Wait until `SELECT count(*) FROM \`taran-interop\`.rtm_lakets_zb.pharmaceutical_microbatch` > 0. Dashboard cold/historical tiles populate. Metrics rows with `mode='microbatch'` if metrics-via-Zerobus is wired; otherwise streaming_metrics table as implemented.
- [ ] **Step 3:** Deploy **realtime**. Cluster must be DBR 18.2, dedicated, Photon off, `spark.databricks.streaming.realTimeMode.enabled=true`, Kafka via `serviceCredential` `iot-rtm-sc`, public `:9198`. Confirm SSE live tiles, LakeTS `sensor_readings` on the industry branch, and (if CDF enabled) cold Delta `pharmaceutical_realtime`.
- [ ] **Step 4:** Capture screenshots under `qa-reports/evidence/` for microbatch dashboard and realtime dashboard (`[Cxxxx]` style names not required unless adding Playwright).
- [ ] **Step 5:** Optional teardown of the test setup via admin delete; shared catalog `taran-interop` and `iot-rtm-sc` **must survive**.
- [ ] **Step 6:** Append an “fe-vm-shared-interop (2026-08-31)” table to `docs/deployment-playbook.md` §7 with app URL, SP ids, warehouse, catalog/schema, Lakebase hosts, zerobus endpoint, credential name. Commit: `docs: record interop workspace RTM/microbatch deploy`.

---

## Self-review notes

- **Spec coverage:** config-driven workspace, catalog-exists/one-schema, MSK-IAM service cred, RTM + microbatch e2e. Zerobus *mode* is available on this workspace but out of scope unless the user adds it.
- **Highest technical risk:** unquoted `taran-interop` in SQL (Task 1). Second: isolated catalog + app SP grants. Third: Lakebase SP OAuth vs native login.
- **Do not reuse** `classic_stable_realtime_demos_catalog` values still in current `app.yaml` (eu-central-1 warehouse `0864d7304e74ad76`, zerobus `7474659563748021…`).
- **Do not write** into schema `taran-interop.lakets`.
- **CLI:** do not block on upgrading CLI unless Apps REST fails; upgrading to ≥0.292 is a follow-up, not a deploy blocker.
