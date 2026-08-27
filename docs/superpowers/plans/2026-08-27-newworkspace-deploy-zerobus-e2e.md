# New-Workspace Deploy + Zerobus E2E — Implementation Plan (Plan 1 of 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy the IoT Digital Twin app to the new workspace and run one **zerobus** pipeline end-to-end (produce → `{industry}_zerobus` Delta, metrics → `{industry}_metrics` via Zerobus, dashboard cold read) — exercising the config/identity/catalog/secrets decisions without needing MSK.

**Architecture:** Collapse the two Lakebase connections into one OAuth connection to a single `iot-app-backend` project; make every workspace value config-driven; provision into the **pre-existing** UC catalog `classic_stable_realtime_demos_catalog` (create only schema `rtm_lakets_zb`); use the **single app SP** for all auth; store the app SP's own client secret as a **UC secret**; write zerobus data + streaming metrics as **managed** tables via the Zerobus SDK.

**Tech Stack:** FastAPI + psycopg/asyncpg (Lakebase OAuth), Databricks SDK (Python) / `databricks api` CLI, Zerobus Ingest SDK, React/TS SPA, Databricks Apps, Unity Catalog, pytest.

Design source of truth: **`docs/app-end-to-end.md`** (decided callouts in §2.1, §2.3, §3.2, §12.6). Read it before starting.

## Global Constraints

- **Nothing hardcoded** — every workspace value in `app.yaml`/env behind a config module. No hosts, bucket/EL names, project names, SP ids, MSK bootstrap, secret-scope names, or workspace ids in source.
- **Target workspace:** `fevm-classic-stable-realtime-demos` (id `7474659563748021`, **eu-central-1**), CLI profile `fevm-classic-stable-realtime-demos`.
- **UC:** catalog `classic_stable_realtime_demos_catalog` (**already exists, owned by another SP — do NOT `CREATE CATALOG`**), schema `rtm_lakets_zb` (create it). All tables **managed**, **`{industry}_`-prefixed**, one schema.
- **Identity:** single app SP `7c59807d-7f0d-47ab-8e83-6d91add2f35c` (numeric `72852008785762`). No `iot-sss-industries`/zerobus SP creation.
- **Secrets:** UC secrets in `classic_stable_realtime_demos_catalog.rtm_lakets_zb` (`/api/2.1/unity-catalog/secrets`); no legacy `iot-secrets` scope.
- **Warehouse:** `0864d7304e74ad76`.
- **Zerobus:** endpoint `7474659563748021.zerobus.eu-central-1.cloud.databricks.com`; timestamp fields = **epoch-microseconds int**.
- **Clusters:** no spot, no Photon (n/a for zerobus serverless, but keep the rule for shared helpers).
- **Git:** conventional commits, **no attribution** (no Isaac/Claude/AI mentions).
- Run tests with the repo `.venv`; `python -m pytest -q`. Deploy via `databricks sync` + `POST /api/2.0/apps/iot-digital-twin/deployments {mode:SNAPSHOT}` on the target profile.

---

### Task 1: Central workspace config module + new-workspace `app.yaml`

**Files:**
- Create: `core/workspace_config.py`
- Modify: `app.yaml`
- Modify: `services/app_backend_db.py` (remove hardcoded host default), `services/industry_catalog_service.py` (remove `s3://taran-shared-interop` / EL literals), `services/pipelines_project_service.py` (project name from config), `client/src/lib/industry-deploy-config.ts` (MSK bootstrap / service-credential / project / zerobus endpoint from injected config), `services/simulator_service.py:94` (drop `e2-demo-field-eng` fallback)
- Test: `tests/test_workspace_config.py`

**Interfaces:**
- Produces: `core/workspace_config.py` with `get_config() -> WorkspaceConfig` (frozen dataclass) exposing: `uc_catalog:str`, `uc_schema:str`, `lakebase_project:str`, `lakebase_endpoint:str`, `pg_host:str`, `warehouse_id:str`, `zerobus_endpoint:str`, `app_sp_client_id:str`, `industries:list[str]`. Every field sourced from env with **no** workspace-specific literal defaults (missing required → raise at startup with a clear message).

- [ ] **Step 1:** Write `tests/test_workspace_config.py` asserting `get_config()` reads each field from env and raises `RuntimeError` naming the missing var when a required one is unset (monkeypatch `os.environ`).
- [ ] **Step 2:** Run `python -m pytest tests/test_workspace_config.py -q` → FAIL (module missing).
- [ ] **Step 3:** Implement `core/workspace_config.py` (frozen dataclass + `get_config()` reading `UC_CATALOG`, `UC_SCHEMA`, `LAKEBASE_PROJECT`, `LAKEBASE_ENDPOINT`, `PGHOST`, `DATABRICKS_SQL_WAREHOUSE_ID`, `ZEROBUS_SERVER_ENDPOINT`, `DATABRICKS_CLIENT_ID`, `INDUSTRIES`).
- [ ] **Step 4:** Grep the repo for the hardcoded literals (`taran-shared-interop`, `iot-pipelines`, `iot-lakets-demo`, `ep-mute-scene`, `ep-fancy-king`, `332040261549026`, `e2-demo-field-eng`, `iot-rtm-sc`, `iot-secrets`, `9927a7fa`) and route each through config/env. For the TS generators, inject values via the deploy payload (they already receive `appDefaults`).
- [ ] **Step 5:** Write `app.yaml` for the new workspace: `UC_CATALOG=classic_stable_realtime_demos_catalog`, `UC_SCHEMA=rtm_lakets_zb`, `LAKEBASE_PROJECT=iot-app-backend`, `LAKEBASE_ENDPOINT=projects/iot-app-backend/branches/production/endpoints/primary`, `DATABRICKS_SQL_WAREHOUSE_ID=0864d7304e74ad76`, `ZEROBUS_SERVER_ENDPOINT=7474659563748021.zerobus.eu-central-1.cloud.databricks.com`, `INDUSTRIES=pharmaceutical,manufacturing`, `ADMIN_EMAILS=taranpreetsingh.grover@databricks.com`, `LAKETS_SQL_PATH=/app/python/source_code/databricks/vendor/lakets.sql`. `PGHOST` left blank until Task 2 creates the Lakebase project.
- [ ] **Step 6:** Run `python -m pytest -q` (full suite) → fix any test referencing the removed literals.
- [ ] **Step 7:** Commit: `refactor: centralize workspace config, remove hardcoded workspace literals`.

---

### Task 2: Single Lakebase project + one OAuth connection

**Files:**
- Modify: `services/app_backend_db.py` (make it the sole connection; host/endpoint from config), `backend.py` (keep `LakebaseConnectionManager` as the base; the app uses one instance), `app.py` (lifespan: one connection init; drop the demo matview-refresh + retention loops; `state.db` and `state.app_backend_db` point at the same manager), `core/state.py`
- Test: `tests/test_boot_single_connection.py`

**Interfaces:**
- Consumes: `core/workspace_config.get_config()`.
- Produces: on the one project, the app-backend schema (`pipeline_setup`, `pipeline_deployment`, `provisioned_resource`) **plus** `user_settings` and `source_bindings` (still used). `pipelines` table + legacy per-pipeline UI are NOT created (retired — full removal is Plan 3; here, just stop creating/initializing them).

- [ ] **Step 1:** Write `tests/test_boot_single_connection.py`: with a mocked connection manager, `app.py` lifespan initializes exactly one Lakebase manager, calls `ensure_app_schema` + `ensure_user_settings_table` + `ensure_source_bindings_table`, and does **not** start matview/retention loops.
- [ ] **Step 2:** Run it → FAIL.
- [ ] **Step 3:** First provision the project (one-off, record for the runbook):
```bash
P=fevm-classic-stable-realtime-demos
databricks api post /api/2.0/postgres/projects -p $P --json '{"project":{"project_id":"iot-app-backend","display_name":"IoT App Backend"}}'
# read the primary endpoint host once ready:
databricks api get "/api/2.0/postgres/projects/iot-app-backend/branches/production/endpoints/primary" -p $P
```
Put the returned host into `app.yaml` `PGHOST`.
- [ ] **Step 4:** Implement: lifespan builds one `AppBackendConnectionManager` from config (OAuth), assigns it to both `state.db` and `state.app_backend_db`, runs the three `ensure_*` schema builders; remove the matview-refresh and retention-loop tasks; guard `user_settings`/`source_bindings` creation on the same manager.
- [ ] **Step 5:** Run `python -m pytest tests/test_boot_single_connection.py -q` → PASS; then full suite, fixing fallout from the removed second connection.
- [ ] **Step 6:** Commit: `refactor: single Lakebase OAuth connection; drop demo matview/retention loops`.

---

### Task 3: Provision into the pre-existing catalog — one schema, managed tables, single-SP grants, UC secret

**Files:**
- Modify: `services/industry_catalog_service.py` (catalog-exists path: don't `CREATE CATALOG`; `CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}`; managed tables, no `LOCATION`; single-SP grants), `services/provisioning_service.py` (drop `ensure_shared_streaming_sp` + separate zerobus SP; grant app SP the branch Lakebase role only), `models/metrics_schema.py` + wherever table FQNs are built (industry-prefixed names in the one schema)
- Create: `services/uc_secrets.py` (write/read UC secrets via `/api/2.1/unity-catalog/secrets`)
- Test: `tests/test_uc_secrets.py`, `tests/test_provision_catalog_exists.py`

**Interfaces:**
- Produces: `uc_secrets.put_secret(catalog, schema, name, value)` and `uc_secrets.get_secret(catalog, schema, name) -> str`; table-name helper `industry_table(industry, kind) -> f"{catalog}.{schema}.{industry}_{kind}"` where kind ∈ {`realtime`,`microbatch`,`zerobus`,`metrics`}; the app SP client secret is stored as UC secret `rtm_lakets_zb.app_sp_oauth`.
- Consumes: `core/workspace_config`, the app SP identity.

- [ ] **Step 1:** Write `tests/test_uc_secrets.py` (mock the SDK/api call) asserting `put_secret`/`get_secret` hit `POST`/`GET /api/2.1/unity-catalog/secrets(/…)` with `catalog_name`/`schema_name`/`name`.
- [ ] **Step 2:** Write `tests/test_provision_catalog_exists.py`: provisioning issues `CREATE SCHEMA IF NOT EXISTS classic_stable_realtime_demos_catalog.rtm_lakets_zb`, **never** `CREATE CATALOG`, and creates managed tables (no `LOCATION`) named `{industry}_…`.
- [ ] **Step 3:** Run both → FAIL.
- [ ] **Step 4:** Implement `services/uc_secrets.py`; update `industry_catalog_service` to the catalog-exists + one-schema + managed-table + industry-prefixed path and single-SP grants (`USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `MODIFY`, `SELECT` on the schema, `READ SECRET` on `app_sp_oauth`); in `provisioning_service`, remove the shared-SP + zerobus-SP creation and grant only the app SP the branch role; write the app SP's `DATABRICKS_CLIENT_SECRET` (from env) into UC secret `app_sp_oauth` during provisioning.
- [ ] **Step 5:** Run both tests → PASS; then full suite.
- [ ] **Step 6:** Commit: `feat: provision one schema in existing catalog, managed tables, single-SP grants, UC secret`.

---

### Task 4: Zerobus deploy path — config-driven, prefixed target, metrics via Zerobus

**Files:**
- Modify: `client/src/lib/data-producer-notebook.ts` (`generateZerobusProducer`: target `{industry}_zerobus`; endpoint + client-id from injected config; read the OAuth secret from the UC secret via `dbutils.secrets.get(catalog=…, schema=…, key="app_sp_oauth")`), `client/src/lib/notebook-template.ts` (`BufferedDeltaMetricsListener`: replace Spark `.save(location)` with a Zerobus push into `{industry}_metrics`, `mode` column, timestamp epoch-µs int — **used by realtime/microbatch in Plan 2; here, add the zerobus-writer helper + table**), `client/src/lib/industry-deploy-config.ts` (prefixed FQNs + config injection), `services/deployment_service.py` (zerobus serverless job unchanged except table/secret wiring)
- Modify: `models/metrics_schema.py` (single `{industry}_metrics` managed-table DDL with a `mode` column, no external location)
- Test: `client` type-check (`npx tsc --noEmit`), `tests/test_metrics_table_ddl.py`

**Interfaces:**
- Consumes: Task 3 `industry_table`, `uc_secrets`.
- Produces: zerobus producer writing to `{industry}_zerobus`; `{industry}_metrics` managed table created at provision (Task 3 wires the DDL from here).

- [ ] **Step 1:** Write `tests/test_metrics_table_ddl.py` asserting the metrics DDL is `CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{industry}_metrics (…, mode STRING, …)` with **no** `LOCATION`.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement the metrics DDL + zerobus producer changes (prefixed target, config endpoint, UC-secret OAuth). Drop the raw-progress STRING blob.
- [ ] **Step 4:** `cd client && npx tsc --noEmit` (clean) and `npm run build`; run `python -m pytest tests/test_metrics_table_ddl.py -q` → PASS.
- [ ] **Step 5:** Commit: `feat: zerobus target + metrics-via-zerobus into one schema, config-driven`.

---

### Task 5: Dashboard + DBSQL read against the one schema / prefixed tables

**Files:**
- Modify: `routes/sensor_data.py` (`_DBSQL_MODE_TABLES` → `{industry}_zerobus` etc. in the one schema; `dbsql-timeseries` + `total_rows` resolve via config + industry; add `industry` param where missing), `services/pipeline_freshness.py` (per-mode table names → prefixed one-schema), `routes/streaming_latency.py` + the metrics read (point at `{industry}_metrics`, filter `mode`), `client/src/pages/mode-dashboard-page.tsx` (metrics tiles + charts both read `{industry}_metrics`)
- Test: `tests/test_dbsql_table_resolution.py`

**Interfaces:**
- Consumes: Task 3 `industry_table`.
- Produces: dashboard cold-tier + metrics reads resolve to the one-schema prefixed tables.

- [ ] **Step 1:** Write `tests/test_dbsql_table_resolution.py` asserting the zerobus cold table resolves to `classic_stable_realtime_demos_catalog.rtm_lakets_zb.{industry}_zerobus` and metrics to `…{industry}_metrics`.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement the table-resolution changes across the routes/services + the client metrics read (tiles and charts on `{industry}_metrics`).
- [ ] **Step 4:** `python -m pytest tests/test_dbsql_table_resolution.py -q` → PASS; full suite; `npx tsc --noEmit` + `npm run build`.
- [ ] **Step 5:** Commit: `feat: dashboard + DBSQL read one-schema prefixed tables + {industry}_metrics`.

---

### Task 6: End-to-end deploy + zerobus pipeline test on the new workspace

**Files:** none (operational). Prereqs from §12: catalog grant applied ✅; Lakebase project created (Task 2); Zerobus available ✅; warehouse ✅.

- [ ] **Step 1:** Build the SPA: `cd client && npm run build`.
- [ ] **Step 2:** Sync + deploy to the target:
```bash
P=fevm-classic-stable-realtime-demos
USER=$(databricks current-user me -p $P -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['userName'])")
TARGET="/Workspace/Users/$USER/iot-digital-twin"
databricks sync . "$TARGET" --watch=false -p $P
databricks api post /api/2.0/apps/iot-digital-twin/deployments -p $P --json '{"source_code_path":"'$TARGET'","mode":"SNAPSHOT"}'
```
- [ ] **Step 2b:** Poll deployment to SUCCEEDED; hit `GET <app-url>/api/health` → `db_available:true`, `db_error:null`.
- [ ] **Step 3:** Provision an industry (pharmaceutical) via `POST /api/admin/industries/pharmaceutical/setup` then `/provision`; verify: schema `rtm_lakets_zb` exists, managed tables `pharmaceutical_zerobus` + `pharmaceutical_metrics` created, app-SP grants present, UC secret `app_sp_oauth` written.
- [ ] **Step 4:** Deploy + run zerobus mode: `POST /api/admin/pipelines/{setup_id}/deploy/zerobus` then start; wait for the run.
- [ ] **Step 5:** Verify data: DBSQL `SELECT count(*) FROM classic_stable_realtime_demos_catalog.rtm_lakets_zb.pharmaceutical_zerobus` > 0, and `…pharmaceutical_metrics` has rows with `mode='zerobus'`? (zerobus mode has no streaming metrics — confirm metrics table exists but stays empty for zerobus; ingest metrics come from `system.lakeflow.zerobus_ingest`).
- [ ] **Step 6:** Load the pharmaceutical **zerobus** dashboard → Cold Rows / historical DBSQL populate.
- [ ] **Step 7:** Tear down the test pipeline (`DELETE /api/admin/pipelines/{setup_id}`) to confirm teardown drops the schema tables + revokes grants cleanly (shared project/SP survive).
- [ ] **Step 8:** Commit any runbook notes: `docs: record new-workspace zerobus e2e runbook`.

---

## Self-review notes
- **Spec coverage (Plan 1 scope):** nothing-hardcoded (T1), single connection + retire demo loops (T2), catalog-exists/one-schema/managed/single-SP/UC-secret (T3), zerobus + metrics-via-zerobus (T4), dashboard one-schema reads + charts-bug fix (T5), e2e (T6). **Deferred to Plan 2 (MSK-gated):** realtime/microbatch, LakeTS, CDF-via-API, watchdog, validation-at-setup, Restart/Repair. **Plan 3:** legacy-UI removal, multi-industry gallery polish, auth unify, histogram/insert-lag, comment cleanup.
- **Verify during execution:** Lakebase SP OAuth to the fresh project (W1) — surfaces at Task 2 e2e; UC-secret read from the zerobus **serverless** notebook needs env v4+ (else read via REST API in the notebook) — surfaces at Task 6.
