# IoT Digital Twin — New Workspace Deployment Playbook

> Lessons learned from deploying the IoT Digital Twin Databricks App to
> `fe-sandbox-rtm-msk-demo.cloud.databricks.com` on 2026-03-17.
> This document captures every issue encountered, the root cause, the fix applied,
> and recommendations for making future deployments smoother.

---

## Table of Contents

1. [Deployment Overview](#1-deployment-overview)
2. [Issue Log](#2-issue-log)
3. [Current Architecture & Auth Flow](#3-current-architecture--auth-flow)
4. [Pre-Deployment Checklist](#4-pre-deployment-checklist)
5. [Step-by-Step Deployment Runbook](#5-step-by-step-deployment-runbook)
6. [Improvement Recommendations](#6-improvement-recommendations)
7. [Environment Reference](#7-environment-reference)

---

## 1. Deployment Overview

The IoT Digital Twin app is a Databricks App (FastAPI + React) that:

- Stores pipeline metadata in **Lakebase** (PostgreSQL-compatible).
- Provisions **Unity Catalog** resources (catalogs, schemas, volumes) per pipeline.
- Streams sensor data via **Zerobus Delta Ingest** and/or Spark Structured Streaming.
- Serves a React frontend from `static/dist/`.

When deployed to a **new workspace**, the app needs:

| Resource | Purpose |
|----------|---------|
| Lakebase Autoscale project | Stores `pipelines` and `sensor_readings` tables |
| UC external storage | S3 bucket for catalog managed storage |
| Zerobus service principal + OAuth secret | Streaming ingestion auth |
| Databricks App + service principal | Runtime identity |

---

## 2. Issue Log

### 2.1 Lakebase: SP Cannot Authenticate via SDK OAuth Tokens

**Symptom:** `InvalidPasswordError: password authentication failed for user '1e553aac-...'`

**Root cause:** The app's service principal (SP) calls `w.postgres.generate_database_credential()`
to get a short-lived JWT token, then connects to Lakebase with that token as the password.
However, Lakebase Autoscale does not auto-create PostgreSQL roles for service principals.
A human user gets a role auto-created on first login, but SPs do not.

**What we tried (and failed):**
- Manually creating a PG role matching the SP's `client_id` — the token was still rejected
  because Lakebase's auth proxy validates the JWT against workspace identities before
  passing it to PostgreSQL. The SP identity was not recognized at the proxy layer.

**Fix applied:** Created a native PostgreSQL user (`iot_app_user`) with a static password,
since `enable_pg_native_login: true` is set on the Lakebase project. Set `PGUSER` and
`PGPASSWORD` in `app.yaml` so `backend.py` uses the static credential path (no SDK
token refresh needed).

**Impact:** No automatic token rotation. The static password does not expire, but it's
less secure than OAuth tokens.

**Recommendation:** Databricks should support auto-provisioning PG roles for service
principals, or provide a way to grant SP access at the Lakebase project level.
Alternatively, the app could be enhanced to use the Lakebase pooler endpoint with
connection-string-based auth.

---

### 2.2 PGUSER vs DATABRICKS_CLIENT_ID Mismatch

**Symptom:** `InvalidPasswordError: password authentication failed for user 'c5790c95-...'`

**Root cause:** Every Databricks App has **two** identity values:
- `oauth2_app_client_id` (`c5790c95-...`) — the OAuth2 application client ID
- `service_principal_client_id` (`1e553aac-...`) — the actual SP identity used at runtime

The `DATABRICKS_CLIENT_ID` env var injected at runtime contains the **service_principal_client_id**,
not the `oauth2_app_client_id`. When we set `PGUSER` to the wrong one, the SDK-generated
token (which is for the real SP identity) didn't match.

**How to find the correct value:** Call `GET /api/2.0/apps/<app-name>` and look at
`service_principal_client_id`, not `oauth2_app_client_id`. Or check the `/api/health`
endpoint which exposes `DATABRICKS_CLIENT_ID`.

**Fix applied:** Removed static `PGUSER` from `app.yaml` so the code falls back to
`os.getenv("DATABRICKS_CLIENT_ID")` which has the correct value.

**Recommendation:** Document the distinction between these two IDs clearly. Never
hardcode `PGUSER` to the `oauth2_app_client_id`.

---

### 2.3 Table Ownership: `must be owner of table pipelines`

**Symptom:** `InsufficientPrivilegeError: must be owner of table pipelines`

**Root cause:** During initial debugging, the `pipelines` and `sensor_readings` tables
were pre-created by the human user (`taranpreetsingh.grover@databricks.com`). When the
app (running as `iot_app_user`) tried to run `CREATE TABLE IF NOT EXISTS` with additional
DDL (like creating indexes), PostgreSQL rejected it because the table owner was different.

`ALTER TABLE ... OWNER TO` also failed because the human user couldn't `SET ROLE` to
`iot_app_user`.

**Fix applied:** Dropped both tables and redeployed the app, letting it recreate them
as `iot_app_user` during startup.

**Recommendation:** Never pre-create tables as a different user than the app will use.
If tables must be pre-created, ensure the app user is the owner, or use `GRANT ALL`
*including* the ability to run DDL. Better yet, let the app's `ensure_pipelines_table()`
handle creation on first deploy.

---

### 2.4 UC Catalog: 403 from Cloud Storage Provider

**Symptom:** `PERMISSION_DENIED: 403 Forbidden error from cloud storage provider`
when creating tables in a catalog.

**Root cause:** Catalogs created without an explicit `storage_root` use the metastore's
default managed storage location (`s3://dbstorage-prod-g4igq/...`). This is a shared
Databricks-managed bucket that service principals typically cannot write to directly.

This affected both `taran_explore` (human-created catalog) and the app's per-pipeline
catalogs.

**Fix applied:**
1. Added `UC_STORAGE_ROOT` env var to `app.yaml` pointing to the workspace's external
   S3 bucket: `s3://rtm-msk-demo-ext-s3-332745928618-p7zdpf`
2. Updated both provisioning endpoints in `app.py` (`/api/provision/uc` and
   `/api/provision/pipeline-catalog`) to pass `storage_root` when calling
   `w.catalogs.create()`:
   ```python
   storage_root = os.getenv("UC_STORAGE_ROOT")
   create_kwargs = {"name": catalog_name}
   if storage_root:
       create_kwargs["storage_root"] = f"{storage_root}/{catalog_name}"
   w.catalogs.create(**create_kwargs)
   ```

**Recommendation:** Always create catalogs with an explicit `storage_root` on
non-default workspaces. The default managed storage is not accessible to SPs.

---

### 2.5 Missing SP Permissions on UC Resources

**Symptom:** `User does not have CREATE MANAGED STORAGE on External Location '...'`

**Root cause:** Even with the storage root set, the SP needs explicit grants on the
external location and storage credential to create managed storage (tables, volumes)
within catalogs that use that location.

**Grants required (applied incrementally as errors appeared):**

| Securable | Privilege | Why |
|-----------|-----------|-----|
| **Metastore** | `CREATE_CATALOG` | SP needs to create per-pipeline catalogs |
| **External Location** | `CREATE_MANAGED_STORAGE` | Write managed table data |
| **External Location** | `READ_FILES`, `WRITE_FILES` | Read/write data files |
| **External Location** | `BROWSE` | List files |
| **External Location** | `CREATE_EXTERNAL_TABLE` | Create external tables |
| **Storage Credential** | `CREATE_EXTERNAL_LOCATION` | Create new external locations |
| **Storage Credential** | `READ_FILES`, `WRITE_FILES` | Access S3 via credential |

**Recommendation:** Grant all these upfront as part of the deployment process, not
reactively as errors appear. See the [Pre-Deployment Checklist](#4-pre-deployment-checklist).

---

### 2.6 Broken Catalog Left Behind After Partial Provisioning

**Symptom:** `Catalog 'iot_rtm_msk_demo_6f986f' already exists` on retry, but the
catalog was incomplete (no volume, no grants, no pipeline DB record).

**Root cause:** The provisioning endpoint created the catalog and schemas successfully,
but failed at the volume creation step (due to storage permissions). The error response
was returned to the frontend, but the partially-created catalog remained. On retry, the
`AlreadyExists` exception was swallowed, but the pipeline DB record still had
`resources: {}` so the idempotent early-return didn't trigger.

**Fix applied:** Manually deleted the broken catalog (had to transfer ownership first
since the SP owned it) and the broken pipeline DB record.

**Recommendation:**
- The provisioning code should be transactional: if any step fails after catalog
  creation, it should clean up the catalog (or at least mark it as failed in the
  pipeline record so retries work).
- The frontend should offer a "retry provisioning" button that re-runs the full
  provisioning flow (not just the step that failed).
- Consider storing partial state in the pipeline's `resources` JSON so the provisioning
  code can resume from the last successful step.

---

### 2.7 Workspace File Sync: Wrong User Path

**Symptom:** `Folder Users is protected` when syncing to the wrong path.

**Root cause:** The workspace username was `taranpreetsingh.grover@databricks.com`,
not `taran.grover@databricks.com`. The sync target path must match exactly.

**Fix applied:** Used `databricks current-user me` to get the correct username.

**Recommendation:** Always resolve the username programmatically before syncing:
```bash
USER=$(databricks current-user me -p <profile> --output json | jq -r '.userName')
databricks sync . "/Workspace/Users/$USER/iot-digital-twin" -p <profile>
```

---

### 2.8 `.whl` File Excluded by `.databricksignore`

**Symptom:** App deployment fails with missing dependency.

**Root cause:** `.databricksignore` excludes `*.whl` files, but `requirements.txt`
references `./iot_data_simulator-0.1.0-py3-none-any.whl`.

**Fix applied:** Manually uploaded the `.whl` file:
```bash
databricks workspace import \
  /Workspace/Users/<user>/iot-digital-twin/iot_data_simulator-0.1.0-py3-none-any.whl \
  --file iot_data_simulator-0.1.0-py3-none-any.whl -p <profile> --overwrite
```

**Recommendation:** Either remove `*.whl` from `.databricksignore` or publish the
wheel to a PyPI-compatible feed and reference it by URL in `requirements.txt`.

---

### 2.9 App Compute Not Ready on First Deploy

**Symptom:** Deployment hangs or fails when the app is first created.

**Root cause:** Databricks Apps compute takes 2-4 minutes to reach `ACTIVE` state
after the app resource is created. Deploying before compute is ready fails silently
or takes longer than expected.

**Fix applied:** Polled the app status until `ComputeState.ACTIVE` before triggering
the first deployment.

**Recommendation:** The deployment script should:
1. Create the app
2. Poll until `compute_status.state == ACTIVE`
3. Then trigger the deployment

---

## 3. Current Architecture & Auth Flow

```
                                 ┌─────────────────────────┐
                                 │   Databricks App (SP)    │
                                 │  1e553aac-e130-...       │
                                 └────┬──────────┬──────────┘
                                      │          │
                    PGUSER/PGPASSWORD │          │ SDK (DATABRICKS_CLIENT_ID/SECRET)
                    (static, native)  │          │
                                      ▼          ▼
                               ┌──────────┐  ┌─────────────┐
                               │ Lakebase │  │ Unity Catalog│
                               │ (PG)     │  │ (S3 storage) │
                               └──────────┘  └─────────────┘

Auth paths:
  Lakebase:  Static native PG credentials (iot_app_user / password)
  UC:        SDK auto-auth via injected DATABRICKS_CLIENT_ID + SECRET
  Zerobus:   Explicit ZEROBUS_CLIENT_ID + SECRET (service principal OAuth)
```

---

## 4. Pre-Deployment Checklist

Run these steps **before** deploying the app to a new workspace.

### 4.1 Workspace Setup

- [ ] Authenticate: `databricks auth login --host https://<workspace>.cloud.databricks.com`
- [ ] Verify profile: `databricks current-user me -p <profile>`
- [ ] Note the workspace ID (from URL or API)

### 4.2 Lakebase

- [ ] Create Lakebase Autoscale project: `iot-digital-twin`
- [ ] Wait for production branch + endpoint to be ready
- [ ] Note the endpoint host: `ep-<name>.database.<region>.cloud.databricks.com`
- [ ] Note the endpoint resource name: `projects/iot-digital-twin/branches/production/endpoints/primary`
- [ ] Create native PG user for the app:
  ```sql
  CREATE ROLE iot_app_user WITH LOGIN PASSWORD '<secure-password>';
  GRANT ALL PRIVILEGES ON DATABASE databricks_postgres TO iot_app_user;
  GRANT ALL PRIVILEGES ON SCHEMA public TO iot_app_user;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT ALL PRIVILEGES ON TABLES TO iot_app_user;
  ```
- [ ] **Do NOT pre-create tables** — let the app create them on first startup

### 4.3 Unity Catalog Storage

- [ ] Identify the workspace's external S3 bucket and external location name
- [ ] Note the S3 URL: `s3://<workspace>-ext-s3-<account>-<id>/`

### 4.4 Zerobus (if using Delta Ingest)

- [ ] Create a service principal for Zerobus
- [ ] Generate an OAuth secret for the SP
- [ ] Create the target Delta table (e.g., `<catalog>.<schema>.iot_sensor_data`)
- [ ] Grant the Zerobus SP write access to the table

### 4.5 Create the Databricks App

- [ ] Create the app: `POST /api/2.0/apps` with `{"name": "iot-digital-twin"}`
- [ ] Wait for compute to reach `ACTIVE` state
- [ ] Note the app's **service_principal_client_id** (from `GET /api/2.0/apps/<name>`)
- [ ] Note the app's **service_principal_id** (numeric)

### 4.6 Grant SP Permissions

Using the **service_principal_client_id** as the principal:

```bash
SP_CLIENT_ID="<service_principal_client_id>"
METASTORE_ID="<metastore-id>"
EXT_LOCATION="<external-location-name>"
STORAGE_CRED="<storage-credential-name>"

# Metastore: allow creating catalogs
databricks unity-catalog permissions update metastore $METASTORE_ID \
  --json '{"changes": [{"principal": "'$SP_CLIENT_ID'", "add": ["CREATE_CATALOG"]}]}'

# External Location: allow managed storage + file access
databricks unity-catalog permissions update external_location $EXT_LOCATION \
  --json '{"changes": [{"principal": "'$SP_CLIENT_ID'", "add": [
    "CREATE_MANAGED_STORAGE", "READ_FILES", "WRITE_FILES",
    "BROWSE", "CREATE_EXTERNAL_TABLE"
  ]}]}'

# Storage Credential: allow creating external locations
databricks unity-catalog permissions update storage_credential $STORAGE_CRED \
  --json '{"changes": [{"principal": "'$SP_CLIENT_ID'", "add": [
    "CREATE_EXTERNAL_LOCATION", "READ_FILES", "WRITE_FILES"
  ]}]}'
```

### 4.7 Update `app.yaml`

Fill in all environment variables with workspace-specific values:

| Variable | Value |
|----------|-------|
| `PGHOST` | Lakebase endpoint host |
| `PGDATABASE` | `databricks_postgres` |
| `PGPORT` | `5432` |
| `PGUSER` | `iot_app_user` |
| `PGPASSWORD` | The password set in step 4.2 |
| `LAKEBASE_ENDPOINT` | Lakebase endpoint resource name |
| `UC_STORAGE_ROOT` | S3 bucket URL (no trailing slash) |
| `ZEROBUS_SERVER_ENDPOINT` | `<workspace-id>.zerobus.<region>.cloud.databricks.com` |
| `ZEROBUS_TABLE_NAME` | `<catalog>.<schema>.<table>` |
| `ZEROBUS_CLIENT_ID` | Zerobus SP client ID |
| `ZEROBUS_CLIENT_SECRET` | Zerobus SP OAuth secret |
| `INDUSTRIES` | Comma-separated industry list |

### 4.8 Sync & Deploy

```bash
PROFILE="<profile>"
USER=$(databricks current-user me -p $PROFILE --output json | jq -r '.userName')
TARGET="/Workspace/Users/$USER/iot-digital-twin"

# Build frontend
cd client && npm run build && cd ..

# Sync files
databricks sync . "$TARGET" --watch=false -p $PROFILE

# Upload .whl (excluded by .databricksignore)
databricks workspace import "$TARGET/iot_data_simulator-0.1.0-py3-none-any.whl" \
  --file iot_data_simulator-0.1.0-py3-none-any.whl -p $PROFILE --overwrite

# Deploy
databricks api post /api/2.0/apps/iot-digital-twin/deployments -p $PROFILE \
  --json '{"source_code_path": "'$TARGET'", "mode": "SNAPSHOT"}'
```

### 4.9 Verify

- [ ] Check `/api/health` — `db_available` should be `true`, `db_error` should be `null`
- [ ] Create a test pipeline — catalog provisioning should succeed
- [ ] Delete the test pipeline to clean up

---

## 5. Step-by-Step Deployment Runbook

This is the condensed, copy-paste-ready version.

```bash
#!/usr/bin/env bash
set -euo pipefail

# ── Configuration ──
WORKSPACE_HOST="https://<workspace>.cloud.databricks.com"
PROFILE="<profile-name>"
APP_NAME="iot-digital-twin"
LAKEBASE_PROJECT="iot-digital-twin"
PG_PASSWORD="<generate-a-secure-password>"
S3_BUCKET="s3://<workspace-ext-s3-bucket>"

# ── 1. Authenticate ──
databricks auth login --host "$WORKSPACE_HOST"
USER=$(databricks current-user me -p "$PROFILE" --output json | jq -r '.userName')
echo "Deploying as: $USER"

# ── 2. Create Lakebase project ──
# (Use MCP tool or REST API to create autoscale project)
# Wait for endpoint to be ready, note the host

# ── 3. Create native PG user ──
# Connect to Lakebase as your user and run:
#   CREATE ROLE iot_app_user WITH LOGIN PASSWORD '$PG_PASSWORD';
#   GRANT ALL ON DATABASE databricks_postgres TO iot_app_user;
#   GRANT ALL ON SCHEMA public TO iot_app_user;
#   ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO iot_app_user;

# ── 4. Create the app ──
databricks api post /api/2.0/apps -p "$PROFILE" \
  --json '{"name": "'$APP_NAME'", "description": "IoT Digital Twin"}'

# Wait for ACTIVE compute
while true; do
  STATE=$(databricks api get "/api/2.0/apps/$APP_NAME" -p "$PROFILE" \
    | jq -r '.compute_status.state')
  echo "Compute: $STATE"
  [ "$STATE" = "ACTIVE" ] && break
  sleep 15
done

# ── 5. Get SP identity ──
SP_CLIENT_ID=$(databricks api get "/api/2.0/apps/$APP_NAME" -p "$PROFILE" \
  | jq -r '.service_principal_client_id')
echo "SP Client ID: $SP_CLIENT_ID"

# ── 6. Grant UC permissions ──
# (Grant CREATE_CATALOG on metastore, CREATE_MANAGED_STORAGE on ext location, etc.)
# See Section 4.6 for full grant commands

# ── 7. Update app.yaml with workspace-specific values ──
# (Edit PGHOST, PGUSER, PGPASSWORD, LAKEBASE_ENDPOINT, UC_STORAGE_ROOT, ZEROBUS_*, etc.)

# ── 8. Build, sync, deploy ──
cd client && npm run build && cd ..
TARGET="/Workspace/Users/$USER/$APP_NAME"
databricks sync . "$TARGET" --watch=false -p "$PROFILE"
databricks workspace import "$TARGET/iot_data_simulator-0.1.0-py3-none-any.whl" \
  --file iot_data_simulator-0.1.0-py3-none-any.whl -p "$PROFILE" --overwrite
databricks api post "/api/2.0/apps/$APP_NAME/deployments" -p "$PROFILE" \
  --json '{"source_code_path": "'$TARGET'", "mode": "SNAPSHOT"}'

echo "Deployment triggered. Monitor at: $WORKSPACE_HOST/apps/$APP_NAME"
```

---

## 6. Improvement Recommendations

### 6.1 Automate SP Permission Grants (High Priority)

**Problem:** Manually granting 7+ permissions across metastore, external location,
and storage credential is error-prone and workspace-specific.

**Recommendation:** Create a `/api/setup/workspace` endpoint in the app that:
1. Detects the workspace's external location and storage credential
2. Outputs the exact grant commands needed
3. Or, if the deployer has admin access, executes them automatically

Alternatively, create a `deploy.py` script that handles all of this.

---

### 6.2 Replace Static PG Credentials with SDK Auth (High Priority)

**Problem:** Static `PGUSER`/`PGPASSWORD` doesn't rotate and is stored in `app.yaml`.

**Recommendation:** Investigate why the SP's `generate_database_credential()` token
is rejected. It may require:
- A Lakebase project-level grant for the SP
- A different endpoint format
- Using the pooler endpoint instead of the direct endpoint

If SDK auth can work for SPs, the app can use automatic token refresh (already
implemented in `backend.py`'s `_token_refresh_loop`).

---

### 6.3 Make Catalog Provisioning Transactional (Medium Priority)

**Problem:** Partial failures leave broken catalogs that block retries.

**Recommendation:**
- Track provisioning state per step in the pipeline's `resources` JSON
- On failure, either roll back (delete catalog) or store the failure point
  so the next attempt can resume
- Add a "Retry Provisioning" button in the UI

---

### 6.4 Publish the Simulator Wheel to a Feed (Medium Priority)

**Problem:** The `.whl` file is excluded by `.databricksignore` but required by
`requirements.txt`. It needs manual upload every time.

**Recommendation:**
- Publish to a private PyPI feed or Databricks artifact store
- Reference by URL in `requirements.txt`:
  `iot-data-simulator @ https://<feed>/iot_data_simulator-0.1.0-py3-none-any.whl`
- Or remove from `.databricksignore` and let sync handle it

---

### 6.5 Create a Deployment Health Check Script (Medium Priority)

**Problem:** After deployment, manual verification is needed (check `/api/health`,
try creating a pipeline, etc.).

**Recommendation:** Create a `verify_deployment.py` script that:
1. Calls `/api/health` and asserts `db_available == true`
2. Calls `/api/provision/pipeline-catalog` with a test payload and verifies success
3. Cleans up the test resources
4. Reports pass/fail with actionable error messages

---

### 6.6 Externalize Workspace-Specific Config (Low Priority)

**Problem:** `app.yaml` contains workspace-specific values that must be manually
edited for each deployment target.

**Recommendation:**
- Create a `config/` directory with per-workspace config files:
  `config/e2demo.yaml`, `config/msk-demo.yaml`, etc.
- A deploy script copies the right config to `app.yaml` before syncing
- Or use Databricks App environment variable overrides via REST API after deployment

---

### 6.7 Document the Two App Identity Values (Low Priority)

**Problem:** `oauth2_app_client_id` vs `service_principal_client_id` confusion
caused the PGUSER mismatch.

**Recommendation:** Add a comment in `app.yaml`:
```yaml
# NOTE: Do NOT use oauth2_app_client_id as PGUSER.
# The runtime DATABRICKS_CLIENT_ID is the service_principal_client_id,
# which is a different value. Use that if setting PGUSER explicitly.
```

---

## 7. Environment Reference

### MSK Demo Workspace (deployed 2026-03-17)

| Resource | Value |
|----------|-------|
| Workspace | `https://fe-sandbox-rtm-msk-demo.cloud.databricks.com` |
| Workspace ID | `7474649530865514` |
| CLI Profile | `msk-demo` |
| App URL | `https://iot-digital-twin-7474649530865514.aws.databricksapps.com` |
| App SP client_id | `1e553aac-e130-4be4-8707-cab5ccb0b413` |
| App SP id (numeric) | `70586883243382` |
| Lakebase Project | `iot-digital-twin` |
| Lakebase Endpoint | `projects/iot-digital-twin/branches/production/endpoints/primary` |
| Lakebase Host | `ep-ancient-dust-d1ch7tek.database.us-west-2.cloud.databricks.com` |
| PG User | `iot_app_user` |
| Metastore ID | `646f2b9a-d861-4deb-92f2-9927875bcc60` |
| External Location | `rtm-msk-demo-ext-role-332745928618-p7zdpf-el-explss` |
| Storage Credential | `rtm-msk-demo-ext-role-332745928618-p7zdpf--explss` |
| S3 Bucket | `s3://rtm-msk-demo-ext-s3-332745928618-p7zdpf` |
| Zerobus SP client_id | `182192cb-5feb-40f0-82a0-6e02067019f6` |
| Zerobus Endpoint | `7474649530865514.zerobus.us-west-2.cloud.databricks.com` |
| UC Catalog (Zerobus) | `taran_explore.iot_simulator.iot_sensor_data` |
