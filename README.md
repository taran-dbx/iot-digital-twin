# IoT Digital Twin

A **Databricks App** that demonstrates three streaming ingestion architectures for IoT
sensor data, side by side, with a live per-pipeline dashboard (KPIs, 3D twin, latency
metrics, hot/cold history). Built as a FastAPI backend serving a React SPA.

The UI is **industry-first**: pick an industry (pharmaceutical, manufacturing, oil & gas, …),
then run one or more **pipeline flavors** against it. Each flavor is a real Databricks
deployment — Kafka topic, Spark/Zerobus job, Lakebase branch, Unity Catalog objects.

## The three pipeline flavors

| Flavor | Path | Latency profile |
|---|---|---|
| **Real-time** | Kafka (MSK) → Spark Structured Streaming (Real-Time Mode, `.trigger(realTime="5 min")`) → **Lakebase LakeTS ChronoTable** (hot, time-series) **and** the app's SSE stream | milliseconds |
| **Micro-batch** | Kafka → Spark micro-batch (`processingTime="15 s"`) → **UC Delta table** | seconds |
| **Zerobus** | Zerobus SDK → **UC Delta table** (no Spark job) | direct-ingest |

## Data tiers on the dashboard

- **SSE (live):** the realtime consumer POSTs each batch to `/api/sss/ingest`; the app
  fans it out over Server-Sent Events (`/api/events`) → live KPI tiles, sensor stream, 3D twin.
- **Lakebase (hot):** the LakeTS ChronoTable (`sensor_readings`) on a per-industry branch of
  the `iot-pipelines` Lakebase project → write-perf, sink health, and LakeTS gap-fill /
  rate-of-change / value-distribution cards.
- **DBSQL (cold):** aged rows sync from Lakebase to a UC Delta table via Lakebase CDF
  (`wal2delta` → the `lb__shadow_sensor_readings_history` synced table) → the historical
  time-series panel, queried through a SQL warehouse.

Latency KPIs come from per-mode metrics tables (`iot_<industry>.metrics.streaming_metrics_<mode>`,
written by the consumer via delta-rs/Spark) for realtime/micro-batch, and from
`system.lakeflow.zerobus_ingest` for zerobus.

## Repo layout

```
app.py                     FastAPI entry (uvicorn app:app — see app.yaml)
backend.py                 LakebaseConnectionManager (OAuth-refreshing psycopg pool)
core/                      config, state, auth, shutdown
routes/                    22 API routers (health, sensor_data, industries, lakets,
                           pipeline_deploy, streaming_latency, settings, …)
services/                  38 service modules (provisioning, deployment, lakets_deploy,
                           industry_*, pipeline_*, validation_*, …)
models/                    Pydantic/DTO schemas (sensor, metrics, sql_ddl, …)
databricks/vendor/         lakets.sql — vendored LakeTS DDL (read at runtime)
client/                    React + Vite + TS SPA (gitignored; built to static/dist)
static/dist/               the built SPA the app serves (committed)
scripts/                   reusable ops/e2e tooling (bootstrap, retention, e2e)
docs/                      deployment playbook + architecture diagram
tests/                     pytest suite (57 files)
requirements.txt           installs the iot_data_simulator wheel (repo root)
```

Key generators live client-side in `client/src/lib/`: `notebook-template.ts` (the Spark
consumer notebooks), `data-producer-notebook.ts` (the Kafka producer), and
`industry-deploy-config.ts` (deterministic per-industry deploy config). The LakeTS
operational notebooks (retention/tiering/rollup/partition-manager) are **inlined** in
`services/lakets_deploy_service.py` and uploaded via the SDK.

## Local development

```bash
# Backend (needs a Databricks CLI profile + a .venv with requirements.txt)
python -m uvicorn app:app --reload --port 8000

# Frontend
cd client && npm install
npm run dev        # dev server (proxies /api to the backend)
npm run build      # production build → ../static/dist (what the deployed app serves)

# Tests
python -m pytest -q
cd client && npx tsc --noEmit
```

## Deploy

The app deploys to the `iot-digital-twin` Databricks App via `databricks sync` + a
SNAPSHOT deployment. Full runbook, prerequisites, and troubleshooting:
**[`docs/deployment-playbook.md`](docs/deployment-playbook.md)**.

### Workspace profiles

Real workspace values (hosts, warehouse id, client ids, broker lists, catalog) are **not**
committed. Only `config/workspaces/*.profile.example` templates with placeholders are tracked;
your real `config/workspaces/<name>.profile` files are gitignored (JSON, no secrets).

```bash
# 1. create your profile from a template and fill in your workspace values
cp config/workspaces/example-us-east.profile.example config/workspaces/my-workspace.profile

# 2. render app.yaml from it (do this right before `databricks sync`)
python scripts/render_app_yaml.py my-workspace

# verify without writing
python scripts/render_app_yaml.py my-workspace --check

# 3. after deploying, restore the placeholder app.yaml so real values are never committed
git checkout -- app.yaml
```

`app.yaml` stays **tracked** (with placeholder values) on purpose: `databricks sync`
honors `.gitignore`, so a gitignored `app.yaml` would silently not be uploaded and the
app would fail to start. `scripts/check_identifiers.py` (pre-commit + CI) and
`tests/test_render_app_yaml.py` fail if workspace-specific values are committed.

Admin access is group-based: members of `ADMIN_GROUP` (default `iot-admins`) are admins.
`ADMIN_EMAILS` remains supported as an optional comma-separated override; leave it out of
committed files.

Dev scripts under `scripts/` never default to a Databricks CLI profile: pass
`--profile <name>` or set `DATABRICKS_CONFIG_PROFILE`.

`databricks sync` honors `.gitignore` and `.databricksignore`. Only `static/dist`,
`databricks/vendor/lakets.sql`, the wheels, and the Python backend ship to the deployed
app — `client/` source, `mockups/`, `design-ref/`, `scripts/`, `config/`, and `docs/`
are dev-only (excluded via `.databricksignore`).
