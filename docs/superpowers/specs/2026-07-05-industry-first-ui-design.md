# Industry-First UI — Design Spec

**Date:** 2026-07-05
**Status:** Approved for planning
**Mockup:** `mockups/industry-first-ui.html` (interactive; also published as an Artifact)
**Design (source of truth for visuals):** Claude Design project `5fecde80-6159-4c72-a0cb-9740c7b60163`
— files `Digital Twin Platform.dc.html` + `Pharma Pipeline Dashboard.dc.html`, built on the
**Databricks DuBois design system** (`_ds/databricks-dubois-design-system-*`: `colors_and_type.css`,
DM Sans, brand marks). Brand assets: `assets/brand/*.svg` (Spark Structured Streaming, Zerobus/
LakeFlow, Lakebase, Unity Catalog).

## 0. Implementation & deployment decisions (2026-07-05)

- **Frontend:** rebuild the design as **React components in the existing `client/` (React 18 + Vite)
  app**, adopting the DuBois tokens (`colors_and_type.css` + DM Sans) and brand SVGs. Not a static
  HTML ship — this becomes the real product, wired to existing backend routes.
- **First deploy scope:** the new gallery shows **all 4 industries × 3 modes**; **Pharmaceutical /
  real-time is live-wired** end-to-end (already works today); the other industries/modes render the
  **start / provisioning flow** and light up as they're enabled.
- **Dashboard data:** wire to the **real** SSE / Lakebase / DBSQL routes where a pipeline is
  provisioned; show empty / "start pipeline" states otherwise. No permanent mock data.
- **Deploy target:** the existing Databricks App on the shared **`fe-vm-shared-interop`** workspace
  (us-east-1) — redeploy, do not provision a new workspace. Backend `INDUSTRIES` env expands from
  `pharmaceutical` to the curated four as their templates land.

---

## 1. Problem & goal

Today the app exposes its full machinery to every user: a 4-step wizard whose
Step 1 is a ~3,300-line surface (`client/src/pages/source-settings-page.tsx`)
for picking a streaming source, entering credentials, sizing a cluster,
generating a notebook, validating connectivity, and deploying a job. That is
powerful but overwhelming, and it is the wrong first impression for a demo /
digital-twin product.

**Goal:** make the product *industry-first*. A user picks an industry and gets a
tailored, live digital-twin dashboard. Everything underneath — catalog, service
principal, Lakebase branch, LakeTS, cluster, notebook, job — is provisioned in
the background. The detailed controls remain, but only for admins, behind an
"Advanced" escape hatch.

**Key enabler:** the backend automation already exists. `services/pipeline_setup_service.py`
is an idempotent one-click orchestrator (service principal → Lakebase project/branch/role
→ table + LakeTS + rollups → ops jobs), and `services/provisioning_service.py`
already creates catalogs, schemas, volumes, and Delta/metrics tables. This work
is **primarily a new frontend flow plus an industry-template and role/auth layer**,
not new provisioning plumbing.

---

## 2. Object model

```
Industry (fixed type)
  ├── Catalog  iot_<industry>            (one per industry, schema per pipeline mode)
  ├── Kafka    iot.<industry>.<pipeline mode>   (one topic per streaming pipeline mode)
  └── Pipeline modes  (up to 3, independently enabled — each is a pipeline + a dashboard)
        ├── real-time   → Kafka → Spark SSS → Lakebase + LakeTS (hot) → CDF → Delta (cold)
        ├── micro-batch → Kafka → Spark SSS (micro-batch) → Delta table
        └── zerobus     → Zerobus SDK → Delta table (no Kafka, no Spark job)

Shared across ALL industries:
  ├── Service principal  iot-sss-industries  (+ secret; one Postgres role)
  ├── Lakebase project   iot-industries
  └── App backend connection
```

### Curated industries (v1)

Manufacturing, Pharmaceutical, Energy, Automotive — each with a themed dashboard
(industry-specific KPIs, units, and a 3D/2D twin). Pharmaceutical reuses the
existing `PharmaPlant.tsx` twin. Other industries in the simulator wheel are out
of scope for v1 (may be added later).

### Pipeline modes (the three architectures)

| Mode | Ingest | Sink | Serving tiers on dashboard | Lakebase branch? |
|---|---|---|---|---|
| **Real-time** | Kafka `iot.<ind>.realtime` | Lakebase + LakeTS (hot), CDF → Delta (cold) | SSE (live) · Lakebase (hot) · DBSQL (cold) | **Yes** |
| **Micro-batch** | Kafka `iot.<ind>.microbatch` | Delta table (UC) | Live preview · Delta · DBSQL | No |
| **Zerobus** | Zerobus SDK (direct) | Delta table (UC) | Live preview · Delta (DBSQL) | No |

- **Multiple pipeline modes can run at once** in one industry. Each enabled pipeline mode is an
  independent pipeline with its **own dedicated dashboard**.
- A **pipeline mode is enabled/started by the user**; provisioning for that pipeline mode happens
  automatically on first start (idempotent). Disabling stops the pipeline but keeps
  provisioned assets.
- The **Lakebase branch `<industry>` is created only when the real-time pipeline mode is
  enabled**, and is then **kept forever** (per the "branches kept forever" intent).
  Delta-only pipeline modes never touch Lakebase.
- **Streaming metrics** (throughput, batch duration, end-to-end latency) apply to
  **real-time & micro-batch only** — Zerobus has no Spark streaming job, so it has no
  streaming-metrics view.

### Source

Fixed to **Apache Kafka** for streaming pipeline modes (real-time, micro-batch); **only
Kafka in v1**. Zerobus bypasses Kafka by design. The source is not user-selectable.

---

## 3. Catalog / storage layout

- **One catalog per industry:** `iot_<industry>`.
- **One schema per pipeline mode:** `realtime`, `microbatch`, `zerobus` (plus existing
  `checkpoints` / volume as needed). Each pipeline mode's tables live in its own schema.
  - Real-time cold-tier Delta (via CDF sync from Lakebase) lands in the `realtime` schema.
  - Micro-batch and Zerobus write their Delta tables into `microbatch` / `zerobus` schemas.
- **One Kafka topic per streaming pipeline mode:** `iot.<industry>.realtime`,
  `iot.<industry>.microbatch` (independent consumer offsets / retention).

---

## 4. Roles & authorization

Two roles, resolved from **Databricks group membership** (via the app's auth
context). **There is no role toggle in the real UI** — the page simply renders
admin-only affordances when the signed-in user is an admin. (The mockup's
End user / Admin switch is a preview device only.)

| Capability | End user | Admin |
|---|---|---|
| See available industries & open dashboards | ✓ | ✓ |
| Enable/disable (start/stop) the provided pipeline modes | ✓ | ✓ |
| Change frequency, bring custom data, toggle streaming metrics | ✓ | ✓ |
| **Add / remove an industry** (new catalog + assets) | — | ✓ |
| **Advanced settings** (credentials, cluster sizing, notebook, tiering overrides) | — | ✓ |

Rationale: enabling a *provided* pipeline mode is operating a pre-defined option, not
creating a custom pipeline. Creating a new **industry** is the admin-gated action.

---

## 5. Screens

### 5.1 Industry gallery (home)
Grid of industry cards. Each card shows the industry, its three pipeline mode chips lit
by what's running, an "*n*/3 running" status, and a few live headline stats.
Admins additionally see an "**+ Add industry pipeline**" tile.

### 5.2 Industry page (pipeline picker + controls)
- **Pipelines** panel: three pipeline cards — **Real-time mode** (subheading
  "Time-series powered by LakeTS"), **Micro-batch mode**, **Zerobus (Delta Ingest)**.
  Do **not** use the word "flavor" in the UI. Each card is in one of three states:
  - **Not started** → a **"▶ Start pipeline"** button.
  - **Starting** → a **staged progress bar** the user watches after clicking Start,
    stepping through: *Checking controls → Producer generating data → Kafka topic
    receiving → Spark SSS consuming → Writing to sink → Dashboard live* (Zerobus:
    *Checking controls → Producer generating data → Zerobus ingest → Writing to
    Delta → Dashboard live*). Each step shows queued / in-progress / done.
  - **Running** → a compact **live health strip** (Producer › Kafka › Spark SSS ›
    sink › Dashboard, each with a status dot) plus **"Open dashboard →"** and **Stop**.
  This gives the user real visibility into whether data is being generated, received
  by the streaming job, and reaching the dashboard.
- **Status / readiness:**
  - **End users** never see infra internals (service principal, catalog, branch).
    They see a simple readiness card: "Controls in place — ready to run", or, if not
    ready, guidance to **contact an admin**.
  - **Admins** see the full auto-provisioning checklist (catalog, shared SP,
    Lakebase branch + LakeTS only when real-time runs, one line per running pipeline),
    updated live as pipelines start/stop.
- **Controls:** data frequency (Hz, shared across the industry's pipelines), bring
  custom data (CSV/JSON or table, mapped to the industry schema), and **capture
  streaming metrics — applies to real-time & micro-batch only** (Zerobus has no
  Spark streaming job, so no streaming metrics).
- **"How your data flows"**: an architecture diagram with **one flow row per running
  pipeline**, color-coded by tier, so users understand the path end-to-end.
- **Advanced settings** (admin only): the existing detailed controls, collapsed.

### 5.3 Per-pipeline mode dashboard
Themed to the industry (KPIs, units, twin) and laid out for the pipeline mode's tiers:
- **Real-time:** live twin + SSE stream, Lakebase hot-tier charts + insertion
  health, streaming metrics (if enabled), CDF cold-tier banner + DBSQL history.
- **Micro-batch:** live preview twin, Delta-table section (batch commit health),
  streaming metrics (if enabled), DBSQL history. No CDF banner.
- **Zerobus:** live preview twin, Delta-table section (ingest health), DBSQL
  history. No SSE/Lakebase tiers, no CDF banner.

Reuses existing dashboard components (`client/src/components/dashboard/*`,
`twin/PharmaPlant.tsx`) and data routes (`routes/sensor_data.py`, `routes/lakets.py`).

---

## 6. Backend impact

**Reused as-is (no or minor change):**
- `services/pipeline_setup_service.py` — one-click orchestrator (SP, Lakebase
  project/branch/role, table, LakeTS, rollups, ops jobs).
- `services/provisioning_service.py` — catalog/schema/volume, Delta/metrics tables.
- Dashboard data routes: `routes/sensor_data.py`, `routes/lakets.py`.

**New / changed:**
- **Industry templates** (`routes/industries.py` today is only a sensor preset):
  extend to define, per curated industry, the sensor set, KPI mapping, twin type,
  and the three pipeline mode definitions (topic names, schema names, sink config).
- **Per-mode pipeline creation**: a mode → pipeline mapping that calls the
  existing orchestrator with pipeline mode-specific params (schema per pipeline mode, topic per
  pipeline mode, Lakebase only for real-time).
- **Shared SP**: one `iot-sss-industries` (create once; reuse), registered once as
  a Postgres role, granted per new catalog/branch — replaces per-pipeline SP creation.
- **Auth/role gate**: resolve admin vs end-user from Databricks group membership;
  gate add-industry + advanced endpoints server-side (not just UI).
- **Frontend**: new gallery → industry → per-pipeline mode-dashboard flow; keep the old
  wizard reachable as admin-only "Advanced".

---

## 7. Out of scope (v1)

- Sources other than Kafka (Kinesis, Event Hub, RabbitMQ, Pub/Sub) — deferred.
- Industries beyond the curated four.
- Multi-tenant isolation of the shared service principal (revisit if the app ever
  becomes multi-tenant).
- Per-pipeline mode frequency (frequency is industry-level in v1).

---

## 8. Open questions (none blocking)

- Exact Databricks group name(s) for admin resolution — decide at implementation.
- Whether custom-data upload maps per-pipeline mode or per-industry (assume per-industry).
- Retention/optimize cadence defaults for Delta-only pipeline modes.
