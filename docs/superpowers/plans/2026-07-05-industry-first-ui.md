# Industry-First UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the IoT Digital Twin frontend as an industry-first app (gallery → per-industry setup → per-mode dashboards) on the Databricks DuBois design system, backed by an industry-template + shared-SP + per-mode-pipeline API, and redeploy it to the shared `fe-vm-shared-interop` workspace with Pharmaceutical/real-time live.

**Architecture:** The existing FastAPI backend (`app.py`, `routes/`, `services/`) already provisions catalogs, Lakebase, LakeTS, and pipelines and serves dashboard data. We add (a) an *industry template* layer that defines the 4 curated industries and their 3 pipeline modes, (b) a shared service principal + schema-per-mode / topic-per-mode wiring over the existing orchestrator, and (c) an admin role gate. The React SPA in `client/` is rebuilt to the vendored DuBois design (`Digital Twin Platform.dc.html`), wired to those APIs, reusing the existing dashboard/twin components. Deploy = `npm run build` → `databricks sync` → `apps/deployments` per `docs/deployment-playbook.md`.

**Tech Stack:** Python 3 / FastAPI / asyncpg / Databricks SDK (backend); React 18 + TypeScript + Vite + react-router-dom v7 + Recharts + Babylon/three (frontend); Databricks Apps + Lakebase + LakeTS + Unity Catalog + Zerobus + Kafka (platform); pytest (backend tests).

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-07-05-industry-first-ui-design.md` is authoritative for behavior; the vendored `Digital Twin Platform.dc.html` is authoritative for visuals (markup + CSS + interaction).
- **Curated industries (v1):** `manufacturing`, `pharmaceutical`, `energy`, `automotive` — no others.
- **Pipeline modes:** exactly three per industry — `realtime` (Kafka→SSS→Lakebase+LakeTS, hot + CDF→Delta cold), `microbatch` (Kafka→SSS→Delta), `zerobus` (Zerobus SDK→Delta). Source is fixed **Kafka** for streaming modes; only Kafka in v1.
- **Catalog:** one catalog `iot_<industry>`; **one schema per mode** (`realtime`/`microbatch`/`zerobus`). **One Kafka topic per streaming mode**: `iot.<industry>.<mode>`.
- **Shared service principal** `iot-sss-industries` (+ secret; one Lakebase Postgres role) across ALL industries — never per-pipeline.
- **Lakebase branch** `<industry>` on shared project (app.yaml `LAKEBASE_ENDPOINT` project) is created **only when the realtime mode is enabled**; kept forever.
- **Streaming metrics** apply to `realtime` + `microbatch` only (Zerobus has no Spark job).
- **Roles:** admin vs end-user resolved from **Databricks group membership**; no role toggle in the UI. Only admins see infra internals (provisioning checklist), the "add industry" tile, and Advanced settings. End users see readiness ("Controls in place" / "contact admin") and operate provided pipelines.
- **Terminology:** never use the word "flavor" in UI copy — use "pipeline"/"mode".
- **Do not regress** the existing pharmaceutical/real-time path (currently `INDUSTRIES=pharmaceutical`).
- **Deploy target:** existing Databricks App on `fe-vm-shared-interop` (us-east-1), CLI profile **`fe-vm-shared-interop`** (host `https://fe-vm-shared-interop.cloud.databricks.com`; token currently expired — re-auth with `databricks auth login -p fe-vm-shared-interop` before Task 15). Redeploy; do not provision a new workspace.
- **First iteration scope (per user, 2026-07-05):** ship & verify **`pharmaceutical` + `realtime` only** end-to-end first — set `INDUSTRIES=pharmaceutical` for iteration 1. Build the templates/components generally (all four industries, three modes) so nothing is throwaway, but only pharma/realtime is wired-live, exercised, and deployed in iteration 1. Expand `INDUSTRIES` to the four in a follow-up iteration once the slice is verified on the workspace.
- **Commit messages:** conventional commits; no AI attribution (per repo/global rules).
- **Git workflow reality (discovered at Task 1):** `client/` is **gitignored** — the React *source* is NOT version-controlled; the repo versions only the built **`static/dist/`** (`databricks sync` honors gitignore). Therefore, for **every frontend task**: implement in `client/` on disk, then `cd client && npm run build`, then commit the **build output** with `git add static/dist` (do NOT `git add client/...` — it will be rejected as ignored). `client/public/*` assets ship by being copied into `static/dist/` during the build. Backend files (`services/`, `routes/`, `tests/`, `core/`, `app.yaml`) are tracked normally.

---

## File Structure

**Backend (new):**
- `services/industry_templates.py` — the 4 curated industry definitions (sensors, KPI mapping, twin type, mode configs). One responsibility: static template data + lookups.
- `services/industry_service.py` — resolve an industry's live state (which modes are provisioned/running) by joining templates with the `pipelines` table; start/stop a mode's pipeline via the existing orchestrator.
- `services/app_permissions.py` — **exists**; extend with `is_admin(request)` group-membership check.
- `routes/industries.py` — **exists**; add `GET /api/industries/catalog`, `POST /api/industries/{id}/modes/{mode}/start`, `.../stop`, `GET /api/industries/{id}/status`.

**Backend (modified):**
- `services/provisioning_service.py` — add `ensure_shared_streaming_sp()` (idempotent shared SP) and schema-per-mode catalog helper.
- `core/config.py` — curated-industry constants + shared SP name.
- `app.yaml` — `INDUSTRIES` expands to the four; add `ADMIN_GROUP`.

**Frontend (new, under `client/src/`):**
- `styles/dubois.css` — vendored DuBois tokens (`colors_and_type.css`) + DM Sans @font-face + the design's component CSS extracted from `Digital Twin Platform.dc.html`.
- `lib/industries-api.ts` — typed client for the industry endpoints.
- `pages/industry-gallery-page.tsx` — the new home (industry cards + "Built on" row).
- `pages/industry-setup-page.tsx` — pipelines grid, ctlbar (frequency/custom data), start-progress, arch diagram, admin-gated provisioning, Advanced.
- `components/industry/pipeline-card.tsx` — one mode card (idle/starting/running states + progress + health).
- `components/industry/arch-diagram.tsx` — per-mode data-flow rows.
- `components/industry/provisioning-panel.tsx` — admin checklist vs end-user readiness.
- `pages/mode-dashboard-page.tsx` — per-mode dashboard wrapper (tier gating) reusing existing `components/dashboard/*` + `twin/PharmaPlant.tsx`.

**Frontend (modified):**
- `client/src/App.tsx` — routes for `/`, `/i/:industry`, `/i/:industry/:mode/dashboard`.
- `client/src/lib/api.ts` — add industry endpoints if the project centralizes fetches here.
- `client/vite.config.ts` / entry — import `styles/dubois.css`.

**Vendored reference (new, not shipped to users):**
- `design-ref/Digital Twin Platform.dc.html`, `design-ref/Pharma Pipeline Dashboard.dc.html`, `design-ref/_ds/**`, `client/public/brand/*.svg`.

---

## Phase 0 — Foundations & vendoring

### Task 1: Vendor the DuBois design system, brand assets, and design HTML

**Files:**
- Create: `design-ref/Digital Twin Platform.dc.html`, `design-ref/Pharma Pipeline Dashboard.dc.html`
- Create: `design-ref/_ds/colors_and_type.css`, `design-ref/_ds/styles.css`, `design-ref/_ds/fonts/DMSans-VariableFont_opsz_wght.ttf`, `design-ref/_ds/fonts/DMSans-Italic-VariableFont_opsz_wght.ttf`
- Create: `client/public/brand/data-streaming-icon-full-color.svg`, `client/public/brand/lakeflow-connect-icon-full-color.svg`, `client/public/brand/lakebase-icon-full-color.svg`, `client/public/brand/unity-catalog-icon-full-color.svg`

**Interfaces:**
- Produces: the vendored design reference + tokens later tasks port from; brand SVGs served from `/brand/*.svg`.

- [ ] **Step 1: Pull each file from the Claude Design project via DesignSync `get_file`** (projectId `5fecde80-6159-4c72-a0cb-9740c7b60163`), writing bytes to the paths above. Fonts are binary — request with base64 handling. Source paths inside the project: `_ds/databricks-dubois-design-system-8be56c9c-7994-43d6-802a-795aad5be92f/{colors_and_type.css,styles.css,fonts/*}`, `assets/brand/*.svg`, and the two `*.dc.html` at project root.

- [ ] **Step 2: Verify the token file loaded**

Run: `grep -c -- '--primary\|--background\|--foreground\|--font-sans' "design-ref/_ds/colors_and_type.css"`
Expected: non-zero (the DuBois CSS custom properties are present).

- [ ] **Step 3: Verify brand SVGs are valid**

Run: `for f in client/public/brand/*.svg; do head -c 5 "$f"; echo " $f"; done`
Expected: each starts with `<svg` or `<?xml`.

- [ ] **Step 4: Commit**

```bash
git add "design-ref" client/public/brand
git commit -m "chore: vendor DuBois design system, brand assets, and design reference"
```

---

### Task 2: Industry template module (backend data)

**Files:**
- Create: `services/industry_templates.py`
- Test: `tests/test_industry_templates.py`

**Interfaces:**
- Produces:
  - `CURATED_INDUSTRIES: list[str]` = `["manufacturing","pharmaceutical","energy","automotive"]`
  - `MODES: list[str]` = `["realtime","microbatch","zerobus"]`
  - `get_template(industry: str) -> IndustryTemplate | None`
  - `IndustryTemplate` dataclass: `id: str`, `name: str`, `accent: str`, `icon: str`, `twin_type: str`, `tag: str`, `lede: str`, `sensors: list[str]`, `kpis: list[dict]`, `default_hz: int`.
  - `mode_config(mode: str) -> dict` → `{id, name, to, desc, uses_kafka: bool, uses_lakebase: bool, has_metrics: bool}`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_industry_templates.py
from services.industry_templates import (
    CURATED_INDUSTRIES, MODES, get_template, mode_config,
)

def test_four_curated_industries():
    assert CURATED_INDUSTRIES == ["manufacturing", "pharmaceutical", "energy", "automotive"]

def test_get_template_pharma_has_kpis_and_twin():
    t = get_template("pharmaceutical")
    assert t is not None
    assert t.twin_type == "vessel"
    assert len(t.kpis) >= 3
    assert "reactor" in " ".join(k["k"].lower() for k in t.kpis)

def test_get_template_unknown_returns_none():
    assert get_template("banking") is None

def test_mode_config_flags():
    assert mode_config("realtime") == {
        "id": "realtime", "name": "Real-time", "to": "Lakebase + LakeTS",
        "desc": mode_config("realtime")["desc"], "uses_kafka": True,
        "uses_lakebase": True, "has_metrics": True,
    }
    assert mode_config("zerobus")["uses_kafka"] is False
    assert mode_config("zerobus")["uses_lakebase"] is False
    assert mode_config("zerobus")["has_metrics"] is False
    assert mode_config("microbatch")["has_metrics"] is True
    assert mode_config("microbatch")["uses_lakebase"] is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_industry_templates.py -v`
Expected: FAIL (module not found).

- [ ] **Step 3: Implement `services/industry_templates.py`**

Port the `IND`, `MODES`, and `MODEMETA` data objects from the vendored `design-ref/Digital Twin Platform.dc.html` `<script type="text/x-dc">` block (the `IND={manufacturing:{…},pharmaceutical:{…},energy:{…},automotive:{…}}` object and `MODES`/`MODEMETA`) into Python. Use a frozen dataclass for `IndustryTemplate`. `sensors` maps to the industry's sensor set — derive names from the design's `stream` arrays (e.g. pharma: reactor temp, vessel pressure, WFI flow, pH, dissolved O₂, agitator RPM). Implement `mode_config` with the flags in the test (realtime/microbatch/zerobus). Keep `desc` strings copied verbatim from `MODES`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_industry_templates.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add services/industry_templates.py tests/test_industry_templates.py
git commit -m "feat: add curated industry templates and mode configs"
```

---

## Phase 1 — Backend industry & pipeline API

### Task 3: Shared streaming service principal (idempotent)

**Files:**
- Modify: `services/provisioning_service.py`
- Test: `tests/test_shared_sp.py`

**Interfaces:**
- Consumes: existing SP helpers in `provisioning_service.py` (the same ones `pipeline_setup_service.ensure_streaming_sp` uses — reuse them, do not duplicate).
- Produces: `ensure_shared_streaming_sp(state) -> dict` returning `{"client_id": str, "display_name": "iot-sss-industries", "created": bool}`, idempotent (returns existing if present).

- [ ] **Step 1: Write the failing test** (mock the SDK client on `state`)

```python
# tests/test_shared_sp.py
import types
from services.provisioning_service import ensure_shared_streaming_sp

class _SPList:
    def __init__(self, existing): self._e = existing
    def list(self, **kw): return self._e
def _state(existing=None):
    st = types.SimpleNamespace()
    st.workspace_client = types.SimpleNamespace(
        service_principals=_SPList(existing or []))
    return st

def test_returns_existing_sp_without_creating(monkeypatch):
    sp = types.SimpleNamespace(application_id="abc-123",
                               display_name="iot-sss-industries")
    out = ensure_shared_streaming_sp(_state([sp]))
    assert out["client_id"] == "abc-123"
    assert out["created"] is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_shared_sp.py -v`
Expected: FAIL (function not defined).

- [ ] **Step 3: Implement `ensure_shared_streaming_sp`** in `services/provisioning_service.py`

Look up an SP named `iot-sss-industries` via `workspace_client.service_principals.list()`. If found, return its `application_id` with `created=False`. If not, create it (mirroring the existing `ensure_streaming_sp` creation path — secret scope `iot-secrets`, OAuth secret, `CAN_USE` app grant, register as Lakebase Postgres role once against the app.yaml `LAKEBASE_ENDPOINT` project), return `created=True`. Reuse existing private helpers; the only new behavior is the fixed shared name and "reuse if exists".

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_shared_sp.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/provisioning_service.py tests/test_shared_sp.py
git commit -m "feat: idempotent shared streaming service principal (iot-sss-industries)"
```

---

### Task 4: Industry service — status resolution

**Files:**
- Create: `services/industry_service.py`
- Test: `tests/test_industry_service.py`

**Interfaces:**
- Consumes: `industry_templates.get_template`, `CURATED_INDUSTRIES`, `MODES`, `mode_config`; the `pipelines` table access already used by `routes/pipelines.py` / `services/pipeline_db.py` (reuse the existing pipeline-list function; do not write new SQL if a helper exists).
- Produces:
  - `async def get_catalog(state) -> list[dict]` → one entry per curated industry: `{id, name, accent, icon, twin_type, tag, modes: [{id,name,to,desc,status}], running_count}` where `status ∈ {"idle","starting","running"}`.
  - `pipeline_name(industry, mode) -> str` = `f"iot-{industry}-{mode}"` (deterministic mapping used to find a mode's pipeline row).
  - `catalog_name(industry) -> str` = `f"iot_{industry}"`; `topic_name(industry, mode) -> str` = `f"iot.{industry}.{mode}"`.

- [ ] **Step 1: Write the failing test** (monkeypatch the pipeline-list dependency)

```python
# tests/test_industry_service.py
import asyncio, types
import services.industry_service as isvc

def test_naming_helpers():
    assert isvc.pipeline_name("energy","microbatch") == "iot-energy-microbatch"
    assert isvc.catalog_name("energy") == "iot_energy"
    assert isvc.topic_name("energy","microbatch") == "iot.energy.microbatch"

def test_get_catalog_marks_running(monkeypatch):
    async def fake_list(_state):
        return [{"name":"iot-pharmaceutical-realtime","status":"running"}]
    monkeypatch.setattr(isvc, "_list_pipelines", fake_list)
    cat = asyncio.get_event_loop().run_until_complete(isvc.get_catalog(None))
    pharma = next(c for c in cat if c["id"]=="pharmaceutical")
    rt = next(m for m in pharma["modes"] if m["id"]=="realtime")
    assert rt["status"] == "running"
    assert pharma["running_count"] == 1
    energy = next(c for c in cat if c["id"]=="energy")
    assert all(m["status"]=="idle" for m in energy["modes"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_industry_service.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement `services/industry_service.py`**

Implement the naming helpers and `get_catalog`. Add a thin `async def _list_pipelines(state)` that delegates to the existing pipeline-list function (inspect `services/pipeline_db.py` / `routes/pipelines.py` for the real accessor and call it; the test monkeypatches `_list_pipelines`). Join the pipeline rows to templates by `pipeline_name(industry, mode)`; map a row's lifecycle state to `running`/`starting`; absent → `idle`. Build each industry entry from `get_template` + `mode_config`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_industry_service.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/industry_service.py tests/test_industry_service.py
git commit -m "feat: industry status resolution service"
```

---

### Task 5: Start/stop a mode's pipeline

**Files:**
- Modify: `services/industry_service.py`
- Test: `tests/test_industry_service_start.py`

**Interfaces:**
- Consumes: `ensure_shared_streaming_sp` (Task 3), `services/pipeline_setup_service.setup_pipeline_lakebase` (existing orchestrator), existing pipeline create + job start functions in `routes/pipelines.py`/`services/pipeline_service.py`.
- Produces:
  - `async def start_mode(state, industry, mode) -> dict` → `{"pipeline": name, "status": "starting", "steps": [...]}`. Provisions (idempotent) then starts: catalog `iot_<industry>` with schema `<mode>`; shared SP; **Lakebase branch + LakeTS only when `mode == "realtime"`**; Kafka topic `iot.<industry>.<mode>` for streaming modes; deploy/start the pipeline job.
  - `async def stop_mode(state, industry, mode) -> dict` → stops the job, keeps assets (branch kept forever).

- [ ] **Step 1: Write the failing test** (mock orchestrator + SP; assert realtime-only branch)

```python
# tests/test_industry_service_start.py
import asyncio, types
import services.industry_service as isvc

def _run(c): return asyncio.get_event_loop().run_until_complete(c)

def test_start_zerobus_skips_lakebase(monkeypatch):
    calls = {"branch": 0, "sp": 0}
    monkeypatch.setattr(isvc, "ensure_shared_streaming_sp",
                        lambda s: calls.__setitem__("sp", calls["sp"]+1) or {"client_id":"x"})
    monkeypatch.setattr(isvc, "_provision_lakebase_branch",
                        lambda *a, **k: calls.__setitem__("branch", calls["branch"]+1))
    monkeypatch.setattr(isvc, "_provision_catalog_schema", lambda *a, **k: None)
    monkeypatch.setattr(isvc, "_deploy_and_start", lambda *a, **k: {"status":"starting"})
    out = _run(isvc.start_mode(None, "automotive", "zerobus"))
    assert out["pipeline"] == "iot-automotive-zerobus"
    assert calls["branch"] == 0   # no Lakebase for zerobus
    assert calls["sp"] == 1

def test_start_realtime_creates_branch(monkeypatch):
    calls = {"branch": 0}
    monkeypatch.setattr(isvc, "ensure_shared_streaming_sp", lambda s: {"client_id":"x"})
    monkeypatch.setattr(isvc, "_provision_lakebase_branch",
                        lambda *a, **k: calls.__setitem__("branch", calls["branch"]+1))
    monkeypatch.setattr(isvc, "_provision_catalog_schema", lambda *a, **k: None)
    monkeypatch.setattr(isvc, "_deploy_and_start", lambda *a, **k: {"status":"starting"})
    _run(isvc.start_mode(None, "manufacturing", "realtime"))
    assert calls["branch"] == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_industry_service_start.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement `start_mode` / `stop_mode`** in `services/industry_service.py`

Add thin private wrappers `_provision_catalog_schema`, `_provision_lakebase_branch`, `_deploy_and_start` that call the existing provisioning/orchestrator/job functions (wire to real names found in `services/provisioning_service.py`, `services/pipeline_setup_service.py`, `services/pipeline_service.py`). `start_mode` order: catalog+schema → shared SP → (realtime only) Lakebase branch+LakeTS via `setup_pipeline_lakebase` → topic (streaming modes) → `_deploy_and_start`. Guard `mode_config(mode)["uses_lakebase"]` for the branch step.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_industry_service_start.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/industry_service.py tests/test_industry_service_start.py
git commit -m "feat: start/stop a mode pipeline with realtime-only Lakebase branch"
```

---

### Task 6: Admin role gate

**Files:**
- Modify: `services/app_permissions.py`
- Test: `tests/test_app_permissions.py` (exists — add cases)

**Interfaces:**
- Produces: `def is_admin(request) -> bool` — true if the caller's identity is in the `ADMIN_GROUP` env group (via SDK group membership) OR the app owner; false otherwise. Reads forwarded identity headers the same way existing code in `app_permissions.py` does.

- [ ] **Step 1: Write the failing test**

```python
def test_is_admin_true_for_group_member(monkeypatch):
    from services import app_permissions as ap
    monkeypatch.setenv("ADMIN_GROUP", "iot-admins")
    monkeypatch.setattr(ap, "_user_groups", lambda req: {"iot-admins", "users"})
    assert ap.is_admin(object()) is True

def test_is_admin_false_for_non_member(monkeypatch):
    from services import app_permissions as ap
    monkeypatch.setenv("ADMIN_GROUP", "iot-admins")
    monkeypatch.setattr(ap, "_user_groups", lambda req: {"users"})
    assert ap.is_admin(object()) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_app_permissions.py -v`
Expected: FAIL on the two new tests.

- [ ] **Step 3: Implement `is_admin` + `_user_groups`** in `services/app_permissions.py`

`_user_groups(request)` resolves the caller's group display names via the SDK (`workspace_client.current_user`/`groups`) using the forwarded user token/header the module already reads. `is_admin` returns `ADMIN_GROUP in groups` (default group `iot-admins` if unset) or app-owner fallback.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_app_permissions.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/app_permissions.py tests/test_app_permissions.py
git commit -m "feat: admin gate via Databricks group membership"
```

---

### Task 7: Industry API routes

**Files:**
- Modify: `routes/industries.py`
- Test: `tests/test_routes_industries.py`

**Interfaces:**
- Consumes: `industry_service.get_catalog/start_mode/stop_mode`, `app_permissions.is_admin`.
- Produces (FastAPI routes):
  - `GET /api/industries/catalog` → `{industries: [...], is_admin: bool}` (from `get_catalog` + `is_admin`).
  - `POST /api/industries/{industry}/modes/{mode}/start` → `start_mode` result (400 if industry/mode not curated).
  - `POST /api/industries/{industry}/modes/{mode}/stop` → `stop_mode` result.

- [ ] **Step 1: Write the failing test** (FastAPI `TestClient`, monkeypatch `industry_service`)

```python
# tests/test_routes_industries.py
from fastapi.testclient import TestClient
import routes.industries as ri

def test_catalog_endpoint(monkeypatch, app_client):  # app_client fixture builds the app
    async def fake_cat(_s): return [{"id":"pharmaceutical","modes":[]}]
    monkeypatch.setattr(ri.industry_service, "get_catalog", fake_cat)
    monkeypatch.setattr(ri.app_permissions, "is_admin", lambda r: False)
    r = app_client.get("/api/industries/catalog")
    assert r.status_code == 200
    assert r.json()["is_admin"] is False
    assert r.json()["industries"][0]["id"] == "pharmaceutical"

def test_start_rejects_unknown_industry(app_client):
    r = app_client.post("/api/industries/banking/modes/realtime/start")
    assert r.status_code == 400
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_routes_industries.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement the routes** in `routes/industries.py` (keep existing endpoints). Validate `industry in CURATED_INDUSTRIES` and `mode in MODES` → 400 otherwise. Wire to `industry_service`. Add `is_admin` to the catalog payload.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_routes_industries.py -v`
Expected: PASS. Then full backend suite: `python -m pytest -q` → no regressions.

- [ ] **Step 5: Commit**

```bash
git add routes/industries.py tests/test_routes_industries.py
git commit -m "feat: industry catalog + mode start/stop API"
```

---

## Phase 2 — Frontend React rebuild (DuBois)

> Frontend tasks verify via `cd client && npm run build` (tsc + vite) and `npm run lint`, plus a visual devloop (`npm run dev`) using the `fe-specialized-agents:web-devloop-tester` agent where noted. There is no vitest harness in the repo; keep logic in small pure functions and lean on typecheck + lint + build as the gate.

### Task 8: DuBois styling in the React app

**Files:**
- Create: `client/src/styles/dubois.css`
- Modify: `client/src/main.tsx` (or the app entry that imports global CSS)

**Interfaces:**
- Produces: global CSS custom properties (`--primary`, `--background`, `--foreground`, `--secondary`, `--border`, `--muted-foreground`, `--font-sans`, `--font-display`, `--font-mono`, tier vars `--sse/--lakebase/--dbsql/--cdf/--ind`) + DM Sans `@font-face` + the design's component classes, available app-wide; `[data-theme]` light/dark support.

- [ ] **Step 1: Assemble `dubois.css`** — copy `design-ref/_ds/colors_and_type.css` contents, add `@font-face` for DM Sans pointing at fonts copied to `client/public/fonts/` (copy the two TTFs from `design-ref/_ds/fonts/`), then paste the `<style>` block from `design-ref/Digital Twin Platform.dc.html` (the app-shell/card/panel/fcard/arch/dashboard classes + the `:root` tier palette + `[data-theme="dark"]` overrides).

- [ ] **Step 2: Import it** — add `import "./styles/dubois.css";` to `client/src/main.tsx`.

- [ ] **Step 3: Build to verify no CSS/asset errors**

Run: `cd client && npm run build`
Expected: build succeeds; `static/dist/assets/*.css` includes the DuBois tokens (`grep -rl -- '--font-display' static/dist/assets`).

- [ ] **Step 4: Commit**

```bash
git add client/src/styles/dubois.css client/public/fonts client/src/main.tsx
git commit -m "feat: adopt DuBois design tokens and fonts in the React app"
```

---

### Task 9: Typed industry API client

**Files:**
- Create: `client/src/lib/industries-api.ts`

**Interfaces:**
- Produces:
  - `type PipelineMode = "realtime" | "microbatch" | "zerobus"`
  - `type ModeStatus = "idle" | "starting" | "running"`
  - `interface IndustryMode { id: PipelineMode; name: string; to: string; desc: string; status: ModeStatus }`
  - `interface Industry { id: string; name: string; accent: string; icon: string; twinType: string; tag: string; lede: string; modes: IndustryMode[]; runningCount: number }`
  - `interface Catalog { industries: Industry[]; isAdmin: boolean }`
  - `getCatalog(): Promise<Catalog>`, `startMode(industry, mode): Promise<void>`, `stopMode(industry, mode): Promise<void>` (fetch the Task 7 routes; camelCase-map the JSON).

- [ ] **Step 1: Implement `industries-api.ts`** with the types and three functions using `fetch` (follow the base-URL/error pattern already in `client/src/lib/api.ts`).

- [ ] **Step 2: Typecheck**

Run: `cd client && npx tsc --noEmit`
Expected: no type errors.

- [ ] **Step 3: Commit**

```bash
git add client/src/lib/industries-api.ts
git commit -m "feat: typed industry API client"
```

---

### Task 10: Industry gallery page

**Files:**
- Create: `client/src/pages/industry-gallery-page.tsx`
- Modify: `client/src/App.tsx` (route `/`)

**Interfaces:**
- Consumes: `getCatalog`, `Industry`. Produces: default route rendering.

- [ ] **Step 1: Implement the page** — port the `#view-gallery` markup + `renderGallery()` logic from `design-ref/Digital Twin Platform.dc.html` into JSX: header (eyebrow/title/lede + badge), the **"Built on"** row (four brand SVGs from `/brand/*.svg`), and the `.cards` grid. Each card = industry name, tag, `.fchips` (mode chips lit by `status==="running"`), `runningCount`/3 status, and headline stats. The admin-only "+ Add industry pipeline" tile renders when `catalog.isAdmin`. Card click → `navigate(\`/i/${id}\`)`. Fetch catalog in `useEffect`; show a loading skeleton.

- [ ] **Step 2: Wire the route** — in `App.tsx`, set `/` to `IndustryGalleryPage` (move the old home to `/legacy` if you want a fallback; not required).

- [ ] **Step 3: Build + visual check**

Run: `cd client && npm run build` then `npm run dev` and use the `fe-specialized-agents:web-devloop-tester` agent to confirm the gallery renders (4 cards + Built-on row, light/dark toggle) with no console errors.
Expected: gallery matches the design; no errors.

- [ ] **Step 4: Commit**

```bash
git add client/src/pages/industry-gallery-page.tsx client/src/App.tsx
git commit -m "feat: industry gallery page"
```

---

### Task 11: Pipeline card + arch diagram + provisioning panel components

**Files:**
- Create: `client/src/components/industry/pipeline-card.tsx`, `client/src/components/industry/arch-diagram.tsx`, `client/src/components/industry/provisioning-panel.tsx`

**Interfaces:**
- Consumes: `IndustryMode`, `Industry`, `startMode`, `stopMode`, catalog `isAdmin`.
- Produces:
  - `<PipelineCard mode industry onOpenDashboard onChanged />` — renders idle (▶ Start), starting (progress bar + staged step list `Checking controls → Producer generating data → Kafka topic receiving → Spark SSS consuming → Writing to sink → Dashboard live`; Zerobus variant), running (health strip + Open dashboard + Stop). Real-time shows "◷ Time-series powered by LakeTS". Calls `startMode`/`stopMode`; polls `getCatalog`/status until `running` to advance the visual (do not fake completion beyond what the backend reports).
  - `<ArchDiagram industry />` — one flow row per running mode (port `archFlow`/`renderArch` markup, using brand SVGs for Kafka/Lakebase/UC/Zerobus nodes).
  - `<ProvisioningPanel industry isAdmin />` — admin: live checklist (catalog, shared SP, Lakebase branch+LakeTS only if realtime running, one line per running mode); end user: "Controls in place — ready to run" + "contact your admin" note. **No SP/catalog internals for non-admins.**

- [ ] **Step 1: Implement the three components** porting markup/classes from the design file (`.fcard`, `.fc-prog`, `.pv-list`, `.arch-flow`, `.prov-list`). Keep the staged step arrays and health chains from the design's `STAGES`/`HEALTH`.

- [ ] **Step 2: Typecheck + lint**

Run: `cd client && npx tsc --noEmit && npm run lint`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add client/src/components/industry
git commit -m "feat: pipeline card, arch diagram, provisioning panel"
```

---

### Task 12: Industry setup page

**Files:**
- Create: `client/src/pages/industry-setup-page.tsx`
- Modify: `client/src/App.tsx` (route `/i/:industry`)

**Interfaces:**
- Consumes: `getCatalog`, `PipelineCard`, `ArchDiagram`, `ProvisioningPanel`.

- [ ] **Step 1: Implement the page** — port `#view-setup`: crumbs, setup head (eyebrow/title/lede + status badge), the **ctlbar** (data frequency range + "View sample" + "Bring custom data" + topic note `iot.<industry>.<mode>`), the `sec-label` "Pipelines", the `.flavors` grid of `<PipelineCard>` (3 modes), the `<ArchDiagram>` panel, the admin-only Advanced `<details>`, and the `<ProvisioningPanel>`. Resolve `:industry` from the route + catalog; `onOpenDashboard(mode)` → `navigate(\`/i/${industry}/${mode}/dashboard\`)`.

- [ ] **Step 2: Wire the route** `/i/:industry` → `IndustrySetupPage`.

- [ ] **Step 3: Build + visual check** (web-devloop-tester): start a non-pharma mode (e.g. energy/microbatch) and confirm the staged progress renders and the arch row appears; confirm end-user vs admin provisioning views differ (temporarily force `isAdmin`).

- [ ] **Step 4: Commit**

```bash
git add client/src/pages/industry-setup-page.tsx client/src/App.tsx
git commit -m "feat: industry setup page with pipelines, arch, provisioning"
```

---

### Task 13: Per-mode dashboard (reuse existing dashboard, tier-gated, real data)

**Files:**
- Create: `client/src/pages/mode-dashboard-page.tsx`
- Modify: `client/src/App.tsx` (route `/i/:industry/:mode/dashboard`)

**Interfaces:**
- Consumes: existing dashboard data hooks/components (`client/src/components/dashboard/*`, `client/src/pages/dashboard-page.tsx` logic, `twin/PharmaPlant.tsx`), existing routes `routes/sensor_data.py` + `routes/lakets.py`.

- [ ] **Step 1: Implement the wrapper** — port `#view-dashboard` chrome (topbar, KPI row, tier sections) styled with DuBois classes, then mount the **existing** dashboard data components. Apply the mode's tier gating from `MODEMETA`: realtime → SSE twin + Lakebase hot + streaming metrics + CDF cold; microbatch → live-preview twin + Delta section + streaming metrics, no CDF; zerobus → Delta only, no SSE/Lakebase, no streaming metrics. For **pharmaceutical/realtime**, wire to the real SSE/Lakebase/DBSQL endpoints (this path already works today). For not-yet-running modes, render empty states with a "Start pipeline" CTA back to setup.

- [ ] **Step 2: Wire the route** `/i/:industry/:mode/dashboard` → `ModeDashboardPage`.

- [ ] **Step 3: Build + visual check** against the live pharma pipeline via web-devloop-tester: confirm live SSE stream, Lakebase charts, and CDF banner render with real data and no console errors; confirm a Delta-only mode hides SSE/Lakebase/metrics.

- [ ] **Step 4: Commit**

```bash
git add client/src/pages/mode-dashboard-page.tsx client/src/App.tsx
git commit -m "feat: per-mode dashboard with tier gating and live pharma data"
```

---

## Phase 3 — Wire & deploy

### Task 14: Config — expand industries + admin group

**Files:**
- Modify: `app.yaml`, `core/config.py`

**Interfaces:**
- Produces: runtime `INDUSTRIES=manufacturing,pharmaceutical,energy,automotive`, `ADMIN_GROUP` env; config constants for the shared SP name.

- [ ] **Step 1:** In `app.yaml`, keep `INDUSTRIES` value at `"pharmaceutical"` for **iteration 1** (thin slice); add a comment `# expand to manufacturing,pharmaceutical,energy,automotive after slice verified`. Add `- name: ADMIN_GROUP` / `value: "iot-admins"` (confirm the real group name with the user before deploy). Add a comment that the shared SP is `iot-sss-industries`.

- [ ] **Step 2:** In `core/config.py`, add `SHARED_SP_NAME = "iot-sss-industries"` and a helper to read `INDUSTRIES` filtered to `CURATED_INDUSTRIES`.

- [ ] **Step 3: Run backend suite**

Run: `python -m pytest -q`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add app.yaml core/config.py
git commit -m "chore: expand INDUSTRIES to curated four + admin group config"
```

---

### Task 15: Build, deploy to fe-vm-shared-interop, verify

**Files:** none (deploy operation). Follow `docs/deployment-playbook.md` §4.8.

- [ ] **Step 1: Build the frontend**

Run: `cd client && npm run build && cd ..`
Expected: `static/dist/index.html` + hashed assets regenerated.

- [ ] **Step 2: Ensure the `fe-vm-shared-interop` profile is authenticated** (token was expired at plan time). If needed the user runs `databricks auth login -p fe-vm-shared-interop` in the session (`! databricks auth login -p fe-vm-shared-interop`).

Run: `databricks current-user me -p fe-vm-shared-interop --output json | jq -r .userName`
Expected: the deploying user's email.

- [ ] **Step 3: Sync + upload wheel + deploy** (app name `iot-digital-twin`)

```bash
PROFILE=fe-vm-shared-interop; APP=iot-digital-twin
USER=$(databricks current-user me -p $PROFILE --output json | jq -r .userName)
TARGET="/Workspace/Users/$USER/$APP"
databricks sync . "$TARGET" --watch=false -p $PROFILE
databricks workspace import "$TARGET/iot_data_simulator-0.1.0-py3-none-any.whl" \
  --file iot_data_simulator-0.1.0-py3-none-any.whl -p $PROFILE --overwrite
databricks api post "/api/2.0/apps/$APP/deployments" -p $PROFILE \
  --json '{"source_code_path":"'$TARGET'","mode":"SNAPSHOT"}'
```
Expected: deployment accepted; poll app status to `ACTIVE`.

- [ ] **Step 4: Verify**

- `GET /api/health` → `db_available: true`.
- `GET /api/industries/catalog` → 4 industries; pharmaceutical/realtime `status: running`.
- Open the app: gallery shows 4 cards + Built-on row; open Pharmaceutical → real-time dashboard shows live SSE/Lakebase data; start Energy/micro-batch → staged progress → Delta dashboard.

- [ ] **Step 5: Tag the deploy**

```bash
git tag -a industry-ui-v1 -m "Industry-first UI v1 on fe-vm-shared-interop"
```

---

## Self-Review

- **Spec coverage:** object model → Tasks 2,4,5; shared SP → Task 3; schema/topic per mode → Tasks 4,5; realtime-only Lakebase branch → Task 5; roles/admin gate → Tasks 6,7,11; metrics realtime+microbatch only → Tasks 2,13; gallery/setup/dashboard screens → Tasks 10,12,13; DuBois design → Tasks 1,8; deploy to shared-interop → Tasks 14,15. Covered.
- **Placeholder scan:** frontend tasks reference the vendored `design-ref/Digital Twin Platform.dc.html` as the concrete markup/CSS source (not a placeholder — it's a committed artifact); backend tasks carry real test code. No TODO/TBD steps.
- **Type consistency:** `PipelineMode`/`ModeStatus`/`Industry`/`IndustryMode`/`Catalog` used consistently across Tasks 9–13; `pipeline_name`/`catalog_name`/`topic_name`/`start_mode`/`stop_mode`/`get_catalog`/`ensure_shared_streaming_sp`/`is_admin` names match across Tasks 3–7.

## Open items to confirm before Task 14–15
- **CLI profile:** `fe-vm-shared-interop` (resolved) — re-auth before deploy (token expired at plan time).
- **Admin group:** defaulting to `iot-admins`; confirm the real Databricks group name before relying on the role gate in production.
- Whether the shared-interop app uses **static PG creds** (playbook §2.1) — if so, no SP token changes needed for the app backend; the shared *streaming* SP (Task 3) is separate.
