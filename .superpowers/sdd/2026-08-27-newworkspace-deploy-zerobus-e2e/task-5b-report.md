# Task 5b — Test Suite Stabilization + SDK Pin

**Date:** 2026-08-27  
**Branch:** feat/industry-first-ui  
**Commit:** 16f4e2c

---

## Final Test Counts

| Result  | Count |
|---------|-------|
| Passed  | 996   |
| Skipped | 21    |
| Failed  | **0** |

(Was: 52 failed before this task.)

---

## What Was Fixed

### 1. `tests/conftest.py` — conditional pre-mock (root cause of all 52 failures)

**Problem:** `_pre_mock_sdk()` unconditionally replaced `databricks.sdk` with a
`MagicMock()` via `sys.modules.setdefault(...)` before any imports ran. Because
a `MagicMock` is not a Python package, any production code that did
`from databricks.sdk.service import jobs as dj` (or any other submodule import)
raised:

```
ModuleNotFoundError: No module named 'databricks.sdk.service'; 'databricks.sdk' is not a package
```

This caused every function that lazily imports an SDK submodule to fail at
runtime (inside try/except blocks), silently swallowing the actual behaviour
the tests were asserting. The 52 failures split across four categories:

- `lakets_deploy_service` — `from databricks.sdk.service import jobs as dj`
- `pipelines_project_service` — `from databricks.sdk.service.postgres import Project`
- `provisioning_service` — `from databricks.sdk.service.postgres import ...`
- `routes/pipelines.py` — `from databricks.sdk.errors import NotFound` (inside
  `_sdk_cleanup()`) caused the entire cleanup to short-circuit; tables.delete
  was never called so assertions failed.
- `routes/provisioning.py` — `from databricks.sdk.errors import AlreadyExists`
  same pattern.

**Fix (lines changed: `tests/conftest.py`):**

`_pre_mock_sdk()` now starts with:

```python
try:
    import databricks.sdk.service.jobs  # noqa: F401
    return  # real SDK available — skip all mocking
except Exception:
    pass
```

When the real SDK is importable (our `.venv/bin/python` case), no stubs are
injected at all. When the import fails (arch-mismatch environments with broken
cffi/cryptography), stubs are still installed — but now every submodule path
that production code imports is individually registered with its own
`MagicMock()` instance so they resolve as packages, not attributes:

New stub names registered (in addition to the original set):
- `databricks.sdk.errors`
- `databricks.sdk.service`
- `databricks.sdk.service.apps`
- `databricks.sdk.service.catalog`
- `databricks.sdk.service.compute`
- `databricks.sdk.service.jobs`
- `databricks.sdk.service.postgres`
- `databricks.sdk.service.sql`
- `databricks.sdk.service.workspace`

All other fixtures (`mock_db`, `mock_app_state`, `test_client`, `mock_ws_client`,
`app_backend_conn`) are unchanged.

---

### 2. `requirements.txt` — SDK version pin

**Change:** `databricks-sdk>=0.50.0` → `databricks-sdk==0.102.0`

Pins to the exact version installed in the venv. The venv has 0.102.0, in which
`JobCluster` moved from `databricks.sdk.service.compute` to
`databricks.sdk.service.jobs`. A search of the entire codebase (`services/`,
`routes/`, `core/`, `app.py`, `backend.py`, `models/`) found **zero** usages of
`JobCluster`, so no import-path corrections were needed for that symbol.

All other ~61 SDK import sites were verified against 0.102.0:

| Symbol | Module in 0.102.0 | Status |
|---|---|---|
| `WorkspaceClient` | `databricks.sdk` | ok |
| `Config` | `databricks.sdk.config` | ok |
| `ClusterSpec`, `DataSecurityMode`, `RuntimeEngine` | `databricks.sdk.service.compute` | ok |
| `Library`, `PythonPyPiLibrary` | `databricks.sdk.service.compute` | ok |
| `AwsAttributes`, `AzureAttributes`, `EbsVolumeType` | `databricks.sdk.service.compute` | ok |
| `Environment` | `databricks.sdk.service.compute` | ok |
| `Language` (compute) | `databricks.sdk.service.compute` | ok |
| `JobCluster` | `databricks.sdk.service.jobs` (moved from compute) | ok — NOT used in codebase |
| `JobSettings`, `JobEnvironment`, `Continuous`, `PauseStatus` | `databricks.sdk.service.jobs` | ok |
| `NotebookTask`, `Task`, `Source`, `SubmitTask` | `databricks.sdk.service.jobs` | ok |
| `ImportFormat`, `Language` (workspace) | `databricks.sdk.service.workspace` | ok |
| `VolumeType` | `databricks.sdk.service.catalog` | ok |
| `TableOperation` | `databricks.sdk.service.catalog` | ok |
| `Privilege`, `PermissionsChange` | `databricks.sdk.service.catalog` | ok |
| `Disposition`, `Format`, `StatementState` | `databricks.sdk.service.sql` | ok |
| `AppAccessControlRequest`, `AppPermissionLevel` | `databricks.sdk.service.apps` | ok |
| `Project`, `Branch`, `BranchSpec` | `databricks.sdk.service.postgres` | ok |
| `Role`, `RoleRoleSpec`, `RoleAuthMethod`, `RoleIdentityType` | `databricks.sdk.service.postgres` | ok |
| `AlreadyExists`, `NotFound` | `databricks.sdk.errors` | ok |

**No symbols required import-path corrections** — all production import sites
already used the correct module for 0.102.0.

---

## Symbols Unresolvable in 0.102.0

None. Every symbol used in the codebase was found in its imported module.

---

## Commit

```
16f4e2c  fix: make SDK pre-mock conditional; pin databricks-sdk==0.102.0
```
