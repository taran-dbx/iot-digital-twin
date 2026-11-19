# Deploying with Declarative Automation Bundles (DABs)

> **WARNING: `prod` resolves to the live app name (`iot-digital-twin`).** Always
> `databricks bundle deployment bind` first, review `databricks bundle plan -t prod`,
> and never deploy without prod overrides
> (`.databricks/bundle/prod/variable-overrides.json`). Workspace variables have no
> defaults, so validation fails until they are supplied; do not work around that
> with placeholder values.

The app is deployed with the bundle in `databricks.yml` + `resources/app.yml`.
It replaces manual `databricks sync`, `.databricksignore` and static
`PGPASSWORD` logins (see [deployment-playbook.md](deployment-playbook.md) for
the historical issue log).

## Layout

| File | Purpose |
| --- | --- |
| `databricks.yml` | Bundle name, variables, `sync` rules, `dev` / `prod` targets |
| `resources/app.yml` | The App: command, env, and bound resources (SQL warehouse, Genie space, Lakebase Postgres) |
| `app.yaml` | Placeholder-only file kept for non-bundle deploys (rendered by `scripts/render_app_yaml.py`); when the bundle sets `config`, the bundle wins |

## Commands

Never commit a workspace host. Pass the CLI profile with `--profile` (or
`DATABRICKS_CONFIG_PROFILE`).

```bash
make bundle-validate PROFILE=<profile>              # validate --strict, target dev
databricks bundle validate --strict -t dev --output json --profile <profile>   # inspect resolved app name
databricks bundle deploy -t dev --profile <profile>
databricks bundle run iot_twin -t dev --profile <profile>   # (re)deploys source + starts the app
databricks apps logs <app-name> --profile <profile>
databricks bundle destroy -t dev --profile <profile>        # dev only
```

Validation needs workspace auth, so it is **not** run in CI; run it locally
(`make bundle-validate`) before changing bundle files.

## Variables

Workspace-specific variables have **no default**; a missing value fails
validation. Copy `variable-overrides.json.example` (placeholders only) to the
gitignored `.databricks/bundle/<target>/variable-overrides.json` and fill in real
values (or use `--var name=value` / `BUNDLE_VAR_<name>`). `make bundle-validate`
stops with a clear message when the file for `BUNDLE_TARGET` is missing.
Variables with defaults (`uc_schema`, `lakebase_branch`, `lakebase_database`,
`industries`, `admin_group`, `admin_emails`, `zerobus_secret_key`)
are not workspace-specific.

| Variable | Env var / resource it feeds | Notes |
| --- | --- | --- |
| `app_name` | App name | Workspace-global, max 30 chars. Dev default is the fixed `iot-twin-dev`, so override it per developer (e.g. `iot-twin-dev-<initials>`, check `bundle validate --output json`); `prod` is fixed to `iot-digital-twin` |
| `uc_catalog`, `uc_schema` | `UC_CATALOG`, `UC_SCHEMA` | |
| `warehouse_id` | `sql-warehouse` resource (CAN_USE) → `DATABRICKS_SQL_WAREHOUSE_ID` via `value_from` | |
| `genie_space_id` | `genie-space` resource (CAN_VIEW) → `GENIE_SPACE_ID` via `value_from` | |
| `lakebase_project`, `lakebase_branch`, `lakebase_database` | `lakebase` Postgres resource (CAN_CONNECT_AND_CREATE), `LAKEBASE_PROJECT`, `LAKEBASE_ENDPOINT` | |
| `pghost` | `PGHOST` | Not injected by the Postgres resource; keep as a variable |
| `pipelines_lakebase_project` | `PIPELINES_LAKEBASE_PROJECT` | |
| `msk_bootstrap_servers` | `MSK_BOOTSTRAP_SERVERS` | |
| `zerobus_server_endpoint`, `zerobus_client_id` | `ZEROBUS_SERVER_ENDPOINT`, `ZEROBUS_CLIENT_ID` | |
| `uc_service_credential` | `UC_SERVICE_CREDENTIAL` | |
| `zerobus_secret_key` | `ZEROBUS_SECRET_KEY` | Default `zerobus-client-secret` (the runtime default). `app.yaml` emits it empty, but the Apps API rejects empty env values in a bundle `config` |
| `industries`, `admin_group`, `admin_emails` | `INDUSTRIES`, `ADMIN_GROUP`, `ADMIN_EMAILS` | |

**Not in the bundle:** `ZEROBUS_TABLE_NAME`, `ZEROBUS_SECRET_SCOPE`, `UC_STORAGE_BASE`
and `UC_EXTERNAL_LOCATION` are empty in `app.yaml` (unset-equivalent at runtime),
and the Apps API rejects empty env values for bundle `config` (deploy fails with
"Must specify environment variable source"). They are therefore left unset, which
the runtime treats identically. To use one, add an `env` entry with a real value
in `resources/app.yml` and a matching variable (and update the drift test's
`empty_only` set in `tests/test_render_app_yaml.py`).

`LAKETS_SQL_PATH` is no longer set: `lakets.sql` is resolved relative to the
package (`iot_twin.core.paths`). The env var still overrides it.

## What ships

`sync` in `databricks.yml` replaces `.databricksignore`. It ships `src/`,
`app.yaml`, `requirements.txt`, `pyproject.toml`, `lib/*.whl` and
`static/dist/` (the built SPA), and excludes `client/`, `docs/`, `tests/`,
`qa/`, `scripts/`, `config/`, `design-ref/` and tooling dirs.

* **Frontend:** `static/dist` is committed and CI fails if it is stale, so the
  bundle does not build it. Run `make frontend-build` and commit before deploying
  frontend changes.
* **Simulator wheel:** `lib/iot_data_simulator-*.whl` is a committed binary (its
  source is not in this repo) installed via `requirements.txt`; there is no
  artifact build for it.

## Dev vs prod

* `dev` (`mode: development`, default): creates its own app with a distinct
  name and a per-user workspace folder. Safe to `destroy`.
* `prod` (`mode: production`): explicit `root_path`, fixed app name
  `iot-digital-twin`, `CAN_MANAGE` for the deployer and `admin_group`.

## Prod must bind, not create

App names are workspace-global. The live app already exists, so
`databricks bundle deploy -t prod` would fail on the name conflict, or (if it
were deleted first) create a new app with a new service principal that has none
of the existing grants. Bind instead:

```bash
databricks bundle deployment bind iot_twin iot-digital-twin -t prod --profile <profile>
databricks bundle deploy -t prod --profile <profile>
```

After binding, the next deploy applies the bundle's `resources` and `config` to
the existing app, replacing its current resources and env. Review
`databricks bundle plan -t prod` first. `bundle destroy` on a bound `prod` app
deletes it; do not use it there.

## Fresh app prerequisites

A new app gets a new service principal. Until it is granted access, it starts
and serves `/api/health` but reports `db_available: false` and DBSQL calls fail:

* Lakebase: the Postgres resource creates the SP's role; grant it schema/table
  privileges (e.g. `USAGE`/`CREATE` on `public` and access to existing tables).
* Unity Catalog: `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`/`SELECT` on
  `uc_catalog.uc_schema`, plus the external location/service credential grants.
* Workspace: membership in `admin_group` is for users, not the SP.
