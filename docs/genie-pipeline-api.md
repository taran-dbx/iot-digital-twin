# Per-pipeline Genie API (manufacturing)

Each **manufacturing** pipeline `(setup_id, mode)` gets its own Genie space,
created automatically after a successful deploy
(`POST /api/admin/pipelines/{setup_id}/deploy/{mode}`) and updated in place on
redeploy. Other industries keep using the static copilot
(`/api/genie/status`, `/api/genie/conversations…`, backed by `GENIE_SPACE_ID`).

`mode` is one of `realtime` | `microbatch` | `zerobus`.

**`{setup_id}` path segment:** accepts either the `pipeline_setup.id` or the
mode's `pipeline_deployment.id`. A deployment id resolves to its setup and must
belong to `{mode}`. A value that is not a UUID, an unknown UUID, or a
deployment id for a different mode returns **404** without querying the database
for invalid ids. The dashboard gets both ids from
`GET /api/industries/catalog` → `industries[].modes[]`, as
`deployment_id` and **`setup_id`** (both `null` until the mode is deployed).

## Space contents

| Data source | FQN |
|---|---|
| Mode telemetry, **realtime** | `{UC_CATALOG}.{UC_SCHEMA}.lb__shadow_manufacturing_sensor_readings_history` (`cdf_reconciliation_service.delta_table_fqn`) |
| Mode telemetry, microbatch / zerobus | `{UC_CATALOG}.{UC_SCHEMA}.manufacturing_{mode}` |
| Streaming metrics | `{UC_CATALOG}.{UC_SCHEMA}.manufacturing_metrics` |
| Intelligence reference | `{UC_CATALOG}.{UC_SCHEMA}.intelligence_{assets,sensor_bindings,maintenance_records,alert_rules,alert_snapshot}` |

**Why realtime is different:** the RTM stream writes to Lakebase, and Lakehouse Sync mirrors it into the
CDF shadow history table, so `manufacturing_realtime` stays empty. Read-only check on a test
workspace, 2026-11-18:

- `manufacturing_realtime` has 0 rows. The shadow table is live and insert-only.
- The shadow table has the same sensor columns: `sensor_name, industry, value, unit, sensor_type,
  event_time, min_value, max_value, nominal_value, fault_active, metadata`.
- It also has `id`, `source` and the CDC columns `_pg_change_type, _pg_lsn, _pg_xid, _timestamp, _sort_by`.

So the OEE, downtime and anomaly definitions are unchanged. For realtime, the instructions, example SQL,
SQL snippets (measures and filters) and the prompt's recent-telemetry summary all apply the dashboard's
row filter from `routes/sensor_data.py`: `_pg_change_type IN ('insert', 'update_postimage') AND
industry = 'manufacturing'`. The filter comes from `genie_space_content.telemetry_row_filter`, because
the shadow table can hold several industries. The CDC bookkeeping columns are excluded from Genie
context. `telemetry_table(industry, mode)` in `services/genie_space_service.py` is the single mapping;
the allowlist and the telemetry summary use the same function.

If the deploy hook's or the admin endpoint's 60 s timeout fires while a space is being created, creation
and the ledger write keep running in the background (`asyncio.shield`). The space therefore still gets
its ledger row and can be torn down later.

Microbatch and zerobus write `manufacturing_{mode}` directly (`services/pipeline_freshness.py`). Those
tables exist but were empty in that workspace, because only realtime was running for manufacturing. The
metrics table has `mode='realtime'` rows only, since zerobus emits no streaming metrics.

The space also carries manufacturing instructions:

- **Downtime**: intervals where `fault_active` is true, counted in distinct minutes.
- **Availability**: 1 − fault minutes / observed minutes.
- **Performance**: `value / nominal_value`, capped at 1.
- **Quality**: the share of readings inside `[min_value, max_value]`.
- **OEE**: the product of the three above. It is an approximation.
- **Anomaly**: a value outside `[min_value, max_value]`.

It also has example SQL, SQL snippets (measures, filters, expressions) and
sample questions that match `/starters`. No new tables are created. Builder:
`services/genie_space_service.py` and `services/genie_space_content.py`.

**Allowlist:** status, conversation, poll and result calls check the space with
`get_space(include_serialized_space=True)`.

- It parses `data_sources.tables` and `data_sources.metric_views`, and requires an exact match with the
  expected set above.
- Any other non-empty `data_sources` kind counts as a mismatch.
- A mismatch blocks the call with 503.
- If the space cannot be inspected, the call fails closed. `GENIE_ALLOWLIST_CONFIRMED` is **not** honored
  for per-pipeline spaces; it applies only to the static copilot.
- Verified results are cached per space id for 60 s. The cache is cleared whenever
  `ensure_pipeline_space` touches that space. A mismatch is never cached.

**Persistence (design choice):** the space id is stored in the `provisioned_resource` ledger as
`resource_type='genie_space'`, with `metadata.mode` set and `teardown_order=45`. There is no
`pipeline_deployment` column and no migration. The lookup is
`genie_space_service.find_space_id(db, setup_id, mode)`, which returns the newest `active` row for the mode.
Teardown trashes the space through the `genie_space` deleter, which calls `trash_space`. A space that is
already gone counts as deleted.

Removing a single mode (`PATCH /api/admin/pipelines/{setup_id}/modes`) also trashes that mode's space
before `delete_deployment` runs. The ledger row has `deployment_id … ON DELETE CASCADE`, so trashing it
first is what keeps the workspace space from being orphaned. If the trash fails, the route returns 502 and
keeps the deployment so the admin can retry.

Before **creating** a space, deploy checks that the `intelligence_*` tables exist (`tables.exists`).
If any are missing, the deploy warning reads `intelligence tables missing: …; run intelligence sync
(POST /api/admin/intelligence/sync) then redeploy`. If the check itself errors, creation goes ahead.

**Config:**

- `GENIE_SPACE_PARENT_PATH` (optional) sets the workspace folder for new spaces. When it is unset, the
  space is created in the app service principal's home folder. That is fine, because users reach the space
  through the app.
- The warehouse is the setup's `warehouse_id`. If it is not set, the app uses `resolve_warehouse_id`
  (`DATABRICKS_SQL_WAREHOUSE_ID` env, then the app-global settings).
- The app service principal needs permission to create Genie spaces, `CAN_USE` on the warehouse, and
  `SELECT` on the tables.

**Deploy result:** on success it adds
`"genie_space": {"space_id": str, "action": "created"|"updated"|"recreated", "tables": [str]}`. On failure it
adds `"warnings": ["Genie space was not provisioned: …"]`, and the deploy itself still succeeds.
Provisioning is capped at 60 s; on timeout the warning is
`"Genie space provisioning timed out after 60s; redeploy to retry"`.

## Common behaviour

- Auth: `require_identity` (forwarded Databricks Apps identity; 401 when missing in the Apps runtime).
- Conversation ownership is process-local (`AppState.genie_conversations`). A conversation is bound to
  `(owner, setup_id, mode, space_id)`. Any other user, pipeline or mode gets **404**. After an app restart
  every conversation returns 404, so the client should show "ask again".
- The recent-telemetry context added to the prompt is capped at 5 s. On timeout it is left out and the
  question is still sent.
- Questions go to Genie inside the trusted/untrusted prompt wrapper. Request bodies accept only
  `{"question": string}` (1–4000 chars). Any other field returns 422.
- SDK calls run off the event loop under the shared semaphore with a 20 s timeout. A timeout returns 504.

| Status | Meaning |
|---|---|
| 401 | identity required |
| 404 | non-UUID or unknown setup/deployment id / deployment id of another mode / non-manufacturing setup (conversation routes and starters) / unknown or foreign conversation / Genie resource not found |
| 422 | unknown mode, or invalid body |
| 503 | app DB unavailable, no space provisioned yet, or table allowlist not verified |
| 504 | Genie SDK timeout |
| 500 | other Genie failure (sanitized message) |

Error bodies follow FastAPI's format: `{"detail": string}`.

## Endpoints

Base: `/api/genie/pipelines/{setup_id}/{mode}`

### `GET …/status`

```json
{"available": true, "space_id": "01ef…"}
```
```json
{"available": false, "space_id": null, "reason": "No Genie space is provisioned for this pipeline yet. Redeploy the pipeline to create one."}
```

`reason` is set only when `available` is false and is safe to show to operators. `space_id` is non-null
when a space exists but cannot be used (for example, an allowlist mismatch). Non-manufacturing setups
return `available: false`. An unknown setup returns 404.

### `GET …/starters`

```json
{"questions": ["What is the approximate OEE per sensor over the last 24 hours?", "…"]}
```

The list depends on the mode: five shared questions (OEE, downtime, anomalies, faults, maintenance) plus
two for the mode.

### `POST …/conversations`

Request: `{"question": "How much downtime did we have today?"}`

Response:
```json
{"conversation_id": "01ef…", "message_id": "01ef…"}
```

### `POST …/conversations/{conversation_id}/messages`

Request: `{"question": "And for yesterday?"}`

Response:
```json
{"conversation_id": "01ef…", "message_id": "01ef…"}
```

### `GET …/conversations/{conversation_id}/messages/{message_id}`

Poll until `status` is terminal (`COMPLETED`, `FAILED` or `CANCELLED`). Other values include
`SUBMITTED`, `FETCHING_METADATA`, `ASKING_AI`, `EXECUTING_QUERY` and so on.

```json
{
  "conversation_id": "01ef…",
  "message_id": "01ef…",
  "status": "COMPLETED",
  "content": "…the prompt as sent to Genie…",
  "error": null,
  "attachments": [
    {
      "attachment_id": "01ef…",
      "text": null,
      "query": {"sql": "SELECT sensor_name, …", "description": "Downtime minutes per sensor"},
      "suggested_questions": []
    },
    {
      "attachment_id": "01ef…",
      "text": "Spindle-3 had the most downtime (42 min).",
      "query": null,
      "suggested_questions": ["What was the OEE for spindle-3?"]
    }
  ]
}
```

Every attachment always has all four keys. `text`, `query` and `query.description` can be null, and
`suggested_questions` is always an array. `error` is a string or null.

### `GET …/conversations/{cid}/messages/{mid}/attachments/{attachment_id}/result`

The result of a query attachment, from `get_message_attachment_query_result`. It is capped at **500 rows**.

```json
{
  "columns": [{"name": "sensor_name", "type": "STRING"}, {"name": "downtime_minutes", "type": "LONG"}],
  "rows": [["spindle-3", "42"], ["press-1", "7"]],
  "truncated": false
}
```

`rows` holds string cells, or null, in column order, exactly as Genie returns them. `truncated` is true
when more rows exist than were returned. If there is no result yet, the response is
`{"columns": [], "rows": [], "truncated": false}`.

## Admin: provision a space for an already-deployed pipeline ("Enable Genie")

Pipelines deployed before this feature existed can get their space **without a
redeploy or a stream restart**. This touches only the Genie space and its ledger row,
never the job, stream, Kafka topic, notebooks or Lakebase resources.

```
POST /api/admin/pipelines/{setup_id}/genie/{mode}
```

- `{setup_id}` accepts either `pipeline_setup.id` or the mode's `pipeline_deployment.id`. It must be a UUID.
- Admin-gated: uses the same `app_permissions.is_admin` check as `POST …/deploy/{mode}`.
- Idempotent: the first call creates the space and writes the ledger row, and later calls update it in
  place. It runs the intelligence-table precheck and has a 60 s timeout.

Response `200`:
```json
{"space_id": "01ef…", "created": true, "warnings": []}
```
`created` is `false` when an existing space was updated. `warnings` is non-empty only when the recorded
space had disappeared and a new one was created.

| Status | Meaning |
|---|---|
| 403 | not an admin |
| 404 | non-UUID/unknown id, deployment id of another mode, or no deployment for `{mode}` |
| 409 | deployment not in a deployed state (needs a `job_id`; status not pending/deploying/failed), or a precondition failed (for example, missing intelligence tables: run `POST /api/admin/intelligence/sync`, then call again) |
| 422 | unknown mode, or the setup is not manufacturing |
| 503 | app DB unavailable |
| 504 | provisioning timed out (60 s) |
| 502 | Genie SDK failure (sanitized) |

Example (from a machine authenticated to the app as an admin):
```bash
curl -X POST "https://<app-url>/api/admin/pipelines/<setup_id-or-deployment_id>/genie/realtime" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN"
```
