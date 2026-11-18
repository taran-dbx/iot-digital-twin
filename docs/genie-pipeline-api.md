# Per-pipeline Genie API (manufacturing)

Each **manufacturing** pipeline `(setup_id, mode)` gets its own Genie space,
created automatically after a successful deploy
(`POST /api/admin/pipelines/{setup_id}/deploy/{mode}`) and updated in place on
redeploy. Other industries keep using the static copilot
(`/api/genie/status`, `/api/genie/conversations…`, backed by `GENIE_SPACE_ID`).

`mode` is one of `realtime` | `microbatch` | `zerobus`.

## Space contents

| Data source | FQN |
|---|---|
| Mode telemetry | `{UC_CATALOG}.{UC_SCHEMA}.manufacturing_{mode}` |
| Streaming metrics | `{UC_CATALOG}.{UC_SCHEMA}.manufacturing_metrics` |
| Intelligence reference | `{UC_CATALOG}.{UC_SCHEMA}.intelligence_{assets,sensor_bindings,maintenance_records,alert_rules,alert_snapshot}` |

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

**Allowlist:** every conversation, poll and result call runs `get_space(include_serialized_space=True)`. It
parses `data_sources.tables` and `data_sources.metric_views`, and requires an exact match with the expected
set above. Any mismatch blocks the call with 503. If the space cannot be inspected, the call fails closed
unless `GENIE_ALLOWLIST_CONFIRMED=true`.

**Persistence (design choice):** the space id is stored in the `provisioned_resource` ledger as
`resource_type='genie_space'`, with `metadata.mode` set and `teardown_order=45`. There is no
`pipeline_deployment` column and no migration. The lookup is
`genie_space_service.find_space_id(db, setup_id, mode)`, which returns the newest `active` row for the mode.
Teardown trashes the space through the `genie_space` deleter, which calls `trash_space`. A space that is
already gone counts as deleted.

**Config:**

- `GENIE_SPACE_PARENT_PATH` (optional) sets the workspace folder for new spaces.
- The warehouse is the setup's `warehouse_id`. If it is not set, the app uses `resolve_warehouse_id`
  (`DATABRICKS_SQL_WAREHOUSE_ID` env, then the app-global settings).
- The app service principal needs permission to create Genie spaces, `CAN_USE` on the warehouse, and
  `SELECT` on the tables.

**Deploy result:** on success it adds
`"genie_space": {"space_id": str, "action": "created"|"updated"|"recreated", "tables": [str]}`. On failure it
adds `"warnings": ["Genie space was not provisioned: …"]`, and the deploy itself still succeeds.

## Common behaviour

- Auth: `require_identity` (forwarded Databricks Apps identity; 401 when missing in the Apps runtime).
- Conversation ownership is process-local (`AppState.genie_conversations`). A conversation is bound to
  `(owner, setup_id, mode, space_id)`. Any other user, pipeline or mode gets **404**. After an app restart
  every conversation returns 404, so the client should show "ask again".
- Questions go to Genie inside the trusted/untrusted prompt wrapper. Request bodies accept only
  `{"question": string}` (1–4000 chars). Any other field returns 422.
- SDK calls run off the event loop under the shared semaphore with a 20 s timeout. A timeout returns 504.

| Status | Meaning |
|---|---|
| 401 | identity required |
| 404 | unknown setup / non-manufacturing setup (conversation routes and starters) / unknown or foreign conversation / Genie resource not found |
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
