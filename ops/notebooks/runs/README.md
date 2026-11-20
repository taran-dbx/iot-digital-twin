# Live-run snapshots

Dated, **redacted** snapshots of `../pipeline_health_check.ipynb` executed for real
(`sample=false`, `redact=true`, `lookback_hours=24`) against a deployed pipeline. They keep the
cell outputs so the report renders on GitHub, and they are **not** regenerated automatically:
each file describes one pipeline at one point in time and goes stale. The committed sample
notebook (`../pipeline_health_check.ipynb`, synthetic data) is unchanged and remains the
reference for the report layout.

| File | Meaning |
|---|---|
| `<industry>_<mode>_<YYYY-MM-DD>.ipynb` | One run: first cell reads `LIVE RUN, redacted, <date>` |

## How they are produced

1. Import the notebook to a temporary workspace path under your own home folder.
2. Submit a one-time serverless run per deployed industry and mode (the base environment must
   include `databricks-sdk>=0.133.0` and `psycopg2-binary`):

   ```bash
   databricks jobs submit --no-wait -p <profile> --json '{
     "run_name": "health-check",
     "tasks": [{"task_key": "main",
       "notebook_task": {"notebook_path": "<temp path>",
         "base_parameters": {"industry": "manufacturing", "mode": "realtime",
           "lookback_hours": "24", "sample": "false", "redact": "true"}},
       "environment_key": "e"}],
     "environments": [{"environment_key": "e", "spec": {"client": "4",
       "dependencies": ["databricks-sdk>=0.133.0", "psycopg2-binary"]}}]}'
   ```

3. Export the finished task run and convert it, keeping the outputs:

   ```bash
   databricks jobs export-run <task_run_id> --views-to-export ALL -p <profile> -o json > export.json
   python ops/export_run_notebook.py export.json \
     ops/notebooks/runs/<industry>_<mode>_<YYYY-MM-DD>.ipynb --date <YYYY-MM-DD>
   ```

   The converter refuses runs where any cell errored or was skipped (for example when the
   Python kernel crashed): re-run instead of committing a partial snapshot.
4. Scan before committing, and delete the temporary workspace notebook afterwards:

   ```bash
   python scripts/check_identifiers.py ops/notebooks/runs/*.ipynb
   pytest tests/test_health_run_snapshots.py
   ```

## Rules

- `redact=true` is mandatory. A run with `redact=false` must never be committed.
- Tests (`tests/test_health_run_snapshots.py`) validate nbformat, the label, the kept outputs and
  the identifier / secret-shape scans over every file here. They do not re-execute anything.
- The pipeline (job) name `iot-<industry>-<mode>` is shown as-is: it is the app's fixed naming
  convention, not a workspace identifier.
- Delete old snapshots rather than editing them.
