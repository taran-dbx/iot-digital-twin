# Databricks notebook source
# COMMAND ----------
%pip install psycopg2-binary --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import json, psycopg2
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me().user_name
report = {"user": me, "checks": []}

for project in ("iot-digital-twin", "iot-lakets-demo"):
    try:
        branches = w.api_client.do("GET", f"/api/2.0/postgres/projects/{project}/branches").get("branches", [])
        if not branches:
            report["checks"].append({"project": project, "error": "no branches"})
            continue
        # Pick branch with branch_id or name 'production', else first
        branch = None
        for b in branches:
            if "production" in (b.get("uid",""), b.get("name","").split("/")[-1]):
                branch = b; break
        branch = branch or branches[0]
        eps = w.api_client.do("GET", f"/api/2.0/postgres/{branch['name']}/endpoints").get("endpoints", [])
        if not eps:
            report["checks"].append({"project": project, "error": "no endpoints"})
            continue
        ep = eps[0]
        host = ep.get("status", {}).get("hosts", {}).get("host")
        cred_resp = w.api_client.do("POST", f"/api/2.0/postgres/{ep['name']}:generateDatabaseCredential", body={})
        token = cred_resp.get("token")
        if not (host and token):
            report["checks"].append({"project": project, "error": "no host or token", "ep": ep}); continue
        conn = psycopg2.connect(host=host, dbname="databricks_postgres", user=me,
                                password=token, port=5432, sslmode="require")
        cur = conn.cursor()
        try:
            cur.execute("SELECT COUNT(*), MAX(event_time) FROM sensor_readings")
            count, latest = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM sensor_readings WHERE event_time > NOW() - INTERVAL '5 minutes'")
            last5m = cur.fetchone()[0]
            report["checks"].append({
                "project": project, "host": host,
                "rows_total": int(count), "latest_event_time": str(latest) if latest else None,
                "rows_last_5min": int(last5m),
            })
        except Exception as q_err:
            report["checks"].append({"project": project, "host": host, "query_err": str(q_err)[:300]})
        conn.close()
    except Exception as e:
        report["checks"].append({"project": project, "error": f"{type(e).__name__}: {e}"[:400]})

dbutils.notebook.exit(json.dumps(report, indent=2))
