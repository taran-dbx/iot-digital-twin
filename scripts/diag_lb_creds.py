# Databricks notebook source
# COMMAND ----------
%pip install psycopg2-binary --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import json, psycopg2
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
report = {"checks": []}

# Use the EXACT same secrets the consumer uses
try:
    lb_user = dbutils.secrets.get("iot-secrets", "lb-user")
    lb_password = dbutils.secrets.get("iot-secrets", "lb-password")
    report["checks"].append({"step": "secrets", "lb_user_len": len(lb_user), "lb_password_len": len(lb_password)})
except Exception as e:
    report["checks"].append({"step": "secrets", "error": f"{type(e).__name__}: {e}"[:200]})
    dbutils.notebook.exit(json.dumps(report, indent=2))

# Use the EXACT same host lookup the consumer uses
try:
    branches = w.api_client.do("GET", "/api/2.0/postgres/projects/iot-digital-twin/branches").get("branches", [])
    branch = branches[0]
    endpoints = w.api_client.do("GET", f"/api/2.0/postgres/{branch['name']}/endpoints").get("endpoints", [])
    host = endpoints[0]["status"]["hosts"]["host"]
    report["checks"].append({"step": "host_resolution", "host": host})
except Exception as e:
    report["checks"].append({"step": "host_resolution", "error": f"{type(e).__name__}: {e}"[:300]})
    dbutils.notebook.exit(json.dumps(report, indent=2))

# Now try to connect — exactly as the ForeachWriter would
try:
    conn = psycopg2.connect(
        host=host, dbname="databricks_postgres",
        user=lb_user, password=lb_password, port=5432, sslmode="require",
    )
    cur = conn.cursor()
    cur.execute("SELECT current_user, version()")
    who, ver = cur.fetchone()
    report["checks"].append({"step": "connect", "current_user": who, "version": ver[:80]})

    # Check table existence + row count
    try:
        cur.execute("SELECT COUNT(*) FROM sensor_readings")
        cnt = cur.fetchone()[0]
        cur.execute("SELECT MAX(event_time) FROM sensor_readings")
        latest = cur.fetchone()[0]
        report["checks"].append({"step": "read", "row_count": cnt, "latest": str(latest) if latest else None})
    except Exception as e:
        report["checks"].append({"step": "read", "error": f"{type(e).__name__}: {e}"[:300]})
        conn.rollback()

    # Attempt an INSERT (same row shape as the consumer)
    try:
        cur.execute(
            "INSERT INTO sensor_readings (event_time, sensor_name, industry, value, unit, sensor_type, fault_active, source) "
            "VALUES (NOW(), 'diag-cred-test', 'diag', 99.9, 'unit', 'test', false, 'diag')"
        )
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM sensor_readings WHERE source = 'diag'")
        n = cur.fetchone()[0]
        report["checks"].append({"step": "insert", "success": True, "diag_rows": n})
        # Clean up
        cur.execute("DELETE FROM sensor_readings WHERE source = 'diag'")
        conn.commit()
    except Exception as e:
        report["checks"].append({"step": "insert", "error": f"{type(e).__name__}: {e}"[:400]})
        conn.rollback()

    # Check privileges
    try:
        cur.execute("""
            SELECT grantee, privilege_type FROM information_schema.role_table_grants
            WHERE table_name = 'sensor_readings' ORDER BY grantee, privilege_type
        """)
        grants = cur.fetchall()
        report["checks"].append({"step": "grants", "list": [[g, p] for g, p in grants]})
    except Exception as e:
        report["checks"].append({"step": "grants", "error": str(e)[:300]})

    conn.close()
except Exception as e:
    report["checks"].append({"step": "connect", "error": f"{type(e).__name__}: {e}"[:400]})

dbutils.notebook.exit(json.dumps(report, indent=2))
