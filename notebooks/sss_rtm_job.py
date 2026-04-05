# Databricks notebook source
# MAGIC %md
# MAGIC # IoT Digital Twin — Spark Structured Streaming
# MAGIC
# MAGIC This notebook runs **two streaming queries** against the IoT simulator's
# MAGIC built-in PySpark custom data source (`IoTSimulatorDataSource`):
# MAGIC
# MAGIC | Query | Pipeline | Sink |
# MAGIC |-------|----------|------|
# MAGIC | **Q1 — Raw** | Raw sensor rows | Lakebase `sensor_readings` (buffered `ForeachWriter`) |
# MAGIC | **Q2 — Aggregated** | 30-second tumbling window per sensor | Lakebase `sensor_aggregates` + HTTP POST to app `/api/sss/ingest` |
# MAGIC
# MAGIC **Cluster requirements:**
# MAGIC - Fixed-size (no autoscaling)
# MAGIC - `spark.sql.shuffle.partitions = 8`

# COMMAND ----------

# MAGIC %pip install /Volumes/taran_explore/iot_simulator/wheels/iot_data_simulator-0.1.0-py3-none-any.whl psycopg2-binary requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metrics Table Parameter

# COMMAND ----------

dbutils.widgets.text("metrics_table", "taran_explore.metrics.streaming_metrics", "Streaming Metrics Table FQN")
metrics_table_fqn = dbutils.widgets.get("metrics_table").strip()

dbutils.widgets.text("pipeline_id", "default-pipeline", "Pipeline ID")
pipeline_id = dbutils.widgets.get("pipeline_id").strip()

print(f"Streaming metrics table: {metrics_table_fqn}")
print(f"Pipeline ID: {pipeline_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Config Parameter

# COMMAND ----------

import json as _json

streaming_config_raw = (
    dbutils.widgets.get("streaming_config")
    if "streaming_config" in [w.name for w in dbutils.widgets.getAll()]
    else "{}"
)
streaming_config = _json.loads(streaming_config_raw) if streaming_config_raw else {}
window_configs = streaming_config.get("windows", [{"type": "tumbling", "duration": "30s"}])
session_config = streaming_config.get("session", {"enabled": False})
state_ops_config = streaming_config.get("state_ops", {})

print(f"window_configs: {window_configs}")
print(f"session_config: {session_config}")
print(f"state_ops_config: {state_ops_config}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve Credentials

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import requests as _req

w = WorkspaceClient()
client_id = dbutils.secrets.get("iot-taran", "ci")
client_secret = dbutils.secrets.get("iot-taran", "cs")

# Lakebase credentials
project_id = "iot-digital-twin"
branch_id = "production"
parent = f"projects/{project_id}/branches/{branch_id}"

endpoints = list(w.postgres.list_endpoints(parent=parent))
if not endpoints:
    raise RuntimeError(f"No Lakebase endpoints found for {parent}")

endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
pg_host = endpoint.status.hosts.host
pg_db = "databricks_postgres"

# Resolve Lakebase credentials from secrets
pg_user = dbutils.secrets.get("iot-taran", "lb-user")
pg_password = dbutils.secrets.get("iot-taran", "lb-password")

print(f"Lakebase host: {pg_host}")

# Resolve workspace and app URLs from secrets / SDK
workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
app_url = dbutils.secrets.get("iot-taran", "app-url")

token_resp = _req.post(
    f"{workspace_url}/oidc/v1/token",
    data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "all-apis",
    },
)
token_resp.raise_for_status()
app_token = token_resp.json()["access_token"]
print(f"OAuth token obtained (length={len(app_token)})")

# Broadcast credentials to executors
token_broadcast = spark.sparkContext.broadcast(app_token)
url_broadcast = spark.sparkContext.broadcast(app_url)
pg_host_broadcast = spark.sparkContext.broadcast(pg_host)
pg_user_broadcast = spark.sparkContext.broadcast(pg_user)
pg_password_broadcast = spark.sparkContext.broadcast(pg_password)
pipeline_id_bc = spark.sparkContext.broadcast(pipeline_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ForeachWriters

# COMMAND ----------

import hashlib
import psycopg2
import psycopg2.extras
import json
import urllib.request
from datetime import datetime, timezone


class LakebaseForeachWriter:
    """Buffered ForeachWriter that batch-inserts raw rows into Lakebase."""

    MAX_BUFFER_SIZE = 50

    def __init__(self):
        self.conn = None
        self.cursor = None
        self.buffer = []

    def open(self, partition_id, epoch_id):
        self.conn = psycopg2.connect(
            host=pg_host_broadcast.value,
            dbname=pg_db,
            user=pg_user_broadcast.value,
            password=pg_password_broadcast.value,
            port=5432,
            sslmode="require",
        )
        self.cursor = self.conn.cursor()
        self.buffer = []
        return True

    def process(self, row):
        self.buffer.append((
            datetime.fromtimestamp(row.timestamp, tz=timezone.utc),
            row.sensor_name,
            row.industry,
            row.value,
            row.unit,
            row.sensor_type,
            row.min_value,
            row.max_value,
            row.nominal_value,
            row.fault_active,
            row.metadata,
        ))
        if len(self.buffer) >= self.MAX_BUFFER_SIZE:
            self._flush()

    def _flush(self):
        if not self.buffer:
            return
        psycopg2.extras.execute_values(
            self.cursor,
            """INSERT INTO sensor_readings
               (event_time, sensor_name, industry, value, unit,
                sensor_type, min_value, max_value, nominal_value,
                fault_active, metadata, source)
               VALUES %s""",
            self.buffer,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'sss')",
        )
        self.conn.commit()
        self.buffer.clear()

    def close(self, error):
        if error:
            if self.conn:
                self.conn.rollback()
        else:
            self._flush()
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


class AppForeachWriter:
    """ForeachWriter that POSTs windowed aggregates to the Databricks App
    endpoint /api/sss/ingest AND inserts them into Lakebase sensor_aggregates.

    Uses a buffered approach: rows are collected until MAX_BUFFER_SIZE, then
    flushed as a single HTTP POST batch and a bulk Lakebase INSERT.
    """

    MAX_BUFFER_SIZE = 20

    def __init__(self):
        self.buffer = []
        self.token = None
        self.base_url = None
        self.conn = None
        self.cursor = None

    def open(self, partition_id, epoch_id):
        self.token = token_broadcast.value
        self.base_url = url_broadcast.value
        self.buffer = []
        self.conn = psycopg2.connect(
            host=pg_host_broadcast.value,
            dbname=pg_db,
            user=pg_user_broadcast.value,
            password=pg_password_broadcast.value,
            port=5432,
            sslmode="require",
        )
        self.cursor = self.conn.cursor()
        return True

    def process(self, row):
        window_start = row["window"]["start"].isoformat()
        window_end = row["window"]["end"].isoformat()
        self.buffer.append({
            "window_start": window_start,
            "window_end": window_end,
            "sensor_name": row["sensor_name"],
            "industry": row["industry"],
            "unit": row["unit"],
            "sensor_type": row["sensor_type"],
            "avg_value": round(float(row["avg_value"]), 3),
            "min_value": round(float(row["min_value"]), 3),
            "max_value": round(float(row["max_value"]), 3),
            "record_count": int(row["record_count"]),
        })
        if len(self.buffer) >= self.MAX_BUFFER_SIZE:
            self._flush()

    def _flush(self):
        if not self.buffer:
            return

        # 1) POST to the app's ingest endpoint
        try:
            payload = json.dumps(self.buffer).encode("utf-8")
            req = urllib.request.Request(
                f"{self.base_url}/api/sss/ingest",
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.token}",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                pass  # 200 OK is sufficient
        except Exception as e:
            print(f"[AppForeachWriter] HTTP POST failed: {e}")

        # 2) Insert into Lakebase sensor_aggregates
        try:
            rows = [
                (
                    r["window_start"],
                    r["window_end"],
                    r["sensor_name"],
                    r["industry"],
                    r["avg_value"],
                    r["min_value"],
                    r["max_value"],
                    r["record_count"],
                    r["unit"],
                    r["sensor_type"],
                )
                for r in self.buffer
            ]
            psycopg2.extras.execute_values(
                self.cursor,
                """INSERT INTO sensor_aggregates
                   (window_start, window_end, sensor_name, industry,
                    avg_value, min_value, max_value, record_count,
                    unit, sensor_type)
                   VALUES %s""",
                rows,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            )
            self.conn.commit()
        except Exception as e:
            print(f"[AppForeachWriter] Lakebase INSERT failed: {e}")
            if self.conn:
                self.conn.rollback()

        self.buffer.clear()

    def close(self, error):
        if error:
            if self.conn:
                self.conn.rollback()
        else:
            self._flush()
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


class SlidingAggregateForeachWriter:
    """Buffered ForeachWriter that batch-inserts sliding window aggregates into
    the Lakebase `sliding_aggregates` table."""

    MAX_BUFFER_SIZE = 20

    def __init__(self):
        self.conn = None
        self.cursor = None
        self.buffer = []

    def open(self, partition_id, epoch_id):
        self.conn = psycopg2.connect(
            host=pg_host_broadcast.value,
            dbname=pg_db,
            user=pg_user_broadcast.value,
            password=pg_password_broadcast.value,
            port=5432,
            sslmode="require",
        )
        self.cursor = self.conn.cursor()
        self.buffer = []
        return True

    def process(self, row):
        self.buffer.append((
            row["pipeline_id"],
            row["sensor_id"],
            row["sensor_type"],
            row["window"]["start"],
            row["window"]["end"],
            row["slide_start"],
            round(float(row["avg_value"]), 3) if row["avg_value"] is not None else None,
            round(float(row["min_value"]), 3) if row["min_value"] is not None else None,
            round(float(row["max_value"]), 3) if row["max_value"] is not None else None,
            int(row["count"]) if row["count"] is not None else None,
        ))
        if len(self.buffer) >= self.MAX_BUFFER_SIZE:
            self._flush()

    def _flush(self):
        if not self.buffer:
            return
        try:
            psycopg2.extras.execute_values(
                self.cursor,
                """INSERT INTO sliding_aggregates
                   (pipeline_id, sensor_id, sensor_type,
                    window_start, window_end, slide_start,
                    avg_value, min_value, max_value, count)
                   VALUES %s""",
                self.buffer,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            )
            self.conn.commit()
        except Exception as e:
            print(f"[SlidingAggregateForeachWriter] Lakebase INSERT failed: {e}")
            if self.conn:
                self.conn.rollback()
        self.buffer = []

    def close(self, error):
        if error:
            if self.conn:
                self.conn.rollback()
        else:
            self._flush()
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


class SessionEventForeachWriter:
    """Buffered ForeachWriter that batch-inserts session window events into
    the Lakebase `session_events` table."""

    MAX_BUFFER_SIZE = 20

    def __init__(self):
        self.conn = None
        self.cursor = None
        self.buffer = []

    def open(self, partition_id, epoch_id):
        self.conn = psycopg2.connect(
            host=pg_host_broadcast.value,
            dbname=pg_db,
            user=pg_user_broadcast.value,
            password=pg_password_broadcast.value,
            port=5432,
            sslmode="require",
        )
        self.cursor = self.conn.cursor()
        self.buffer = []
        return True

    def process(self, row):
        session_start = row["session_window"]["start"]
        session_end = row["session_window"]["end"]
        duration_sec = (
            (session_end - session_start).total_seconds()
            if session_start and session_end
            else None
        )
        session_id = hashlib.md5(
            f"{row['sensor_id']}{session_start}".encode()
        ).hexdigest()
        self.buffer.append((
            row["pipeline_id"],
            row["sensor_id"],
            row["sensor_type"],
            session_id,
            session_start,
            session_end,
            duration_sec,
            int(row["reading_count"]) if row["reading_count"] is not None else None,
            round(float(row["avg_value"]), 3) if row["avg_value"] is not None else None,
            round(float(row["min_value"]), 3) if row["min_value"] is not None else None,
            round(float(row["max_value"]), 3) if row["max_value"] is not None else None,
        ))
        if len(self.buffer) >= self.MAX_BUFFER_SIZE:
            self._flush()

    def _flush(self):
        if not self.buffer:
            return
        try:
            psycopg2.extras.execute_values(
                self.cursor,
                """INSERT INTO session_events
                   (pipeline_id, sensor_id, sensor_type, session_id,
                    session_start, session_end, duration_sec, reading_count,
                    avg_value, min_value, max_value)
                   VALUES %s""",
                self.buffer,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            )
            self.conn.commit()
        except Exception as e:
            print(f"[SessionEventForeachWriter] Lakebase INSERT failed: {e}")
            if self.conn:
                self.conn.rollback()
        self.buffer = []

    def close(self, error):
        if error:
            if self.conn:
                self.conn.rollback()
        else:
            self._flush()
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


class StateForeachWriter:
    """ForeachWriter that upserts per-sensor state snapshots into the Lakebase
    `state_snapshots` table."""

    MAX_BUFFER_SIZE = 20

    def __init__(self):
        self.conn = None
        self.cursor = None
        self.buffer = []

    def open(self, partition_id, epoch_id):
        self.conn = psycopg2.connect(
            host=pg_host_broadcast.value,
            dbname=pg_db,
            user=pg_user_broadcast.value,
            password=pg_password_broadcast.value,
            port=5432,
            sslmode="require",
        )
        self.cursor = self.conn.cursor()
        self.buffer = []
        return True

    def process(self, row):
        self.buffer.append((
            row["pipeline_id"],
            row["sensor_id"],
            row["sensor_type"],
            row["running_avg"],
            row["running_min"],
            row["running_max"],
            int(row["readings_since_reset"]) if row["readings_since_reset"] is not None else 0,
            row["last_value"],
            row["prev_value"],
            bool(row["threshold_breached"]) if row["threshold_breached"] is not None else False,
        ))
        if len(self.buffer) >= self.MAX_BUFFER_SIZE:
            self._flush()

    def _flush(self):
        if not self.buffer:
            return
        try:
            psycopg2.extras.execute_values(
                self.cursor,
                """INSERT INTO state_snapshots
                   (pipeline_id, sensor_id, sensor_type,
                    running_avg, running_min, running_max,
                    readings_since_reset, last_value, prev_value,
                    threshold_breached, updated_at)
                   VALUES %s
                   ON CONFLICT (pipeline_id, sensor_id) DO UPDATE SET
                       sensor_type = EXCLUDED.sensor_type,
                       running_avg = EXCLUDED.running_avg,
                       running_min = EXCLUDED.running_min,
                       running_max = EXCLUDED.running_max,
                       readings_since_reset = EXCLUDED.readings_since_reset,
                       last_value = EXCLUDED.last_value,
                       prev_value = EXCLUDED.prev_value,
                       threshold_breached = EXCLUDED.threshold_breached,
                       updated_at = now()""",
                self.buffer,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())",
            )
            self.conn.commit()

            # POST any breached alerts to the app
            # Buffer tuple layout: (pipeline_id, sensor_id, sensor_type,
            #   running_avg, running_min, running_max, readings_since_reset,
            #   last_value, prev_value, threshold_breached)
            breached_alerts = [
                {
                    "pipeline_id": r[0],
                    "sensor_id": r[1],
                    "sensor_type": r[2],
                    "value": r[7],
                    "threshold_breached": r[9],
                }
                for r in self.buffer
                if r[9]
            ]
            if breached_alerts and url_broadcast.value:
                try:
                    req = urllib.request.Request(
                        f"{url_broadcast.value}/api/sss/alerts",
                        data=json.dumps({"alerts": breached_alerts}).encode(),
                        headers={
                            "Content-Type": "application/json",
                            "Authorization": f"Bearer {token_broadcast.value}",
                        },
                        method="POST",
                    )
                    urllib.request.urlopen(req, timeout=5)
                except Exception as alert_err:
                    print(f"[StateForeachWriter] Failed to POST threshold alerts: {alert_err}")
        except Exception as e:
            print(f"[StateForeachWriter] Lakebase UPSERT failed: {e}")
            if self.conn:
                self.conn.rollback()
        self.buffer = []

    def close(self, error):
        if error:
            if self.conn:
                self.conn.rollback()
        else:
            self._flush()
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register IoT Simulator & Start Streaming

# COMMAND ----------

from iot_simulator.pyspark_datasource import IoTSimulatorDataSource

spark.dataSource.register(IoTSimulatorDataSource)

raw_stream = (
    spark.readStream
    .format("iot_simulator")
    .option("industries", "pharmaceutical")
    .option("rowsPerBatch", "5")
    .load()
)

print(f"Schema: {raw_stream.schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1 — Raw sensor data to Lakebase

# COMMAND ----------

raw_writer = LakebaseForeachWriter()

query_raw = (
    raw_stream
    .writeStream
    .foreach(raw_writer)
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .option(
        "checkpointLocation",
        "/Volumes/taran_explore/iot_simulator/checkpoints/iot_sss_raw",
    )
    .queryName("sss_raw_to_lakebase")
    .start()
)

print(f"Query 1 (raw) started: {query_raw.name} (id={query_raw.id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2 — Windowed aggregation to App + Lakebase

# COMMAND ----------

from pyspark.sql.functions import (
    from_unixtime, window, avg, min as spark_min, max as spark_max,
    count, col,
)

raw_with_ts = raw_stream.withColumn(
    "event_time", from_unixtime("timestamp").cast("timestamp")
)

aggregated = (
    raw_with_ts
    .withWatermark("event_time", "1 minute")
    .groupBy(
        window("event_time", "30 seconds"),
        "sensor_name", "industry", "unit", "sensor_type",
    )
    .agg(
        avg("value").alias("avg_value"),
        spark_min("value").alias("min_value"),
        spark_max("value").alias("max_value"),
        count("*").alias("record_count"),
    )
)

app_writer = AppForeachWriter()

query_agg = (
    aggregated
    .writeStream
    .foreach(app_writer)
    .outputMode("update")
    .trigger(processingTime="10 seconds")
    .option(
        "checkpointLocation",
        "/Volumes/taran_explore/iot_simulator/checkpoints/iot_sss_agg",
    )
    .queryName("sss_agg_to_app")
    .start()
)

print(f"Query 2 (aggregated) started: {query_agg.name} (id={query_agg.id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3 — Sliding window aggregations

# COMMAND ----------

from pyspark.sql import functions as F

checkpoint_base = "/Volumes/taran_explore/iot_simulator/checkpoints"

sliding_queries = []

for _wc in window_configs:
    if _wc.get("type") != "sliding":
        continue
    _duration = _wc.get("duration", "60s")
    _slide = _wc.get("slide", "30s")

    _raw_with_ts = raw_stream.withColumn(
        "event_time", F.from_unixtime("timestamp").cast("timestamp")
    )

    _sliding_agg = (
        _raw_with_ts
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            F.window("event_time", _duration, _slide),
            "sensor_name", "sensor_type",
        )
        .agg(
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.count("*").alias("count"),
            F.first(F.from_unixtime("timestamp").cast("timestamp")).alias("slide_start"),
        )
        .withColumnRenamed("sensor_name", "sensor_id")
        .withColumn("pipeline_id", F.lit(pipeline_id))
    )

    _q = (
        _sliding_agg
        .writeStream
        .foreach(SlidingAggregateForeachWriter())
        .outputMode("update")
        .trigger(processingTime="15 seconds")
        .option(
            "checkpointLocation",
            f"{checkpoint_base}/sliding_{_duration}_{_slide}",
        )
        .queryName(f"sss_sliding_{_duration}_{_slide}")
        .start()
    )
    sliding_queries.append(_q)
    print(f"Query 3 (sliding {_duration}/{_slide}) started: {_q.name} (id={_q.id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 4 — Session windows

# COMMAND ----------

query_session = None

if session_config.get("enabled", False):
    _gap = session_config.get("gap", "30 seconds")

    _raw_with_ts_session = raw_stream.withColumn(
        "event_time", F.from_unixtime("timestamp").cast("timestamp")
    )

    _session_agg = (
        _raw_with_ts_session
        .withWatermark("event_time", "5 minutes")
        .groupBy(
            F.session_window("event_time", _gap),
            "sensor_name", "sensor_type",
        )
        .agg(
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.count("*").alias("reading_count"),
        )
        .withColumnRenamed("sensor_name", "sensor_id")
        .withColumn("pipeline_id", F.lit(pipeline_id))
    )

    query_session = (
        _session_agg
        .writeStream
        .foreach(SessionEventForeachWriter())
        .outputMode("update")
        .trigger(processingTime="20 seconds")
        .option(
            "checkpointLocation",
            f"{checkpoint_base}/sessions",
        )
        .queryName("sss_session_windows")
        .start()
    )
    print(f"Query 4 (session) started: {query_session.name} (id={query_session.id})")
else:
    print("Query 4 (session) skipped — session_config.enabled is False")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 5 — mapGroupsWithState (per-sensor EWMA + threshold)

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, IntegerType, TimestampType,
)

# Broadcast configs for use inside the state update function
_state_ops_broadcast = spark.sparkContext.broadcast(state_ops_config)

_STATE_OUTPUT_SCHEMA = StructType([
    StructField("pipeline_id", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("sensor_type", StringType(), True),
    StructField("running_avg", DoubleType(), True),
    StructField("running_min", DoubleType(), True),
    StructField("running_max", DoubleType(), True),
    StructField("readings_since_reset", IntegerType(), True),
    StructField("last_value", DoubleType(), True),
    StructField("prev_value", DoubleType(), True),
    StructField("threshold_breached", BooleanType(), True),
])

_STATE_SCHEMA = StructType([
    StructField("running_avg", DoubleType(), True),
    StructField("running_min", DoubleType(), True),
    StructField("running_max", DoubleType(), True),
    StructField("readings_since_reset", IntegerType(), True),
    StructField("last_value", DoubleType(), True),
    StructField("prev_value", DoubleType(), True),
    StructField("threshold_breached", BooleanType(), True),
])

_EWMA_ALPHA = 2.0 / (20 + 1)  # alpha for N=20 period EWMA


def update_sensor_state(key, rows_iter, state):
    """Maintain per-sensor EWMA running average, min/max, and threshold breach.

    key   — (pipeline_id, sensor_id, sensor_type)
    rows_iter — iterator of incoming rows for this micro-batch
    state — GroupState carrying serialised sensor snapshot
    """
    pipeline_id, sensor_id, sensor_type = key
    ops = _state_ops_broadcast.value
    thresholds = ops.get("threshold_alerts", {})
    sensor_thresholds = thresholds.get(sensor_type, None)

    if state.exists:
        s = state.get
        running_avg = s["running_avg"]
        running_min = s["running_min"]
        running_max = s["running_max"]
        readings = s["readings_since_reset"]
        last_value = s["last_value"]
        prev_value = s["prev_value"]
    else:
        running_avg = None
        running_min = None
        running_max = None
        readings = 0
        last_value = None
        prev_value = None

    output_rows = []
    for row in rows_iter:
        value = float(row["value"])
        prev_value = last_value
        last_value = value
        readings += 1

        # EWMA update
        if running_avg is None:
            running_avg = value
        else:
            running_avg = _EWMA_ALPHA * value + (1.0 - _EWMA_ALPHA) * running_avg

        # Min / max tracking
        running_min = value if running_min is None else min(running_min, value)
        running_max = value if running_max is None else max(running_max, value)

        # Threshold breach detection
        breached = False
        if sensor_thresholds is not None:
            if value < sensor_thresholds.get("min", float("-inf")):
                breached = True
            if value > sensor_thresholds.get("max", float("inf")):
                breached = True
        threshold_breached = breached

        output_rows.append((
            pipeline_id,
            sensor_id,
            sensor_type,
            running_avg,
            running_min,
            running_max,
            readings,
            last_value,
            prev_value,
            threshold_breached,
        ))

    # Persist updated state
    state.update((
        running_avg,
        running_min,
        running_max,
        readings,
        last_value,
        prev_value,
        threshold_breached if output_rows else False,
    ))

    return iter(pd.DataFrame(
        output_rows,
        columns=[f.name for f in _STATE_OUTPUT_SCHEMA.fields],
    ))


_raw_for_state = (
    raw_stream
    .withColumn("event_time", F.from_unixtime("timestamp").cast("timestamp"))
    .withColumn("pipeline_id", F.lit(pipeline_id))
    .withColumnRenamed("sensor_name", "sensor_id")
    .withWatermark("event_time", "2 minutes")
)

_state_stream = (
    _raw_for_state
    .groupBy("pipeline_id", "sensor_id", "sensor_type")
    .applyInPandasWithState(
        update_sensor_state,
        _STATE_OUTPUT_SCHEMA,
        _STATE_SCHEMA,
        "append",
        "ProcessingTime",
        "10 minutes",
    )
)

query_state = (
    _state_stream
    .writeStream
    .foreach(StateForeachWriter())
    .outputMode("append")
    .trigger(processingTime="15 seconds")
    .option(
        "checkpointLocation",
        f"{checkpoint_base}/state",
    )
    .queryName("sss_state_snapshots")
    .start()
)

print(f"Query 5 (state) started: {query_state.name} (id={query_state.id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitor both queries

# COMMAND ----------

import time
import json as _json
from pyspark.sql.types import StructType, StructField, StringType, LongType

_metrics_schema = StructType([
    StructField("query_id", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("query_name", StringType(), True),
    StructField("batch_id", LongType(), True),
    StructField("timestamp", StringType(), True),
    StructField("progress", StringType(), True),
])

_seen_batches: set[tuple[str, int]] = set()


def _write_metrics(query, progress: dict) -> None:
    """Write the full lastProgress JSON to the streaming_metrics table."""
    qid = str(query.id)
    batch_id = progress.get("batchId")
    if batch_id is None:
        return
    key = (qid, int(batch_id))
    if key in _seen_batches:
        return
    _seen_batches.add(key)

    row_data = [(
        qid,
        str(query.runId),
        query.name or "",
        int(batch_id),
        progress.get("timestamp", ""),
        _json.dumps(progress),
    )]
    try:
        df = spark.createDataFrame(row_data, schema=_metrics_schema)
        df.write.mode("append").saveAsTable(metrics_table_fqn)
    except Exception as e:
        print(f"[metrics] Failed to write batch {batch_id} for {query.name}: {e}")


_optional_queries = [q for q in sliding_queries + [query_session, query_state] if q is not None]
all_queries = [query_raw, query_agg] + _optional_queries

try:
    while any(q.isActive for q in all_queries):
        for q in all_queries:
            if q.isActive and q.lastProgress:
                prog = q.lastProgress
                print(
                    f"[{q.name}] batch={prog.get('batchId', '?')} "
                    f"rows={prog.get('numInputRows', '?')} "
                    f"processedRowsPerSecond={prog.get('processedRowsPerSecond', '?')}"
                )
                _write_metrics(q, prog)
        time.sleep(10)
except KeyboardInterrupt:
    print("Stopping queries...")
    for q in all_queries:
        q.stop()
    print(f"All {len(all_queries)} queries stopped.")
