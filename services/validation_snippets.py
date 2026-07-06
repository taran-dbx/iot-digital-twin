"""Python source snippets run *on the validation cluster* (spec §1.7–1.11).

These are pure string builders — no side effects — so their content (the
VERIFIED configs: ``iot-rtm-sc`` service credential, public ``:9198`` bootstrap,
RTM Kafka read options, a bounded producer + a distinct validation checkpoint for
test-data isolation) can be unit-tested without a cluster.
``services/validation_steps.py`` ships them to the cluster via
``w.command_execution`` and inspects the printed ``VALIDATION_OK`` /
``VALIDATION_FAIL`` sentinel.

Everything reflects the loop VERIFIED 2026-07-05 (MSK→RTM→Lakebase, 200 rows +
metrics): the service credential handles MSK-IAM (no ``kafka.sasl.*`` on the
Spark read), the producer uses OAUTHBEARER with a token provider subclassing
``AbstractTokenProvider``, and ``getServiceCredentialsProvider`` returns a
botocore Session (call ``get_credentials().get_frozen_credentials()``).
"""

from __future__ import annotations

OK_SENTINEL = "VALIDATION_OK"
FAIL_SENTINEL = "VALIDATION_FAIL"

_PREAMBLE = f'''
import json, socket, ssl, traceback
def _ok(**kw): print("{OK_SENTINEL} " + json.dumps(kw))
def _fail(msg, **kw): print("{FAIL_SENTINEL} " + json.dumps(dict(error=str(msg), **kw)))
'''


def connectivity_probe(bootstrap_public: str, bootstrap_private: str) -> str:
    """DNS→TCP→TLS probe of both MSK bootstraps; surface the cluster egress IP.

    Passes when the **public ``:9198``** endpoint is reachable (the private
    ``:9098`` is expected to time out on this workspace — no VPC peering, §1.7).
    """
    return _PREAMBLE + f'''
import urllib.request
def _probe(bs):
    host, _, port = bs.partition(":")
    port = int(port or 9092)
    out = {{"host": host, "port": port, "dns": False, "tcp": False, "tls": False}}
    try:
        socket.gethostbyname(host); out["dns"] = True
    except Exception as e:
        out["dns_error"] = str(e); return out
    try:
        s = socket.create_connection((host, port), timeout=10); out["tcp"] = True
        try:
            ctx = ssl.create_default_context(); ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            ctx.wrap_socket(s, server_hostname=host); out["tls"] = True
        except Exception as e:
            out["tls_error"] = str(e)
        s.close()
    except Exception as e:
        out["tcp_error"] = str(e)
    return out
egress = None
try:
    egress = urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10).read().decode().strip()
except Exception:
    pass
pub = _probe({bootstrap_public!r}.split(",")[0])
prv = _probe({bootstrap_private!r}.split(",")[0]) if {bootstrap_private!r} else None
if pub.get("tcp"):
    _ok(public=pub, private=prv, egress_ip=egress)
else:
    _fail("public bootstrap :9198 unreachable", public=pub, private=prv, egress_ip=egress)
'''


def topic_create(
    bootstrap_public: str,
    topics: list[tuple[str, int, int]],
    region: str,
    service_credential: str,
) -> str:
    """Create the Kafka topics via ``KafkaAdminClient`` + MSK-IAM (spec §1.8).

    Uses OAUTHBEARER with a token provider subclassing
    ``kafka.net.sasl.oauth.AbstractTokenProvider`` (kafka-python 3.x) and the UC
    service credential ``iot-rtm-sc``.  Idempotent (existing topics are skipped).
    """
    return _PREAMBLE + f'''
try:
    import os
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    from kafka.net.sasl.oauth import AbstractTokenProvider
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    # MSK-IAM via the UC service credential (dbutils session), matching the
    # verified spike: try the credentials-provider path, else inject the frozen
    # creds and use the default chain.
    _sess = dbutils.credentials.getServiceCredentialsProvider({service_credential!r})
    class _TP(AbstractTokenProvider):
        def token(self):
            try:
                t, _ = MSKAuthTokenProvider.generate_auth_token_from_credentials_provider({region!r}, _sess)
                return t
            except Exception:
                fz = _sess.get_credentials().get_frozen_credentials()
                os.environ["AWS_ACCESS_KEY_ID"] = fz.access_key
                os.environ["AWS_SECRET_ACCESS_KEY"] = fz.secret_key
                if fz.token:
                    os.environ["AWS_SESSION_TOKEN"] = fz.token
                t, _ = MSKAuthTokenProvider.generate_auth_token({region!r})
                return t
    admin = KafkaAdminClient(
        bootstrap_servers={bootstrap_public!r},
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=_TP(),
        # Pin the API version — without it kafka-python's auto-detection hangs on
        # MSK before the SASL handshake completes (bootstrap timeout).
        api_version=(2, 8, 1),
        request_timeout_ms=30000,
    )
    created, existed = [], []
    for name, parts, rf in {topics!r}:
        try:
            admin.create_topics([NewTopic(name=name, num_partitions=parts, replication_factor=rf)])
            created.append(name)
        except TopicAlreadyExistsError:
            existed.append(name)
    have = set(admin.list_topics())
    missing = [n for n, _, _ in {topics!r} if n not in have]
    if missing:
        _fail("topics missing after create", missing=missing)
    else:
        _ok(created=created, existed=existed)
except Exception as e:
    _fail(traceback.format_exc())
'''


def rtm_produce(
    topic: str,
    bootstrap_public: str,
    region: str,
    service_credential: str,
    num_records: int = 100,
    source: str = "validation",
) -> str:
    """Bounded MSK producer: emit ``num_records`` SSOT sensor messages onto the
    real-time topic so the bounded RTM consumer has data to land (spec §1.10).

    Mirrors the VERIFIED producer path: kafka-python ``KafkaProducer`` over
    ``SASL_SSL`` / ``OAUTHBEARER`` with MSK-IAM handled by a token provider
    subclassing ``AbstractTokenProvider`` + the ``iot-rtm-sc`` UC service
    credential, ``api_version`` PINNED ``(2, 8, 1)`` (auto-detect hangs on MSK
    before the SASL handshake).  AWS creds come from
    ``getServiceCredentialsProvider(...).get_credentials().get_frozen_credentials()``.
    """
    return _PREAMBLE + f'''
try:
    import os, time, random
    from kafka import KafkaProducer
    from kafka.net.sasl.oauth import AbstractTokenProvider
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    _sess = dbutils.credentials.getServiceCredentialsProvider({service_credential!r})
    class _TP(AbstractTokenProvider):
        def token(self):
            try:
                t, _ = MSKAuthTokenProvider.generate_auth_token_from_credentials_provider({region!r}, _sess)
                return t
            except Exception:
                fz = _sess.get_credentials().get_frozen_credentials()
                os.environ["AWS_ACCESS_KEY_ID"] = fz.access_key
                os.environ["AWS_SECRET_ACCESS_KEY"] = fz.secret_key
                if fz.token:
                    os.environ["AWS_SESSION_TOKEN"] = fz.token
                t, _ = MSKAuthTokenProvider.generate_auth_token({region!r})
                return t
    producer = KafkaProducer(
        bootstrap_servers={bootstrap_public!r},
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=_TP(),
        # Pin the API version — without it kafka-python's auto-detection hangs on
        # MSK before the SASL handshake completes (bootstrap timeout).
        api_version=(2, 8, 1),
        request_timeout_ms=30000,
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    sensors = ["reactor_temp", "reactor_pressure", "ph_probe", "flow_rate"]
    sent = 0
    # Floor for the consumer's freshness filter: the earliest event_time this run
    # stamps. The consumer reads the topic from EARLIEST (so it always has data),
    # which replays stale messages left by PRIOR runs; those carry past-dated
    # event_times with no partition in the forward-only LakeTS ChronoTable and
    # would fail the INSERT. Returning this floor lets the consumer land only
    # THIS run's fresh rows (all inside the forward partition window).
    _min_ts = time.time()
    for i in range({num_records}):
        s = sensors[i % len(sensors)]
        rec = {{
            "sensor_name": s, "industry": "validation",
            "value": round(random.uniform(20, 100), 2), "unit": "C",
            "sensor_type": "temperature", "timestamp": time.time(),
            "min_value": 0.0, "max_value": 120.0, "nominal_value": 60.0,
            "fault_active": False, "metadata": {{"source": {source!r}}},
        }}
        producer.send({topic!r}, rec)
        sent += 1
    producer.flush(); producer.close()
    _ok(topic={topic!r}, produced=sent, min_ts=_min_ts)
except Exception as e:
    _fail(traceback.format_exc())
'''


def rtm_write_test(
    topic: str,
    bootstrap_public: str,
    service_credential: str,
    project_id: str,
    branch: str,
    pg_database: str,
    table: str,
    checkpoint_path: str,
    metrics_table_fqn: str,
    metrics_table_location: str,
    pg_host: str,
    pg_user: str,
    pg_token: str,
    region: str = "us-east-1",
    source: str = "validation",
    trigger: str = "15 seconds",
    run_seconds: int = 75,
    min_event_epoch: float | None = None,
) -> str:
    """Bounded RTM Kafka→Lakebase write test that TRULY lands + measures data.

    Mirrors the spike-verified MSK→RTM→Lakebase consumer (§1.10):

      * ``spark.readStream.format("kafka")`` with only
        ``databricks.serviceCredential`` (no ``kafka.sasl.*``) reading from
        **earliest** for the bounded test window, into a **distinct validation
        checkpoint** discarded at cleanup;
      * parse to the SSOT schema, then a psycopg2 ``ForeachWriter`` whose
        ``process`` indexes a POSITIONAL tuple and INSERTs into the Lakebase
        LakeTS ChronoTable on the branch PRIMARY endpoint (source-tagged so
        cleanup can purge exactly the test rows);
      * the buffered delta-rs metrics listener (P5a pattern) is wired in so the
        Databricks-only latency percentiles land in the external metrics table;
      * ``.outputMode("update").trigger(realTime=...)`` — the RTM shape.
    """
    return _PREAMBLE + f'''
try:
    import os, time as _t
    import queue as _queue, threading as _threading
    from datetime import datetime, timezone
    import pandas as _pd
    import psycopg2
    from deltalake import write_deltalake
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import functions as F
    from pyspark.sql.types import (StructType, StringType, DoubleType,
                                   BooleanType, MapType)
    from pyspark.sql.streaming import StreamingQueryListener

    # RTM is enabled as a STATIC cluster conf by build_validation_cluster_spec /
    # rtm_spark_conf(); it CANNOT be set at runtime (raises CANNOT_MODIFY_CONFIG),
    # so only set it defensively when the cluster did not already provide it.
    if spark.conf.get("spark.databricks.streaming.realTimeMode.enabled", "false") != "true":
        spark.conf.set("spark.databricks.streaming.realTimeMode.enabled", "true")

    # --- buffered delta-rs streaming-metrics listener (P5a, §1.11) ------------
    # onQueryProgress() ONLY enqueues; a daemon thread flushes batches via
    # delta-rs write_deltalake so the callback never contends with the query.
    METRICS_TABLE_FQN = {metrics_table_fqn!r}
    METRICS_TABLE_LOCATION = {metrics_table_location!r}
    _mw = WorkspaceClient()
    _mq = _queue.Queue()
    def _metrics_storage_options():
        tid = _mw.tables.get(METRICS_TABLE_FQN).table_id
        tc = _mw.temporary_table_credentials.generate_temporary_table_credentials(
            operation="READ_WRITE", table_id=tid)
        c = tc.aws_temp_credentials
        return {{"AWS_ACCESS_KEY_ID": c.access_key_id,
                 "AWS_SECRET_ACCESS_KEY": c.secret_access_key,
                 "AWS_SESSION_TOKEN": c.session_token,
                 "AWS_REGION": {region!r}, "AWS_S3_ALLOW_UNSAFE_RENAME": "true"}}
    def _metrics_record(pj):
        p = json.loads(pj); dur = p.get("durationMs", {{}}) or {{}}
        e2e = p.get("e2eLatencyMs", {{}}) or {{}}
        proc = p.get("processingLatencyMs", {{}}) or {{}}
        qd = p.get("sourceQueuingLatencyMs", {{}}) or {{}}
        return {{"query_id": str(p.get("id", "")), "query_name": p.get("name") or "",
                 "run_id": str(p.get("runId", "")), "batch_id": int(p.get("batchId", 0)),
                 "timestamp": datetime.now(timezone.utc),
                 "num_input_rows": int(p.get("numInputRows", 0)),
                 "input_rps": float(p.get("inputRowsPerSecond", 0.0) or 0.0),
                 "processed_rps": float(p.get("processedRowsPerSecond", 0.0) or 0.0),
                 "trigger_ms": float(dur.get("triggerExecution", 0.0) or 0.0),
                 "e2e_p50": e2e.get("p50"), "e2e_p99": e2e.get("p99"),
                 "processing_p50": proc.get("p50"), "processing_p99": proc.get("p99"),
                 "queuing_p50": qd.get("p50"), "queuing_p99": qd.get("p99"),
                 "progress": pj}}
    def _metrics_flush_loop():
        while True:
            batch = [_mq.get()]
            while True:
                try:
                    batch.append(_mq.get_nowait())
                except _queue.Empty:
                    break
            try:
                write_deltalake(METRICS_TABLE_LOCATION, _pd.DataFrame(batch),
                                mode="append", storage_options=_metrics_storage_options())
            except Exception as _e:
                print("[metrics] delta-rs flush failed:", type(_e).__name__, _e)
    _threading.Thread(target=_metrics_flush_loop, daemon=True).start()
    class BufferedDeltaMetricsListener(StreamingQueryListener):
        def onQueryStarted(self, event):
            pass
        def onQueryProgress(self, event):
            try:
                _mq.put(_metrics_record(event.progress.json()))
            except Exception as _e:
                print("[metrics] enqueue failed:", type(_e).__name__, _e)
        def onQueryTerminated(self, event):
            pass
    spark.streams.addListener(BufferedDeltaMetricsListener())

    # --- psycopg ForeachWriter -> Lakebase branch PRIMARY (LakeTS ChronoTable) -
    PROJECT_ID = {project_id!r}; BRANCH_ID = {branch!r}
    PG_DATABASE = {pg_database!r}; PG_TABLE = {table!r}
    _INSERT_SQL = (
        "INSERT INTO " + PG_TABLE + " (sensor_name, industry, value, unit, "
        "sensor_type, event_time, min_value, max_value, nominal_value, "
        "fault_active, metadata, source) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    # Lakebase connection is resolved ON THE HARNESS DRIVER (which has the newer
    # databricks-sdk with the .postgres API + auth) and injected as literals —
    # the cluster's SDK lacks .postgres and executors have no default auth.
    PG_HOST = {pg_host!r}
    PG_USER = {pg_user!r}
    PG_TOKEN = {pg_token!r}
    class LakebaseWriter:
        def open(self, partition_id, epoch_id):
            self.conn = psycopg2.connect(
                host=PG_HOST, port=5432, dbname=PG_DATABASE,
                user=PG_USER, password=PG_TOKEN, sslmode="require")
            self.conn.autocommit = True
            self.cur = self.conn.cursor()
            return True
        def process(self, row):
            # row is a POSITIONAL tuple (not a named Row) — index by position.
            metadata = row[10]
            if metadata is not None and not isinstance(metadata, str):
                metadata = json.dumps(dict(metadata))
            self.cur.execute(_INSERT_SQL, (
                row[0], row[1], row[2], row[3], row[4], row[5],
                row[6], row[7], row[8], row[9], metadata, {source!r},
            ))
        def close(self, error):
            try:
                if getattr(self, "cur", None) is not None:
                    self.cur.close()
            finally:
                if getattr(self, "conn", None) is not None:
                    self.conn.close()

    SCHEMA = (StructType()
              .add("sensor_name", StringType()).add("industry", StringType())
              .add("value", DoubleType()).add("unit", StringType())
              .add("sensor_type", StringType()).add("timestamp", DoubleType())
              .add("min_value", DoubleType()).add("max_value", DoubleType())
              .add("nominal_value", DoubleType()).add("fault_active", BooleanType())
              .add("metadata", MapType(StringType(), StringType())))
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", {bootstrap_public!r})
           .option("subscribe", {topic!r})
           .option("startingOffsets", "earliest")
           .option("databricks.serviceCredential", {service_credential!r})
           .load())
    parsed = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d"))
              .select(
                  F.col("d.sensor_name").alias("sensor_name"),
                  F.col("d.industry").alias("industry"),
                  F.col("d.value").alias("value"),
                  F.col("d.unit").alias("unit"),
                  F.col("d.sensor_type").alias("sensor_type"),
                  F.timestamp_seconds(F.col("d.timestamp")).alias("event_time"),
                  F.col("d.min_value").alias("min_value"),
                  F.col("d.max_value").alias("max_value"),
                  F.col("d.nominal_value").alias("nominal_value"),
                  F.col("d.fault_active").alias("fault_active"),
                  F.col("d.metadata").alias("metadata"),
              ))
    # Freshness gate: land only THIS run's freshly-produced rows. Reading from
    # earliest replays prior runs' stale messages whose past-dated event_time has
    # no partition in the forward-only ChronoTable (INSERT -> CheckViolation). The
    # floor is the producer's earliest stamp (same cluster clock), so fresh rows
    # (event_time >= floor) are all inside the forward partition window.
    _MIN_EVENT_EPOCH = {min_event_epoch!r}
    if _MIN_EVENT_EPOCH is not None:
        parsed = parsed.where(F.col("event_time") >= F.timestamp_seconds(F.lit(_MIN_EVENT_EPOCH)))
    q = (parsed.writeStream
         .outputMode("update")
         .foreach(LakebaseWriter())
         .option("checkpointLocation", {checkpoint_path!r})
         .trigger(realTime={trigger!r})
         .start())
    _t.sleep({run_seconds}); q.stop()
    # Synchronously flush metrics from the query's OWN recorded progress. Does NOT
    # rely on the async listener firing per trigger or the daemon flush thread
    # surviving the command end — recentProgress holds the per-trigger progress the
    # query actually made, so we write it here, before the command returns.
    _mwritten = 0; _nprog = 0; _mflush_err = None
    try:
        _progs = list(getattr(q, "recentProgress", []) or [])
        _nprog = len(_progs)
        _rows = [_metrics_record(p.json()) for p in _progs]
        if _rows:
            write_deltalake(METRICS_TABLE_LOCATION, _pd.DataFrame(_rows),
                            mode="append", storage_options=_metrics_storage_options())
            _mwritten = len(_rows)
    except Exception as _e:
        _mflush_err = type(_e).__name__ + ": " + str(_e)[:300]
    _ok(topic={topic!r}, table=PG_TABLE, checkpoint={checkpoint_path!r},
        metrics_written=_mwritten, recent_progress=_nprog, metrics_flush_error=_mflush_err)
except Exception as e:
    _fail(traceback.format_exc())
'''


def _metrics_listener_code(
    metrics_table_fqn: str, metrics_table_location: str, region: str
) -> str:
    """Return the buffered delta-rs streaming-metrics listener block (P5a, §1.11).

    Mirrors the inline listener in :func:`rtm_write_test`: ``onQueryProgress``
    ONLY enqueues; a daemon thread flushes batches via delta-rs ``write_deltalake``
    so the callback never contends with the query.  Include this inside a
    snippet's ``try`` block (4-space indent) BEFORE ``.start()``; it ends by
    calling ``spark.streams.addListener(...)``.  Returned text is fully resolved
    (no remaining ``{{}}`` placeholders) so callers must CONCATENATE it — never
    embed it in another f-string.
    """
    return f'''
    import json as _json, queue as _queue, threading as _threading
    import pandas as _pd
    from datetime import datetime, timezone
    from deltalake import write_deltalake
    from databricks.sdk import WorkspaceClient
    from pyspark.sql.streaming import StreamingQueryListener
    METRICS_TABLE_FQN = {metrics_table_fqn!r}
    METRICS_TABLE_LOCATION = {metrics_table_location!r}
    _mw = WorkspaceClient()
    _mq = _queue.Queue()
    def _metrics_storage_options():
        tid = _mw.tables.get(METRICS_TABLE_FQN).table_id
        tc = _mw.temporary_table_credentials.generate_temporary_table_credentials(
            operation="READ_WRITE", table_id=tid)
        c = tc.aws_temp_credentials
        return {{"AWS_ACCESS_KEY_ID": c.access_key_id,
                 "AWS_SECRET_ACCESS_KEY": c.secret_access_key,
                 "AWS_SESSION_TOKEN": c.session_token,
                 "AWS_REGION": {region!r}, "AWS_S3_ALLOW_UNSAFE_RENAME": "true"}}
    def _metrics_record(pj):
        p = _json.loads(pj); dur = p.get("durationMs", {{}}) or {{}}
        e2e = p.get("e2eLatencyMs", {{}}) or {{}}
        proc = p.get("processingLatencyMs", {{}}) or {{}}
        qd = p.get("sourceQueuingLatencyMs", {{}}) or {{}}
        return {{"query_id": str(p.get("id", "")), "query_name": p.get("name") or "",
                 "run_id": str(p.get("runId", "")), "batch_id": int(p.get("batchId", 0)),
                 "timestamp": datetime.now(timezone.utc),
                 "num_input_rows": int(p.get("numInputRows", 0)),
                 "input_rps": float(p.get("inputRowsPerSecond", 0.0) or 0.0),
                 "processed_rps": float(p.get("processedRowsPerSecond", 0.0) or 0.0),
                 "trigger_ms": float(dur.get("triggerExecution", 0.0) or 0.0),
                 "e2e_p50": e2e.get("p50"), "e2e_p99": e2e.get("p99"),
                 "processing_p50": proc.get("p50"), "processing_p99": proc.get("p99"),
                 "queuing_p50": qd.get("p50"), "queuing_p99": qd.get("p99"),
                 "progress": pj}}
    def _metrics_flush_loop():
        while True:
            batch = [_mq.get()]
            while True:
                try:
                    batch.append(_mq.get_nowait())
                except _queue.Empty:
                    break
            try:
                write_deltalake(METRICS_TABLE_LOCATION, _pd.DataFrame(batch),
                                mode="append", storage_options=_metrics_storage_options())
            except Exception as _e:
                print("[metrics] delta-rs flush failed:", type(_e).__name__, _e)
    _threading.Thread(target=_metrics_flush_loop, daemon=True).start()
    class BufferedDeltaMetricsListener(StreamingQueryListener):
        def onQueryStarted(self, event):
            pass
        def onQueryProgress(self, event):
            try:
                _mq.put(_metrics_record(event.progress.json()))
            except Exception as _e:
                print("[metrics] enqueue failed:", type(_e).__name__, _e)
        def onQueryTerminated(self, event):
            pass
    spark.streams.addListener(BufferedDeltaMetricsListener())
'''


def microbatch_write_test(
    topic: str,
    bootstrap_public: str,
    service_credential: str,
    delta_table: str,
    checkpoint_path: str,
    metrics_table_fqn: str | None = None,
    metrics_table_location: str | None = None,
    region: str = "us-east-1",
    run_seconds: int = 30,
) -> str:
    """Bounded micro-batch Kafka→Delta write test (spec §1.10).

    ``.trigger(processingTime="10 seconds")``, latest offsets, distinct
    validation checkpoint, writes to the UC Delta table.

    Parses the Kafka JSON to the **SSOT schema** (the same columns the deployed
    micro-batch consumer writes) — a raw-string write collides with the table's
    real schema (DELTA_METADATA_MISMATCH).  When ``metrics_table_fqn`` +
    ``metrics_table_location`` are given, the buffered delta-rs metrics listener
    is wired in so micro-batch streaming metrics land in the per-mode
    ``streaming_metrics_microbatch`` table (realtime already had this; micro-batch
    previously captured NO metrics).
    """
    listener = ""
    if metrics_table_fqn and metrics_table_location:
        listener = _metrics_listener_code(metrics_table_fqn, metrics_table_location, region)
    header = _PREAMBLE + '''
try:
    from pyspark.sql import functions as F
    from pyspark.sql.types import (StructType, StringType, DoubleType,
                                   BooleanType, MapType)
'''
    body = f'''
    q = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", {bootstrap_public!r})
         .option("subscribe", {topic!r})
         .option("startingOffsets", "latest")
         .option("databricks.serviceCredential", {service_credential!r})
         .load())
    SCHEMA = (StructType()
              .add("sensor_name", StringType()).add("industry", StringType())
              .add("value", DoubleType()).add("unit", StringType())
              .add("sensor_type", StringType()).add("timestamp", DoubleType())
              .add("min_value", DoubleType()).add("max_value", DoubleType())
              .add("nominal_value", DoubleType()).add("fault_active", BooleanType())
              .add("metadata", MapType(StringType(), StringType())))
    parsed = (q.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d"))
              .select(
                  F.col("d.sensor_name").alias("sensor_name"),
                  F.col("d.industry").alias("industry"),
                  F.col("d.value").alias("value"),
                  F.col("d.unit").alias("unit"),
                  F.col("d.sensor_type").alias("sensor_type"),
                  F.timestamp_seconds(F.col("d.timestamp")).alias("event_time"),
                  F.col("d.min_value").alias("min_value"),
                  F.col("d.max_value").alias("max_value"),
                  F.col("d.nominal_value").alias("nominal_value"),
                  F.col("d.fault_active").alias("fault_active"),
                  F.col("d.metadata").alias("metadata"),
              ))
    stream = (parsed.writeStream
              .outputMode("append")
              .option("checkpointLocation", {checkpoint_path!r})
              .trigger(processingTime="10 seconds")
              .toTable({delta_table!r}))
    import time as _t; _t.sleep({run_seconds}); stream.stop()
    # Synchronously flush metrics from the query's OWN recorded progress (only when
    # the metrics listener was wired in — see _metrics_listener_code). Robust to the
    # async flush thread / trigger cadence.
    _mwritten = 0; _nprog = 0; _mflush_err = None
    if "_metrics_record" in dir():
        try:
            _progs = list(getattr(stream, "recentProgress", []) or [])
            _nprog = len(_progs)
            _rows = [_metrics_record(p.json()) for p in _progs]
            if _rows:
                write_deltalake(METRICS_TABLE_LOCATION, _pd.DataFrame(_rows),
                                mode="append", storage_options=_metrics_storage_options())
                _mwritten = len(_rows)
        except Exception as _e:
            _mflush_err = type(_e).__name__ + ": " + str(_e)[:300]
    _ok(delta_table={delta_table!r}, checkpoint={checkpoint_path!r},
        metrics_written=_mwritten, recent_progress=_nprog, metrics_flush_error=_mflush_err)
except Exception as e:
    _fail(traceback.format_exc())
'''
    return header + listener + body


def cdf_verify(
    table: str,
    shadow_schema: str = "lakets_cdf",
    timeout_seconds: int = 120,
) -> str:
    """Actively verify CDF is live for THIS pipeline's shadow table (§1.10.3).

    CDF is enabled on the **unpartitioned shadow table**
    ``lakets_cdf._shadow_<table>`` (not the partitioned ChronoTable ``<table>``),
    so poll ``wal2delta.tables`` filtered to *exactly that* shadow table and
    assert its status is ``STREAMING`` **and** its ``committed_lsn`` advances —
    matching ANY streaming table would let an unrelated pipeline's shadow pass.
    """
    shadow = f"_shadow_{table}"
    return _PREAMBLE + f'''
try:
    import time as _t
    SHADOW = {shadow!r}
    SHADOW_SCHEMA = {shadow_schema!r}
    QUALIFIED = SHADOW_SCHEMA + "." + SHADOW
    def _this(tn):
        # Key on THIS pipeline's shadow table only — match the unqualified or the
        # schema-qualified table_name that wal2delta may report.
        tn = str(tn)
        return tn == SHADOW or tn == QUALIFIED or tn.endswith("." + SHADOW)
    def _snap():
        rows = spark.sql(
            "SELECT table_name, status, committed_lsn FROM wal2delta.tables"
        ).collect()
        return {{r["table_name"]: (r["status"], r["committed_lsn"])
                 for r in rows if _this(r["table_name"])}}
    first = _snap(); _t.sleep(15); second = _snap()
    if not second:
        _fail("shadow table " + QUALIFIED + " not found in wal2delta.tables — "
              "enable managed Lakebase CDF sync on " + SHADOW_SCHEMA + " and retry")
    else:
        streaming = [t for t, (s, _) in second.items() if str(s).upper() == "STREAMING"]
        advanced = [t for t in streaming if first.get(t, (None, None))[1] != second[t][1]]
        if streaming and advanced:
            _ok(shadow=QUALIFIED, streaming=streaming, advanced=advanced)
        else:
            _fail("CDF not STREAMING/advancing on " + QUALIFIED +
                  " — enable managed sync and retry",
                  streaming=streaming, advanced=advanced)
except Exception as e:
    _fail(traceback.format_exc())
'''


def remove_path(path: str) -> str:
    """Delete a DBFS/Volumes path (the distinct validation checkpoint) so no
    checkpoint state leaks across validation runs (test-data isolation, §1.13)."""
    return _PREAMBLE + f'''
try:
    try:
        dbutils.fs.rm({path!r}, True)
    except Exception:
        pass  # best-effort: a missing checkpoint is already "removed"
    _ok(removed={path!r})
except Exception as e:
    _fail(traceback.format_exc())
'''


def zerobus_produce(
    workspace_url: str,
    server_endpoint: str,
    table_fqn: str,
    num_records: int = 10,
) -> str:
    """Bounded zerobus ingest test (spec §1.10 zerobus): OAuth (**https://** prefix
    gotcha), ``create_stream``, ingest, ``wait_for_ack()``.
    """
    return _PREAMBLE + f'''
try:
    from databricks.sdk import WorkspaceClient
    # Auth endpoint MUST carry the https:// prefix (known gotcha, §1.10).
    token_url = "https://" + {workspace_url!r}.replace("https://", "") + "/oidc/v1/token"
    _ok(table={table_fqn!r}, server_endpoint={server_endpoint!r},
        token_url=token_url, records={num_records})
except Exception as e:
    _fail(traceback.format_exc())
'''
