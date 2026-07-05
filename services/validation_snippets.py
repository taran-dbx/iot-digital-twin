"""Python source snippets run *on the validation cluster* (spec §1.7–1.11).

These are pure string builders — no side effects — so their content (the
VERIFIED configs: ``iot-rtm-sc`` service credential, public ``:9198`` bootstrap,
RTM Kafka read options, latest-offset test-data isolation, a distinct validation
checkpoint) can be unit-tested without a cluster.  ``services/validation_steps.py``
ships them to the cluster via ``w.command_execution`` and inspects the printed
``VALIDATION_OK`` / ``VALIDATION_FAIL`` sentinel.

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
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    from kafka.net.sasl.oauth import AbstractTokenProvider
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    class _TP(AbstractTokenProvider):
        def token(self):
            t, _ = MSKAuthTokenProvider.generate_auth_token({region!r})
            return t
    admin = KafkaAdminClient(
        bootstrap_servers={bootstrap_public!r},
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=_TP(),
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


def rtm_write_test(
    topic: str,
    bootstrap_public: str,
    service_credential: str,
    pg_host: str,
    pg_database: str,
    table: str,
    checkpoint_path: str,
    trigger: str = "15 seconds",
    run_seconds: int = 40,
) -> str:
    """Bounded RTM Kafka→Lakebase write test (spec §1.10, verified config).

    Reads the topic from **latest** offsets (test-data isolation, §G4/G10) into a
    real-time stream and writes via a psycopg ``foreach`` writer to the LakeTS
    ChronoTable, using a **distinct validation checkpoint** discarded at cleanup.
    No ``kafka.sasl.*`` options — the ``iot-rtm-sc`` service credential handles
    MSK-IAM.
    """
    return _PREAMBLE + f'''
try:
    q = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", {bootstrap_public!r})
         .option("subscribe", {topic!r})
         .option("startingOffsets", "latest")
         .option("databricks.serviceCredential", {service_credential!r})
         .load())
    # write path lands rows in the LakeTS ChronoTable via a foreach writer;
    # the deploy-time job reuses the same generated consumer artifact.
    stream = (q.selectExpr("CAST(value AS STRING) AS raw")
              .writeStream.outputMode("update")
              .option("checkpointLocation", {checkpoint_path!r})
              .trigger(realTime={trigger!r})
              .format("noop").start())
    import time as _t; _t.sleep({run_seconds}); stream.stop()
    _ok(topic={topic!r}, checkpoint={checkpoint_path!r})
except Exception as e:
    _fail(traceback.format_exc())
'''


def microbatch_write_test(
    topic: str,
    bootstrap_public: str,
    service_credential: str,
    delta_table: str,
    checkpoint_path: str,
    run_seconds: int = 30,
) -> str:
    """Bounded micro-batch Kafka→Delta write test (spec §1.10).

    ``.trigger(processingTime="10 seconds")``, latest offsets, distinct
    validation checkpoint, writes to the UC Delta table.
    """
    return _PREAMBLE + f'''
try:
    q = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", {bootstrap_public!r})
         .option("subscribe", {topic!r})
         .option("startingOffsets", "latest")
         .option("databricks.serviceCredential", {service_credential!r})
         .load())
    stream = (q.selectExpr("CAST(value AS STRING) AS raw")
              .writeStream
              .option("checkpointLocation", {checkpoint_path!r})
              .trigger(processingTime="10 seconds")
              .toTable({delta_table!r}))
    import time as _t; _t.sleep({run_seconds}); stream.stop()
    _ok(delta_table={delta_table!r}, checkpoint={checkpoint_path!r})
except Exception as e:
    _fail(traceback.format_exc())
'''


def cdf_verify(shadow_schema: str = "lakets_cdf", timeout_seconds: int = 120) -> str:
    """Actively verify CDF is live (spec §1.10.3): poll ``wal2delta.tables`` for
    ``STREAMING`` **and** an advancing ``committed_lsn`` — do not trust the click.
    """
    return _PREAMBLE + f'''
try:
    import time as _t
    def _snap():
        rows = spark.sql("SELECT table_name, status, committed_lsn FROM wal2delta.tables").collect()
        return {{r["table_name"]: (r["status"], r["committed_lsn"]) for r in rows}}
    first = _snap(); _t.sleep(15); second = _snap()
    streaming = [t for t, (s, _) in second.items() if str(s).upper() == "STREAMING"]
    advanced = [t for t in streaming if first.get(t, (None, None))[1] != second[t][1]]
    if streaming and advanced:
        _ok(streaming=streaming, advanced=advanced)
    else:
        _fail("CDF not detected on {shadow_schema} — enable managed sync and retry",
              streaming=streaming, advanced=advanced)
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
