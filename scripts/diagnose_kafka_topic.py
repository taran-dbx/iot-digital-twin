# Databricks notebook source
# COMMAND ----------
# Diagnose whether the IoT-demo Kafka topic is actually receiving messages.
# Run this as a notebook on E2DEMO (same workspace as the pipeline job).
# COMMAND ----------
%pip install kafka-python --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import json, time
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka import TopicPartition

BOOTSTRAP = "b-2.oetrta.kpgu3r.c1.kafka.us-west-2.amazonaws.com:9092,b-3.oetrta.kpgu3r.c1.kafka.us-west-2.amazonaws.com:9092,b-1.oetrta.kpgu3r.c1.kafka.us-west-2.amazonaws.com:9092"
TOPIC = "iot-rtm-demo"

report = {"bootstrap": BOOTSTRAP, "topic": TOPIC, "steps": []}
def step(name, **kv): report["steps"].append({"step": name, **kv})

try:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, security_protocol="PLAINTEXT", request_timeout_ms=15000)
    topics = admin.list_topics()
    step("topic_exists", value=(TOPIC in topics), total_topics=len(topics))
    if TOPIC in topics:
        cfg = admin.describe_topics([TOPIC])[0]
        step("topic_describe", partitions=len(cfg.get("partitions", [])), cfg=str(cfg)[:400])
    admin.close()
except Exception as e:
    step("admin_error", type=type(e).__name__, msg=str(e)[:400])

# 2. End offsets
try:
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, security_protocol="PLAINTEXT",
                             request_timeout_ms=15000, consumer_timeout_ms=5000)
    parts = consumer.partitions_for_topic(TOPIC) or set()
    tps = [TopicPartition(TOPIC, p) for p in parts]
    if tps:
        consumer.assign(tps)
        beg = consumer.beginning_offsets(tps)
        end = consumer.end_offsets(tps)
        total_msgs = sum(end[tp] - beg[tp] for tp in tps)
        step("end_offsets_before", total=total_msgs,
             partitions=[{"p": tp.partition, "beg": beg[tp], "end": end[tp]} for tp in tps])
    else:
        step("end_offsets_before", error="no partitions")
        tps, beg, total_msgs = [], {}, 0
    consumer.close()
except Exception as e:
    step("consumer_offsets_error", type=type(e).__name__, msg=str(e)[:400])
    tps, beg, total_msgs = [], {}, 0

# 3. Round-trip send
try:
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, security_protocol="PLAINTEXT",
                             acks=1, request_timeout_ms=10000)
    futs = [producer.send(TOPIC, value=f"diag-test-{i}-{int(time.time())}".encode()) for i in range(3)]
    producer.flush()
    results = []
    for i, f in enumerate(futs):
        try:
            md = f.get(timeout=10)
            results.append({"i": i, "partition": md.partition, "offset": md.offset})
        except Exception as e:
            results.append({"i": i, "error": f"{type(e).__name__}: {e}"[:200]})
    producer.close()
    step("round_trip", results=results)
except Exception as e:
    step("producer_error", type=type(e).__name__, msg=str(e)[:400])

# 4. End offsets again
try:
    if tps:
        consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, security_protocol="PLAINTEXT",
                                 request_timeout_ms=15000, consumer_timeout_ms=5000)
        consumer.assign(tps)
        end2 = consumer.end_offsets(tps)
        new_total = sum(end2[tp] - beg[tp] for tp in tps)
        step("end_offsets_after", total=new_total, delta=new_total - total_msgs)
        consumer.close()
except Exception as e:
    step("after_offsets_error", type=type(e).__name__, msg=str(e)[:400])

dbutils.notebook.exit(json.dumps(report))
