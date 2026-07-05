"""Guard tests for the industry-first consumer notebook generators.

The RTM / micro-batch consumers are generated client-side in TypeScript
(``client/src/lib/notebook-template.ts``); there is no TS test runner in this
repo, so — following the same approach as ``tests/test_sensor_schema.py`` — these
guards read the template source and assert the spike-VERIFIED Python patterns
(spec §1.10, §1.11, §1.15) are present. If the template drifts from the proven
config, these tests fail.

Verified invariants asserted here:
  * RTM/micro-batch read Kafka with ONLY ``databricks.serviceCredential`` — no
    ``kafka.sasl.*`` / ``kafka.security.protocol`` (UC credential handles MSK-IAM).
  * Real-time writer: ``.outputMode("update").foreach(LakebaseWriter())
    .trigger(realTime=...)`` with a POSITIONAL-tuple ForeachWriter INSERTing into
    the LakeTS ChronoTable via ``w.postgres.generate_database_credential``.
  * Micro-batch writer: Delta ``.trigger(processingTime="10 seconds")``.
  * Metrics: buffered delta-rs listener (queue + daemon thread +
    ``write_deltalake(mode="append")``; NO ``saveAsTable``; ``progress`` STRING;
    UC temp-table-creds + ``AWS_REGION`` + ``AWS_S3_ALLOW_UNSAFE_RENAME``).
  * Parse schema = all 11 SSOT fields (models/sensor_schema.py).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from models import sensor_schema as ss

NOTEBOOK_TS = (
    Path(__file__).resolve().parent.parent
    / "client" / "src" / "lib" / "notebook-template.ts"
)

pytestmark = pytest.mark.skipif(
    not NOTEBOOK_TS.exists(), reason="notebook template not present (client/ gitignored)"
)


def _ts() -> str:
    return NOTEBOOK_TS.read_text()


def _kafka_read_template() -> str:
    """Extract the shared ``kafkaReadTemplate`` template-literal body."""
    text = _ts()
    m = re.search(r"const kafkaReadTemplate = .*?=> `(.*?)`;", text, re.DOTALL)
    assert m is not None, "could not locate kafkaReadTemplate"
    return m.group(1)


def _ssot_schema_block() -> str:
    text = _ts()
    m = re.search(r"const SSOT_FROM_JSON_SCHEMA = `(.*?)`;", text, re.DOTALL)
    assert m is not None, "could not locate SSOT_FROM_JSON_SCHEMA"
    return m.group(1)


def _metrics_listener_block() -> str:
    """Region from the buffered-metrics header to addListener()."""
    text = _ts()
    m = re.search(
        r"# Streaming metrics — buffered async delta-rs write.*?"
        r"spark\.streams\.addListener\(BufferedDeltaMetricsListener\(\)\)",
        text,
        re.DOTALL,
    )
    assert m is not None, "could not locate buffered metrics listener block"
    return m.group(0)


class TestKafkaRead:
    def test_uses_service_credential(self) -> None:
        block = _kafka_read_template()
        assert 'databricks.serviceCredential' in block

    def test_no_sasl_or_security_protocol(self) -> None:
        # MSK-IAM is handled by the UC service credential; sasl/security.protocol
        # options must NOT appear in the verified read (spec §1.10).
        block = _kafka_read_template()
        assert "kafka.sasl" not in block
        assert "security.protocol" not in block

    def test_reads_kafka_earliest(self) -> None:
        block = _kafka_read_template()
        assert 'spark.readStream.format("kafka")' in block
        assert '.option("startingOffsets", "earliest")' in block


class TestParseSchema:
    def test_all_eleven_ssot_fields_present(self) -> None:
        block = _ssot_schema_block()
        for spec in ss.SENSOR_FIELD_MAP:
            assert f'"{spec.record_field}"' in block, (
                f"parse schema missing SSOT field {spec.record_field}"
            )

    def test_field_names_match_ssot_order(self) -> None:
        block = _ssot_schema_block()
        names = re.findall(r'\.add\("([^"]+)"', block)
        assert names == [f for f, _ in ss.kafka_schema_fields()]

    def test_epoch_timestamp_is_double_metadata_is_map(self) -> None:
        block = _ssot_schema_block()
        ts_line = next(ln for ln in block.splitlines() if '"timestamp"' in ln)
        md_line = next(ln for ln in block.splitlines() if '"metadata"' in ln)
        assert "DoubleType" in ts_line
        assert "MapType" in md_line


class TestRealtimeWriter:
    def test_writer_chain_is_verified_rtm_shape(self) -> None:
        text = _ts()
        assert '.outputMode("update")' in text
        assert ".foreach(LakebaseWriter())" in text
        assert ".trigger(realTime=" in text

    def test_realtime_spark_conf_enables_rtm(self) -> None:
        text = _ts()
        assert "spark.databricks.streaming.realTimeMode.enabled" in text

    def test_default_realtime_interval_is_15_seconds(self) -> None:
        text = _ts()
        assert 'DEFAULT_REALTIME_INTERVAL = "15 seconds"' in text

    def test_foreachwriter_uses_positional_tuple(self) -> None:
        text = _ts()
        assert "def process(self, row):" in text
        # POSITIONAL tuple access (row[0]..row[10]), not named-Row (r.field).
        assert "row[0]" in text and "row[10]" in text
        assert "POSITIONAL tuple" in text

    def test_lakebase_oauth_primary_endpoint(self) -> None:
        text = _ts()
        assert "w.postgres.generate_database_credential" in text
        assert "w.postgres.list_endpoints" in text
        # writes go to the READ_WRITE primary, never a replica.
        assert "READ_WRITE" in text
        assert 'sslmode="require"' in text

    def test_inserts_into_chronotable_with_all_columns(self) -> None:
        text = _ts()
        # The INSERT column list must carry all SSOT columns + source.
        for col in (
            "sensor_name", "industry", "value", "unit", "sensor_type",
            "event_time", "min_value", "max_value", "nominal_value",
            "fault_active", "metadata",
        ):
            assert col in text, f"INSERT missing column {col}"


class TestMicrobatchWriter:
    def test_processing_time_ten_seconds(self) -> None:
        text = _ts()
        assert 'DEFAULT_MICROBATCH_INTERVAL = "10 seconds"' in text
        assert '.trigger(processingTime=' in text

    def test_writes_to_delta_table(self) -> None:
        text = _ts()
        assert '.format("delta")' in text
        assert ".toTable(DELTA_TABLE)" in text


class TestBufferedDeltaMetricsListener:
    def test_buffers_via_queue_and_daemon_thread(self) -> None:
        block = _metrics_listener_block()
        assert "queue" in block.lower()
        assert "daemon=True" in block
        # callback enqueues only — no Spark work.
        assert "_metrics_q.put" in block

    def test_flushes_via_delta_rs_append(self) -> None:
        block = _metrics_listener_block()
        assert "write_deltalake" in block
        assert 'mode="append"' in block

    def test_never_uses_saveastable(self) -> None:
        # saveAsTable from the listener launches a contending Spark job (§1.11);
        # the actual call form must never appear (the comment naming it is fine).
        block = _metrics_listener_block()
        assert ".saveAsTable(" not in block

    def test_regenerates_temp_creds_per_batch_with_region_and_rename(self) -> None:
        block = _metrics_listener_block()
        assert "generate_temporary_table_credentials" in block
        assert 'AWS_REGION": "us-east-1"' in block
        assert 'AWS_S3_ALLOW_UNSAFE_RENAME": "true"' in block

    def test_progress_stored_as_string(self) -> None:
        block = _metrics_listener_block()
        # progress kept as STRING (delta-rs can't VARIANT); parse_json at read.
        assert '"progress": progress_json' in block


class TestPipInstall:
    def test_does_not_upgrade_boto3(self) -> None:
        text = _ts()
        # No actual %pip install command may upgrade boto3 (breaks the kernel).
        pip_cmds = [ln for ln in text.splitlines() if ln.lstrip().startswith("%pip install")]
        assert pip_cmds, "expected at least one %pip install command"
        for cmd in pip_cmds:
            assert "boto3" not in cmd
        assert "databricks-sdk==0.120.0" in text
        assert "deltalake" in text
