"""Content tests for the cluster-side validation snippets (spec §1.7–1.11).

The snippets run on the harness cluster where they can't be unit-executed, but
their *content* must carry the VERIFIED configs — so we assert those tokens are
present (service credential, public :9198 bootstrap, RTM read options, latest
offsets for test-data isolation, distinct validation checkpoint, https:// OIDC).
"""

from __future__ import annotations

from services import validation_snippets as snip


def test_connectivity_probe_checks_both_and_egress():
    code = snip.connectivity_probe("b-1:9198", "b-1:9098")
    assert "9198" in code and "9098" in code
    assert "checkip.amazonaws.com" in code  # egress IP diagnostic (§1.7)
    assert snip.OK_SENTINEL in code and snip.FAIL_SENTINEL in code


def test_topic_create_uses_msk_iam_oauth():
    code = snip.topic_create(
        "b-1:9198", [("iot.x.realtime", 3, 3)], "us-east-1", "iot-rtm-sc"
    )
    assert "OAUTHBEARER" in code
    assert "AbstractTokenProvider" in code           # kafka-python 3.x subclass
    assert "MSKAuthTokenProvider" in code
    assert "us-east-1" in code
    assert "iot.x.realtime" in code


def test_rtm_write_test_verified_read_config():
    code = snip.rtm_write_test(
        "iot.x.realtime", "b-1:9198", "iot-rtm-sc",
        pg_host="h", pg_database="databricks_postgres",
        table="sensor_readings", checkpoint_path="/Volumes/x/checkpoints/v/validation/realtime",
    )
    # Service credential handles MSK-IAM — no plaintext SASL on the Spark read.
    assert '"databricks.serviceCredential"' in code
    assert "iot-rtm-sc" in code
    assert "kafka.sasl" not in code
    # Test-data isolation: latest offsets + distinct validation checkpoint.
    assert '"latest"' in code
    assert "/validation/realtime" in code
    # Real-time trigger + update output mode.
    assert "realTime=" in code
    assert 'outputMode("update")' in code


def test_microbatch_write_test_processing_time_trigger():
    code = snip.microbatch_write_test(
        "iot.x.microbatch", "b-1:9198", "iot-rtm-sc",
        "iot_x.microbatch.sensor_data", "/Volumes/x/checkpoints/v/validation/microbatch",
    )
    assert 'processingTime="10 seconds"' in code
    assert '"latest"' in code
    assert "iot_x.microbatch.sensor_data" in code


def test_cdf_verify_polls_streaming_and_lsn():
    code = snip.cdf_verify()
    assert "wal2delta.tables" in code
    assert "STREAMING" in code
    assert "committed_lsn" in code


def test_zerobus_produce_https_prefix_gotcha():
    code = snip.zerobus_produce("myws.cloud.databricks.com", "ws.zerobus.x", "iot_x.zerobus.iot_sensor_data")
    assert "/oidc/v1/token" in code
    assert 'https://' in code  # https:// prefix is the known gotcha (§1.10)
