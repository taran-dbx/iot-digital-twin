"""Tests for ``services/zerobus_validation.py`` (a REAL Zerobus producer for
the pipeline validation flow, replacing the no-op stubs in
``services/validation_snippets.py``/``services/validation_steps.py``).

``zerobus_produce_snippet`` is a pure string builder (content tests, mirroring
``tests/test_validation_snippets.py``'s style) — no cluster / network needed.
``auth_probe`` runs on the app driver; its network calls are mocked so this
file has no live dependencies.
"""

from __future__ import annotations

import ast
from unittest.mock import MagicMock, patch

import pytest

from services import zerobus_validation as zbv


# ---------------------------------------------------------------------------
# zerobus_produce_snippet — content / syntax tests
# ---------------------------------------------------------------------------
def _snippet(**overrides) -> str:
    kwargs = dict(
        industry="pharmaceutical",
        catalog="iot_pharmaceutical",
        table_fqn="iot_pharmaceutical.zerobus.iot_sensor_data",
        workspace_url="https://fe-vm-shared-interop.cloud.databricks.com",
        server_endpoint="332040261549026.zerobus.us-east-1.cloud.databricks.com",
        num_records=25,
        source="validation",
    )
    kwargs.update(overrides)
    return zbv.zerobus_produce_snippet(**kwargs)


def test_snippet_is_valid_python_syntax():
    ast.parse(_snippet())


def test_snippet_uses_zerobus_sdk_not_a_stub():
    code = _snippet()
    assert "ZerobusSdk" in code
    assert "create_stream(" in code
    assert "ingest_record(" in code
    assert "wait_for_ack(" in code
    assert "stream.close()" in code
    # Not the old no-op stub which only prints the intended config.
    assert "dbutils.secrets.get" in code


def test_snippet_carries_ok_fail_sentinels():
    code = _snippet()
    assert zbv.OK_SENTINEL in code
    assert zbv.FAIL_SENTINEL in code
    assert "VALIDATION_OK" in code
    assert "VALIDATION_FAIL" in code


def test_snippet_record_uses_delta_column_names_not_record_field_names():
    """The Delta table (models.sensor_schema.delta_table_ddl) uses column
    names — 'event_time', not the raw SensorRecord field 'timestamp' that
    zerobus_record_keys()/ZerobusSink emit. Sending the wrong key silently
    drops the column (see module docstring point 1)."""
    code = _snippet()
    for column in (
        "sensor_name",
        "industry",
        "value",
        "unit",
        "sensor_type",
        "event_time",
        "min_value",
        "max_value",
        "nominal_value",
        "fault_active",
        "metadata",
    ):
        assert f"'{column}'" in code, f"missing SSOT column {column!r} in generated record"
    # The raw record-field name for the timestamp must NOT appear as a dict key.
    assert "'timestamp':" not in code


def test_snippet_timestamp_is_epoch_micros_int_not_iso_string():
    """LIVE-VERIFIED: an ISO-8601 string fails ingestion with INVALID_ARGUMENT
    'invalid digit found in string' — the server expects a numeric epoch."""
    code = _snippet()
    assert "int(time.time() * 1_000_000)" in code
    assert "isoformat()" not in code


def test_snippet_workspace_url_gets_https_prefix_guard():
    code = _snippet(workspace_url="fe-vm-shared-interop.cloud.databricks.com")
    assert 'WORKSPACE_URL.startswith("http")' in code


def test_snippet_embeds_table_and_endpoint_and_num_records():
    code = _snippet(table_fqn="iot_energy.zerobus.iot_sensor_data",
                     server_endpoint="123.zerobus.us-west-2.cloud.databricks.com",
                     num_records=42)
    assert "iot_energy.zerobus.iot_sensor_data" in code
    assert "123.zerobus.us-west-2.cloud.databricks.com" in code
    assert "range(42)" in code


def test_snippet_uses_secret_scope_and_keys():
    code = _snippet()
    assert "'iot-secrets'" in code
    assert "'zerobus-client-id'" in code
    assert "'zerobus-client-secret'" in code
    code2 = _snippet()
    code2 = zbv.zerobus_produce_snippet(
        industry="pharmaceutical",
        catalog="iot_pharmaceutical",
        table_fqn="iot_pharmaceutical.zerobus.iot_sensor_data",
        workspace_url="https://fe-vm-shared-interop.cloud.databricks.com",
        server_endpoint="332040261549026.zerobus.us-east-1.cloud.databricks.com",
        secret_scope="custom-scope",
        client_id_key="custom-id-key",
        client_secret_key="custom-secret-key",
    )
    assert "'custom-scope'" in code2
    assert "'custom-id-key'" in code2
    assert "'custom-secret-key'" in code2


# ---------------------------------------------------------------------------
# auth_probe — driver-side reachability/credential probe (mocked network)
# ---------------------------------------------------------------------------
def test_auth_probe_empty_endpoint_fails_fast():
    ok, detail = zbv.auth_probe("https://x.cloud.databricks.com", "", "cat.sch.tbl")
    assert ok is False
    assert "error" in detail


def test_auth_probe_dns_failure():
    with patch("socket.gethostbyname", side_effect=OSError("nodename nor servname provided")):
        ok, detail = zbv.auth_probe(
            "https://x.cloud.databricks.com", "nonexistent.zerobus.example.com", "cat.sch.tbl"
        )
    assert ok is False
    assert detail["dns"] is False
    assert "dns_error" in detail


def test_auth_probe_tcp_failure():
    with patch("socket.gethostbyname", return_value="10.0.0.1"), \
         patch("socket.create_connection", side_effect=OSError("connection refused")):
        ok, detail = zbv.auth_probe(
            "https://x.cloud.databricks.com", "host.zerobus.example.com", "cat.sch.tbl"
        )
    assert ok is False
    assert detail["dns"] is True
    assert detail["tcp"] is False
    assert "tcp_error" in detail


def test_auth_probe_reachable_no_credentials_skips_token_check():
    with patch("socket.gethostbyname", return_value="10.0.0.1"), \
         patch("socket.create_connection", MagicMock()), \
         patch.dict("os.environ", {}, clear=True):
        ok, detail = zbv.auth_probe(
            "https://x.cloud.databricks.com", "123.zerobus.us-east-1.cloud.databricks.com",
            "cat.sch.tbl",
        )
    assert ok is True
    assert detail["tcp"] is True
    assert detail["token_ok"] is None
    assert "token_skipped" in detail


def test_auth_probe_token_exchange_success():
    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.json.return_value = {"access_token": "eyFakeToken"}
    with patch("socket.gethostbyname", return_value="10.0.0.1"), \
         patch("socket.create_connection", MagicMock()), \
         patch("requests.post", return_value=fake_resp) as mock_post:
        ok, detail = zbv.auth_probe(
            "https://x.cloud.databricks.com",
            "123.zerobus.us-east-1.cloud.databricks.com",
            "iot_pharmaceutical.zerobus.iot_sensor_data",
            client_id="cid",
            client_secret="csecret",
        )
    assert ok is True
    assert detail["token_ok"] is True
    kwargs = mock_post.call_args.kwargs
    assert kwargs["auth"] == ("cid", "csecret")
    assert "authorization_details" in kwargs["data"]
    assert "iot_pharmaceutical" in kwargs["data"]["authorization_details"]


def test_auth_probe_token_exchange_missing_grants_fails_clearly():
    """LIVE-VERIFIED failure mode: SP without USE_CATALOG/USE_SCHEMA/SELECT/
    MODIFY grants gets a 401 invalid_authorization_details."""
    fake_resp = MagicMock()
    fake_resp.status_code = 401
    fake_resp.text = '{"error":"invalid_authorization_details","error_description":"User is not authorized"}'
    fake_resp.json.return_value = {}
    with patch("socket.gethostbyname", return_value="10.0.0.1"), \
         patch("socket.create_connection", MagicMock()), \
         patch("requests.post", return_value=fake_resp):
        ok, detail = zbv.auth_probe(
            "https://x.cloud.databricks.com",
            "123.zerobus.us-east-1.cloud.databricks.com",
            "iot_pharmaceutical.zerobus.iot_sensor_data",
            client_id="cid",
            client_secret="csecret",
        )
    assert ok is False
    assert detail["token_ok"] is False
    assert "invalid_authorization_details" in detail["token_error"]


def test_auth_probe_bad_table_fqn_fails_before_network_call():
    with patch("socket.gethostbyname", return_value="10.0.0.1"), \
         patch("socket.create_connection", MagicMock()), \
         patch("requests.post") as mock_post:
        ok, detail = zbv.auth_probe(
            "https://x.cloud.databricks.com",
            "123.zerobus.us-east-1.cloud.databricks.com",
            "not_a_three_part_name",
            client_id="cid",
            client_secret="csecret",
        )
    assert ok is False
    assert "catalog.schema.table" in detail["token_error"]
    mock_post.assert_not_called()
