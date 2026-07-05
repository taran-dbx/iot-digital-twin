"""Tests for the streaming-metrics external Delta SSOT (spec §1.11)."""

from __future__ import annotations

from models import metrics_schema as ms


class TestMetricsColumns:
    def test_progress_is_string_not_variant(self) -> None:
        by_col = {s.column: s.delta_type for s in ms.METRICS_FIELD_MAP}
        # delta-rs cannot write VARIANT → progress must be STRING (§1.11).
        assert by_col["progress"] == "STRING"

    def test_has_all_typed_latency_percentiles(self) -> None:
        cols = set(ms.metrics_columns())
        for c in (
            "batch_id", "timestamp", "num_input_rows", "input_rps", "processed_rps",
            "trigger_ms", "e2e_p50", "e2e_p99", "processing_p50", "processing_p99",
            "queuing_p50", "queuing_p99", "progress",
        ):
            assert c in cols, f"missing metrics column {c}"

    def test_mode_tagging_columns_present(self) -> None:
        cols = set(ms.metrics_columns())
        assert {"query_id", "query_name", "run_id"} <= cols


class TestMetricsDdl:
    def test_fqn_and_location(self) -> None:
        assert ms.metrics_table_fqn("iot_spiketest") == (
            "iot_spiketest.metrics.streaming_metrics"
        )
        loc = ms.metrics_table_location("iot_spiketest", "s3://taran-shared-interop")
        assert loc == (
            "s3://taran-shared-interop/iot-digital-twin/iot_spiketest/metrics/"
            "streaming_metrics/"
        )

    def test_ddl_is_external_with_location(self) -> None:
        ddl = ms.metrics_external_table_ddl("iot_spiketest", "s3://taran-shared-interop")
        assert "CREATE TABLE IF NOT EXISTS iot_spiketest.metrics.streaming_metrics" in ddl
        assert "USING DELTA" in ddl
        assert "LOCATION 's3://taran-shared-interop/iot-digital-twin/" in ddl
        assert "progress STRING" in ddl

    def test_location_trims_trailing_slash_on_base(self) -> None:
        loc = ms.metrics_table_location("iot_x", "s3://bucket/")
        assert loc == "s3://bucket/iot-digital-twin/iot_x/metrics/streaming_metrics/"
