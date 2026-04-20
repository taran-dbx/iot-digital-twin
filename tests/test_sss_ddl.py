"""Tests for the Track-B stateful streaming DDL constants in models.sql_ddl."""

from __future__ import annotations

import pytest

from models import sql_ddl


class TestSlidingAggregatesDdl:
    """Verify SLIDING_AGGREGATES_DDL is well-formed."""

    def test_constant_is_string(self) -> None:
        assert isinstance(sql_ddl.SLIDING_AGGREGATES_DDL, str)

    def test_constant_is_non_empty(self) -> None:
        assert len(sql_ddl.SLIDING_AGGREGATES_DDL) > 0

    def test_creates_sliding_aggregates_table(self) -> None:
        assert "sliding_aggregates" in sql_ddl.SLIDING_AGGREGATES_DDL

    def test_contains_if_not_exists(self) -> None:
        assert "IF NOT EXISTS" in sql_ddl.SLIDING_AGGREGATES_DDL

    def test_has_pipeline_id_column(self) -> None:
        assert "pipeline_id" in sql_ddl.SLIDING_AGGREGATES_DDL

    def test_has_sensor_id_column(self) -> None:
        assert "sensor_id" in sql_ddl.SLIDING_AGGREGATES_DDL

    def test_has_sensor_type_column(self) -> None:
        assert "sensor_type" in sql_ddl.SLIDING_AGGREGATES_DDL

    def test_has_window_start_column(self) -> None:
        assert "window_start" in sql_ddl.SLIDING_AGGREGATES_DDL

    def test_has_window_end_column(self) -> None:
        assert "window_end" in sql_ddl.SLIDING_AGGREGATES_DDL

    def test_has_slide_start_column(self) -> None:
        assert "slide_start" in sql_ddl.SLIDING_AGGREGATES_DDL

    def test_has_aggregate_columns(self) -> None:
        ddl = sql_ddl.SLIDING_AGGREGATES_DDL
        assert "avg_value" in ddl
        assert "min_value" in ddl
        assert "max_value" in ddl
        assert "count" in ddl

    def test_has_index_on_pipeline_time(self) -> None:
        assert "idx_sliding_agg_pipeline_time" in sql_ddl.SLIDING_AGGREGATES_DDL


class TestSessionEventsDdl:
    """Verify SESSION_EVENTS_DDL is well-formed."""

    def test_constant_is_string(self) -> None:
        assert isinstance(sql_ddl.SESSION_EVENTS_DDL, str)

    def test_constant_is_non_empty(self) -> None:
        assert len(sql_ddl.SESSION_EVENTS_DDL) > 0

    def test_creates_session_events_table(self) -> None:
        assert "session_events" in sql_ddl.SESSION_EVENTS_DDL

    def test_contains_if_not_exists(self) -> None:
        assert "IF NOT EXISTS" in sql_ddl.SESSION_EVENTS_DDL

    def test_has_pipeline_id_column(self) -> None:
        assert "pipeline_id" in sql_ddl.SESSION_EVENTS_DDL

    def test_has_session_id_column(self) -> None:
        assert "session_id" in sql_ddl.SESSION_EVENTS_DDL

    def test_has_session_start_and_end_columns(self) -> None:
        ddl = sql_ddl.SESSION_EVENTS_DDL
        assert "session_start" in ddl
        assert "session_end" in ddl

    def test_has_duration_column(self) -> None:
        assert "duration_sec" in sql_ddl.SESSION_EVENTS_DDL

    def test_has_reading_count_column(self) -> None:
        assert "reading_count" in sql_ddl.SESSION_EVENTS_DDL

    def test_has_aggregate_columns(self) -> None:
        ddl = sql_ddl.SESSION_EVENTS_DDL
        assert "avg_value" in ddl
        assert "min_value" in ddl
        assert "max_value" in ddl

    def test_has_index_on_pipeline_time(self) -> None:
        assert "idx_session_events_pipeline_time" in sql_ddl.SESSION_EVENTS_DDL


class TestStateSnapshotsDdl:
    """Verify STATE_SNAPSHOTS_DDL is well-formed."""

    def test_constant_is_string(self) -> None:
        assert isinstance(sql_ddl.STATE_SNAPSHOTS_DDL, str)

    def test_constant_is_non_empty(self) -> None:
        assert len(sql_ddl.STATE_SNAPSHOTS_DDL) > 0

    def test_creates_state_snapshots_table(self) -> None:
        assert "state_snapshots" in sql_ddl.STATE_SNAPSHOTS_DDL

    def test_contains_if_not_exists(self) -> None:
        assert "IF NOT EXISTS" in sql_ddl.STATE_SNAPSHOTS_DDL

    def test_has_primary_key_on_pipeline_sensor(self) -> None:
        assert "PRIMARY KEY (pipeline_id, sensor_id)" in sql_ddl.STATE_SNAPSHOTS_DDL

    def test_has_ewma_column(self) -> None:
        assert "running_avg" in sql_ddl.STATE_SNAPSHOTS_DDL

    def test_has_running_min_max_columns(self) -> None:
        ddl = sql_ddl.STATE_SNAPSHOTS_DDL
        assert "running_min" in ddl
        assert "running_max" in ddl

    def test_has_threshold_breached_column(self) -> None:
        assert "threshold_breached" in sql_ddl.STATE_SNAPSHOTS_DDL

    def test_has_last_and_prev_value_columns(self) -> None:
        ddl = sql_ddl.STATE_SNAPSHOTS_DDL
        assert "last_value" in ddl
        assert "prev_value" in ddl

    def test_has_readings_since_reset_column(self) -> None:
        assert "readings_since_reset" in sql_ddl.STATE_SNAPSHOTS_DDL
