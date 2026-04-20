"""SQL DDL constants for Lakebase provisioning."""

from __future__ import annotations

# LakeTS — probe installed version.  Returns NULL/empty if the toolkit is absent.
LAKETS_VERSION_CHECK_SQL = (
    "SELECT version FROM lakets._version ORDER BY installed_at DESC LIMIT 1"
)

SENSOR_READINGS_DDL = """
CREATE TABLE IF NOT EXISTS {table_name} (
    id BIGSERIAL PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    sensor_name TEXT NOT NULL,
    industry TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    unit TEXT,
    sensor_type TEXT,
    fault_active BOOLEAN DEFAULT FALSE,
    source TEXT DEFAULT 'sss'
)
"""

MATVIEW_1M_DDL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_1m AS
SELECT
    date_trunc('minute', event_time) AS bucket,
    sensor_name, sensor_type, industry,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    COUNT(*) AS sample_count,
    BOOL_OR(fault_active) AS any_fault
FROM sensor_readings
GROUP BY bucket, sensor_name, sensor_type, industry
"""

MATVIEW_5M_DDL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_5m AS
SELECT
    date_trunc('hour', event_time)
      + INTERVAL '5 min' * FLOOR(EXTRACT(MINUTE FROM event_time) / 5) AS bucket,
    sensor_type, industry,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    COUNT(*) AS sample_count,
    BOOL_OR(fault_active) AS any_fault
FROM sensor_readings
GROUP BY bucket, sensor_type, industry
"""

MATVIEW_INDEX_DDL = [
    "CREATE UNIQUE INDEX IF NOT EXISTS sensor_1m_pk ON sensor_1m (bucket, sensor_name, sensor_type, industry)",
    "CREATE UNIQUE INDEX IF NOT EXISTS sensor_5m_pk ON sensor_5m (bucket, sensor_type, industry)",
]

SOURCE_BINDINGS_DDL = """
CREATE TABLE IF NOT EXISTS source_bindings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id UUID NOT NULL,
    source_type TEXT NOT NULL,
    source_key TEXT NOT NULL,
    partition_count INT,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_by TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    CONSTRAINT fk_pipeline FOREIGN KEY (pipeline_id)
        REFERENCES pipelines(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_active_source
    ON source_bindings (source_type, source_key)
    WHERE status = 'active';
"""

SLIDING_AGGREGATES_DDL = """
CREATE TABLE IF NOT EXISTS sliding_aggregates (
    pipeline_id UUID NOT NULL,
    sensor_id TEXT NOT NULL,
    sensor_type TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    slide_start TIMESTAMPTZ NOT NULL,
    avg_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    count INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sliding_agg_pipeline_time
    ON sliding_aggregates (pipeline_id, window_start DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_sliding_agg
    ON sliding_aggregates (pipeline_id, sensor_id, sensor_type, window_start, window_end, slide_start);
"""

SESSION_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS session_events (
    pipeline_id UUID NOT NULL,
    sensor_id TEXT NOT NULL,
    sensor_type TEXT NOT NULL,
    session_id TEXT NOT NULL,
    session_start TIMESTAMPTZ NOT NULL,
    session_end TIMESTAMPTZ NOT NULL,
    duration_sec DOUBLE PRECISION,
    reading_count INT,
    avg_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_session_events_pipeline_time
    ON session_events (pipeline_id, session_start DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_session_event
    ON session_events (pipeline_id, sensor_id, session_id);
"""

STATE_SNAPSHOTS_DDL = """
CREATE TABLE IF NOT EXISTS state_snapshots (
    pipeline_id UUID NOT NULL,
    sensor_id TEXT NOT NULL,
    sensor_type TEXT NOT NULL,
    running_avg DOUBLE PRECISION,
    running_min DOUBLE PRECISION,
    running_max DOUBLE PRECISION,
    readings_since_reset INT DEFAULT 0,
    last_value DOUBLE PRECISION,
    prev_value DOUBLE PRECISION,
    threshold_breached BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_id, sensor_id)
);
"""

# Lakebase role constants
LAKEBASE_ROLE_NAME = "iot_app_user"
LAKEBASE_ROLE_ID = "iot-app-user"
