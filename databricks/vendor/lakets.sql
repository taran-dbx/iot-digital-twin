-- =============================================================================
-- LakeTS v0.1.2 — Time Series Toolkit for Databricks Lakebase
--
-- Version:    0.1.2
-- Built:      2026-04-13T17:36:16Z
-- Git SHA:    7410c9c
--
-- Install:    psql -f lakets.sql
--         or: conn.cursor().execute(open('lakets.sql').read())
--
-- This is a concatenated build. Do not edit — regenerate with: make build
-- =============================================================================


-- ######################################################################
-- ## MODULE: 00_version.sql
-- ######################################################################

-- =============================================================================
-- LakeTS Version Tracking
-- Manages install/upgrade lifecycle. Must be the first module executed.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS lakets;

CREATE TABLE IF NOT EXISTS lakets._version (
    version       TEXT        NOT NULL,
    installed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    modules       TEXT[]      NOT NULL DEFAULT '{}'
);

-- Version guard: prevents downgrades, allows reinstall and upgrade.
DO $$
DECLARE
    v_installed       TEXT;
    v_incoming        TEXT := coalesce(nullif('0.1.2', '__LAKE' || 'TS_VERSION__'), '0.1.2');
    v_installed_parts INT[];
    v_incoming_parts  INT[];
BEGIN
    SELECT version INTO v_installed
    FROM lakets._version
    ORDER BY installed_at DESC
    LIMIT 1;

    -- Clean up any rows with unresolved build placeholders from prior bad installs
    DELETE FROM lakets._version WHERE version !~ '^\d+\.\d+\.\d+';

    SELECT version INTO v_installed
    FROM lakets._version
    ORDER BY installed_at DESC
    LIMIT 1;

    IF v_installed IS NULL THEN
        INSERT INTO lakets._version (version, modules)
        VALUES (v_incoming, '{}');
        RAISE NOTICE 'LakeTS %: fresh install', v_incoming;
    ELSE
        v_installed_parts := string_to_array(v_installed, '.')::INT[];
        v_incoming_parts  := string_to_array(v_incoming, '.')::INT[];

        IF v_incoming_parts < v_installed_parts THEN
            RAISE EXCEPTION 'LakeTS downgrade blocked: installed=%, incoming=%',
                v_installed, v_incoming;
        END IF;

        IF v_incoming = v_installed THEN
            UPDATE lakets._version
            SET installed_at = now()
            WHERE version = v_incoming;
            RAISE NOTICE 'LakeTS %: reinstalling (idempotent)', v_incoming;
        ELSE
            INSERT INTO lakets._version (version, modules)
            VALUES (v_incoming, '{}');
            RAISE NOTICE 'LakeTS %: upgrading from %', v_incoming, v_installed;
        END IF;
    END IF;
END $$;


-- ######################################################################
-- ## MODULE: 01_schema.sql
-- ######################################################################

-- =============================================================================
-- LakeTS Core Schema
-- Metadata tables for hypertable management, chunk tracking, and policies.
-- Requires: 00_version.sql (creates the lakets schema)
--
-- Multi-tenant install: pass -v lakets_schema=<name> to psql to install
-- metadata under a custom schema instead of the default 'lakets'.
-- Example: psql -v lakets_schema=lakets_dev -f sql/99_install.sql
-- If the variable is not set, psql substitutes it as the literal string
-- ':lakets_schema' which is intentionally not used -- all objects use the
-- 'lakets' schema by default, hardcoded for compatibility with v0.1.x.
-- Full parameterization is planned for v0.3.0 (see docs/design/multi-tenant-namespacing.md).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- ChronoTable Registry: tracks all tables converted to time-partitioned tables
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakets._chronotable_registry (
    id SERIAL PRIMARY KEY,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    time_column TEXT NOT NULL,
    chunk_interval INTERVAL NOT NULL DEFAULT '7 days',
    space_column TEXT,
    space_partitions INT DEFAULT 1,
    compression_enabled BOOLEAN DEFAULT FALSE,
    retention_interval INTERVAL,
    shadow_table_name TEXT,
    sync_enabled BOOLEAN DEFAULT FALSE,
    last_synced_lsn BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(schema_name, table_name)
);

-- ---------------------------------------------------------------------------
-- Chunk Metadata: tracks individual partitions (chunks) of each hypertable
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakets._chunk_metadata (
    id SERIAL PRIMARY KEY,
    chronotable_id INT NOT NULL REFERENCES lakets._chronotable_registry(id) ON DELETE CASCADE,
    chunk_name TEXT NOT NULL,
    range_start TIMESTAMPTZ NOT NULL,
    range_end TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    row_count BIGINT,
    size_bytes BIGINT,
    compressed_at TIMESTAMPTZ,
    tiered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT valid_status CHECK (status IN ('active', 'compressed', 'tiered', 'dropped'))
);

CREATE INDEX IF NOT EXISTS idx_chunk_metadata_hypertable
    ON lakets._chunk_metadata(chronotable_id);

CREATE INDEX IF NOT EXISTS idx_chunk_metadata_range
    ON lakets._chunk_metadata(range_start, range_end);

-- ---------------------------------------------------------------------------
-- Policy Registry: tracks compression, retention, and tiering policies
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakets._policy_registry (
    id SERIAL PRIMARY KEY,
    chronotable_id INT NOT NULL REFERENCES lakets._chronotable_registry(id) ON DELETE CASCADE,
    policy_type TEXT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}',
    enabled BOOLEAN DEFAULT TRUE,
    last_run_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT valid_policy_type CHECK (policy_type IN ('compression', 'retention', 'tiered_retention'))
);

-- ---------------------------------------------------------------------------
-- RollUp Registry: tracks all RollUps and their refresh state
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakets._rollup_registry (
    id                    SERIAL PRIMARY KEY,
    name                  TEXT UNIQUE NOT NULL,
    source_chronotable_id INT NOT NULL REFERENCES lakets._chronotable_registry(id) ON DELETE CASCADE,
    rollup_table          TEXT NOT NULL,
    realtime_view         TEXT,
    bucket_interval       INTERVAL NOT NULL,
    refresh_mode          TEXT NOT NULL DEFAULT 'incremental',
    refresh_lag           INTERVAL DEFAULT '1 hour',
    watermark             TIMESTAMPTZ,
    query_text            TEXT NOT NULL,
    last_refreshed_at     TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT valid_refresh_mode CHECK (refresh_mode IN ('full', 'incremental'))
);

-- ---------------------------------------------------------------------------
-- RollUp Invalidation Log: tracks dirty time buckets needing re-aggregation
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakets._rollup_invalidation_log (
    id             SERIAL PRIMARY KEY,
    rollup_id      INT NOT NULL REFERENCES lakets._rollup_registry(id) ON DELETE CASCADE,
    bucket_start   TIMESTAMPTZ NOT NULL,
    tier           TEXT NOT NULL DEFAULT 'hot',
    invalidated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_rollup_invalidation UNIQUE (rollup_id, bucket_start),
    CONSTRAINT valid_tier CHECK (tier IN ('hot', 'cold'))
);

CREATE INDEX IF NOT EXISTS idx_rollup_invalidation_rollup_id
    ON lakets._rollup_invalidation_log(rollup_id, tier, bucket_start);

-- ---------------------------------------------------------------------------
-- LVC Registry: tracks Last Value Cache configs per ChronoTable
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakets._lvc_registry (
    id              SERIAL PRIMARY KEY,
    chronotable_id  INT NOT NULL REFERENCES lakets._chronotable_registry(id) ON DELETE CASCADE,
    cache_table_name TEXT NOT NULL,
    key_columns     TEXT[] NOT NULL,
    value_columns   TEXT[] NOT NULL DEFAULT '{}',
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chronotable_id)
);

-- ---------------------------------------------------------------------------
-- Downsample Registry: tracks multi-resolution pipeline definitions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakets._downsample_registry (
    id              SERIAL PRIMARY KEY,
    name            TEXT UNIQUE NOT NULL,
    source_table    TEXT NOT NULL,
    source_schema   TEXT NOT NULL DEFAULT 'public',
    intervals       INTERVAL[] NOT NULL,
    retention       INTERVAL[] NOT NULL,
    agg_expressions TEXT[] NOT NULL,
    group_by        TEXT[],
    delta_catalog   TEXT NOT NULL DEFAULT 'main',
    delta_schema    TEXT NOT NULL DEFAULT 'lakets_rollups',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Additional indexes and constraints for performance and correctness
-- ---------------------------------------------------------------------------

-- Unique constraint for _ensure_partitions ON CONFLICT target
DO $$ BEGIN
    ALTER TABLE lakets._chunk_metadata
        ADD CONSTRAINT uq_chunk_metadata_ct_range UNIQUE (chronotable_id, range_start);
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

-- Index for _touch_chunk_metadata trigger lookup by partition name
CREATE UNIQUE INDEX IF NOT EXISTS idx_chunk_metadata_chunk_name
    ON lakets._chunk_metadata(chunk_name);

-- FK index on _policy_registry (scanned by compression/retention jobs)
CREATE INDEX IF NOT EXISTS idx_policy_registry_ct_type
    ON lakets._policy_registry(chronotable_id, policy_type) WHERE enabled = TRUE;

-- FK index on _rollup_registry (scanned on every data write via invalidation trigger)
CREATE INDEX IF NOT EXISTS idx_rollup_registry_source_ct
    ON lakets._rollup_registry(source_chronotable_id, refresh_mode)
    WHERE refresh_mode = 'incremental';

-- ---------------------------------------------------------------------------
-- _resolve_partition_parent: Resolves a partition's parent table name.
-- Shared helper used by all LakeTS triggers that fire on partitions.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._resolve_partition_parent(p_schema TEXT, p_table TEXT)
RETURNS TEXT
LANGUAGE sql STABLE
AS $$
    SELECT p.relname
    FROM pg_inherits i
    JOIN pg_class ch ON i.inhrelid = ch.oid
    JOIN pg_class p ON i.inhparent = p.oid
    JOIN pg_namespace n ON ch.relnamespace = n.oid
    WHERE n.nspname = p_schema AND ch.relname = p_table
    LIMIT 1;
$$;


-- ######################################################################
-- ## MODULE: 02_chronotable.sql
-- ######################################################################

-- =============================================================================
-- LakeTS ChronoTable Manager
-- Functions for creating and managing time-partitioned tables.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- _ensure_partitions: Pre-creates partitions for a hypertable.
-- Supports both relative (past/future count) and explicit range.
-- Idempotent — skips partitions that already exist.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._ensure_partitions(
    p_chronotable_id INT,
    p_past_count INT DEFAULT 1,
    p_future_count INT DEFAULT 3,
    p_range_start TIMESTAMPTZ DEFAULT NULL,
    p_range_end TIMESTAMPTZ DEFAULT NULL
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_start TIMESTAMPTZ;
    v_end TIMESTAMPTZ;
    v_partitions_created INT := 0;
    v_now TIMESTAMPTZ := now();
    v_interval INTERVAL;
    v_schema TEXT;
    v_table TEXT;
    v_rs TIMESTAMPTZ;
    v_re TIMESTAMPTZ;
BEGIN
    SELECT schema_name, table_name, chunk_interval
    INTO v_schema, v_table, v_interval
    FROM lakets._chronotable_registry
    WHERE id = p_chronotable_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Hypertable with id % not found', p_chronotable_id;
    END IF;

    -- Use explicit range if provided, otherwise calculate from now
    IF p_range_start IS NOT NULL THEN
        v_rs := date_bin(v_interval, p_range_start, '2000-01-01'::timestamptz);
    ELSE
        v_rs := date_bin(v_interval, v_now, '2000-01-01'::timestamptz)
                - (v_interval * p_past_count);
    END IF;

    IF p_range_end IS NOT NULL THEN
        v_re := date_bin(v_interval, p_range_end, '2000-01-01'::timestamptz) + v_interval;
    ELSE
        v_re := date_bin(v_interval, v_now, '2000-01-01'::timestamptz)
                + (v_interval * (p_future_count + 1));
    END IF;

    v_start := v_rs;
    WHILE v_start < v_re LOOP
        v_end := v_start + v_interval;

        IF NOT EXISTS (
            SELECT 1 FROM lakets._chunk_metadata
            WHERE chronotable_id = p_chronotable_id
              AND range_start = v_start AND range_end = v_end
              AND status != 'dropped'
        ) THEN
            BEGIN
                EXECUTE format(
                    'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
                    v_schema,
                    v_table || '_' || to_char(v_start, 'YYYYMMDD_HH24MISS'),
                    v_schema, v_table, v_start, v_end
                );
                INSERT INTO lakets._chunk_metadata
                    (chronotable_id, chunk_name, range_start, range_end, status)
                VALUES (p_chronotable_id,
                        v_schema || '.' || v_table || '_' || to_char(v_start, 'YYYYMMDD_HH24MISS'),
                        v_start, v_end, 'active')
                ON CONFLICT (chronotable_id, range_start) DO NOTHING;
                v_partitions_created := v_partitions_created + 1;
            EXCEPTION WHEN duplicate_table THEN
                NULL;
            END;
        END IF;
        v_start := v_end;
    END LOOP;

    RETURN v_partitions_created;
END;
$$;

-- ---------------------------------------------------------------------------
-- create_hypertable: Converts a regular table to a time-partitioned table.
-- Scans existing data to create partitions covering the full range.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.create_hypertable(
    p_table_name TEXT,
    p_time_column TEXT,
    p_chunk_interval INTERVAL DEFAULT '7 days',
    p_schema_name TEXT DEFAULT 'public',
    p_if_not_exists BOOLEAN DEFAULT FALSE
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_chronotable_id INT;
    v_col_type TEXT;
    v_is_partitioned BOOLEAN;
    v_tmp_table TEXT;
    v_col_rec RECORD;
    v_col_defs TEXT := '';
    v_min_time TIMESTAMPTZ;
    v_max_time TIMESTAMPTZ;
BEGIN
    SELECT id INTO v_chronotable_id
    FROM lakets._chronotable_registry
    WHERE schema_name = p_schema_name AND table_name = p_table_name;

    IF FOUND THEN
        IF p_if_not_exists THEN RETURN v_chronotable_id; END IF;
        RAISE EXCEPTION 'Table %.% is already a ChronoTable (id=%)',
            p_schema_name, p_table_name, v_chronotable_id;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = p_schema_name AND table_name = p_table_name
    ) THEN
        RAISE EXCEPTION 'Table %.% does not exist', p_schema_name, p_table_name;
    END IF;

    SELECT data_type INTO v_col_type
    FROM information_schema.columns
    WHERE table_schema = p_schema_name
      AND table_name = p_table_name
      AND column_name = p_time_column;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Column % does not exist in %.%',
            p_time_column, p_schema_name, p_table_name;
    END IF;

    IF v_col_type NOT IN (
        'timestamp with time zone', 'timestamp without time zone', 'date'
    ) THEN
        RAISE EXCEPTION 'Column % has type %, expected timestamp/timestamptz/date',
            p_time_column, v_col_type;
    END IF;

    SELECT (relkind = 'p') INTO v_is_partitioned
    FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = p_schema_name AND c.relname = p_table_name;

    IF v_is_partitioned THEN
        RAISE EXCEPTION 'Table %.% is already partitioned', p_schema_name, p_table_name;
    END IF;

    -- Detect data range for partition creation
    EXECUTE format('SELECT min(%I), max(%I) FROM %I.%I',
        p_time_column, p_time_column, p_schema_name, p_table_name)
    INTO v_min_time, v_max_time;

    -- Build column definitions
    FOR v_col_rec IN
        SELECT column_name, udt_name, is_nullable, column_default, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = p_schema_name AND table_name = p_table_name
        ORDER BY ordinal_position
    LOOP
        IF v_col_defs != '' THEN v_col_defs := v_col_defs || ', '; END IF;
        v_col_defs := v_col_defs || format('%I %s', v_col_rec.column_name, v_col_rec.udt_name);
        IF v_col_rec.character_maximum_length IS NOT NULL THEN
            v_col_defs := v_col_defs || format('(%s)', v_col_rec.character_maximum_length);
        END IF;
        IF v_col_rec.is_nullable = 'NO' THEN v_col_defs := v_col_defs || ' NOT NULL'; END IF;
        IF v_col_rec.column_default IS NOT NULL THEN
            -- Guard: reject defaults containing SQL injection patterns
            IF v_col_rec.column_default ~* '(;|--|/\*)' THEN
                RAISE EXCEPTION 'Unsafe column default detected on %: %',
                    v_col_rec.column_name, v_col_rec.column_default;
            END IF;
            v_col_defs := v_col_defs || format(' DEFAULT %s', v_col_rec.column_default);
        END IF;
    END LOOP;

    -- Rename original
    v_tmp_table := p_table_name || '_lakets_orig';
    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I', p_schema_name, p_table_name, v_tmp_table);

    -- Create partitioned table
    EXECUTE format('CREATE TABLE %I.%I (%s) PARTITION BY RANGE (%I)',
        p_schema_name, p_table_name, v_col_defs, p_time_column);

    -- Register
    INSERT INTO lakets._chronotable_registry (schema_name, table_name, time_column, chunk_interval)
    VALUES (p_schema_name, p_table_name, p_time_column, p_chunk_interval)
    RETURNING id INTO v_chronotable_id;

    -- Create partitions covering existing data + future
    IF v_min_time IS NOT NULL THEN
        PERFORM lakets._ensure_partitions(
            p_chronotable_id := v_chronotable_id,
            p_range_start := v_min_time,
            p_range_end := v_max_time + '1 day'::interval
        );
    END IF;
    PERFORM lakets._ensure_partitions(p_chronotable_id := v_chronotable_id);

    -- Copy data
    EXECUTE format('INSERT INTO %I.%I SELECT * FROM %I.%I',
        p_schema_name, p_table_name, p_schema_name, v_tmp_table);

    -- Index on time column
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I (%I DESC)',
        'idx_' || p_table_name || '_' || p_time_column,
        p_schema_name, p_table_name, p_time_column);

    -- Drop original
    EXECUTE format('DROP TABLE %I.%I', p_schema_name, v_tmp_table);

    RETURN v_chronotable_id;
END;
$$;

-- ---------------------------------------------------------------------------
-- set_chunk_interval: Changes the chunk interval for future partitions.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.set_chunk_interval(
    p_table_name TEXT,
    p_chunk_interval INTERVAL,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE lakets._chronotable_registry
    SET chunk_interval = p_chunk_interval
    WHERE schema_name = p_schema_name AND table_name = p_table_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table %.% is not a registered ChronoTable',
            p_schema_name, p_table_name;
    END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- show_chunks: Lists partitions with metadata for a hypertable.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.show_chunks(
    p_table_name TEXT,
    p_older_than INTERVAL DEFAULT NULL,
    p_newer_than INTERVAL DEFAULT NULL,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS TABLE (
    chunk_name TEXT,
    range_start TIMESTAMPTZ,
    range_end TIMESTAMPTZ,
    status TEXT,
    row_count BIGINT,
    size_bytes BIGINT,
    created_at TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_chronotable_id INT;
BEGIN
    SELECT id INTO v_chronotable_id
    FROM lakets._chronotable_registry
    WHERE schema_name = p_schema_name AND table_name = p_table_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table %.% is not a registered ChronoTable',
            p_schema_name, p_table_name;
    END IF;

    RETURN QUERY
    SELECT cm.chunk_name, cm.range_start, cm.range_end,
           cm.status, cm.row_count, cm.size_bytes, cm.created_at
    FROM lakets._chunk_metadata cm
    WHERE cm.chronotable_id = v_chronotable_id
      AND cm.status != 'dropped'
      AND (p_older_than IS NULL OR cm.range_end < (now() - p_older_than))
      AND (p_newer_than IS NULL OR cm.range_start > (now() - p_newer_than))
    ORDER BY cm.range_start;
END;
$$;

-- ---------------------------------------------------------------------------
-- drop_chunks: Drops partitions older than a given interval.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.drop_chunks(
    p_table_name TEXT,
    p_older_than INTERVAL,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_chronotable_id INT;
    v_chunk RECORD;
    v_dropped INT := 0;
    v_cutoff TIMESTAMPTZ;
    v_parts TEXT[];
BEGIN
    SELECT id INTO v_chronotable_id
    FROM lakets._chronotable_registry
    WHERE schema_name = p_schema_name AND table_name = p_table_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table %.% is not a registered ChronoTable',
            p_schema_name, p_table_name;
    END IF;

    v_cutoff := now() - p_older_than;

    FOR v_chunk IN
        SELECT cm.id, cm.chunk_name
        FROM lakets._chunk_metadata cm
        WHERE cm.chronotable_id = v_chronotable_id
          AND cm.status = 'active'
          AND cm.range_end <= v_cutoff
    LOOP
        v_parts := string_to_array(v_chunk.chunk_name, '.');
        BEGIN
            EXECUTE format('DROP TABLE IF EXISTS %I.%I', v_parts[1], v_parts[2]);
            UPDATE lakets._chunk_metadata SET status = 'dropped' WHERE id = v_chunk.id;
            v_dropped := v_dropped + 1;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Failed to drop chunk %: %', v_chunk.chunk_name, SQLERRM;
        END;
    END LOOP;

    RETURN v_dropped;
END;
$$;

-- ---------------------------------------------------------------------------
-- create_chronotable: V2 alias for create_hypertable.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.create_chronotable(
    p_table_name TEXT,
    p_time_column TEXT,
    p_chunk_interval INTERVAL DEFAULT '7 days',
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS INT
LANGUAGE sql
AS $$
    SELECT lakets.create_hypertable(p_table_name, p_time_column, p_chunk_interval, p_schema_name);
$$;


-- ######################################################################
-- ## MODULE: 03_timeseries_functions.sql
-- ######################################################################

-- =============================================================================
-- LakeTS Time Series Functions
-- Time series analytical functions: time_bucket, first, last,
-- time_bucket_gapfill, locf, interpolate, delta, rate, histogram.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- time_bucket: Truncates a timestamp to the nearest bucket boundary.
-- For sub-month intervals, delegates to date_bin.
-- For month/year intervals, uses date_trunc + interval math.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.time_bucket(
    p_interval INTERVAL,
    p_timestamp TIMESTAMPTZ,
    p_origin TIMESTAMPTZ DEFAULT '2000-01-01 00:00:00+00'::timestamptz
)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_months INT;
BEGIN
    -- Extract months component (for month/year intervals)
    v_months := EXTRACT(YEAR FROM p_interval)::INT * 12
                + EXTRACT(MONTH FROM p_interval)::INT;

    IF v_months > 0 THEN
        -- Month-based bucketing: truncate to month, then align to interval
        DECLARE
            v_origin_months INT;
            v_ts_months INT;
            v_bucket_months INT;
        BEGIN
            v_origin_months := EXTRACT(YEAR FROM p_origin)::INT * 12
                               + EXTRACT(MONTH FROM p_origin)::INT - 1;
            v_ts_months := EXTRACT(YEAR FROM p_timestamp)::INT * 12
                           + EXTRACT(MONTH FROM p_timestamp)::INT - 1;
            v_bucket_months := v_origin_months
                               + ((v_ts_months - v_origin_months) / v_months) * v_months;
            RETURN make_timestamptz(
                v_bucket_months / 12,
                (v_bucket_months % 12) + 1,
                1, 0, 0, 0
            );
        END;
    ELSE
        -- Sub-month intervals: use native date_bin
        RETURN date_bin(p_interval, p_timestamp, p_origin);
    END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- first: Aggregate that returns the value associated with the earliest time.
-- ---------------------------------------------------------------------------

-- State type for first/last aggregates (idempotent: drop + recreate)
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid
               WHERE n.nspname = 'lakets' AND t.typname = '_first_last_state') THEN
        -- Drop dependents first, they'll be recreated below
        DROP AGGREGATE IF EXISTS lakets.first(DOUBLE PRECISION, TIMESTAMPTZ);
        DROP AGGREGATE IF EXISTS lakets.last(DOUBLE PRECISION, TIMESTAMPTZ);
        DROP FUNCTION IF EXISTS lakets._first_ffunc(lakets._first_last_state);
        DROP FUNCTION IF EXISTS lakets._last_ffunc(lakets._first_last_state);
        DROP FUNCTION IF EXISTS lakets._first_sfunc(lakets._first_last_state, DOUBLE PRECISION, TIMESTAMPTZ);
        DROP FUNCTION IF EXISTS lakets._last_sfunc(lakets._first_last_state, DOUBLE PRECISION, TIMESTAMPTZ);
        DROP TYPE lakets._first_last_state;
    END IF;
END $$;
CREATE TYPE lakets._first_last_state AS (
    value DOUBLE PRECISION,
    ts TIMESTAMPTZ
);

-- State transition: keep the row with the earliest timestamp
CREATE OR REPLACE FUNCTION lakets._first_sfunc(
    state lakets._first_last_state,
    value DOUBLE PRECISION,
    ts TIMESTAMPTZ
)
RETURNS lakets._first_last_state
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF state IS NULL OR state.ts IS NULL THEN
        RETURN ROW(value, ts)::lakets._first_last_state;
    END IF;
    IF ts < state.ts THEN
        RETURN ROW(value, ts)::lakets._first_last_state;
    END IF;
    RETURN state;
END;
$$;

-- Final function: extract the value
CREATE OR REPLACE FUNCTION lakets._first_ffunc(state lakets._first_last_state)
RETURNS DOUBLE PRECISION
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN state.value;
END;
$$;

-- Create the aggregate
DROP AGGREGATE IF EXISTS lakets.first(DOUBLE PRECISION, TIMESTAMPTZ);
CREATE AGGREGATE lakets.first(DOUBLE PRECISION, TIMESTAMPTZ) (
    SFUNC = lakets._first_sfunc,
    STYPE = lakets._first_last_state,
    FINALFUNC = lakets._first_ffunc
);

-- ---------------------------------------------------------------------------
-- last: Aggregate that returns the value associated with the latest time.
-- ---------------------------------------------------------------------------

-- State transition: keep the row with the latest timestamp
CREATE OR REPLACE FUNCTION lakets._last_sfunc(
    state lakets._first_last_state,
    value DOUBLE PRECISION,
    ts TIMESTAMPTZ
)
RETURNS lakets._first_last_state
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF state IS NULL OR state.ts IS NULL THEN
        RETURN ROW(value, ts)::lakets._first_last_state;
    END IF;
    IF ts > state.ts THEN
        RETURN ROW(value, ts)::lakets._first_last_state;
    END IF;
    RETURN state;
END;
$$;

-- Final function: extract the value
CREATE OR REPLACE FUNCTION lakets._last_ffunc(state lakets._first_last_state)
RETURNS DOUBLE PRECISION
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN state.value;
END;
$$;

-- Create the aggregate
DROP AGGREGATE IF EXISTS lakets.last(DOUBLE PRECISION, TIMESTAMPTZ);
CREATE AGGREGATE lakets.last(DOUBLE PRECISION, TIMESTAMPTZ) (
    SFUNC = lakets._last_sfunc,
    STYPE = lakets._first_last_state,
    FINALFUNC = lakets._last_ffunc
);

-- ===========================================================================
-- PHASE 2: Advanced Time Series Functions
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- time_bucket_gapfill: Returns a set of time buckets between start and finish,
-- filling gaps where no data exists. Use with LEFT JOIN to get gapfilled series.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.time_bucket_gapfill(
    p_interval INTERVAL,
    p_start TIMESTAMPTZ,
    p_finish TIMESTAMPTZ
)
RETURNS SETOF TIMESTAMPTZ
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT generate_series(
        lakets.time_bucket(p_interval, p_start),
        lakets.time_bucket(p_interval, p_finish),
        p_interval
    );
$$;

-- ---------------------------------------------------------------------------
-- locf: Last Observation Carried Forward.
-- Fills NULL values with the most recent non-NULL value in window order.
-- Usage: lakets.locf(value) OVER (PARTITION BY device ORDER BY time)
-- Note: This is a wrapper — must be called within a window context via a
-- helper query pattern since PG doesn't allow window in function body.
-- We provide a table-returning helper instead.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.locf(
    p_value DOUBLE PRECISION,
    p_prev_value DOUBLE PRECISION DEFAULT NULL
)
RETURNS DOUBLE PRECISION
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT COALESCE(p_value, p_prev_value);
$$;

-- ---------------------------------------------------------------------------
-- interpolate: Linear interpolation between two known values.
-- Given a NULL value, computes the interpolated value based on the
-- previous and next known values and their timestamps.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.interpolate(
    p_value DOUBLE PRECISION,
    p_prev_value DOUBLE PRECISION,
    p_next_value DOUBLE PRECISION,
    p_prev_time TIMESTAMPTZ,
    p_curr_time TIMESTAMPTZ,
    p_next_time TIMESTAMPTZ
)
RETURNS DOUBLE PRECISION
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE
        WHEN p_value IS NOT NULL THEN p_value
        WHEN p_prev_value IS NULL OR p_next_value IS NULL THEN NULL
        WHEN p_next_time = p_prev_time THEN p_prev_value
        ELSE p_prev_value + (p_next_value - p_prev_value)
             * EXTRACT(EPOCH FROM (p_curr_time - p_prev_time))
             / EXTRACT(EPOCH FROM (p_next_time - p_prev_time))
    END;
$$;

-- ---------------------------------------------------------------------------
-- delta: Computes the difference between consecutive values.
-- Handles counter resets (where value drops below previous).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.delta(
    p_value DOUBLE PRECISION,
    p_prev_value DOUBLE PRECISION,
    p_handle_resets BOOLEAN DEFAULT TRUE
)
RETURNS DOUBLE PRECISION
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE
        WHEN p_prev_value IS NULL THEN NULL
        WHEN p_handle_resets AND p_value < p_prev_value THEN p_value
        ELSE p_value - p_prev_value
    END;
$$;

-- ---------------------------------------------------------------------------
-- rate: Computes rate of change per second between consecutive points.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.rate(
    p_value DOUBLE PRECISION,
    p_prev_value DOUBLE PRECISION,
    p_time TIMESTAMPTZ,
    p_prev_time TIMESTAMPTZ,
    p_handle_resets BOOLEAN DEFAULT TRUE
)
RETURNS DOUBLE PRECISION
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE
        WHEN p_prev_value IS NULL OR p_prev_time IS NULL THEN NULL
        WHEN p_time = p_prev_time THEN NULL
        ELSE lakets.delta(p_value, p_prev_value, p_handle_resets)
             / EXTRACT(EPOCH FROM (p_time - p_prev_time))
    END;
$$;

-- ---------------------------------------------------------------------------
-- histogram: Returns a frequency distribution of values as an integer array.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.histogram(
    p_value DOUBLE PRECISION,
    p_min DOUBLE PRECISION,
    p_max DOUBLE PRECISION,
    p_num_buckets INT
)
RETURNS INT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE
        WHEN p_value IS NULL THEN NULL
        WHEN p_value < p_min THEN 0
        WHEN p_value >= p_max THEN p_num_buckets - 1
        ELSE LEAST(
            FLOOR((p_value - p_min) / ((p_max - p_min) / p_num_buckets))::INT,
            p_num_buckets - 1
        )
    END;
$$;


-- ######################################################################
-- ## MODULE: 04_rollup.sql
-- ######################################################################

-- =============================================================================
-- LakeTS RollUp Engine
-- Incrementally-maintained time-bucketed aggregations over ChronoTables.
-- Backed by regular tables (not materialized views) for surgical per-bucket
-- refresh. Supports hot-tier (Lakebase) and cold-tier (Delta Lake) data.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- create_rollup: Creates a RollUp table with initial data load.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.create_rollup(
    p_name            TEXT,
    p_query           TEXT,
    p_bucket_interval INTERVAL DEFAULT '1 hour',
    p_source_table    TEXT     DEFAULT NULL,
    p_source_schema   TEXT     DEFAULT 'public',
    p_refresh_mode    TEXT     DEFAULT 'incremental',
    p_depends_on      TEXT[]   DEFAULT '{}'
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_rollup_id INT;
    v_chronotable_id INT;
    v_rollup_table TEXT;
    v_rt_view TEXT;
    v_idx_cols TEXT := '';
    v_col RECORD;
    v_watermark TIMESTAMPTZ;
    v_bucket_col TEXT;
    v_time_col TEXT;
    v_dep_ids INT[] := '{}';
    v_dep_name TEXT;
    v_dep_id INT;
BEGIN
    IF EXISTS (SELECT 1 FROM lakets._rollup_registry WHERE name = p_name) THEN
        RAISE EXCEPTION 'RollUp % already exists', p_name;
    END IF;

    IF p_source_table IS NOT NULL THEN
        SELECT id, time_column INTO v_chronotable_id, v_time_col
        FROM lakets._chronotable_registry
        WHERE schema_name = p_source_schema AND table_name = p_source_table;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'Source table %.% is not a registered ChronoTable',
                p_source_schema, p_source_table;
        END IF;
    END IF;

    -- Resolve depends_on names to IDs (M25)
    IF p_depends_on IS NOT NULL AND array_length(p_depends_on, 1) > 0 THEN
        FOREACH v_dep_name IN ARRAY p_depends_on LOOP
            SELECT id INTO v_dep_id FROM lakets._rollup_registry WHERE name = v_dep_name;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'Dependency RollUp % not found', v_dep_name;
            END IF;
            v_dep_ids := array_append(v_dep_ids, v_dep_id);
        END LOOP;
    END IF;

    v_rollup_table := '_rollup_' || p_name;
    v_rt_view := '_rollup_rt_' || p_name;

    -- Create table with initial data load
    EXECUTE format('CREATE TABLE public.%I AS %s', v_rollup_table, p_query);

    -- Auto-detect bucket column (M27)
    v_bucket_col := lakets._detect_bucket_column(p_query);

    -- Build unique index on ALL columns (supports UPSERT and dedup)
    FOR v_col IN
        SELECT a.attname FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = 'public' AND c.relname = v_rollup_table
          AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
    LOOP
        IF v_idx_cols != '' THEN v_idx_cols := v_idx_cols || ', '; END IF;
        v_idx_cols := v_idx_cols || format('%I', v_col.attname);
    END LOOP;

    IF v_idx_cols != '' THEN
        EXECUTE format('CREATE UNIQUE INDEX %I ON public.%I (%s)',
            'idx_' || v_rollup_table || '_unique', v_rollup_table, v_idx_cols);
    END IF;

    -- Read initial watermark using detected bucket column
    EXECUTE format('SELECT max(%I) FROM public.%I', v_bucket_col, v_rollup_table)
        INTO v_watermark;

    -- Register with new columns
    INSERT INTO lakets._rollup_registry
        (name, source_chronotable_id, rollup_table, realtime_view,
         bucket_interval, refresh_mode, query_text, watermark, last_refreshed_at,
         bucket_column, source_time_column, depends_on)
    VALUES (p_name, v_chronotable_id, v_rollup_table, v_rt_view,
            p_bucket_interval, p_refresh_mode, p_query, v_watermark, now(),
            v_bucket_col, v_time_col, v_dep_ids)
    RETURNING id INTO v_rollup_id;

    RETURN v_rollup_id;
END;
$$;

-- ---------------------------------------------------------------------------
-- refresh_rollup: Incremental or full refresh of a RollUp.
-- Returns TRUE if refreshed, FALSE if skipped (refresh_lag).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.refresh_rollup(p_name TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_rec RECORD;
    v_dirty_from TIMESTAMPTZ;
    v_new_watermark TIMESTAMPTZ;
    v_bucket_col TEXT;
    v_query TEXT;
    v_dirty_buckets TIMESTAMPTZ[];
BEGIN
    SELECT r.*, cr.time_column
    INTO v_rec
    FROM lakets._rollup_registry r
    LEFT JOIN lakets._chronotable_registry cr ON r.source_chronotable_id = cr.id
    WHERE r.name = p_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'RollUp % not found', p_name;
    END IF;

    -- Serialize concurrent refresh per rollup via advisory lock
    IF NOT pg_try_advisory_xact_lock('lakets._rollup_registry'::regclass::oid::int, v_rec.id) THEN
        RAISE NOTICE 'refresh_rollup: % is already being refreshed by another session, skipping', p_name;
        RETURN FALSE;
    END IF;

    -- Honor refresh_lag: skip if refreshed too recently
    IF v_rec.last_refreshed_at IS NOT NULL
       AND v_rec.refresh_lag IS NOT NULL
       AND (v_rec.last_refreshed_at + v_rec.refresh_lag) > now() THEN
        RETURN FALSE;
    END IF;

    -- Use detected bucket column or default (M27)
    v_bucket_col := COALESCE(v_rec.bucket_column, 'bucket');

    IF v_rec.refresh_mode = 'full' THEN
        -- Full mode: TRUNCATE + re-INSERT
        EXECUTE format('TRUNCATE public.%I', v_rec.rollup_table);
        EXECUTE format('INSERT INTO public.%I %s', v_rec.rollup_table, v_rec.query_text);
    ELSE
        -- Incremental mode: only re-compute dirty window
        v_dirty_from := COALESCE(v_rec.watermark, '-infinity'::timestamptz)
                        - v_rec.bucket_interval;

        -- Phase 1: Watermark-based refresh (current window)
        -- Attempt predicate injection for scan-level pruning (M23)
        IF COALESCE(v_rec.predicate_injection, TRUE) AND v_rec.source_time_column IS NOT NULL THEN
            v_query := lakets._inject_time_predicate(
                v_rec.query_text, v_rec.source_time_column, v_dirty_from
            );
        ELSE
            v_query := v_rec.query_text;
        END IF;

        EXECUTE format('DELETE FROM public.%I WHERE %I >= %L',
            v_rec.rollup_table, v_bucket_col, v_dirty_from);

        EXECUTE format(
            'INSERT INTO public.%I SELECT * FROM (%s) _q WHERE _q.%I >= %L',
            v_rec.rollup_table, v_query, v_bucket_col, v_dirty_from
        );

        -- Phase 2: Batch refresh of hot-tier invalidation log entries (M24)
        SELECT array_agg(DISTINCT bucket_start ORDER BY bucket_start)
        INTO v_dirty_buckets
        FROM lakets._rollup_invalidation_log
        WHERE rollup_id = v_rec.id
          AND tier = 'hot'
          AND bucket_start < v_dirty_from;

        IF v_dirty_buckets IS NOT NULL AND array_length(v_dirty_buckets, 1) > 0 THEN
            PERFORM lakets._refresh_buckets_chunked(
                v_rec.id, v_rec.rollup_table, v_rec.query_text,
                v_bucket_col, v_dirty_buckets, 100
            );
        END IF;

        -- Clear only processed hot-tier entries (scoped to avoid racing with concurrent writes)
        DELETE FROM lakets._rollup_invalidation_log
        WHERE rollup_id = v_rec.id AND tier = 'hot'
          AND bucket_start < v_dirty_from;
    END IF;

    -- Advance watermark using detected bucket column
    EXECUTE format('SELECT max(%I) FROM public.%I', v_bucket_col, v_rec.rollup_table)
        INTO v_new_watermark;

    UPDATE lakets._rollup_registry
    SET watermark = COALESCE(v_new_watermark, watermark),
        last_refreshed_at = now()
    WHERE name = p_name;

    RETURN TRUE;
END;
$$;

-- ---------------------------------------------------------------------------
-- _rollup_watermark: Returns the stored watermark for a RollUp.
-- Declared STABLE so PostgreSQL caches within a single query execution.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._rollup_watermark(p_name TEXT)
RETURNS TIMESTAMPTZ
LANGUAGE sql
STABLE
AS $$
    SELECT watermark FROM lakets._rollup_registry WHERE name = p_name;
$$;

-- ---------------------------------------------------------------------------
-- create_rollup_view: Creates a real-time UNION view over a RollUp.
-- The raw_query should use lakets._rollup_watermark(name) as the boundary.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.create_rollup_view(
    p_name TEXT,
    p_raw_query TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_rollup_table TEXT;
    v_rt_view TEXT;
BEGIN
    SELECT rollup_table, realtime_view
    INTO v_rollup_table, v_rt_view
    FROM lakets._rollup_registry
    WHERE name = p_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'RollUp % not found', p_name;
    END IF;

    EXECUTE format('DROP VIEW IF EXISTS public.%I', v_rt_view);
    EXECUTE format(
        'CREATE VIEW public.%I AS SELECT * FROM public.%I UNION ALL %s',
        v_rt_view, v_rollup_table, p_raw_query
    );
END;
$$;

-- ---------------------------------------------------------------------------
-- drop_rollup: Drops a RollUp and all associated objects.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.drop_rollup(p_name TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_rollup_table TEXT;
    v_rt_view TEXT;
BEGIN
    SELECT rollup_table, realtime_view
    INTO v_rollup_table, v_rt_view
    FROM lakets._rollup_registry
    WHERE name = p_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'RollUp % not found', p_name;
    END IF;

    EXECUTE format('DROP VIEW IF EXISTS public.%I', v_rt_view);
    EXECUTE format('DROP TABLE IF EXISTS public.%I', v_rollup_table);
    DELETE FROM lakets._rollup_registry WHERE name = p_name;
END;
$$;

-- ---------------------------------------------------------------------------
-- show_rollups: Lists all RollUps with status and watermark.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.show_rollups()
RETURNS TABLE (
    name              TEXT,
    rollup_table      TEXT,
    realtime_view     TEXT,
    bucket_interval   INTERVAL,
    refresh_mode      TEXT,
    refresh_lag       INTERVAL,
    watermark         TIMESTAMPTZ,
    last_refreshed_at TIMESTAMPTZ,
    source_table      TEXT,
    bucket_column     TEXT,
    depends_on        INT[],
    export_enabled    BOOLEAN
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        r.name,
        r.rollup_table,
        r.realtime_view,
        r.bucket_interval,
        r.refresh_mode,
        r.refresh_lag,
        r.watermark,
        r.last_refreshed_at,
        COALESCE(cr.schema_name || '.' || cr.table_name, 'N/A'),
        COALESCE(r.bucket_column, 'bucket'),
        r.depends_on,
        COALESCE(r.export_enabled, FALSE)
    FROM lakets._rollup_registry r
    LEFT JOIN lakets._chronotable_registry cr ON r.source_chronotable_id = cr.id
    ORDER BY r.name;
END;
$$;

-- ---------------------------------------------------------------------------
-- _rollup_invalidation_trigger_fn: Per-row trigger that computes dirty
-- time buckets via date_bin and upserts into the invalidation log.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._rollup_invalidation_trigger_fn()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_rec RECORD;
    v_bucket_start TIMESTAMPTZ;
    v_parent_table TEXT;
    v_time_col TEXT;
BEGIN
    -- Resolve partition parent via shared helper
    SELECT lakets._resolve_partition_parent(TG_TABLE_SCHEMA, TG_TABLE_NAME)
    INTO v_parent_table;

    -- Get time column for this ChronoTable
    SELECT cr.time_column INTO v_time_col
    FROM lakets._chronotable_registry cr
    WHERE cr.schema_name = TG_TABLE_SCHEMA
      AND cr.table_name = COALESCE(v_parent_table, TG_TABLE_NAME);

    IF v_time_col IS NULL THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
    END IF;

    -- Find all incremental RollUps registered against this ChronoTable
    FOR v_rec IN
        SELECT r.id, r.bucket_interval
        FROM lakets._rollup_registry r
        JOIN lakets._chronotable_registry cr ON r.source_chronotable_id = cr.id
        WHERE cr.schema_name = TG_TABLE_SCHEMA
          AND cr.table_name = COALESCE(v_parent_table, TG_TABLE_NAME)
          AND r.refresh_mode = 'incremental'
    LOOP
        IF TG_OP = 'DELETE' THEN
            EXECUTE format('SELECT date_bin(%L, ($1).%I, %L::timestamptz)',
                v_rec.bucket_interval, v_time_col, '2000-01-01')
                INTO v_bucket_start USING OLD;
        ELSE
            EXECUTE format('SELECT date_bin(%L, ($1).%I, %L::timestamptz)',
                v_rec.bucket_interval, v_time_col, '2000-01-01')
                INTO v_bucket_start USING NEW;
        END IF;

        -- Upsert into invalidation log (tier = 'hot' for Lakebase mutations)
        INSERT INTO lakets._rollup_invalidation_log (rollup_id, bucket_start, tier)
        VALUES (v_rec.id, v_bucket_start, 'hot')
        ON CONFLICT (rollup_id, bucket_start) DO UPDATE
            SET invalidated_at = now(), tier = 'hot';
    END LOOP;

    IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- enable_rollup_invalidation: Installs the invalidation trigger on a
-- source ChronoTable. One trigger per table handles all RollUps.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.enable_rollup_invalidation(p_rollup_name TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema TEXT;
    v_table TEXT;
    v_mode TEXT;
BEGIN
    SELECT cr.schema_name, cr.table_name, r.refresh_mode
    INTO v_schema, v_table, v_mode
    FROM lakets._rollup_registry r
    JOIN lakets._chronotable_registry cr ON r.source_chronotable_id = cr.id
    WHERE r.name = p_rollup_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'RollUp % not found', p_rollup_name;
    END IF;

    IF v_mode != 'incremental' THEN
        RAISE EXCEPTION 'Invalidation requires refresh_mode = incremental (current: %)', v_mode;
    END IF;

    -- Per-row trigger for UPDATE/DELETE (existing behavior)
    EXECUTE format(
        'CREATE OR REPLACE TRIGGER trg_lakets_rollup_invalidation '
        'AFTER UPDATE OR DELETE ON %I.%I '
        'FOR EACH ROW EXECUTE FUNCTION lakets._rollup_invalidation_trigger_fn()',
        v_schema, v_table
    );

    -- Statement-level trigger for INSERT (M27: handles COPY + multi-row INSERT)
    EXECUTE format(
        'CREATE OR REPLACE TRIGGER trg_lakets_rollup_bulk_invalidation '
        'AFTER INSERT ON %I.%I '
        'REFERENCING NEW TABLE AS _new_rows '
        'FOR EACH STATEMENT EXECUTE FUNCTION lakets._bulk_import_invalidation()',
        v_schema, v_table
    );

    -- M23: Install _touch_chunk_metadata on all existing partitions
    -- so chunk-skip pruning (_get_dirty_chunks) can track last_modified_at
    DECLARE
        v_part RECORD;
    BEGIN
        FOR v_part IN
            SELECT c.relname AS part_name, n.nspname AS part_schema
            FROM pg_inherits i
            JOIN pg_class c ON i.inhrelid = c.oid
            JOIN pg_class p ON i.inhparent = p.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_namespace pn ON p.relnamespace = pn.oid
            WHERE pn.nspname = v_schema AND p.relname = v_table
        LOOP
            EXECUTE format(
                'CREATE OR REPLACE TRIGGER trg_lakets_touch_chunk '
                'AFTER INSERT ON %I.%I '
                'FOR EACH STATEMENT EXECUTE FUNCTION lakets._touch_chunk_metadata()',
                v_part.part_schema, v_part.part_name
            );
        END LOOP;
    END;
END;
$$;

-- ---------------------------------------------------------------------------
-- disable_rollup_invalidation: Removes invalidation trigger if no other
-- RollUps on the same source table need it.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.disable_rollup_invalidation(p_rollup_name TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_schema TEXT;
    v_table TEXT;
    v_rollup_id INT;
    v_other_count INT;
BEGIN
    SELECT cr.schema_name, cr.table_name, r.id
    INTO v_schema, v_table, v_rollup_id
    FROM lakets._rollup_registry r
    JOIN lakets._chronotable_registry cr ON r.source_chronotable_id = cr.id
    WHERE r.name = p_rollup_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'RollUp % not found', p_rollup_name;
    END IF;

    -- Acquire advisory lock to prevent racing with refresh_rollup
    PERFORM pg_advisory_xact_lock('lakets._rollup_registry'::regclass::oid::int, v_rollup_id);

    -- Clear invalidation log entries for this RollUp
    DELETE FROM lakets._rollup_invalidation_log WHERE rollup_id = v_rollup_id;

    -- Check if other incremental RollUps on the same source table still need the trigger
    SELECT count(*) INTO v_other_count
    FROM lakets._rollup_registry r
    JOIN lakets._chronotable_registry cr ON r.source_chronotable_id = cr.id
    WHERE cr.schema_name = v_schema AND cr.table_name = v_table
      AND r.name != p_rollup_name
      AND r.refresh_mode = 'incremental';

    IF v_other_count = 0 THEN
        EXECUTE format(
            'DROP TRIGGER IF EXISTS trg_lakets_rollup_invalidation ON %I.%I',
            v_schema, v_table
        );
        -- Also drop statement-level trigger (M27)
        EXECUTE format(
            'DROP TRIGGER IF EXISTS trg_lakets_rollup_bulk_invalidation ON %I.%I',
            v_schema, v_table
        );
    END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- invalidate_rollup_range: Manually marks a time range as dirty.
-- Use for bulk imports (COPY bypasses triggers) or cold-tier corrections.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.invalidate_rollup_range(
    p_name TEXT,
    p_from TIMESTAMPTZ,
    p_to   TIMESTAMPTZ,
    p_tier TEXT DEFAULT NULL  -- NULL = auto-detect from chunk metadata (M26)
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_rec RECORD;
    v_bucket TIMESTAMPTZ;
    v_tier TEXT;
    v_count INT := 0;
BEGIN
    SELECT r.id, r.bucket_interval, r.source_chronotable_id
    INTO v_rec
    FROM lakets._rollup_registry r WHERE r.name = p_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'RollUp % not found', p_name;
    END IF;

    -- Generate all bucket_start values in the range
    FOR v_bucket IN
        SELECT generate_series(
            date_bin(v_rec.bucket_interval, p_from, '2000-01-01'::timestamptz),
            date_bin(v_rec.bucket_interval, p_to, '2000-01-01'::timestamptz),
            v_rec.bucket_interval
        )
    LOOP
        -- Auto-detect tier if not specified (M26)
        IF p_tier IS NULL THEN
            v_tier := COALESCE(
                lakets._resolve_bucket_tier(v_rec.source_chronotable_id, v_bucket),
                'hot'
            );
        ELSE
            v_tier := p_tier;
        END IF;

        INSERT INTO lakets._rollup_invalidation_log (rollup_id, bucket_start, tier)
        VALUES (v_rec.id, v_bucket, v_tier)
        ON CONFLICT (rollup_id, bucket_start) DO UPDATE
            SET invalidated_at = now(), tier = v_tier;
        v_count := v_count + 1;
    END LOOP;

    RETURN v_count;
END;
$$;


-- ######################################################################
-- ## MODULE: 05_compression.sql
-- ######################################################################

-- =============================================================================
-- LakeTS Compression & Tiering Policies
-- Register policies for automatic data tiering from Lakebase to Delta Lake.
-- Actual tiering is executed by Databricks workflow jobs.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- add_compression_policy: Registers a compression/tiering policy.
-- After compress_after interval, the Databricks compression job will:
--   1. Ensure data is synced to Delta via Lakehouse Sync
--   2. Optimize the Delta table (Z-ORDER / Liquid Clustering)
--   3. Drop the Lakebase partition
--   4. Update chunk metadata status to 'tiered'
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.add_compression_policy(
    p_table_name TEXT,
    p_compress_after INTERVAL,
    p_segment_by TEXT DEFAULT NULL,
    p_order_by TEXT DEFAULT NULL,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_chronotable_id INT;
    v_policy_id INT;
    v_config JSONB;
BEGIN
    SELECT id INTO v_chronotable_id
    FROM lakets._chronotable_registry
    WHERE schema_name = p_schema_name AND table_name = p_table_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table %.% is not a registered ChronoTable',
            p_schema_name, p_table_name;
    END IF;

    -- Check for existing compression policy
    IF EXISTS (
        SELECT 1 FROM lakets._policy_registry
        WHERE chronotable_id = v_chronotable_id AND policy_type = 'compression'
    ) THEN
        RAISE EXCEPTION 'Compression policy already exists for %.%',
            p_schema_name, p_table_name;
    END IF;

    v_config := jsonb_build_object(
        'compress_after', p_compress_after::TEXT,
        'segment_by', p_segment_by,
        'order_by', COALESCE(p_order_by, (
            SELECT time_column FROM lakets._chronotable_registry WHERE id = v_chronotable_id
        ) || ' DESC')
    );

    INSERT INTO lakets._policy_registry
        (chronotable_id, policy_type, config, enabled)
    VALUES (v_chronotable_id, 'compression', v_config, TRUE)
    RETURNING id INTO v_policy_id;

    -- Mark the hypertable as compression-enabled
    UPDATE lakets._chronotable_registry
    SET compression_enabled = TRUE
    WHERE id = v_chronotable_id;

    RETURN v_policy_id;
END;
$$;

-- ---------------------------------------------------------------------------
-- compress_chunk: Marks a chunk for compression/tiering.
-- The actual tiering to Delta is done by the Databricks workflow job.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.compress_chunk(
    p_chunk_name TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE lakets._chunk_metadata
    SET status = 'compressed',
        compressed_at = now()
    WHERE chunk_name = p_chunk_name AND status = 'active';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Chunk % not found or not active', p_chunk_name;
    END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- decompress_chunk: Marks a tiered chunk for re-ingestion.
-- The actual data restoration from Delta is done by the Databricks workflow.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.decompress_chunk(
    p_chunk_name TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE lakets._chunk_metadata
    SET status = 'active',
        compressed_at = NULL,
        tiered_at = NULL
    WHERE chunk_name = p_chunk_name AND status IN ('compressed', 'tiered');

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Chunk % not found or not compressed/tiered', p_chunk_name;
    END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- show_compression_policy: Returns the compression policy for a hypertable.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.show_compression_policy(
    p_table_name TEXT,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS TABLE (
    policy_id INT,
    compress_after TEXT,
    segment_by TEXT,
    order_by TEXT,
    enabled BOOLEAN,
    last_run_at TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_chronotable_id INT;
BEGIN
    SELECT id INTO v_chronotable_id
    FROM lakets._chronotable_registry
    WHERE schema_name = p_schema_name AND table_name = p_table_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table %.% is not a registered ChronoTable',
            p_schema_name, p_table_name;
    END IF;

    RETURN QUERY
    SELECT
        pr.id,
        pr.config->>'compress_after',
        pr.config->>'segment_by',
        pr.config->>'order_by',
        pr.enabled,
        pr.last_run_at
    FROM lakets._policy_registry pr
    WHERE pr.chronotable_id = v_chronotable_id AND pr.policy_type = 'compression';
END;
$$;

-- ---------------------------------------------------------------------------
-- remove_compression_policy: Removes the compression policy.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.remove_compression_policy(
    p_table_name TEXT,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_chronotable_id INT;
BEGIN
    SELECT id INTO v_chronotable_id
    FROM lakets._chronotable_registry
    WHERE schema_name = p_schema_name AND table_name = p_table_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table %.% is not a registered ChronoTable',
            p_schema_name, p_table_name;
    END IF;

    DELETE FROM lakets._policy_registry
    WHERE chronotable_id = v_chronotable_id AND policy_type = 'compression';

    UPDATE lakets._chronotable_registry
    SET compression_enabled = FALSE
    WHERE id = v_chronotable_id;
END;
$$;

-- ---------------------------------------------------------------------------
-- _get_chunks_to_compress: Returns chunks eligible for compression.
-- Used by the Databricks compression job to find work.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._get_chunks_to_compress(
    p_table_name TEXT,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS TABLE (
    chunk_id INT,
    chunk_name TEXT,
    range_start TIMESTAMPTZ,
    range_end TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_chronotable_id INT;
    v_compress_after INTERVAL;
BEGIN
    SELECT hr.id, (pr.config->>'compress_after')::INTERVAL
    INTO v_chronotable_id, v_compress_after
    FROM lakets._chronotable_registry hr
    JOIN lakets._policy_registry pr ON hr.id = pr.chronotable_id
    WHERE hr.schema_name = p_schema_name
      AND hr.table_name = p_table_name
      AND pr.policy_type = 'compression'
      AND pr.enabled = TRUE;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    RETURN QUERY
    SELECT cm.id, cm.chunk_name, cm.range_start, cm.range_end
    FROM lakets._chunk_metadata cm
    WHERE cm.chronotable_id = v_chronotable_id
      AND cm.status = 'active'
      AND cm.range_end <= (now() - v_compress_after)
    ORDER BY cm.range_start;
END;
$$;


-- ######################################################################
-- ## MODULE: 06_retention.sql
-- ######################################################################

-- =============================================================================
-- LakeTS Retention Policies
-- Automated data lifecycle management across Lakebase and Delta Lake tiers.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- add_retention_policy: Auto-drops Lakebase partitions older than interval.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.add_retention_policy(
    p_table_name TEXT,
    p_drop_after INTERVAL,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_chronotable_id INT;
    v_policy_id INT;
BEGIN
    SELECT id INTO v_chronotable_id
    FROM lakets._chronotable_registry
    WHERE schema_name = p_schema_name AND table_name = p_table_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table %.% is not a registered ChronoTable',
            p_schema_name, p_table_name;
    END IF;

    IF EXISTS (
        SELECT 1 FROM lakets._policy_registry
        WHERE chronotable_id = v_chronotable_id AND policy_type = 'retention'
    ) THEN
        RAISE EXCEPTION 'Retention policy already exists for %.%',
            p_schema_name, p_table_name;
    END IF;

    INSERT INTO lakets._policy_registry
        (chronotable_id, policy_type, config, enabled)
    VALUES (v_chronotable_id, 'retention',
            jsonb_build_object('drop_after', p_drop_after::TEXT), TRUE)
    RETURNING id INTO v_policy_id;

    UPDATE lakets._chronotable_registry
    SET retention_interval = p_drop_after
    WHERE id = v_chronotable_id;

    RETURN v_policy_id;
END;
$$;

-- ---------------------------------------------------------------------------
-- add_tiered_retention_policy: Tier to Delta after tier_after,
-- delete from Delta after drop_after.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.add_tiered_retention_policy(
    p_table_name TEXT,
    p_tier_after INTERVAL,
    p_drop_after INTERVAL,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_chronotable_id INT;
    v_policy_id INT;
BEGIN
    SELECT id INTO v_chronotable_id
    FROM lakets._chronotable_registry
    WHERE schema_name = p_schema_name AND table_name = p_table_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table %.% is not a registered ChronoTable',
            p_schema_name, p_table_name;
    END IF;

    IF p_tier_after >= p_drop_after THEN
        RAISE EXCEPTION 'tier_after (%) must be less than drop_after (%)',
            p_tier_after, p_drop_after;
    END IF;

    IF EXISTS (
        SELECT 1 FROM lakets._policy_registry
        WHERE chronotable_id = v_chronotable_id AND policy_type = 'tiered_retention'
    ) THEN
        RAISE EXCEPTION 'Tiered retention policy already exists for %.%',
            p_schema_name, p_table_name;
    END IF;

    INSERT INTO lakets._policy_registry
        (chronotable_id, policy_type, config, enabled)
    VALUES (v_chronotable_id, 'tiered_retention',
            jsonb_build_object(
                'tier_after', p_tier_after::TEXT,
                'drop_after', p_drop_after::TEXT
            ), TRUE)
    RETURNING id INTO v_policy_id;

    RETURN v_policy_id;
END;
$$;

-- ---------------------------------------------------------------------------
-- remove_retention_policy: Removes retention or tiered_retention policy.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.remove_retention_policy(
    p_table_name TEXT,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_chronotable_id INT;
BEGIN
    SELECT id INTO v_chronotable_id
    FROM lakets._chronotable_registry
    WHERE schema_name = p_schema_name AND table_name = p_table_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table %.% is not a registered ChronoTable',
            p_schema_name, p_table_name;
    END IF;

    DELETE FROM lakets._policy_registry
    WHERE chronotable_id = v_chronotable_id
      AND policy_type IN ('retention', 'tiered_retention');

    UPDATE lakets._chronotable_registry
    SET retention_interval = NULL
    WHERE id = v_chronotable_id;
END;
$$;

-- ---------------------------------------------------------------------------
-- execute_retention: Runs retention policy — drops expired chunks.
-- Called by the Databricks retention job.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.execute_retention(
    p_table_name TEXT,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_chronotable_id INT;
    v_drop_after INTERVAL;
    v_dropped INT;
BEGIN
    SELECT hr.id, (pr.config->>'drop_after')::INTERVAL
    INTO v_chronotable_id, v_drop_after
    FROM lakets._chronotable_registry hr
    JOIN lakets._policy_registry pr ON hr.id = pr.chronotable_id
    WHERE hr.schema_name = p_schema_name
      AND hr.table_name = p_table_name
      AND pr.policy_type IN ('retention', 'tiered_retention')
      AND pr.enabled = TRUE;

    IF NOT FOUND THEN
        RETURN 0;
    END IF;

    SELECT lakets.drop_chunks(p_table_name, v_drop_after, p_schema_name)
    INTO v_dropped;

    -- Update policy last_run_at
    UPDATE lakets._policy_registry
    SET last_run_at = now()
    WHERE chronotable_id = v_chronotable_id
      AND policy_type IN ('retention', 'tiered_retention');

    RETURN v_dropped;
END;
$$;

-- ---------------------------------------------------------------------------
-- show_retention_policy: Returns retention policy details.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.show_retention_policy(
    p_table_name TEXT,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS TABLE (
    policy_id INT,
    policy_type TEXT,
    drop_after TEXT,
    tier_after TEXT,
    enabled BOOLEAN,
    last_run_at TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_chronotable_id INT;
BEGIN
    SELECT id INTO v_chronotable_id
    FROM lakets._chronotable_registry
    WHERE schema_name = p_schema_name AND table_name = p_table_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table %.% is not a registered ChronoTable',
            p_schema_name, p_table_name;
    END IF;

    RETURN QUERY
    SELECT
        pr.id,
        pr.policy_type,
        pr.config->>'drop_after',
        pr.config->>'tier_after',
        pr.enabled,
        pr.last_run_at
    FROM lakets._policy_registry pr
    WHERE pr.chronotable_id = v_chronotable_id
      AND pr.policy_type IN ('retention', 'tiered_retention');
END;
$$;


-- ######################################################################
-- ## MODULE: 07_monitoring.sql
-- ######################################################################

-- =============================================================================
-- LakeTS Monitoring & Metrics
-- SQL functions that expose operational metrics for Prometheus/Grafana.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- lakets_metrics: Returns all LakeTS operational metrics as key-value rows.
-- Compatible with sql_exporter for Prometheus scraping.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.lakets_metrics()
RETURNS TABLE (
    metric_name TEXT,
    metric_value DOUBLE PRECISION,
    labels JSONB
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Hypertable count
    RETURN QUERY
    SELECT 'lakets_hypertables_total'::TEXT, count(*)::DOUBLE PRECISION, '{}'::JSONB
    FROM lakets._chronotable_registry;

    -- Total chunks by status
    RETURN QUERY
    SELECT 'lakets_chunks_total'::TEXT, count(*)::DOUBLE PRECISION,
           jsonb_build_object('status', cm.status)
    FROM lakets._chunk_metadata cm
    GROUP BY cm.status;

    -- Total row estimate per hypertable (from pg_stat)
    RETURN QUERY
    SELECT 'lakets_estimated_rows'::TEXT,
           COALESCE(s.n_live_tup, 0)::DOUBLE PRECISION,
           jsonb_build_object('table', hr.schema_name || '.' || hr.table_name)
    FROM lakets._chronotable_registry hr
    LEFT JOIN pg_stat_user_tables s
        ON s.schemaname = hr.schema_name AND s.relname = hr.table_name;

    -- Compression policy status
    RETURN QUERY
    SELECT 'lakets_compression_enabled'::TEXT,
           (CASE WHEN hr.compression_enabled THEN 1 ELSE 0 END)::DOUBLE PRECISION,
           jsonb_build_object('table', hr.schema_name || '.' || hr.table_name)
    FROM lakets._chronotable_registry hr;

    -- RollUp watermark lag (seconds between now and watermark per RollUp)
    RETURN QUERY
    SELECT 'lakets_rollup_watermark_lag_seconds'::TEXT,
           EXTRACT(EPOCH FROM (now() - r.watermark))::DOUBLE PRECISION,
           jsonb_build_object('rollup', r.name, 'refresh_mode', r.refresh_mode)
    FROM lakets._rollup_registry r
    WHERE r.watermark IS NOT NULL;

    -- RollUp refresh lag (seconds since last refresh)
    RETURN QUERY
    SELECT 'lakets_rollup_refresh_lag_seconds'::TEXT,
           EXTRACT(EPOCH FROM (now() - r.last_refreshed_at))::DOUBLE PRECISION,
           jsonb_build_object('rollup', r.name)
    FROM lakets._rollup_registry r
    WHERE r.last_refreshed_at IS NOT NULL;

    -- RollUp invalidation log depth (pending dirty buckets per RollUp by tier)
    RETURN QUERY
    SELECT 'lakets_rollup_invalidation_log_depth'::TEXT,
           count(*)::DOUBLE PRECISION,
           jsonb_build_object('rollup', r.name, 'tier', il.tier)
    FROM lakets._rollup_invalidation_log il
    JOIN lakets._rollup_registry r ON il.rollup_id = r.id
    GROUP BY r.name, il.tier;

    -- Sync status per hypertable
    RETURN QUERY
    SELECT 'lakets_sync_enabled'::TEXT,
           (CASE WHEN hr.sync_enabled THEN 1 ELSE 0 END)::DOUBLE PRECISION,
           jsonb_build_object('table', hr.schema_name || '.' || hr.table_name)
    FROM lakets._chronotable_registry hr;

    -- Policy count by type
    RETURN QUERY
    SELECT 'lakets_policies_total'::TEXT, count(*)::DOUBLE PRECISION,
           jsonb_build_object('type', pr.policy_type)
    FROM lakets._policy_registry pr
    WHERE pr.enabled = TRUE
    GROUP BY pr.policy_type;

    -- Database size
    RETURN QUERY
    SELECT 'lakets_database_size_bytes'::TEXT,
           pg_database_size(current_database())::DOUBLE PRECISION,
           '{}'::JSONB;
END;
$$;

-- ---------------------------------------------------------------------------
-- chunk_health: Detailed per-hypertable chunk health report.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.chunk_health()
RETURNS TABLE (
    hypertable TEXT,
    total_chunks BIGINT,
    active_chunks BIGINT,
    compressed_chunks BIGINT,
    tiered_chunks BIGINT,
    dropped_chunks BIGINT,
    oldest_active TIMESTAMPTZ,
    newest_active TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        hr.schema_name || '.' || hr.table_name,
        count(*)::BIGINT,
        count(*) FILTER (WHERE cm.status = 'active')::BIGINT,
        count(*) FILTER (WHERE cm.status = 'compressed')::BIGINT,
        count(*) FILTER (WHERE cm.status = 'tiered')::BIGINT,
        count(*) FILTER (WHERE cm.status = 'dropped')::BIGINT,
        min(cm.range_start) FILTER (WHERE cm.status = 'active'),
        max(cm.range_end) FILTER (WHERE cm.status = 'active')
    FROM lakets._chronotable_registry hr
    JOIN lakets._chunk_metadata cm ON hr.id = cm.chronotable_id
    GROUP BY hr.schema_name, hr.table_name
    ORDER BY hr.schema_name, hr.table_name;
END;
$$;

-- ---------------------------------------------------------------------------
-- query_stats: Wraps pg_stat_statements for LakeTS-relevant queries.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.query_stats(p_limit INT DEFAULT 20)
RETURNS TABLE (
    query TEXT,
    calls BIGINT,
    total_time_ms DOUBLE PRECISION,
    mean_time_ms DOUBLE PRECISION,
    rows_returned BIGINT
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- pg_stat_statements may not be available; handle gracefully
    BEGIN
        RETURN QUERY EXECUTE format(
            'SELECT left(query, 200), calls, total_exec_time, mean_exec_time, rows
             FROM pg_stat_statements
             WHERE query LIKE ''%%lakets%%'' OR query LIKE ''%%time_bucket%%''
             ORDER BY total_exec_time DESC
             LIMIT %s', p_limit
        );
    EXCEPTION WHEN undefined_table THEN
        -- pg_stat_statements not available
        RETURN;
    END;
END;
$$;


-- ######################################################################
-- ## MODULE: 08_metric_table.sql
-- ######################################################################

-- =============================================================================
-- LakeTS Multi-Metric ChronoTables
-- InfluxDB-style tag + field model with cardinality management.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- create_metric_table: Creates a ChronoTable optimized for multi-metric data.
-- Tags are indexed TEXT columns; fields are DOUBLE PRECISION value columns.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.create_metric_table(
    p_table_name TEXT,
    p_tag_columns TEXT[],
    p_field_columns TEXT[],
    p_chunk_interval INTERVAL DEFAULT '1 day',
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_col_defs TEXT := 'time TIMESTAMPTZ NOT NULL';
    v_tag TEXT;
    v_field TEXT;
    v_idx_cols TEXT := '';
    v_chronotable_id INT;
BEGIN
    -- Build column definitions: tags as TEXT, fields as DOUBLE PRECISION
    FOREACH v_tag IN ARRAY p_tag_columns LOOP
        v_col_defs := v_col_defs || format(', %I TEXT NOT NULL', v_tag);
        IF v_idx_cols != '' THEN v_idx_cols := v_idx_cols || ', '; END IF;
        v_idx_cols := v_idx_cols || format('%I', v_tag);
    END LOOP;

    FOREACH v_field IN ARRAY p_field_columns LOOP
        v_col_defs := v_col_defs || format(', %I DOUBLE PRECISION', v_field);
    END LOOP;

    -- Create the table
    EXECUTE format('CREATE TABLE %I.%I (%s)', p_schema_name, p_table_name, v_col_defs);

    -- Convert to ChronoTable (partitioned by time)
    v_chronotable_id := lakets.create_hypertable(
        p_table_name, 'time', p_chunk_interval, p_schema_name
    );

    -- Create composite index for series-level queries: (tags..., time DESC)
    IF v_idx_cols != '' THEN
        EXECUTE format(
            'CREATE INDEX %I ON %I.%I (%s, time DESC)',
            'idx_' || p_table_name || '_series',
            p_schema_name, p_table_name, v_idx_cols
        );
    END IF;

    -- Create BRIN index on time for large range scans
    EXECUTE format(
        'CREATE INDEX %I ON %I.%I USING BRIN (time)',
        'idx_' || p_table_name || '_brin',
        p_schema_name, p_table_name
    );

    RETURN v_chronotable_id;
END;
$$;

-- ---------------------------------------------------------------------------
-- cardinality_stats: Returns distinct value counts per tag column.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.cardinality_stats(
    p_table_name TEXT,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS TABLE (
    column_name TEXT,
    distinct_values BIGINT,
    total_rows BIGINT,
    pct_of_rows NUMERIC
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_col RECORD;
    v_total BIGINT;
BEGIN
    -- Get total row count
    EXECUTE format('SELECT count(*) FROM %I.%I', p_schema_name, p_table_name)
    INTO v_total;

    -- For each TEXT column (tags), count distinct values
    FOR v_col IN
        SELECT a.attname
        FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_type t ON a.atttypid = t.oid
        WHERE n.nspname = p_schema_name AND c.relname = p_table_name
          AND a.attnum > 0 AND NOT a.attisdropped
          AND t.typname = 'text'
        ORDER BY a.attnum
    LOOP
        RETURN QUERY EXECUTE format(
            'SELECT %L::TEXT, count(DISTINCT %I)::BIGINT, %s::BIGINT,
                    CASE WHEN %s > 0 THEN round(count(DISTINCT %I)::NUMERIC / %s * 100, 3) ELSE 0 END
             FROM %I.%I',
            v_col.attname, v_col.attname, v_total, v_total, v_col.attname, v_total,
            p_schema_name, p_table_name
        );
    END LOOP;
END;
$$;

-- ---------------------------------------------------------------------------
-- cardinality_check: Warns if combined series cardinality exceeds threshold.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.cardinality_check(
    p_table_name TEXT,
    p_max_series BIGINT DEFAULT 100000,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS TABLE (
    status TEXT,
    combined_cardinality BIGINT,
    max_allowed BIGINT,
    tag_columns TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_tag_cols TEXT := '';
    v_col RECORD;
    v_cardinality BIGINT;
BEGIN
    -- Build list of tag columns
    FOR v_col IN
        SELECT a.attname
        FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_type t ON a.atttypid = t.oid
        WHERE n.nspname = p_schema_name AND c.relname = p_table_name
          AND a.attnum > 0 AND NOT a.attisdropped
          AND t.typname = 'text'
        ORDER BY a.attnum
    LOOP
        IF v_tag_cols != '' THEN v_tag_cols := v_tag_cols || ', '; END IF;
        v_tag_cols := v_tag_cols || format('%I', v_col.attname);
    END LOOP;

    IF v_tag_cols = '' THEN
        RETURN QUERY SELECT 'OK'::TEXT, 0::BIGINT, p_max_series, ''::TEXT;
        RETURN;
    END IF;

    EXECUTE format(
        'SELECT count(*) FROM (SELECT DISTINCT %s FROM %I.%I) t',
        v_tag_cols, p_schema_name, p_table_name
    ) INTO v_cardinality;

    RETURN QUERY SELECT
        CASE
            WHEN v_cardinality > p_max_series THEN 'CRITICAL'
            WHEN v_cardinality > p_max_series * 0.8 THEN 'WARNING'
            ELSE 'OK'
        END,
        v_cardinality,
        p_max_series,
        v_tag_cols;
END;
$$;


-- ######################################################################
-- ## MODULE: 09_lvc.sql
-- ######################################################################

-- =============================================================================
-- LakeTS Last Value Cache (LVC)
-- Trigger-maintained cache for sub-10ms latest-state queries.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- _lvc_trigger_fn: Generic trigger that upserts into the LVC cache table.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._lvc_trigger_fn()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_cache_table TEXT;
    v_key_cols TEXT[];
    v_val_cols TEXT[];
    v_set_clause TEXT := '';
    v_ins_cols TEXT := '';
    v_ins_vals TEXT := '';
    v_col TEXT;
BEGIN
    -- Look up the LVC config from registry via parent table resolution
    SELECT lr.cache_table_name, lr.key_columns, lr.value_columns
    INTO v_cache_table, v_key_cols, v_val_cols
    FROM lakets._lvc_registry lr
    JOIN lakets._chronotable_registry hr ON lr.chronotable_id = hr.id
    WHERE hr.table_name = COALESCE(
        lakets._resolve_partition_parent(TG_TABLE_SCHEMA, TG_TABLE_NAME),
        TG_TABLE_NAME
    ) AND hr.schema_name = TG_TABLE_SCHEMA;

    IF v_cache_table IS NULL THEN
        RETURN NEW;
    END IF;

    -- Build column lists: only key + value columns (not all source columns)
    FOREACH v_col IN ARRAY v_key_cols LOOP
        IF v_ins_cols != '' THEN v_ins_cols := v_ins_cols || ', '; v_ins_vals := v_ins_vals || ', '; END IF;
        v_ins_cols := v_ins_cols || format('%I', v_col);
        v_ins_vals := v_ins_vals || format('($1).%I', v_col);
    END LOOP;

    FOREACH v_col IN ARRAY v_val_cols LOOP
        IF v_ins_cols != '' THEN v_ins_cols := v_ins_cols || ', '; v_ins_vals := v_ins_vals || ', '; END IF;
        v_ins_cols := v_ins_cols || format('%I', v_col);
        v_ins_vals := v_ins_vals || format('($1).%I', v_col);
        IF v_set_clause != '' THEN v_set_clause := v_set_clause || ', '; END IF;
        v_set_clause := v_set_clause || format('%I = EXCLUDED.%I', v_col, v_col);
    END LOOP;

    v_ins_cols := v_ins_cols || ', last_updated';
    v_ins_vals := v_ins_vals || ', now()';
    v_set_clause := v_set_clause || ', last_updated = now()';

    -- UPSERT into cache (only key + value columns)
    EXECUTE format(
        'INSERT INTO %I.%I (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s',
        TG_TABLE_SCHEMA, v_cache_table,
        v_ins_cols, v_ins_vals,
        array_to_string(v_key_cols, ', '), v_set_clause
    ) USING NEW;

    RETURN NEW;
END;
$$;

-- ---------------------------------------------------------------------------
-- enable_lvc: Creates a cache table + trigger for a ChronoTable.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.enable_lvc(
    p_table_name TEXT,
    p_key_columns TEXT[],
    p_value_columns TEXT[],
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_ht_id INT;
    v_cache_name TEXT;
    v_col_defs TEXT := '';
    v_col TEXT;
    v_col_type TEXT;
BEGIN
    SELECT id INTO v_ht_id FROM lakets._chronotable_registry
    WHERE schema_name = p_schema_name AND table_name = p_table_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION '%.% is not a registered ChronoTable', p_schema_name, p_table_name;
    END IF;

    IF EXISTS (SELECT 1 FROM lakets._lvc_registry WHERE chronotable_id = v_ht_id) THEN
        RAISE EXCEPTION 'LVC already enabled for %.%', p_schema_name, p_table_name;
    END IF;

    v_cache_name := '_lvc_' || p_table_name;

    -- Build cache table: key columns + value columns + last_updated
    FOREACH v_col IN ARRAY p_key_columns LOOP
        SELECT format_type(a.atttypid, a.atttypmod) INTO v_col_type
        FROM pg_attribute a JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = p_schema_name AND c.relname = p_table_name AND a.attname = v_col;
        v_col_defs := v_col_defs || format('%I %s NOT NULL, ', v_col, v_col_type);
    END LOOP;

    FOREACH v_col IN ARRAY p_value_columns LOOP
        SELECT format_type(a.atttypid, a.atttypmod) INTO v_col_type
        FROM pg_attribute a JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = p_schema_name AND c.relname = p_table_name AND a.attname = v_col;
        v_col_defs := v_col_defs || format('%I %s, ', v_col, v_col_type);
    END LOOP;

    v_col_defs := v_col_defs || 'last_updated TIMESTAMPTZ NOT NULL DEFAULT now()';

    -- Create cache table with PK on key columns
    EXECUTE format('CREATE TABLE %I.%I (%s, PRIMARY KEY (%s))',
        p_schema_name, v_cache_name, v_col_defs,
        array_to_string(p_key_columns, ', '));

    -- Create trigger on ChronoTable
    EXECUTE format(
        'CREATE OR REPLACE TRIGGER trg_lakets_lvc AFTER INSERT ON %I.%I
         FOR EACH ROW EXECUTE FUNCTION lakets._lvc_trigger_fn()',
        p_schema_name, p_table_name
    );

    -- Register in LVC registry
    INSERT INTO lakets._lvc_registry (chronotable_id, cache_table_name, key_columns, value_columns)
    VALUES (v_ht_id, v_cache_name, p_key_columns, p_value_columns);
END;
$$;

-- ---------------------------------------------------------------------------
-- disable_lvc: Removes cache table and trigger.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.disable_lvc(
    p_table_name TEXT,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_ht_id INT;
    v_cache_name TEXT;
BEGIN
    SELECT hr.id, lr.cache_table_name INTO v_ht_id, v_cache_name
    FROM lakets._chronotable_registry hr
    JOIN lakets._lvc_registry lr ON hr.id = lr.chronotable_id
    WHERE hr.schema_name = p_schema_name AND hr.table_name = p_table_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'LVC not enabled for %.%', p_schema_name, p_table_name;
    END IF;

    EXECUTE format('DROP TRIGGER IF EXISTS trg_lakets_lvc ON %I.%I', p_schema_name, p_table_name);
    EXECUTE format('DROP TABLE IF EXISTS %I.%I', p_schema_name, v_cache_name);
    DELETE FROM lakets._lvc_registry WHERE chronotable_id = v_ht_id;
END;
$$;

-- ---------------------------------------------------------------------------
-- latest_values: Reads from LVC cache table. Sub-10ms.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.latest_values(
    p_table_name TEXT,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS SETOF RECORD
LANGUAGE plpgsql
AS $$
DECLARE
    v_cache_name TEXT;
BEGIN
    SELECT lr.cache_table_name INTO v_cache_name
    FROM lakets._chronotable_registry hr
    JOIN lakets._lvc_registry lr ON hr.id = lr.chronotable_id
    WHERE hr.schema_name = p_schema_name AND hr.table_name = p_table_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'LVC not enabled for %.%', p_schema_name, p_table_name;
    END IF;

    RETURN QUERY EXECUTE format('SELECT * FROM %I.%I ORDER BY last_updated DESC',
        p_schema_name, v_cache_name);
END;
$$;

-- ---------------------------------------------------------------------------
-- lvc_stats: Cache statistics across all LVC-enabled tables.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.lvc_stats()
RETURNS TABLE (
    chronotable TEXT,
    cache_table TEXT,
    cached_series BIGINT,
    key_columns TEXT,
    value_columns TEXT,
    enabled BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_rec RECORD;
    v_count BIGINT;
BEGIN
    FOR v_rec IN
        SELECT hr.schema_name, hr.table_name, lr.cache_table_name,
               lr.key_columns, lr.value_columns, lr.enabled
        FROM lakets._lvc_registry lr
        JOIN lakets._chronotable_registry hr ON lr.chronotable_id = hr.id
    LOOP
        BEGIN
            EXECUTE format('SELECT count(*) FROM %I.%I',
                v_rec.schema_name, v_rec.cache_table_name) INTO v_count;
        EXCEPTION WHEN OTHERS THEN
            v_count := 0;
        END;

        chronotable := v_rec.schema_name || '.' || v_rec.table_name;
        cache_table := v_rec.cache_table_name;
        cached_series := v_count;
        key_columns := array_to_string(v_rec.key_columns, ', ');
        value_columns := array_to_string(v_rec.value_columns, ', ');
        enabled := v_rec.enabled;
        RETURN NEXT;
    END LOOP;
END;
$$;


-- ######################################################################
-- ## MODULE: 10_downsample.sql
-- ######################################################################

-- =============================================================================
-- LakeTS Downsampling Pipeline Registry
-- Metadata-only on Lakebase; execution happens on Databricks.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- create_downsample_pipeline: Registers a multi-resolution pipeline.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.create_downsample_pipeline(
    p_name TEXT,
    p_source_table TEXT,
    p_intervals INTERVAL[],
    p_retention INTERVAL[],
    p_agg_expressions TEXT[],
    p_group_by TEXT[] DEFAULT NULL,
    p_delta_catalog TEXT DEFAULT 'main',
    p_delta_schema TEXT DEFAULT 'lakets_rollups',
    p_source_schema TEXT DEFAULT 'public'
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_id INT;
BEGIN
    IF array_length(p_intervals, 1) != array_length(p_retention, 1) THEN
        RAISE EXCEPTION 'intervals and retention arrays must have same length';
    END IF;

    IF EXISTS (SELECT 1 FROM lakets._downsample_registry WHERE name = p_name) THEN
        RAISE EXCEPTION 'Downsample pipeline % already exists', p_name;
    END IF;

    INSERT INTO lakets._downsample_registry
        (name, source_table, source_schema, intervals, retention,
         agg_expressions, group_by, delta_catalog, delta_schema)
    VALUES (p_name, p_source_table, p_source_schema, p_intervals, p_retention,
            p_agg_expressions, p_group_by, p_delta_catalog, p_delta_schema)
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$;

-- ---------------------------------------------------------------------------
-- show_downsample_pipelines: Lists all registered pipelines.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.show_downsample_pipelines()
RETURNS TABLE (
    name TEXT,
    source TEXT,
    intervals TEXT,
    retention TEXT,
    agg_expressions TEXT,
    delta_target TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        dr.name,
        dr.source_schema || '.' || dr.source_table,
        array_to_string(dr.intervals::TEXT[], ', '),
        array_to_string(dr.retention::TEXT[], ', '),
        array_to_string(dr.agg_expressions, ', '),
        dr.delta_catalog || '.' || dr.delta_schema
    FROM lakets._downsample_registry dr
    ORDER BY dr.name;
END;
$$;

-- ---------------------------------------------------------------------------
-- query_auto_resolution: Returns the best Delta table for a given time range.
-- Picks the finest resolution whose retention covers the requested range.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.query_auto_resolution(
    p_name TEXT,
    p_start TIMESTAMPTZ,
    p_end TIMESTAMPTZ DEFAULT now()
)
RETURNS TABLE (
    resolution INTERVAL,
    delta_table TEXT,
    covers_range BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_rec RECORD;
    v_range INTERVAL;
    v_i INT;
BEGIN
    SELECT * INTO v_rec FROM lakets._downsample_registry WHERE name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Downsample pipeline % not found', p_name;
    END IF;

    v_range := p_end - p_start;

    -- Check each resolution from finest to coarsest
    FOR v_i IN 1..array_length(v_rec.intervals, 1) LOOP
        resolution := v_rec.intervals[v_i];
        delta_table := format('%s.%s.%s_%s',
            v_rec.delta_catalog, v_rec.delta_schema,
            v_rec.source_table,
            regexp_replace(v_rec.intervals[v_i]::TEXT, ' ', '_')
        );
        covers_range := (v_range <= v_rec.retention[v_i]);
        RETURN NEXT;
    END LOOP;

    -- Also include raw source (hot data in Lakebase)
    resolution := '0 seconds'::INTERVAL;
    delta_table := v_rec.source_schema || '.' || v_rec.source_table || ' (Lakebase hot)';
    covers_range := TRUE;
    RETURN NEXT;
END;
$$;

-- ---------------------------------------------------------------------------
-- drop_downsample_pipeline: Removes a pipeline from the registry.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.drop_downsample_pipeline(p_name TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM lakets._downsample_registry WHERE name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Downsample pipeline % not found', p_name;
    END IF;
END;
$$;


-- ######################################################################
-- ## MODULE: 11_alerts.sql
-- ######################################################################

-- =============================================================================
-- LakeTS Alert Rules
-- SQL-native alerting on hot Lakebase data.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- alert_check: Runs a query and returns rows that match alert conditions.
-- The query should return rows that are "firing" (e.g., WHERE cpu > 90).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.alert_check(
    p_name TEXT,
    p_query TEXT,
    p_severity TEXT DEFAULT 'warning'
)
RETURNS TABLE (
    alert_name TEXT,
    severity TEXT,
    fired_at TIMESTAMPTZ,
    alert_data JSONB
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_row RECORD;
BEGIN
    FOR v_row IN EXECUTE p_query LOOP
        alert_name := p_name;
        severity := p_severity;
        fired_at := now();
        alert_data := to_jsonb(v_row);
        RETURN NEXT;
    END LOOP;
END;
$$;

-- ---------------------------------------------------------------------------
-- alert_deadman: Detects series that haven't reported data within timeout.
-- Returns group keys that are "dead" (no data for > timeout).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.alert_deadman(
    p_name TEXT,
    p_table_name TEXT,
    p_group_by TEXT,
    p_timeout INTERVAL,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS TABLE (
    alert_name TEXT,
    severity TEXT,
    fired_at TIMESTAMPTZ,
    dead_key TEXT,
    last_seen TIMESTAMPTZ,
    silent_for INTERVAL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_time_col TEXT;
BEGIN
    SELECT time_column INTO v_time_col
    FROM lakets._chronotable_registry
    WHERE schema_name = p_schema_name AND table_name = p_table_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION '%.% is not a registered ChronoTable', p_schema_name, p_table_name;
    END IF;

    -- Validate p_group_by is a simple column name to prevent SQL injection
    IF p_group_by !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
        RAISE EXCEPTION 'p_group_by must be a simple column name, got: %', p_group_by;
    END IF;

    RETURN QUERY EXECUTE format(
        'SELECT %L::TEXT, ''critical''::TEXT, now(),
                %I::TEXT, max(%I), now() - max(%I)
         FROM %I.%I
         GROUP BY %I
         HAVING max(%I) < now() - %L::INTERVAL',
        p_name, p_group_by, v_time_col, v_time_col,
        p_schema_name, p_table_name,
        p_group_by, v_time_col, p_timeout::TEXT
    );
END;
$$;


-- ######################################################################
-- ## MODULE: 12_ingest.sql
-- ######################################################################

-- =============================================================================
-- LakeTS Bulk Ingest Functions
-- Server-side batch ingest for edge devices and protocol adapters.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- ingest_batch: Inserts multiple rows from a JSONB array into a ChronoTable.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.ingest_batch(
    p_table_name TEXT,
    p_data JSONB,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_row JSONB;
    v_cols TEXT := '';
    v_vals TEXT := '';
    v_key TEXT;
    v_inserted INT := 0;
    v_first BOOLEAN;
BEGIN
    IF jsonb_typeof(p_data) != 'array' THEN
        RAISE EXCEPTION 'p_data must be a JSONB array';
    END IF;

    FOR v_row IN SELECT * FROM jsonb_array_elements(p_data) LOOP
        v_cols := '';
        v_vals := '';
        v_first := TRUE;

        FOR v_key IN SELECT * FROM jsonb_object_keys(v_row) LOOP
            IF NOT v_first THEN
                v_cols := v_cols || ', ';
                v_vals := v_vals || ', ';
            END IF;
            v_cols := v_cols || format('%I', v_key);

            -- Handle different JSON types
            IF jsonb_typeof(v_row->v_key) = 'string' THEN
                v_vals := v_vals || format('%L', v_row->>v_key);
            ELSIF jsonb_typeof(v_row->v_key) = 'null' THEN
                v_vals := v_vals || 'NULL';
            ELSE
                v_vals := v_vals || format('%L', v_row->>v_key);
            END IF;
            v_first := FALSE;
        END LOOP;

        EXECUTE format('INSERT INTO %I.%I (%s) VALUES (%s)',
            p_schema_name, p_table_name, v_cols, v_vals);
        v_inserted := v_inserted + 1;
    END LOOP;

    RETURN v_inserted;
END;
$$;

-- ---------------------------------------------------------------------------
-- ingest_prometheus: Inserts a single Prometheus-style metric.
-- Target table must have: time, metric_name, labels (JSONB), value columns.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.ingest_prometheus(
    p_table_name TEXT,
    p_metric_name TEXT,
    p_labels JSONB,
    p_value DOUBLE PRECISION,
    p_timestamp TIMESTAMPTZ DEFAULT now(),
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format(
        'INSERT INTO %I.%I (time, metric_name, labels, value) VALUES ($1, $2, $3, $4)',
        p_schema_name, p_table_name
    ) USING p_timestamp, p_metric_name, p_labels, p_value;
END;
$$;


-- ######################################################################
-- ## MODULE: 13_shadow_sync.sql
-- ######################################################################

-- =============================================================================
-- LakeTS Shadow Sync
-- Shadow table + trigger pattern for Lakehouse Sync (wal2delta CDC).
-- Required because Lakehouse Sync does not support partitioned tables.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- _sync_trigger_fn: Trigger function that forwards writes to shadow table.
-- Dynamically routes based on TG_TABLE_SCHEMA and TG_TABLE_NAME.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._sync_trigger_fn()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_shadow TEXT;
    v_parent TEXT;
BEGIN
    -- For partitioned tables, TG_TABLE_NAME is the partition name.
    -- Look up the parent table to find the shadow table name.
    SELECT lakets._resolve_partition_parent(TG_TABLE_SCHEMA, TG_TABLE_NAME)
    INTO v_parent;

    SELECT shadow_table_name INTO v_shadow
    FROM lakets._chronotable_registry
    WHERE schema_name = TG_TABLE_SCHEMA
      AND table_name = COALESCE(v_parent, TG_TABLE_NAME);

    IF v_shadow IS NULL THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
    END IF;

    IF TG_OP = 'DELETE' THEN
        EXECUTE format('INSERT INTO %I.%I SELECT ($1).*', TG_TABLE_SCHEMA, v_shadow)
        USING OLD;
        RETURN OLD;
    ELSE
        EXECUTE format('INSERT INTO %I.%I SELECT ($1).*', TG_TABLE_SCHEMA, v_shadow)
        USING NEW;
        RETURN NEW;
    END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- enable_sync: Creates shadow table and trigger for Lakehouse Sync.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.enable_sync(
    p_table_name TEXT,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_chronotable_id INT;
    v_shadow_name TEXT;
    v_col_rec RECORD;
    v_col_defs TEXT := '';
    v_time_col TEXT;
BEGIN
    SELECT id, time_column INTO v_chronotable_id, v_time_col
    FROM lakets._chronotable_registry
    WHERE schema_name = p_schema_name AND table_name = p_table_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table %.% is not a registered ChronoTable',
            p_schema_name, p_table_name;
    END IF;

    v_shadow_name := '_shadow_' || p_table_name;

    -- Build column definitions from the partitioned table
    FOR v_col_rec IN
        SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) as col_type,
               a.attnotnull
        FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = p_schema_name AND c.relname = p_table_name
          AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
    LOOP
        IF v_col_defs != '' THEN v_col_defs := v_col_defs || ', '; END IF;
        v_col_defs := v_col_defs || format('%I %s', v_col_rec.attname, v_col_rec.col_type);
        IF v_col_rec.attnotnull THEN
            v_col_defs := v_col_defs || ' NOT NULL';
        END IF;
    END LOOP;

    -- Create shadow table (unpartitioned)
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (%s)',
        p_schema_name, v_shadow_name, v_col_defs);

    -- Set REPLICA IDENTITY FULL for CDC capture
    EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL',
        p_schema_name, v_shadow_name);

    -- Create trigger on the partitioned parent table
    EXECUTE format(
        'CREATE OR REPLACE TRIGGER trg_lakets_sync
         AFTER INSERT OR UPDATE OR DELETE ON %I.%I
         FOR EACH ROW EXECUTE FUNCTION lakets._sync_trigger_fn()',
        p_schema_name, p_table_name
    );

    -- Update registry
    UPDATE lakets._chronotable_registry
    SET shadow_table_name = v_shadow_name,
        sync_enabled = TRUE
    WHERE id = v_chronotable_id;
END;
$$;

-- ---------------------------------------------------------------------------
-- disable_sync: Removes shadow table and trigger.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.disable_sync(
    p_table_name TEXT,
    p_schema_name TEXT DEFAULT 'public'
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_chronotable_id INT;
    v_shadow_name TEXT;
BEGIN
    SELECT id, shadow_table_name INTO v_chronotable_id, v_shadow_name
    FROM lakets._chronotable_registry
    WHERE schema_name = p_schema_name AND table_name = p_table_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table %.% is not a registered ChronoTable',
            p_schema_name, p_table_name;
    END IF;

    -- Drop trigger
    EXECUTE format(
        'DROP TRIGGER IF EXISTS trg_lakets_sync ON %I.%I',
        p_schema_name, p_table_name
    );

    -- Drop shadow table
    IF v_shadow_name IS NOT NULL THEN
        EXECUTE format('DROP TABLE IF EXISTS %I.%I', p_schema_name, v_shadow_name);
    END IF;

    -- Update registry
    UPDATE lakets._chronotable_registry
    SET shadow_table_name = NULL,
        sync_enabled = FALSE
    WHERE id = v_chronotable_id;
END;
$$;


-- ######################################################################
-- ## MODULE: 14_rollup_optimization.sql
-- ######################################################################

-- =============================================================================
-- LakeTS RollUp Optimization — Modules 23–28
-- Smart refresh, batch processing, DAG orchestration, tier routing,
-- bulk import invalidation, and RollUp export.
--
-- Requires: 00_schema.sql, 03_rollup.sql (applied first via 99_install.sql)
-- =============================================================================

-- ═══════════════════════════════════════════════════════════════════════════════
-- SCHEMA EXTENSIONS
-- ═══════════════════════════════════════════════════════════════════════════════

-- M23: Chunk-skip pruning needs last_modified_at on chunk metadata
ALTER TABLE lakets._chunk_metadata
    ADD COLUMN IF NOT EXISTS last_modified_at TIMESTAMPTZ;

-- M23–M28: New columns on _rollup_registry
ALTER TABLE lakets._rollup_registry
    ADD COLUMN IF NOT EXISTS bucket_column       TEXT    DEFAULT 'bucket',
    ADD COLUMN IF NOT EXISTS source_time_column  TEXT,
    ADD COLUMN IF NOT EXISTS predicate_injection BOOLEAN DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS depends_on          INT[]   DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS cold_query_text     TEXT,
    ADD COLUMN IF NOT EXISTS export_enabled      BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS export_delta_table  TEXT,
    ADD COLUMN IF NOT EXISTS export_mode         TEXT    DEFAULT 'incremental',
    ADD COLUMN IF NOT EXISTS last_exported_at    TIMESTAMPTZ;

-- M26: Covering index for fast tier lookups on chunk metadata
CREATE INDEX IF NOT EXISTS idx_chunk_metadata_ct_status_range
    ON lakets._chunk_metadata(chronotable_id, status, range_start, range_end);


-- ═══════════════════════════════════════════════════════════════════════════════
-- MODULE 23: Smart Refresh Optimizer
-- ═══════════════════════════════════════════════════════════════════════════════

-- ---------------------------------------------------------------------------
-- _touch_chunk_metadata: Statement-level trigger that updates last_modified_at
-- on the chunk metadata row whenever a partition receives writes.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._touch_chunk_metadata()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE lakets._chunk_metadata
    SET last_modified_at = now()
    WHERE chunk_name = TG_TABLE_NAME;
    RETURN NULL;  -- AFTER trigger, no row modification needed
END;
$$;

-- ---------------------------------------------------------------------------
-- _get_dirty_chunks: Returns chunks with data modified since a given timestamp.
-- Used by refresh_rollup to skip unchanged chunks (chunk-skip pruning).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._get_dirty_chunks(
    p_chronotable_id  INT,
    p_since           TIMESTAMPTZ
)
RETURNS TABLE(chunk_name TEXT, range_start TIMESTAMPTZ, range_end TIMESTAMPTZ)
LANGUAGE sql STABLE
AS $$
    SELECT cm.chunk_name, cm.range_start, cm.range_end
    FROM lakets._chunk_metadata cm
    WHERE cm.chronotable_id = p_chronotable_id
      AND cm.status = 'active'
      AND (cm.last_modified_at IS NULL OR cm.last_modified_at >= p_since)
    ORDER BY cm.range_start;
$$;

-- ---------------------------------------------------------------------------
-- _detect_bucket_column: Auto-detects the time bucket column name from the
-- query output. Returns the first TIMESTAMPTZ column, or 'bucket' as fallback.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._detect_bucket_column(p_query_text TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_col RECORD;
BEGIN
    -- Create temp table from query with LIMIT 0 to inspect column types
    BEGIN
        EXECUTE format(
            'CREATE TEMP TABLE _lakets_detect_cols ON COMMIT DROP AS %s LIMIT 0',
            p_query_text
        );
    EXCEPTION WHEN OTHERS THEN
        RETURN 'bucket';  -- fallback to convention
    END;

    -- Find the first timestamp column
    FOR v_col IN
        SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS col_type
        FROM pg_attribute a
        WHERE a.attrelid = '_lakets_detect_cols'::regclass
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
    LOOP
        IF v_col.col_type IN ('timestamp with time zone', 'timestamp without time zone') THEN
            DROP TABLE IF EXISTS _lakets_detect_cols;
            RETURN v_col.attname;
        END IF;
    END LOOP;

    DROP TABLE IF EXISTS _lakets_detect_cols;
    RETURN 'bucket';
END;
$$;

-- ---------------------------------------------------------------------------
-- _inject_time_predicate: Best-effort injection of a time predicate into the
-- inner source query for scan-level partition pruning.
-- Falls back to the original query if injection fails EXPLAIN validation.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._inject_time_predicate(
    p_query_text  TEXT,
    p_time_column TEXT,
    p_dirty_from  TIMESTAMPTZ
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_predicate TEXT;
    v_injected  TEXT;
BEGIN
    IF p_time_column IS NULL THEN
        RETURN p_query_text;
    END IF;

    v_predicate := format('%I >= %L', p_time_column, p_dirty_from);

    -- Strategy: append to existing WHERE or insert before GROUP BY
    -- Note: PostgreSQL uses \y for word boundary (not \b which is backspace in POSIX regex)
    IF p_query_text ~* '\yWHERE\y' THEN
        v_injected := regexp_replace(
            p_query_text,
            '(\yWHERE\y\s+)',
            format('\1%s AND ', v_predicate),
            'i'
        );
    ELSIF p_query_text ~* '\yGROUP\s+BY\y' THEN
        v_injected := regexp_replace(
            p_query_text,
            '(\yGROUP\s+BY\y)',
            format('WHERE %s \1', v_predicate),
            'i'
        );
    ELSE
        -- No WHERE, no GROUP BY — append WHERE at the end
        v_injected := p_query_text || format(' WHERE %s', v_predicate);
    END IF;

    -- Validate: run EXPLAIN to verify syntactic correctness
    BEGIN
        EXECUTE format('EXPLAIN %s', v_injected);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'LakeTS: predicate injection failed, falling back to outer filter. Error: %', SQLERRM;
        RETURN p_query_text;
    END;

    RETURN v_injected;
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- MODULE 24: Batch Set-Based Bucket Refresh
-- ═══════════════════════════════════════════════════════════════════════════════

-- ---------------------------------------------------------------------------
-- _refresh_buckets_batch: Refreshes all dirty buckets in 2 SQL statements
-- (1 DELETE + 1 INSERT) using ANY(array) predicates.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._refresh_buckets_batch(
    p_rollup_id       INT,
    p_rollup_table    TEXT,
    p_query_text      TEXT,
    p_bucket_column   TEXT,
    p_dirty_buckets   TIMESTAMPTZ[]
)
RETURNS INT
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_dirty_buckets IS NULL OR array_length(p_dirty_buckets, 1) IS NULL THEN
        RETURN 0;
    END IF;

    -- Single DELETE for all dirty buckets
    EXECUTE format(
        'DELETE FROM public.%I WHERE %I = ANY($1)',
        p_rollup_table, p_bucket_column
    ) USING p_dirty_buckets;

    -- Single INSERT for all dirty buckets
    EXECUTE format(
        'INSERT INTO public.%I SELECT * FROM (%s) _q WHERE _q.%I = ANY($1)',
        p_rollup_table, p_query_text, p_bucket_column
    ) USING p_dirty_buckets;

    RETURN array_length(p_dirty_buckets, 1);
END;
$$;

-- ---------------------------------------------------------------------------
-- _refresh_buckets_chunked: Splits large dirty sets into chunks and
-- batch-refreshes each chunk. Avoids planner degradation on large arrays.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._refresh_buckets_chunked(
    p_rollup_id       INT,
    p_rollup_table    TEXT,
    p_query_text      TEXT,
    p_bucket_column   TEXT,
    p_dirty_buckets   TIMESTAMPTZ[],
    p_chunk_size      INT DEFAULT 100
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_total     INT := array_length(p_dirty_buckets, 1);
    v_offset    INT := 1;
    v_chunk     TIMESTAMPTZ[];
    v_refreshed INT := 0;
BEGIN
    IF v_total IS NULL OR v_total = 0 THEN
        RETURN 0;
    END IF;

    -- Small enough for a single batch
    IF v_total <= p_chunk_size THEN
        RETURN lakets._refresh_buckets_batch(
            p_rollup_id, p_rollup_table, p_query_text,
            p_bucket_column, p_dirty_buckets
        );
    END IF;

    -- Process in chunks
    WHILE v_offset <= v_total LOOP
        v_chunk := p_dirty_buckets[v_offset : LEAST(v_offset + p_chunk_size - 1, v_total)];

        v_refreshed := v_refreshed + lakets._refresh_buckets_batch(
            p_rollup_id, p_rollup_table, p_query_text,
            p_bucket_column, v_chunk
        );

        v_offset := v_offset + p_chunk_size;
    END LOOP;

    RETURN v_refreshed;
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- MODULE 25: RollUp DAG Orchestrator
-- ═══════════════════════════════════════════════════════════════════════════════

-- ---------------------------------------------------------------------------
-- _validate_rollup_dependencies: Trigger that validates dependency references
-- on INSERT/UPDATE to _rollup_registry. Prevents self-deps and missing refs.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._validate_rollup_dependencies()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_dep_id INT;
BEGIN
    IF NEW.depends_on IS NOT NULL AND array_length(NEW.depends_on, 1) > 0 THEN
        FOREACH v_dep_id IN ARRAY NEW.depends_on LOOP
            IF NOT EXISTS (SELECT 1 FROM lakets._rollup_registry WHERE id = v_dep_id) THEN
                RAISE EXCEPTION 'RollUp dependency ID % does not exist', v_dep_id;
            END IF;
            IF v_dep_id = NEW.id THEN
                RAISE EXCEPTION 'RollUp cannot depend on itself (ID %)', v_dep_id;
            END IF;
        END LOOP;
    END IF;
    RETURN NEW;
END;
$$;

-- Install the validation trigger (idempotent)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_validate_rollup_deps'
          AND tgrelid = 'lakets._rollup_registry'::regclass
    ) THEN
        CREATE TRIGGER trg_validate_rollup_deps
            BEFORE INSERT OR UPDATE ON lakets._rollup_registry
            FOR EACH ROW
            EXECUTE FUNCTION lakets._validate_rollup_dependencies();
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- _build_rollup_dag: Topological sort of the RollUp dependency graph.
-- Returns an ordered array of rollup IDs (dependencies first).
-- Raises exception on circular dependencies.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._build_rollup_dag(
    p_root_ids INT[] DEFAULT NULL
)
RETURNS INT[]
LANGUAGE plpgsql
AS $$
DECLARE
    v_all_ids   INT[];
    v_ordered   INT[] := '{}';
    v_queue     INT[] := '{}';
    v_in_deg    INT[];
    v_id        INT;
    v_dep_id    INT;
    v_idx       INT;
    v_dep_idx   INT;
    v_deps      INT[];
    v_processed INT := 0;
BEGIN
    -- Gather all RollUp IDs in scope
    IF p_root_ids IS NULL THEN
        SELECT array_agg(id ORDER BY id) INTO v_all_ids
        FROM lakets._rollup_registry;
    ELSE
        -- Expand: include roots + all their transitive dependencies
        WITH RECURSIVE dep_tree AS (
            SELECT id, depends_on
            FROM lakets._rollup_registry
            WHERE id = ANY(p_root_ids)
            UNION
            SELECT r.id, r.depends_on
            FROM lakets._rollup_registry r
            JOIN dep_tree dt ON r.id = ANY(dt.depends_on)
        )
        SELECT array_agg(DISTINCT id ORDER BY id) INTO v_all_ids FROM dep_tree;
    END IF;

    IF v_all_ids IS NULL OR array_length(v_all_ids, 1) IS NULL THEN
        RETURN '{}';
    END IF;

    -- Compute in-degrees (how many dependencies each node has within the set)
    v_in_deg := array_fill(0, ARRAY[array_length(v_all_ids, 1)]);

    FOR v_idx IN 1..array_length(v_all_ids, 1) LOOP
        SELECT depends_on INTO v_deps
        FROM lakets._rollup_registry
        WHERE id = v_all_ids[v_idx];

        IF v_deps IS NOT NULL THEN
            FOREACH v_dep_id IN ARRAY v_deps LOOP
                -- Only count deps within our set
                v_dep_idx := array_position(v_all_ids, v_dep_id);
                IF v_dep_idx IS NOT NULL THEN
                    v_in_deg[v_idx] := v_in_deg[v_idx] + 1;
                END IF;
            END LOOP;
        END IF;
    END LOOP;

    -- Seed queue with nodes that have zero in-degree
    FOR v_idx IN 1..array_length(v_all_ids, 1) LOOP
        IF v_in_deg[v_idx] = 0 THEN
            v_queue := array_append(v_queue, v_idx);
        END IF;
    END LOOP;

    -- Kahn's algorithm
    WHILE array_length(v_queue, 1) > 0 LOOP
        v_idx := v_queue[1];
        v_queue := v_queue[2:];
        v_ordered := array_append(v_ordered, v_all_ids[v_idx]);
        v_processed := v_processed + 1;

        -- For each node that depends on v_all_ids[v_idx], decrement in-degree
        FOR v_dep_idx IN 1..array_length(v_all_ids, 1) LOOP
            SELECT depends_on INTO v_deps
            FROM lakets._rollup_registry
            WHERE id = v_all_ids[v_dep_idx];

            IF v_deps IS NOT NULL AND v_all_ids[v_idx] = ANY(v_deps) THEN
                v_in_deg[v_dep_idx] := v_in_deg[v_dep_idx] - 1;
                IF v_in_deg[v_dep_idx] = 0 THEN
                    v_queue := array_append(v_queue, v_dep_idx);
                END IF;
            END IF;
        END LOOP;
    END LOOP;

    -- Cycle detection
    IF v_processed != array_length(v_all_ids, 1) THEN
        RAISE EXCEPTION 'Circular dependency detected in RollUp DAG. Processed % of % nodes.',
            v_processed, array_length(v_all_ids, 1);
    END IF;

    RETURN v_ordered;
END;
$$;

-- ---------------------------------------------------------------------------
-- refresh_rollup_cascade: Refreshes all RollUps in dependency order.
-- If p_name is NULL, refreshes ALL RollUps. Otherwise refreshes the named
-- RollUp and all its transitive dependencies.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.refresh_rollup_cascade(
    p_name TEXT DEFAULT NULL
)
RETURNS TABLE(rollup_name TEXT, refreshed BOOLEAN, refresh_ms FLOAT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_root_ids  INT[];
    v_dag       INT[];
    v_id        INT;
    v_rname     TEXT;
    v_start     TIMESTAMPTZ;
    v_result    BOOLEAN;
BEGIN
    IF p_name IS NOT NULL THEN
        SELECT ARRAY[id] INTO v_root_ids
        FROM lakets._rollup_registry WHERE name = p_name;
        IF v_root_ids IS NULL THEN
            RAISE EXCEPTION 'RollUp % not found', p_name;
        END IF;
    END IF;

    v_dag := lakets._build_rollup_dag(v_root_ids);

    FOREACH v_id IN ARRAY v_dag LOOP
        SELECT r.name INTO v_rname
        FROM lakets._rollup_registry r WHERE r.id = v_id;

        v_start := clock_timestamp();
        v_result := lakets.refresh_rollup(v_rname);

        rollup_name := v_rname;
        refreshed := v_result;
        refresh_ms := extract(epoch FROM clock_timestamp() - v_start) * 1000;
        RETURN NEXT;
    END LOOP;
END;
$$;

-- ---------------------------------------------------------------------------
-- show_rollup_dag: Human-readable DAG visualization.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.show_rollup_dag()
RETURNS TABLE(
    rollup_name     TEXT,
    depends_on_names TEXT[],
    refresh_order   INT,
    bucket_interval INTERVAL,
    last_refreshed  TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_dag  INT[];
    v_id   INT;
    v_ord  INT := 0;
BEGIN
    v_dag := lakets._build_rollup_dag(NULL);

    FOREACH v_id IN ARRAY v_dag LOOP
        v_ord := v_ord + 1;

        SELECT
            r.name,
            (SELECT array_agg(r2.name)
             FROM lakets._rollup_registry r2
             WHERE r2.id = ANY(r.depends_on)),
            v_ord,
            r.bucket_interval,
            r.last_refreshed_at
        INTO rollup_name, depends_on_names, refresh_order, bucket_interval, last_refreshed
        FROM lakets._rollup_registry r
        WHERE r.id = v_id;

        RETURN NEXT;
    END LOOP;
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- MODULE 26: Intelligent Tier Router
-- ═══════════════════════════════════════════════════════════════════════════════

-- ---------------------------------------------------------------------------
-- _resolve_bucket_tier: Auto-detects whether a bucket's source data is in
-- the hot tier (Lakebase) or cold tier (Delta Lake) by checking chunk status.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._resolve_bucket_tier(
    p_chronotable_id  INT,
    p_bucket_start    TIMESTAMPTZ
)
RETURNS TEXT
LANGUAGE sql STABLE
AS $$
    SELECT CASE
        WHEN cm.status IN ('active', 'compressed') THEN 'hot'
        WHEN cm.status = 'tiered' THEN 'cold'
        ELSE 'hot'
    END
    FROM lakets._chunk_metadata cm
    WHERE cm.chronotable_id = p_chronotable_id
      AND p_bucket_start >= cm.range_start
      AND p_bucket_start < cm.range_end
    LIMIT 1;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- MODULE 27: Bulk Import Invalidation
-- ═══════════════════════════════════════════════════════════════════════════════

-- ---------------------------------------------------------------------------
-- _bulk_import_invalidation: Statement-level AFTER INSERT trigger.
-- Uses REFERENCING NEW TABLE to capture the time range of all inserted rows
-- (including COPY FROM) and auto-invalidates affected RollUp buckets.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets._bulk_import_invalidation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_min_time    TIMESTAMPTZ;
    v_max_time    TIMESTAMPTZ;
    v_time_col    TEXT;
    v_ct_id       INT;
    v_parent      TEXT;
    v_rollup      RECORD;
BEGIN
    -- Resolve partition parent using shared helper
    SELECT lakets._resolve_partition_parent(TG_TABLE_SCHEMA, TG_TABLE_NAME)
    INTO v_parent;

    -- Get the time column and ChronoTable ID
    SELECT cr.id, cr.time_column INTO v_ct_id, v_time_col
    FROM lakets._chronotable_registry cr
    WHERE cr.schema_name = TG_TABLE_SCHEMA
      AND cr.table_name = COALESCE(v_parent, TG_TABLE_NAME);

    IF v_time_col IS NULL THEN
        RETURN NULL;
    END IF;

    -- Find time range of inserted rows via transition table
    EXECUTE format('SELECT min(%I), max(%I) FROM _new_rows', v_time_col, v_time_col)
        INTO v_min_time, v_max_time;

    IF v_min_time IS NULL THEN
        RETURN NULL;
    END IF;

    -- Invalidate all RollUps that source from this ChronoTable
    FOR v_rollup IN
        SELECT r.id, r.name, r.bucket_interval, r.watermark
        FROM lakets._rollup_registry r
        WHERE r.source_chronotable_id = v_ct_id
          AND r.refresh_mode = 'incremental'
    LOOP
        -- Only invalidate buckets below the watermark (above is handled by Phase 1)
        IF v_min_time < COALESCE(v_rollup.watermark, 'infinity'::timestamptz) THEN
            PERFORM lakets.invalidate_rollup_range(
                v_rollup.name,
                v_min_time,
                LEAST(v_max_time, COALESCE(v_rollup.watermark, v_max_time))
                    + v_rollup.bucket_interval
            );
        END IF;
    END LOOP;

    -- Also touch chunk metadata for chunk-skip tracking (M23)
    UPDATE lakets._chunk_metadata
    SET last_modified_at = now()
    WHERE chunk_name = TG_TABLE_NAME;

    RETURN NULL;
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- MODULE 28: RollUp Export Pipeline (Lakebase API)
-- ═══════════════════════════════════════════════════════════════════════════════

-- ---------------------------------------------------------------------------
-- enable_rollup_export: Enables periodic export of RollUp rows to Delta Lake.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.enable_rollup_export(
    p_rollup_name  TEXT,
    p_delta_table  TEXT,
    p_export_mode  TEXT DEFAULT 'incremental'
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_export_mode NOT IN ('full', 'incremental') THEN
        RAISE EXCEPTION 'export_mode must be ''full'' or ''incremental'', got: %', p_export_mode;
    END IF;

    UPDATE lakets._rollup_registry
    SET export_enabled = TRUE,
        export_delta_table = p_delta_table,
        export_mode = p_export_mode
    WHERE name = p_rollup_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'RollUp % not found', p_rollup_name;
    END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- disable_rollup_export: Disables RollUp export.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.disable_rollup_export(p_rollup_name TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE lakets._rollup_registry
    SET export_enabled = FALSE
    WHERE name = p_rollup_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'RollUp % not found', p_rollup_name;
    END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- show_rollup_exports: Shows export status and lag for all export-enabled RollUps.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.show_rollup_exports()
RETURNS TABLE(
    rollup_name      TEXT,
    delta_table      TEXT,
    export_mode      TEXT,
    last_exported_at TIMESTAMPTZ,
    watermark        TIMESTAMPTZ,
    export_lag       INTERVAL
)
LANGUAGE sql STABLE
AS $$
    SELECT
        r.name,
        r.export_delta_table,
        r.export_mode,
        r.last_exported_at,
        r.watermark,
        CASE
            WHEN r.last_exported_at IS NOT NULL AND r.watermark IS NOT NULL
            THEN r.watermark - r.last_exported_at
            ELSE NULL
        END
    FROM lakets._rollup_registry r
    WHERE r.export_enabled = TRUE
    ORDER BY r.name;
$$;


-- ######################################################################
-- ## MODULE: 15_uc_integration.sql
-- ######################################################################

-- =============================================================================
-- LakeTS Unity Catalog Integration -- Module 14
-- Auto-register and tag Delta tables written by LakeTS under Unity Catalog.
--
-- Requires: 00_schema.sql, 13_rollup_optimization.sql (applied first via 99_install.sql)
-- =============================================================================

-- Schema extension: UC registration tracking table
CREATE TABLE IF NOT EXISTS lakets._uc_registry (
    id              SERIAL PRIMARY KEY,
    rollup_name     TEXT NOT NULL,
    uc_catalog      TEXT NOT NULL,
    uc_schema       TEXT NOT NULL,
    uc_table        TEXT NOT NULL,
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_tagged_at  TIMESTAMPTZ,
    tags            JSONB NOT NULL DEFAULT '{}',
    UNIQUE(rollup_name),
    UNIQUE(uc_catalog, uc_schema, uc_table)
);

CREATE INDEX IF NOT EXISTS idx_uc_registry_rollup_name
    ON lakets._uc_registry(rollup_name);

-- ---------------------------------------------------------------------------
-- register_uc_table: Record that a RollUp Delta export has been registered
-- in Unity Catalog (called by uc_registration.py after the REST API call).
-- Returns the registry row id.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.register_uc_table(
    p_rollup_name TEXT,
    p_uc_catalog  TEXT,
    p_uc_schema   TEXT
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_rollup     lakets._rollup_registry%ROWTYPE;
    v_uc_table   TEXT;
    v_id         INT;
BEGIN
    SELECT * INTO v_rollup
    FROM lakets._rollup_registry
    WHERE name = p_rollup_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'RollUp % not found in _rollup_registry', p_rollup_name;
    END IF;

    IF v_rollup.export_delta_table IS NULL THEN
        RAISE EXCEPTION
            'RollUp % has no export_delta_table configured; enable export first',
            p_rollup_name;
    END IF;

    -- Derive UC table name from leaf of the Delta table 3-part name
    v_uc_table := split_part(v_rollup.export_delta_table, '.', 3);
    IF v_uc_table = '' THEN
        v_uc_table := regexp_replace(p_rollup_name, '[^a-zA-Z0-9_]', '_', 'g');
    END IF;

    INSERT INTO lakets._uc_registry (rollup_name, uc_catalog, uc_schema, uc_table, registered_at)
    VALUES (p_rollup_name, p_uc_catalog, p_uc_schema, v_uc_table, now())
    ON CONFLICT (rollup_name) DO UPDATE
        SET uc_catalog    = EXCLUDED.uc_catalog,
            uc_schema     = EXCLUDED.uc_schema,
            uc_table      = EXCLUDED.uc_table,
            registered_at = now()
    RETURNING id INTO v_id;

    RAISE NOTICE 'Registered % -> %.%.%', p_rollup_name, p_uc_catalog, p_uc_schema, v_uc_table;
    RETURN v_id;
END;
$$;

-- ---------------------------------------------------------------------------
-- tag_uc_table: Persist UC tag metadata. Merges system tags
-- (lakets.source, lakets.version, lakets.rollup_name) with user tags.
-- User tags take precedence on key collision.
-- Returns the merged tag set.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.tag_uc_table(
    p_rollup_name TEXT,
    p_tags        JSONB DEFAULT '{}'
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_version     TEXT;
    v_system_tags JSONB;
    v_merged_tags JSONB;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM lakets._uc_registry WHERE rollup_name = p_rollup_name) THEN
        RAISE EXCEPTION
            'RollUp % not found in _uc_registry; call register_uc_table first',
            p_rollup_name;
    END IF;

    SELECT version INTO v_version FROM lakets._version LIMIT 1;

    v_system_tags := jsonb_build_object(
        'lakets.source',      'lakets',
        'lakets.version',     COALESCE(v_version, 'unknown'),
        'lakets.rollup_name', p_rollup_name
    );

    v_merged_tags := v_system_tags || p_tags;

    UPDATE lakets._uc_registry
    SET tags           = v_merged_tags,
        last_tagged_at = now()
    WHERE rollup_name = p_rollup_name;

    RETURN v_merged_tags;
END;
$$;

-- ---------------------------------------------------------------------------
-- get_uc_registrations: Return all UC-registered exports (optional filter).
-- Used by uc_registration.py to build its work list.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.get_uc_registrations(
    p_rollup_name TEXT DEFAULT NULL
)
RETURNS TABLE (
    rollup_name    TEXT,
    uc_catalog     TEXT,
    uc_schema      TEXT,
    uc_table       TEXT,
    full_uc_name   TEXT,
    delta_table    TEXT,
    registered_at  TIMESTAMPTZ,
    last_tagged_at TIMESTAMPTZ,
    tags           JSONB
)
LANGUAGE sql
STABLE
AS $$
    SELECT
        r.rollup_name,
        r.uc_catalog,
        r.uc_schema,
        r.uc_table,
        r.uc_catalog || '.' || r.uc_schema || '.' || r.uc_table AS full_uc_name,
        rr.export_delta_table                                     AS delta_table,
        r.registered_at,
        r.last_tagged_at,
        r.tags
    FROM lakets._uc_registry r
    JOIN lakets._rollup_registry rr ON rr.name = r.rollup_name
    WHERE p_rollup_name IS NULL OR r.rollup_name = p_rollup_name
    ORDER BY r.rollup_name;
$$;

-- ---------------------------------------------------------------------------
-- unregister_uc_table: Remove a UC registration record.
-- Does NOT drop the actual UC table in Databricks.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakets.unregister_uc_table(
    p_rollup_name TEXT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted INT;
BEGIN
    DELETE FROM lakets._uc_registry WHERE rollup_name = p_rollup_name;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    IF v_deleted = 0 THEN
        RAISE NOTICE 'No UC registration found for %', p_rollup_name;
        RETURN FALSE;
    END IF;
    RAISE NOTICE 'Removed UC registration for %', p_rollup_name;
    RETURN TRUE;
END;
$$;


-- ######################################################################
-- ## INSTALLATION VERIFICATION
-- ######################################################################

UPDATE lakets._version
SET modules = ARRAY['00_version.sql','01_schema.sql','02_chronotable.sql','03_timeseries_functions.sql','04_rollup.sql','05_compression.sql','06_retention.sql','07_monitoring.sql','08_metric_table.sql','09_lvc.sql','10_downsample.sql','11_alerts.sql','12_ingest.sql','13_shadow_sync.sql','14_rollup_optimization.sql','15_uc_integration.sql']
WHERE version = '0.1.2'
  AND installed_at = (
      SELECT MAX(installed_at) FROM lakets._version WHERE version = '0.1.2'
  );

DO $$
DECLARE
    v_func_count  INT;
    v_table_count INT;
    v_version     TEXT;
BEGIN
    SELECT version INTO v_version
    FROM lakets._version ORDER BY installed_at DESC LIMIT 1;

    SELECT count(*) INTO v_func_count
    FROM information_schema.routines WHERE routine_schema = 'lakets';

    SELECT count(*) INTO v_table_count
    FROM information_schema.tables WHERE table_schema = 'lakets';

    RAISE NOTICE '===========================================';
    RAISE NOTICE 'LakeTS v% installed successfully', v_version;
    RAISE NOTICE '  Functions: %', v_func_count;
    RAISE NOTICE '  Tables:    %', v_table_count;
    RAISE NOTICE '===========================================';
END $$;
