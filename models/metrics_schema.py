"""SSOT for the streaming-metrics external Delta table (spec §1.11).

Streaming metrics for the real-time and micro-batch Spark modes are captured by
a ``StreamingQueryListener`` and flushed — buffered, async — to a **UC EXTERNAL
Delta table** via delta-rs (pure Python, so no Spark job contends with the
streaming query).

delta-rs cannot write VARIANT, so the raw progress payload is stored as a
``STRING`` column (``progress``) and read back with ``parse_json(progress):field``
via DBSQL.  The remaining columns are the typed values extracted from each
``onQueryProgress`` event, plus ``query_id`` / ``query_name`` / ``run_id`` so a
single per-industry table can be tagged by streaming mode.

This module is the single source of truth for that table's column set and DDL so
the provisioning engine (which creates the table) and the Phase-4 metrics writer
(which appends to it) can never drift.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = [
    "MetricsFieldSpec",
    "METRICS_FIELD_MAP",
    "METRICS_SCHEMA",
    "METRICS_TABLE",
    "metrics_columns",
    "metrics_external_table_ddl",
    "metrics_table_location",
    "metrics_table_fqn",
]

# Fixed schema + table name for the per-industry metrics table.
METRICS_SCHEMA = "metrics"
METRICS_TABLE = "streaming_metrics"


@dataclass(frozen=True)
class MetricsFieldSpec:
    """One metrics column mapped to its Delta type."""

    column: str
    delta_type: str


# The canonical metrics column set (spec §1.11).  ``progress`` is the raw JSON
# payload kept as STRING (delta-rs can't write VARIANT); the rest are the typed
# values extracted from ``StreamingQueryProgress`` — the ``*_p50/p99`` latency
# percentiles are Databricks-runtime-only fields.
METRICS_FIELD_MAP: tuple[MetricsFieldSpec, ...] = (
    MetricsFieldSpec("query_id", "STRING"),
    MetricsFieldSpec("query_name", "STRING"),
    MetricsFieldSpec("run_id", "STRING"),
    MetricsFieldSpec("batch_id", "BIGINT"),
    MetricsFieldSpec("timestamp", "TIMESTAMP"),
    MetricsFieldSpec("num_input_rows", "BIGINT"),
    MetricsFieldSpec("input_rps", "DOUBLE"),
    MetricsFieldSpec("processed_rps", "DOUBLE"),
    MetricsFieldSpec("trigger_ms", "DOUBLE"),
    MetricsFieldSpec("e2e_p50", "DOUBLE"),
    MetricsFieldSpec("e2e_p99", "DOUBLE"),
    MetricsFieldSpec("processing_p50", "DOUBLE"),
    MetricsFieldSpec("processing_p99", "DOUBLE"),
    MetricsFieldSpec("queuing_p50", "DOUBLE"),
    MetricsFieldSpec("queuing_p99", "DOUBLE"),
    # Raw StreamingQueryProgress JSON — parse_json(progress):field at read time.
    MetricsFieldSpec("progress", "STRING"),
)


def metrics_columns() -> tuple[str, ...]:
    """Return the metrics column names in canonical order."""
    return tuple(spec.column for spec in METRICS_FIELD_MAP)


def metrics_table_fqn(catalog: str) -> str:
    """Return the fully-qualified metrics table name for ``catalog``."""
    return f"{catalog}.{METRICS_SCHEMA}.{METRICS_TABLE}"


def metrics_table_location(catalog: str, storage_base: str) -> str:
    """Return the S3 location for the external metrics table.

    ``storage_base`` is the shared external-location root (e.g.
    ``s3://taran-shared-interop``).  Layout (spec §1.11):
    ``<base>/iot-digital-twin/<catalog>/metrics/streaming_metrics/``.
    """
    base = storage_base.rstrip("/")
    return f"{base}/iot-digital-twin/{catalog}/metrics/{METRICS_TABLE}/"


def metrics_external_table_ddl(catalog: str, storage_base: str) -> str:
    """Return the ``CREATE TABLE ... USING DELTA LOCATION`` DDL (external).

    The explicit ``LOCATION`` makes this an EXTERNAL table so delta-rs can write
    to it with UC temporary-table-credentials (requires ``EXTERNAL USE SCHEMA``
    on the metrics schema).
    """
    fqn = metrics_table_fqn(catalog)
    location = metrics_table_location(catalog, storage_base)
    columns = ",\n".join(f"    {s.column} {s.delta_type}" for s in METRICS_FIELD_MAP)
    return (
        f"CREATE TABLE IF NOT EXISTS {fqn} (\n{columns}\n) "
        f"USING DELTA\nLOCATION '{location}'"
    )
