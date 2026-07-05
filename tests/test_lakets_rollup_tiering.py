"""Tests for services.lakets_service.create_rollup (RollUp creation).

The asyncpg pool is mocked with an ``AsyncMock``; we assert that
``create_rollup`` calls ``lakets.create_rollup`` with the mapped arguments and
is idempotent against ``lakets._rollup_registry``.
"""

from __future__ import annotations

import datetime as _dt
from unittest.mock import AsyncMock

import pytest

from services import lakets_service


def _pool(*, existing: bool = False) -> AsyncMock:
    """Mocked asyncpg pool.

    ``fetchval`` is used both for the existence probe and for the
    ``SELECT lakets.create_rollup(...)`` call.  By default the rollup does not
    yet exist (probe -> None) and creation returns id 7.
    """
    pool = AsyncMock()
    # First fetchval -> existence probe; second -> create_rollup id.
    pool.fetchval.side_effect = [
        (1 if existing else None),  # existence probe
        7,  # create_rollup return id (only reached when not existing)
    ]
    return pool


class TestCreateRollup:
    @pytest.mark.asyncio
    async def test_creates_rollup_with_mapped_args(self) -> None:
        pool = _pool(existing=False)

        result = await lakets_service.create_rollup(
            pool,
            table="sensor_readings",
            name="sensor_hourly",
            bucket="1 hour",
            aggregates=[
                {"fn": "avg", "column": "value"},
                {"fn": "max", "column": "value"},
            ],
        )

        assert result == {
            "resource": "LakeTS rollup sensor_hourly",
            "status": "created",
        }

        # The create_rollup SQL call is the 2nd fetchval invocation.
        create_call = pool.fetchval.await_args_list[1]
        args = create_call.args
        # SELECT lakets.create_rollup($1=name, $2=query, $3=bucket,
        #                             $4=source_table, $5=source_schema)
        assert "lakets.create_rollup" in args[0]
        assert args[1] == "sensor_hourly"  # p_name
        query = args[2]  # p_query
        assert "lakets.time_bucket" in query
        assert "avg(value)" in query
        assert "max(value)" in query
        assert "from public.sensor_readings" in query.lower()
        assert "group by" in query.lower()
        # bucket interval mapped to a timedelta (asyncpg INTERVAL on the wire)
        assert args[3] == _dt.timedelta(hours=1)
        assert args[4] == "sensor_readings"  # p_source_table
        assert args[5] == "public"  # p_source_schema

    @pytest.mark.asyncio
    async def test_idempotent_when_already_exists(self) -> None:
        pool = _pool(existing=True)

        result = await lakets_service.create_rollup(
            pool,
            table="sensor_readings",
            name="sensor_hourly",
            bucket="1 hour",
            aggregates=[{"fn": "avg", "column": "value"}],
        )

        assert result == {
            "resource": "LakeTS rollup sensor_hourly",
            "status": "already_exists",
        }
        # Only the existence probe ran; create_rollup was never called.
        assert pool.fetchval.await_count == 1

    @pytest.mark.asyncio
    async def test_accepts_raw_sql_aggregate_spec(self) -> None:
        pool = _pool(existing=False)

        await lakets_service.create_rollup(
            pool,
            table="sensor_readings",
            name="sensor_hourly",
            bucket="15 minutes",
            aggregates="avg(value) AS avg_value, count(*) AS n",
        )

        query = pool.fetchval.await_args_list[1].args[2]
        assert "avg(value) AS avg_value, count(*) AS n" in query
        assert "lakets.time_bucket" in query

    @pytest.mark.asyncio
    async def test_custom_schema(self) -> None:
        pool = _pool(existing=False)

        await lakets_service.create_rollup(
            pool,
            table="readings",
            name="r_roll",
            bucket="1 day",
            aggregates=[{"fn": "sum", "column": "delta"}],
            schema="iot",
        )

        create_args = pool.fetchval.await_args_list[1].args
        assert create_args[5] == "iot"  # p_source_schema
        assert "from iot.readings" in create_args[2].lower()

    @pytest.mark.asyncio
    async def test_never_raises_returns_failed(self) -> None:
        pool = AsyncMock()
        pool.fetchval.side_effect = [None, RuntimeError("boom")]

        result = await lakets_service.create_rollup(
            pool,
            table="sensor_readings",
            name="sensor_hourly",
            bucket="1 hour",
            aggregates=[{"fn": "avg", "column": "value"}],
        )

        assert result["status"] == "failed"
        assert "boom" in result["error"]

    @pytest.mark.asyncio
    async def test_named_aggregate_uses_alias(self) -> None:
        pool = _pool(existing=False)

        await lakets_service.create_rollup(
            pool,
            table="sensor_readings",
            name="sensor_hourly",
            bucket="1 hour",
            aggregates=[{"fn": "avg", "column": "value", "as": "avg_v"}],
        )

        query = pool.fetchval.await_args_list[1].args[2]
        assert "avg(value) AS avg_v" in query
