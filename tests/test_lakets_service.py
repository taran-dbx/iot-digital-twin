"""Tests for services.lakets_service — LakeTS bootstrap + version probe."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services import lakets_service


# ---------------------------------------------------------------------------
# get_installed_version
# ---------------------------------------------------------------------------
class TestGetInstalledVersion:
    """Queries `lakets._version` and returns the latest installed version."""

    @pytest.mark.asyncio
    async def test_returns_none_when_lakets_missing(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(side_effect=Exception('relation "lakets._version" does not exist'))
        result = await lakets_service.get_installed_version(pool)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_table_empty(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(return_value=None)
        result = await lakets_service.get_installed_version(pool)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_version_string(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(return_value="0.1.0")
        result = await lakets_service.get_installed_version(pool)
        assert result == "0.1.0"

    @pytest.mark.asyncio
    async def test_queries_latest_row_by_installed_at(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(return_value="0.1.0")
        await lakets_service.get_installed_version(pool)
        sql_arg = pool.fetchval.await_args.args[0]
        assert "lakets._version" in sql_arg
        assert "ORDER BY installed_at DESC" in sql_arg
        assert "LIMIT 1" in sql_arg


# ---------------------------------------------------------------------------
# resolve_lakets_source
# ---------------------------------------------------------------------------
class TestResolveLakeTsSource:
    """Resolves where to read lakets.sql from — local path or git clone."""

    def test_uses_explicit_path_arg(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- LakeTS v0.1.0\n")
        resolved = lakets_service.resolve_lakets_source(explicit_path=str(sql_file))
        assert resolved == sql_file

    def test_raises_when_explicit_path_missing(self, tmp_path: Path) -> None:
        missing = tmp_path / "nope.sql"
        with pytest.raises(FileNotFoundError, match="LakeTS SQL not found"):
            lakets_service.resolve_lakets_source(explicit_path=str(missing))

    def test_uses_env_sql_path_when_no_arg(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- LakeTS v0.1.0\n")
        monkeypatch.setenv("LAKETS_SQL_PATH", str(sql_file))
        monkeypatch.delenv("LAKETS_GIT_URL", raising=False)
        resolved = lakets_service.resolve_lakets_source()
        assert resolved == sql_file

    def test_clones_git_url_when_no_local_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("LAKETS_SQL_PATH", raising=False)
        monkeypatch.setenv("LAKETS_GIT_URL", "https://example.com/fake/LakeTS.git")

        def fake_clone(url: str, dest: Path) -> None:
            (dest / "dist").mkdir(parents=True)
            (dest / "dist" / "lakets.sql").write_text("-- cloned\n")

        with patch.object(lakets_service, "_git_clone", side_effect=fake_clone) as mock_clone:
            resolved = lakets_service.resolve_lakets_source()
            assert resolved.name == "lakets.sql"
            assert resolved.read_text() == "-- cloned\n"
            mock_clone.assert_called_once()

    def test_raises_when_no_source_configured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("LAKETS_SQL_PATH", raising=False)
        monkeypatch.delenv("LAKETS_GIT_URL", raising=False)
        with pytest.raises(RuntimeError, match="LakeTS source not configured"):
            lakets_service.resolve_lakets_source()


# ---------------------------------------------------------------------------
# install_lakets
# ---------------------------------------------------------------------------
class TestInstallLakeTs:
    """Executes lakets.sql and returns the installed version."""

    @pytest.mark.asyncio
    async def test_executes_sql_against_pool(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- dummy LakeTS SQL\nCREATE SCHEMA IF NOT EXISTS lakets;\n")
        pool = MagicMock()
        pool.execute = AsyncMock(return_value=None)
        pool.fetchval = AsyncMock(return_value="0.1.0")

        version = await lakets_service.install_lakets(pool, sql_file)

        assert version == "0.1.0"
        pool.execute.assert_awaited_once()
        executed_sql = pool.execute.await_args.args[0]
        assert "lakets" in executed_sql

    @pytest.mark.asyncio
    async def test_returns_version_from_registry(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- body\n")
        pool = MagicMock()
        pool.execute = AsyncMock(return_value=None)
        pool.fetchval = AsyncMock(return_value="0.1.0")

        version = await lakets_service.install_lakets(pool, sql_file)
        assert version == "0.1.0"

    @pytest.mark.asyncio
    async def test_idempotent_second_call_skips_exec_when_already_installed(
        self, tmp_path: Path
    ) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- body\n")
        pool = MagicMock()
        pool.execute = AsyncMock(return_value=None)
        pool.fetchval = AsyncMock(return_value="0.1.0")

        version = await lakets_service.install_lakets(pool, sql_file, skip_if_installed=True)

        assert version == "0.1.0"
        pool.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_raises_when_sql_file_missing(self, tmp_path: Path) -> None:
        missing = tmp_path / "nope.sql"
        pool = MagicMock()
        with pytest.raises(FileNotFoundError):
            await lakets_service.install_lakets(pool, missing)

    @pytest.mark.asyncio
    async def test_propagates_pool_execute_errors(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- body\n")
        pool = MagicMock()
        pool.execute = AsyncMock(side_effect=RuntimeError("permission denied"))
        pool.fetchval = AsyncMock(return_value=None)

        with pytest.raises(RuntimeError, match="permission denied"):
            await lakets_service.install_lakets(pool, sql_file)


# ---------------------------------------------------------------------------
# create_chronotable
# ---------------------------------------------------------------------------
class TestCreateChronotable:
    """Wraps lakets.create_chronotable; idempotent via _chronotable_registry probe."""

    @pytest.mark.asyncio
    async def test_creates_when_not_registered(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(side_effect=[None, 42])  # registry probe, create return
        pool.execute = AsyncMock(return_value=None)

        already = await lakets_service.create_chronotable(
            pool, table="sensor_readings", time_col="event_time", chunk="15 minutes",
        )

        assert already is False
        call_sql = pool.fetchval.await_args_list[1].args[0]
        assert "lakets.create_chronotable" in call_sql

    @pytest.mark.asyncio
    async def test_skips_when_already_registered(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(return_value=7)  # registry probe returns existing id

        already = await lakets_service.create_chronotable(
            pool, table="sensor_readings", time_col="event_time", chunk="15 minutes",
        )

        assert already is True
        pool.fetchval.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_passes_time_column_and_chunk_to_sql(self) -> None:
        import datetime as _dt
        pool = MagicMock()
        pool.fetchval = AsyncMock(side_effect=[None, 1])

        await lakets_service.create_chronotable(
            pool, table="sensor_readings", time_col="event_time", chunk="15 minutes",
        )

        create_call = pool.fetchval.await_args_list[1]
        assert create_call.args[1] == "sensor_readings"
        assert create_call.args[2] == "event_time"
        assert create_call.args[3] == _dt.timedelta(minutes=15)


# ---------------------------------------------------------------------------
# apply_tier_policy
# ---------------------------------------------------------------------------
class TestApplyTierPolicy:
    """Applies tiered-retention policy idempotently."""

    @pytest.mark.asyncio
    async def test_applies_when_no_existing_policy(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(side_effect=[None, 99])  # probe, apply
        pool.execute = AsyncMock(return_value=None)

        already = await lakets_service.apply_tier_policy(
            pool, table="sensor_readings", hot="1 hour", drop="7 days",
        )

        assert already is False
        apply_sql = pool.fetchval.await_args_list[1].args[0]
        assert "lakets.add_tiered_retention_policy" in apply_sql

    @pytest.mark.asyncio
    async def test_skips_when_policy_exists(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(return_value=17)

        already = await lakets_service.apply_tier_policy(
            pool, table="sensor_readings", hot="1 hour", drop="7 days",
        )

        assert already is True
        pool.fetchval.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_passes_intervals_as_params(self) -> None:
        import datetime as _dt
        pool = MagicMock()
        pool.fetchval = AsyncMock(side_effect=[None, 1])

        await lakets_service.apply_tier_policy(
            pool, table="sensor_readings", hot="1 hour", drop="7 days",
        )

        apply_call = pool.fetchval.await_args_list[1]
        assert apply_call.args[1] == "sensor_readings"
        assert apply_call.args[2] == _dt.timedelta(hours=1)
        assert apply_call.args[3] == _dt.timedelta(days=7)

    @pytest.mark.asyncio
    async def test_rejects_hot_gte_drop(self) -> None:
        pool = MagicMock()
        with pytest.raises(ValueError, match="hot .* must be less than drop"):
            await lakets_service.apply_tier_policy(
                pool, table="sensor_readings", hot="7 days", drop="1 hour",
            )
        with pytest.raises(ValueError, match="hot .* must be less than drop"):
            await lakets_service.apply_tier_policy(
                pool, table="sensor_readings", hot="1 day", drop="1 day",
            )


# ---------------------------------------------------------------------------
# check_lakehouse_sync_readiness
# ---------------------------------------------------------------------------
class TestCheckLakehouseSyncReadiness:
    """Reports whether the database is ready to enable Lakehouse Sync."""

    @pytest.mark.asyncio
    async def test_all_green(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(side_effect=[
            "databricks_postgres",  # current_database
            True,                    # wal2delta available
            "0.1.0",                 # wal2delta installed version
            True,                    # lakets.enable_sync present
        ])
        result = await lakets_service.check_lakehouse_sync_readiness(pool)

        assert result["database_compatible"] is True
        assert result["wal2delta_available"] is True
        assert result["wal2delta_installed"] is True
        assert result["lakets_sync_functions_present"] is True
        assert result["ready_to_enable"] is True
        assert result.get("reason") in (None, "")

    @pytest.mark.asyncio
    async def test_database_not_compatible(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(side_effect=[
            "custom_db", True, "0.1.0", True,
        ])
        result = await lakets_service.check_lakehouse_sync_readiness(pool)

        assert result["database_compatible"] is False
        assert result["current_database"] == "custom_db"
        assert result["ready_to_enable"] is False
        assert "databricks_postgres" in result["reason"]

    @pytest.mark.asyncio
    async def test_wal2delta_does_not_gate_readiness(self) -> None:
        # wal2delta is managed by the platform — absence doesn't block sync.
        pool = MagicMock()
        pool.fetchval = AsyncMock(side_effect=[
            "databricks_postgres", True, None, True,
        ])
        result = await lakets_service.check_lakehouse_sync_readiness(pool)

        assert result["wal2delta_available"] is True
        assert result["wal2delta_installed"] is False
        assert result["ready_to_enable"] is True  # <-- key behaviour change
        assert result.get("reason") in (None, "")

    @pytest.mark.asyncio
    async def test_wal2delta_not_available_is_still_ready(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(side_effect=[
            "databricks_postgres", False, None, True,
        ])
        result = await lakets_service.check_lakehouse_sync_readiness(pool)

        assert result["wal2delta_available"] is False
        assert result["wal2delta_installed"] is False
        assert result["ready_to_enable"] is True

    @pytest.mark.asyncio
    async def test_lakets_sync_missing(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(side_effect=[
            "databricks_postgres", True, "0.1.0", False,
        ])
        result = await lakets_service.check_lakehouse_sync_readiness(pool)

        assert result["lakets_sync_functions_present"] is False
        assert result["ready_to_enable"] is False
        assert "LakeTS" in result["reason"]

    @pytest.mark.asyncio
    async def test_pool_error_returns_failed_status(self) -> None:
        pool = MagicMock()
        pool.fetchval = AsyncMock(side_effect=RuntimeError("connection timeout"))
        result = await lakets_service.check_lakehouse_sync_readiness(pool)

        assert result["ready_to_enable"] is False
        assert "connection timeout" in result.get("error", "")


# ---------------------------------------------------------------------------
# run_retention
# ---------------------------------------------------------------------------
class TestRunRetention:
    """Discovers tables with retention policies and invokes lakets.execute_retention."""

    @pytest.mark.asyncio
    async def test_returns_empty_dict_when_no_policies(self) -> None:
        pool = MagicMock()
        pool.fetch = AsyncMock(return_value=[])

        result = await lakets_service.run_retention(pool)
        assert result == {}

    @pytest.mark.asyncio
    async def test_invokes_execute_retention_for_each_table(self) -> None:
        pool = MagicMock()
        pool.fetch = AsyncMock(return_value=[
            {"schema_name": "public", "table_name": "sensor_readings"},
            {"schema_name": "public", "table_name": "sliding_aggregates"},
        ])
        pool.fetchval = AsyncMock(side_effect=[5, 0])

        result = await lakets_service.run_retention(pool)
        assert result == {
            "public.sensor_readings": 5,
            "public.sliding_aggregates": 0,
        }
        # One fetchval per table
        assert pool.fetchval.await_count == 2

    @pytest.mark.asyncio
    async def test_per_table_failure_does_not_abort_rest(self) -> None:
        pool = MagicMock()
        pool.fetch = AsyncMock(return_value=[
            {"schema_name": "public", "table_name": "a"},
            {"schema_name": "public", "table_name": "b"},
        ])
        pool.fetchval = AsyncMock(side_effect=[RuntimeError("locked"), 3])

        result = await lakets_service.run_retention(pool)
        assert result["public.a"] == "error: locked"
        assert result["public.b"] == 3
