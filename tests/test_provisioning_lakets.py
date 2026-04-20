"""Tests for provision_lakebase_table's LakeTS install integration (Phase 1.3)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services import provisioning_service


def _stub_conn() -> MagicMock:
    """Return a mocked asyncpg connection."""
    conn = MagicMock()
    conn.execute = AsyncMock(return_value=None)
    conn.fetchval = AsyncMock(return_value="0.1.2")
    conn.close = AsyncMock(return_value=None)
    return conn


class TestProvisionLakebaseTableInstallsLakeTs:
    """After the base DDL, provision_lakebase_table must install LakeTS."""

    @pytest.mark.asyncio
    async def test_installs_lakets_and_reports_version(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- LakeTS body\n")

        conn = _stub_conn()
        # First fetchval call (skip_if_installed probe) returns None, second returns version.
        conn.fetchval = AsyncMock(side_effect=[None, "0.1.2"])

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            result = await provisioning_service.provision_lakebase_table({
                "mode": "direct",
                "host": "pg.example",
                "user": "u",
                "password": "p",
                "port": 5432,
                "pg_table": "sensor_readings",
                "lakets_sql_path": str(sql_file),
            })

        assert result["status"] == "created"
        assert result.get("lakets_version") == "0.1.2"
        # Two execute calls: base DDL + LakeTS body.
        execute_calls = [call.args[0] for call in conn.execute.await_args_list]
        assert any("sensor_readings" in sql for sql in execute_calls)
        assert any("LakeTS body" in sql for sql in execute_calls)

    @pytest.mark.asyncio
    async def test_skips_lakets_when_flag_false(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- LakeTS body\n")

        conn = _stub_conn()

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            result = await provisioning_service.provision_lakebase_table({
                "mode": "direct",
                "host": "pg.example",
                "user": "u",
                "password": "p",
                "port": 5432,
                "pg_table": "sensor_readings",
                "lakets_sql_path": str(sql_file),
                "install_lakets": False,
            })

        assert result["status"] == "created"
        assert "lakets_version" not in result
        executed = [call.args[0] for call in conn.execute.await_args_list]
        assert not any("LakeTS body" in sql for sql in executed)

    @pytest.mark.asyncio
    async def test_lakets_failure_surfaces_as_warning_not_hard_fail(
        self, tmp_path: Path
    ) -> None:
        missing = tmp_path / "missing.sql"

        conn = _stub_conn()

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            result = await provisioning_service.provision_lakebase_table({
                "mode": "direct",
                "host": "pg.example",
                "user": "u",
                "password": "p",
                "port": 5432,
                "pg_table": "sensor_readings",
                "lakets_sql_path": str(missing),
            })

        assert result["status"] == "created"
        assert "lakets_error" in result
        assert "LakeTS SQL not found" in result["lakets_error"]


class TestProvisionLakebaseTableAppliesTiering:
    """When lb.tiering is supplied, ChronoTable + tier policy are applied."""

    @pytest.mark.asyncio
    async def test_applies_chronotable_and_policy(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- body\n")

        conn = _stub_conn()
        # fetchval sequence: install probe=None, post-install version=0.1.2,
        # chronotable probe=None, chronotable create=1,
        # policy probe=None, policy apply=1
        conn.fetchval = AsyncMock(side_effect=[None, "0.1.2", None, 1, None, 1])

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            result = await provisioning_service.provision_lakebase_table({
                "mode": "direct",
                "host": "pg.example", "user": "u", "password": "p", "port": 5432,
                "pg_table": "sensor_readings",
                "lakets_sql_path": str(sql_file),
                "tiering": {
                    "hot": "1 hour",
                    "drop": "7 days",
                    "chunk": "15 minutes",
                },
            })

        assert result["status"] == "created"
        assert result.get("lakets_version") == "0.1.2"
        assert result.get("tiering_applied") == {
            "hot": "1 hour", "drop": "7 days", "chunk": "15 minutes",
        }

    @pytest.mark.asyncio
    async def test_tiering_absent_skips_chronotable(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- body\n")

        conn = _stub_conn()
        conn.fetchval = AsyncMock(side_effect=[None, "0.1.2"])

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            result = await provisioning_service.provision_lakebase_table({
                "mode": "direct",
                "host": "pg.example", "user": "u", "password": "p", "port": 5432,
                "pg_table": "sensor_readings",
                "lakets_sql_path": str(sql_file),
            })

        assert result.get("lakets_version") == "0.1.2"
        assert "tiering_applied" not in result
        assert "tiering_error" not in result

    @pytest.mark.asyncio
    async def test_tiering_error_does_not_fail_provision(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "lakets.sql"
        sql_file.write_text("-- body\n")

        conn = _stub_conn()
        # install probe None, version "0.1.2", chronotable probe raises
        conn.fetchval = AsyncMock(
            side_effect=[None, "0.1.2", RuntimeError("registry unavailable")]
        )

        with patch("asyncpg.connect", new=AsyncMock(return_value=conn)):
            result = await provisioning_service.provision_lakebase_table({
                "mode": "direct",
                "host": "pg.example", "user": "u", "password": "p", "port": 5432,
                "pg_table": "sensor_readings",
                "lakets_sql_path": str(sql_file),
                "tiering": {"hot": "1 hour", "drop": "7 days"},
            })

        assert result["status"] == "created"
        assert result.get("lakets_version") == "0.1.2"
        assert "tiering_error" in result
        assert "registry unavailable" in result["tiering_error"]
