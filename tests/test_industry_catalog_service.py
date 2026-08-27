"""Tests for the canonical per-industry UC provisioning + grants + secrets.

Covers plan Phase 2 Task 2.2 + 2.4 / spec §2.5–2.7, §1.11.  Unit tests mock the
Databricks ``WorkspaceClient`` and the DBSQL executor.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services import industry_catalog_service as ics


class _NotFound(Exception):
    def __str__(self) -> str:  # pragma: no cover - trivial
        return "RESOURCE_DOES_NOT_EXIST: not found"


class _AlreadyExists(Exception):
    def __str__(self) -> str:  # pragma: no cover - trivial
        return "catalog already exists"


def _make_ws(*, ext_loc=True, has_warehouse=True):
    ws = MagicMock()
    if ext_loc:
        ws.external_locations.get = MagicMock(return_value=MagicMock())
    else:
        ws.external_locations.get = MagicMock(side_effect=_NotFound())
    if has_warehouse:
        wh = MagicMock()
        wh.id = "wh-1"
        ws.warehouses.list = MagicMock(return_value=[wh])
    else:
        ws.warehouses.list = MagicMock(return_value=[])
    ws.catalogs.create = MagicMock(return_value=MagicMock())
    ws.schemas.create = MagicMock(return_value=MagicMock())
    ws.volumes.create = MagicMock(return_value=MagicMock())
    return ws


# ---------------------------------------------------------------------------
# Name helpers
# ---------------------------------------------------------------------------
class TestNames:
    def test_catalog_name_sanitizes(self) -> None:
        assert ics.catalog_name("Pharmaceutical") == "iot_pharmaceutical"
        assert ics.catalog_name("Oil & Gas") == "iot_oil_gas"
        assert ics.catalog_name("spiketest") == "iot_spiketest"

    def test_catalog_name_rejects_empty(self) -> None:
        with pytest.raises(ValueError):
            ics.catalog_name("  ")

    def test_managed_location_with_storage_base(self, monkeypatch) -> None:
        monkeypatch.setenv("UC_STORAGE_BASE", "s3://taran-shared-interop")
        assert ics.catalog_managed_location("iot_spiketest") == (
            "s3://taran-shared-interop/iot_spiketest"
        )

    def test_managed_location_empty_when_no_storage_base(self, monkeypatch) -> None:
        monkeypatch.delenv("UC_STORAGE_BASE", raising=False)
        assert ics.catalog_managed_location("iot_spiketest") == ""


# ---------------------------------------------------------------------------
# Pre-flight
# ---------------------------------------------------------------------------
class TestPreflight:
    def test_all_pass_with_external_location(self, monkeypatch) -> None:
        monkeypatch.setenv("UC_EXTERNAL_LOCATION", "taran-shared-interop")
        monkeypatch.setenv("UC_STORAGE_BASE", "s3://taran-shared-interop")
        checks = ics.preflight_checks(ws=_make_ws())
        assert all(c["status"] == "pass" for c in checks)
        ics.raise_on_failed_preflight(checks)  # no raise

    def test_all_pass_without_external_location(self, monkeypatch) -> None:
        """No external location configured → skip the check (metastore-managed)."""
        monkeypatch.delenv("UC_EXTERNAL_LOCATION", raising=False)
        checks = ics.preflight_checks(ws=_make_ws(ext_loc=False))
        assert all(c["status"] == "pass" for c in checks)
        ics.raise_on_failed_preflight(checks)  # no raise

    def test_missing_external_location_fails_when_configured(self, monkeypatch) -> None:
        monkeypatch.setenv("UC_EXTERNAL_LOCATION", "taran-shared-interop")
        checks = ics.preflight_checks(ws=_make_ws(ext_loc=False))
        assert any(c["status"] == "fail" and "External location" in c["check"] for c in checks)
        with pytest.raises(RuntimeError, match="Pre-flight"):
            ics.raise_on_failed_preflight(checks)

    def test_missing_warehouse_fails(self) -> None:
        checks = ics.preflight_checks(ws=_make_ws(has_warehouse=False))
        assert any(c["status"] == "fail" and "warehouse" in c["check"].lower() for c in checks)


# ---------------------------------------------------------------------------
# provision_industry_catalog
# ---------------------------------------------------------------------------
class TestProvisionCatalog:
    @pytest.mark.asyncio
    async def test_creates_catalog_with_managed_location(self, monkeypatch) -> None:
        monkeypatch.setenv("UC_STORAGE_BASE", "s3://taran-shared-interop")
        ws = _make_ws()
        with patch.object(ics, "execute_dbsql", new=AsyncMock(return_value={})):
            res = await ics.provision_industry_catalog("spiketest", ws=ws)
        assert res["catalog"] == "iot_spiketest"
        _, kwargs = ws.catalogs.create.call_args
        assert kwargs["name"] == "iot_spiketest"
        assert kwargs["storage_root"] == "s3://taran-shared-interop/iot_spiketest"

    @pytest.mark.asyncio
    async def test_creates_catalog_without_managed_location(self, monkeypatch) -> None:
        """When UC_STORAGE_BASE is unset, catalog is created with no storage_root."""
        monkeypatch.delenv("UC_STORAGE_BASE", raising=False)
        ws = _make_ws()
        with patch.object(ics, "execute_dbsql", new=AsyncMock(return_value={})):
            res = await ics.provision_industry_catalog("spiketest", ws=ws)
        assert res["catalog"] == "iot_spiketest"
        args, kwargs = ws.catalogs.create.call_args
        assert kwargs["name"] == "iot_spiketest"
        assert "storage_root" not in kwargs

    @pytest.mark.asyncio
    async def test_creates_five_schemas_and_volume(self) -> None:
        ws = _make_ws()
        with patch.object(ics, "execute_dbsql", new=AsyncMock(return_value={})):
            await ics.provision_industry_catalog("spiketest", ws=ws)
        created_schemas = {c.kwargs["name"] for c in ws.schemas.create.call_args_list}
        assert created_schemas == set(ics.CANONICAL_SCHEMAS)
        ws.volumes.create.assert_called_once()
        _, vkwargs = ws.volumes.create.call_args
        assert vkwargs["schema_name"] == "checkpoints"
        assert vkwargs["name"] == "stream_checkpoints"

    @pytest.mark.asyncio
    async def test_external_metrics_tables_ddl_executed_for_streaming_modes(self) -> None:
        ws = _make_ws()
        exec_mock = AsyncMock(return_value={})
        with patch.object(ics, "execute_dbsql", new=exec_mock):
            res = await ics.provision_industry_catalog("spiketest", ws=ws)
        ddl_calls = [c.args[0] for c in exec_mock.call_args_list]
        for mode in ("realtime", "microbatch"):
            assert any(
                f"streaming_metrics_{mode}" in s and "LOCATION" in s for s in ddl_calls
            ), f"missing DDL for mode {mode}"
        # Zerobus is not a Spark stream → no metrics table for it.
        assert not any("streaming_metrics_zerobus" in s for s in ddl_calls)
        assert res["metrics_table"] == {
            "realtime": "iot_spiketest.metrics.streaming_metrics_realtime",
            "microbatch": "iot_spiketest.metrics.streaming_metrics_microbatch",
        }
        assert res["metrics_tables"] == res["metrics_table"]
        assert set(res["metrics_locations"]) == {"realtime", "microbatch"}

    @pytest.mark.asyncio
    async def test_idempotent_catalog_already_exists(self) -> None:
        ws = _make_ws()
        ws.catalogs.create = MagicMock(side_effect=_AlreadyExists())
        with patch.object(ics, "execute_dbsql", new=AsyncMock(return_value={})):
            res = await ics.provision_industry_catalog("spiketest", ws=ws)
        statuses = {r["resource"]: r["status"] for r in res["results"]}
        assert statuses["UC catalog 'iot_spiketest'"] == "already_exists"

    @pytest.mark.asyncio
    async def test_records_resources_in_ledger(self) -> None:
        ws = _make_ws()
        recorded: list[dict] = []

        async def _rec(_db, **kw):
            recorded.append(kw)
            return kw

        with patch.object(ics, "execute_dbsql", new=AsyncMock(return_value={})), \
                patch.object(ics, "record_resource", side_effect=_rec):
            await ics.provision_industry_catalog(
                "spiketest", ws=ws, db=MagicMock(), setup_id="s1",
            )
        types = [r["resource_type"] for r in recorded]
        assert "uc_catalog" in types
        assert "uc_schema" in types
        assert "uc_volume" in types
        assert types.count("metrics_table") == 2


# ---------------------------------------------------------------------------
# Grants
# ---------------------------------------------------------------------------
class TestGrants:
    @pytest.mark.asyncio
    async def test_grants_include_external_use_schema_and_create_table(self) -> None:
        ws = _make_ws()
        exec_mock = AsyncMock(return_value={})
        with patch.object(ics, "execute_dbsql", new=exec_mock):
            await ics.grant_streaming_sp("spiketest", ws=ws, principal="app-123")
        sqls = " ".join(c.args[0] for c in exec_mock.call_args_list)
        assert "EXTERNAL USE SCHEMA ON SCHEMA iot_spiketest.metrics" in sqls
        assert "CREATE TABLE ON SCHEMA iot_spiketest.zerobus" in sqls
        assert "USE CATALOG ON CATALOG iot_spiketest" in sqls
        assert "`app-123`" in sqls

    @pytest.mark.asyncio
    async def test_grants_recorded_in_ledger(self) -> None:
        ws = _make_ws()
        recorded: list[dict] = []

        async def _rec(_db, **kw):
            recorded.append(kw)

        with patch.object(ics, "execute_dbsql", new=AsyncMock(return_value={})), \
                patch.object(ics, "record_resource", side_effect=_rec):
            await ics.grant_streaming_sp(
                "spiketest", ws=ws, principal="app-123",
                db=MagicMock(), setup_id="s1",
            )
        assert recorded and all(r["resource_type"] == "grant" for r in recorded)

    def test_resolve_principal_falls_back(self, monkeypatch) -> None:
        """Falls back to DATABRICKS_CLIENT_ID when SP lookup returns None."""
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "fallback-sp-id")
        ws = MagicMock()
        with patch.object(ics, "_find_sp_by_display_name", return_value=None):
            assert ics.resolve_streaming_sp_principal(ws) == "fallback-sp-id"

    @pytest.mark.asyncio
    async def test_app_backend_read_grants_catalog_wide_select(self) -> None:
        # The app SP needs catalog-wide SELECT so it can read the CDF-synced
        # lb_<shadow>_history table the managed sync creates later.
        ws = _make_ws()
        exec_mock = AsyncMock(return_value={})
        with patch.object(ics, "execute_dbsql", new=exec_mock):
            res = await ics.grant_app_backend_read(
                "spiketest", ws=ws, principal="app-sp-999",
            )
        sqls = " ".join(c.args[0] for c in exec_mock.call_args_list)
        assert "SELECT ON CATALOG iot_spiketest" in sqls
        assert "USE CATALOG ON CATALOG iot_spiketest" in sqls
        assert "`app-sp-999`" in sqls
        assert res["principal"] == "app-sp-999"

    @pytest.mark.asyncio
    async def test_app_backend_gets_metrics_write_grants(self) -> None:
        # The per-mode metrics listener runs as the app SP on the validation
        # cluster, so it needs EXTERNAL USE SCHEMA + MODIFY on metrics to write
        # via delta-rs UC temp-table-credentials (else the tables stay empty).
        ws = _make_ws()
        exec_mock = AsyncMock(return_value={})
        with patch.object(ics, "execute_dbsql", new=exec_mock):
            res = await ics.grant_app_backend_read(
                "spiketest", ws=ws, principal="app-sp-999",
            )
        sqls = " ".join(c.args[0] for c in exec_mock.call_args_list)
        assert "EXTERNAL USE SCHEMA ON SCHEMA iot_spiketest.metrics TO `app-sp-999`" in sqls
        assert "MODIFY ON SCHEMA iot_spiketest.metrics TO `app-sp-999`" in sqls
        labels = {r["resource"] for r in res["results"]}
        assert any("EXTERNAL USE SCHEMA on metrics" in label for label in labels)
        assert any("MODIFY on metrics" in label for label in labels)

    @pytest.mark.asyncio
    async def test_app_backend_read_skips_when_principal_unresolved(self) -> None:
        ws = MagicMock()
        ws.current_user.me.side_effect = Exception("no identity")
        with patch.dict("os.environ", {}, clear=False) as _env:
            import os
            os.environ.pop("DATABRICKS_CLIENT_ID", None)
            res = await ics.grant_app_backend_read("spiketest", ws=ws)
        assert res["principal"] is None
        assert res["results"][0]["status"] == "skipped"

    def test_resolve_app_principal_prefers_client_id(self) -> None:
        import os
        with patch.dict(os.environ, {"DATABRICKS_CLIENT_ID": "cid-123"}):
            assert ics.resolve_app_principal(MagicMock()) == "cid-123"

    @pytest.mark.asyncio
    async def test_zerobus_sp_grants_create_table_on_zerobus(self) -> None:
        # The Zerobus ingest SP needs CREATE TABLE + write on the zerobus schema
        # to land rows — the industry path must grant it (not just the legacy path).
        ws = _make_ws()
        exec_mock = AsyncMock(return_value={})
        with patch.object(ics, "execute_dbsql", new=exec_mock):
            res = await ics.grant_zerobus_sp("spiketest", ws=ws, principal="zb-999")
        sqls = " ".join(c.args[0] for c in exec_mock.call_args_list)
        assert "CREATE TABLE ON SCHEMA iot_spiketest.zerobus" in sqls
        assert "USE CATALOG ON CATALOG iot_spiketest" in sqls
        assert "`zb-999`" in sqls
        assert res["principal"] == "zb-999"

    @pytest.mark.asyncio
    async def test_zerobus_sp_skips_when_client_id_unset(self) -> None:
        import os
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ZEROBUS_CLIENT_ID", None)
            res = await ics.grant_zerobus_sp("spiketest", ws=MagicMock())
        assert res["principal"] is None
        assert res["results"][0]["status"] == "skipped"


# ---------------------------------------------------------------------------
# Secret scope
# ---------------------------------------------------------------------------
class TestSecretScope:
    @pytest.mark.asyncio
    async def test_creates_scope_and_records_shared(self) -> None:
        ws = MagicMock()
        ws.secrets.create_scope = MagicMock(return_value=None)
        recorded: list[dict] = []

        async def _rec(_db, **kw):
            recorded.append(kw)

        with patch.object(ics, "record_resource", side_effect=_rec):
            res = await ics.ensure_secret_scope(ws=ws, db=MagicMock(), setup_id="s1")
        assert res["status"] == "created"
        assert recorded[0]["resource_type"] == "secret_scope"
        assert recorded[0]["metadata"]["shared"] is True

    @pytest.mark.asyncio
    async def test_scope_already_exists(self) -> None:
        ws = MagicMock()
        ws.secrets.create_scope = MagicMock(side_effect=Exception("scope already exists"))
        res = await ics.ensure_secret_scope(ws=ws)
        assert res["status"] == "already_exists"
