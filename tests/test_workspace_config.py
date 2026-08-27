"""Tests for core/workspace_config.py — the central workspace configuration module.

TDD step 1: these tests define the contract for get_config() before the module exists.
"""

from __future__ import annotations

import os

import pytest

# Required env vars and the WorkspaceConfig field each maps to.
_REQUIRED_ENV: dict[str, str] = {
    "UC_CATALOG": "uc_catalog",
    "UC_SCHEMA": "uc_schema",
    "LAKEBASE_PROJECT": "lakebase_project",
    "LAKEBASE_ENDPOINT": "lakebase_endpoint",
    "DATABRICKS_SQL_WAREHOUSE_ID": "warehouse_id",
    "ZEROBUS_SERVER_ENDPOINT": "zerobus_endpoint",
    "DATABRICKS_CLIENT_ID": "app_sp_client_id",
    "INDUSTRIES": "industries",  # list field — tested separately
}

_FULL_ENV: dict[str, str] = {
    "UC_CATALOG": "test_catalog",
    "UC_SCHEMA": "test_schema",
    "LAKEBASE_PROJECT": "iot-app-backend",
    "LAKEBASE_ENDPOINT": "projects/iot-app-backend/branches/production/endpoints/primary",
    "DATABRICKS_SQL_WAREHOUSE_ID": "0864d7304e74ad76",
    "ZEROBUS_SERVER_ENDPOINT": "7474659563748021.zerobus.eu-central-1.cloud.databricks.com",
    "DATABRICKS_CLIENT_ID": "7c59807d-7f0d-47ab-8e83-6d91add2f35c",
    "INDUSTRIES": "pharmaceutical,manufacturing",
    "PGHOST": "ep-test.database.eu-central-1.cloud.databricks.com",
}


@pytest.fixture()
def full_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch all required (and optional) env vars for workspace config."""
    for key, val in _FULL_ENV.items():
        monkeypatch.setenv(key, val)


class TestGetConfigReadsEnvVars:
    """get_config() must read every field from the corresponding env var."""

    def test_uc_catalog(self, full_env: None) -> None:
        from core.workspace_config import get_config
        cfg = get_config()
        assert cfg.uc_catalog == "test_catalog"

    def test_uc_schema(self, full_env: None) -> None:
        from core.workspace_config import get_config
        cfg = get_config()
        assert cfg.uc_schema == "test_schema"

    def test_lakebase_project(self, full_env: None) -> None:
        from core.workspace_config import get_config
        cfg = get_config()
        assert cfg.lakebase_project == "iot-app-backend"

    def test_lakebase_endpoint(self, full_env: None) -> None:
        from core.workspace_config import get_config
        cfg = get_config()
        assert cfg.lakebase_endpoint == "projects/iot-app-backend/branches/production/endpoints/primary"

    def test_warehouse_id(self, full_env: None) -> None:
        from core.workspace_config import get_config
        cfg = get_config()
        assert cfg.warehouse_id == "0864d7304e74ad76"

    def test_zerobus_endpoint(self, full_env: None) -> None:
        from core.workspace_config import get_config
        cfg = get_config()
        assert cfg.zerobus_endpoint == "7474659563748021.zerobus.eu-central-1.cloud.databricks.com"

    def test_app_sp_client_id(self, full_env: None) -> None:
        from core.workspace_config import get_config
        cfg = get_config()
        assert cfg.app_sp_client_id == "7c59807d-7f0d-47ab-8e83-6d91add2f35c"

    def test_industries_parsed_as_list(self, full_env: None) -> None:
        from core.workspace_config import get_config
        cfg = get_config()
        assert cfg.industries == ["pharmaceutical", "manufacturing"]

    def test_pg_host_optional_present(self, full_env: None) -> None:
        from core.workspace_config import get_config
        cfg = get_config()
        assert cfg.pg_host == "ep-test.database.eu-central-1.cloud.databricks.com"

    def test_pg_host_optional_absent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """PGHOST may be empty or missing at startup — must not raise."""
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.delenv("PGHOST", raising=False)
        from core.workspace_config import get_config
        cfg = get_config()
        assert cfg.pg_host == ""

    def test_config_is_frozen(self, full_env: None) -> None:
        from core.workspace_config import get_config
        cfg = get_config()
        with pytest.raises((AttributeError, TypeError)):
            cfg.uc_catalog = "modified"  # type: ignore[misc]


class TestGetConfigRaisesOnMissingRequired:
    """get_config() must raise RuntimeError naming the missing var."""

    @pytest.mark.parametrize("missing_var", list(_REQUIRED_ENV.keys()))
    def test_raises_when_var_missing(
        self, monkeypatch: pytest.MonkeyPatch, missing_var: str
    ) -> None:
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.delenv(missing_var, raising=False)
        from core.workspace_config import get_config
        with pytest.raises(RuntimeError, match=missing_var):
            get_config()

    def test_error_message_names_missing_var(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.delenv("UC_CATALOG", raising=False)
        monkeypatch.delenv("DATABRICKS_SQL_WAREHOUSE_ID", raising=False)
        from core.workspace_config import get_config
        with pytest.raises(RuntimeError) as exc_info:
            get_config()
        msg = str(exc_info.value)
        assert "UC_CATALOG" in msg
        assert "DATABRICKS_SQL_WAREHOUSE_ID" in msg

    def test_pghost_empty_does_not_raise(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """PGHOST blank/absent must not trigger the missing-var error."""
        for key, val in _FULL_ENV.items():
            monkeypatch.setenv(key, val)
        monkeypatch.setenv("PGHOST", "")
        from core.workspace_config import get_config
        cfg = get_config()  # must not raise
        assert cfg.pg_host == ""


class TestWorkspaceConfigStructure:
    """WorkspaceConfig must be a frozen dataclass with the expected field set."""

    def test_expected_fields(self, full_env: None) -> None:
        from core.workspace_config import WorkspaceConfig, get_config
        cfg = get_config()
        assert isinstance(cfg, WorkspaceConfig)
        expected = {
            "uc_catalog", "uc_schema", "lakebase_project", "lakebase_endpoint",
            "pg_host", "warehouse_id", "zerobus_endpoint", "app_sp_client_id",
            "industries",
        }
        actual = {f for f in cfg.__dataclass_fields__}
        assert actual == expected

    def test_industries_is_list(self, full_env: None) -> None:
        from core.workspace_config import get_config
        cfg = get_config()
        assert isinstance(cfg.industries, list)
        assert all(isinstance(i, str) for i in cfg.industries)
