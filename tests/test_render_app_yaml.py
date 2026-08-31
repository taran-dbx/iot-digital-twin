"""Tests for scripts/render_app_yaml.py — workspace deployment profile renderer."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "render_app_yaml.py"
PROFILES_DIR = ROOT / "config" / "workspaces"

_EXPECTED_ENV_NAMES = (
    "UC_CATALOG",
    "UC_SCHEMA",
    "PGHOST",
    "PGDATABASE",
    "PGPORT",
    "LAKEBASE_PROJECT",
    "LAKEBASE_ENDPOINT",
    "DATABRICKS_SQL_WAREHOUSE_ID",
    "ZEROBUS_SERVER_ENDPOINT",
    "ZEROBUS_TABLE_NAME",
    "ZEROBUS_CLIENT_ID",
    "ZEROBUS_SECRET_SCOPE",
    "ZEROBUS_SECRET_KEY",
    "UC_STORAGE_BASE",
    "UC_EXTERNAL_LOCATION",
    "UC_SERVICE_CREDENTIAL",
    "PIPELINES_LAKEBASE_PROJECT",
    "MSK_BOOTSTRAP_SERVERS",
    "INDUSTRIES",
    "ADMIN_GROUP",
    "ADMIN_EMAILS",
    "LAKETS_SQL_PATH",
)

_MINIMAL_PROFILE = {
    "UC_CATALOG": "test-catalog",
    "UC_SCHEMA": "test_schema",
    "PGHOST": "",
    "LAKEBASE_PROJECT": "iot-app-backend",
    "LAKEBASE_ENDPOINT": "projects/iot-app-backend/branches/production/endpoints/primary",
    "DATABRICKS_SQL_WAREHOUSE_ID": "warehouse-id",
    "ZEROBUS_SERVER_ENDPOINT": "123.zerobus.us-east-1.cloud.databricks.com",
    "ZEROBUS_CLIENT_ID": "",
    "ZEROBUS_SECRET_SCOPE": "",
    "ZEROBUS_SECRET_KEY": "",
    "UC_STORAGE_BASE": "",
    "UC_EXTERNAL_LOCATION": "",
    "UC_SERVICE_CREDENTIAL": "",
    "PIPELINES_LAKEBASE_PROJECT": "",
    "MSK_BOOTSTRAP_SERVERS": "",
    "INDUSTRIES": "pharmaceutical",
    "ADMIN_GROUP": "iot-admins",
    "ADMIN_EMAILS": "admin@example.com",
    "LAKETS_SQL_PATH": "/app/python/source_code/databricks/vendor/lakets.sql",
}


def _load_renderer():
    spec = importlib.util.spec_from_file_location("render_app_yaml", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["render_app_yaml"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def renderer():
    return _load_renderer()


@pytest.fixture()
def profiles_dir(tmp_path: Path) -> Path:
    workspace_dir = tmp_path / "workspaces"
    workspace_dir.mkdir()
    (workspace_dir / "sample.profile").write_text(
        json.dumps(_MINIMAL_PROFILE, indent=2) + "\n",
        encoding="utf-8",
    )
    return workspace_dir


def test_unknown_profile_raises(renderer, profiles_dir: Path) -> None:
    with pytest.raises(renderer.ProfileError, match="unknown profile"):
        renderer.load_profile("no-such-profile", profiles_dir)


def test_missing_required_key_raises(renderer, profiles_dir: Path) -> None:
    incomplete = dict(_MINIMAL_PROFILE)
    del incomplete["UC_CATALOG"]
    (profiles_dir / "broken.profile").write_text(json.dumps(incomplete), encoding="utf-8")

    with pytest.raises(renderer.ProfileError, match="UC_CATALOG"):
        renderer.load_profile("broken", profiles_dir)


def test_render_includes_runtime_command_and_all_env_names(renderer) -> None:
    rendered = renderer.render_app_yaml(_MINIMAL_PROFILE)

    assert 'uvicorn app:app --host 0.0.0.0 --port ${DATABRICKS_APP_PORT:-8000}' in rendered
    for env_name in _EXPECTED_ENV_NAMES:
        assert f"name: {env_name}" in rendered


def test_render_quotes_values(renderer) -> None:
    rendered = renderer.render_app_yaml(_MINIMAL_PROFILE)
    assert 'value: "test-catalog"' in rendered
    assert 'value: ""' in rendered


def test_classic_stable_profile_values(renderer) -> None:
    profile = renderer.load_profile("classic-stable", PROFILES_DIR)

    assert profile["UC_CATALOG"] == "classic_stable_realtime_demos_catalog"
    assert profile["DATABRICKS_SQL_WAREHOUSE_ID"] == "0864d7304e74ad76"
    assert profile["ZEROBUS_CLIENT_ID"] == "7c59807d-7f0d-47ab-8e83-6d91add2f35c"
    assert profile["UC_SERVICE_CREDENTIAL"] == ""
    assert profile["MSK_BOOTSTRAP_SERVERS"] == ""
    assert profile["ZEROBUS_SECRET_SCOPE"] == "iot-secrets"
    assert profile["ZEROBUS_SECRET_KEY"] == "zerobus-client-secret"


def test_interop_profile_values(renderer) -> None:
    profile = renderer.load_profile("fe-vm-shared-interop", PROFILES_DIR)

    assert profile["UC_CATALOG"] == "taran-interop"
    assert profile["UC_SCHEMA"] == "rtm_lakets_zb"
    assert profile["DATABRICKS_SQL_WAREHOUSE_ID"] == "31df823c0f824066"
    assert (
        profile["ZEROBUS_SERVER_ENDPOINT"]
        == "332040261549026.zerobus.us-east-1.cloud.databricks.com"
    )
    assert profile["UC_SERVICE_CREDENTIAL"] == "iot-rtm-sc"
    assert profile["PIPELINES_LAKEBASE_PROJECT"] == "iot-pipelines-interop"
    assert profile["PGHOST"] == "ep-lucky-firefly-d2xaldy5.database.us-east-1.cloud.databricks.com"
    assert profile["LAKEBASE_PROJECT"] == "iot-app-backend-interop"
    assert (
        profile["LAKEBASE_ENDPOINT"]
        == "projects/iot-app-backend-interop/branches/production/endpoints/primary"
    )
    assert profile["ZEROBUS_CLIENT_ID"] == "6ce122f2-9f0e-4939-ba31-a8d66e1d1f16"
    assert profile["ZEROBUS_SECRET_SCOPE"] == ""
    assert profile["ZEROBUS_SECRET_KEY"] == ""
    assert profile["MSK_BOOTSTRAP_SERVERS"] == (
        "b-1-public.iotdemo.2ol92p.c6.kafka.us-east-1.amazonaws.com:9198,"
        "b-2-public.iotdemo.2ol92p.c6.kafka.us-east-1.amazonaws.com:9198,"
        "b-3-public.iotdemo.2ol92p.c6.kafka.us-east-1.amazonaws.com:9198"
    )


def test_interop_render_uses_blank_zerobus_secret_env(renderer) -> None:
    profile = renderer.load_profile("fe-vm-shared-interop", PROFILES_DIR)
    rendered = renderer.render_app_yaml(profile)

    assert 'name: ZEROBUS_SECRET_SCOPE\n    value: ""' in rendered
    assert 'name: ZEROBUS_SECRET_KEY\n    value: ""' in rendered
    assert "iot-secrets" not in rendered


def test_classic_stable_render_preserves_legacy_zerobus_secret_env(renderer) -> None:
    profile = renderer.load_profile("classic-stable", PROFILES_DIR)
    rendered = renderer.render_app_yaml(profile)

    assert 'name: ZEROBUS_SECRET_SCOPE\n    value: "iot-secrets"' in rendered
    assert 'name: ZEROBUS_SECRET_KEY\n    value: "zerobus-client-secret"' in rendered


def test_committed_app_yaml_matches_active_interop_profile(renderer) -> None:
    profile = renderer.load_profile("fe-vm-shared-interop", PROFILES_DIR)
    app_yaml = ROOT / "app.yaml"

    assert app_yaml.is_file(), "app.yaml must exist at repo root"
    assert renderer.check_app_yaml(app_yaml, profile), (
        "app.yaml drifted from fe-vm-shared-interop profile; "
        "run: python scripts/render_app_yaml.py fe-vm-shared-interop"
    )


def test_check_matches_generated_content(renderer, tmp_path: Path) -> None:
    app_yaml = tmp_path / "app.yaml"
    app_yaml.write_text(renderer.render_app_yaml(_MINIMAL_PROFILE), encoding="utf-8")

    assert renderer.check_app_yaml(app_yaml, _MINIMAL_PROFILE) is True


def test_check_detects_mismatch(renderer, tmp_path: Path) -> None:
    app_yaml = tmp_path / "app.yaml"
    app_yaml.write_text("command: []\nenv: []\n", encoding="utf-8")

    assert renderer.check_app_yaml(app_yaml, _MINIMAL_PROFILE) is False


def test_cli_check_exit_code(renderer, profiles_dir: Path, tmp_path: Path) -> None:
    app_yaml = tmp_path / "app.yaml"
    app_yaml.write_text(renderer.render_app_yaml(_MINIMAL_PROFILE), encoding="utf-8")

    ok = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "sample",
            "--profiles-dir",
            str(profiles_dir),
            "--output",
            str(app_yaml),
            "--check",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert ok.returncode == 0

    bad = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "sample",
            "--profiles-dir",
            str(profiles_dir),
            "--output",
            str(tmp_path / "missing.yaml"),
            "--check",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert bad.returncode != 0


def test_cli_writes_output(renderer, profiles_dir: Path, tmp_path: Path) -> None:
    out = tmp_path / "app.yaml"
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "sample",
            "--profiles-dir",
            str(profiles_dir),
            "--output",
            str(out),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    assert out.exists()
    assert renderer.check_app_yaml(out, _MINIMAL_PROFILE)
