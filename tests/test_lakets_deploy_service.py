"""Tests for services.lakets_deploy_service.deploy_retention_job."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from services import lakets_deploy_service


def _fake_workspace_client() -> MagicMock:
    w = MagicMock()
    w.current_user.me.return_value = MagicMock(user_name="taran@databricks.com")
    w.workspace.mkdirs = MagicMock(return_value=None)
    w.workspace.import_ = MagicMock(return_value=None)
    w.jobs.list.return_value = []
    w.jobs.create.return_value = MagicMock(job_id=42)
    w.jobs.reset = MagicMock(return_value=None)
    w.config.host = "https://e2.cloud.databricks.com"
    return w


class TestDeployRetentionJob:
    def test_raises_on_missing_project_id(self) -> None:
        with pytest.raises(ValueError, match="project_id is required"):
            lakets_deploy_service.deploy_retention_job("")

    def test_creates_job_when_none_exists(self) -> None:
        w = _fake_workspace_client()
        with patch("databricks.sdk.WorkspaceClient", return_value=w), patch.object(
            lakets_deploy_service,
            "_notebook_source_bytes",
            return_value=b"# Databricks notebook source\nprint('hi')\n",
        ):
            result = lakets_deploy_service.deploy_retention_job(
                project_id="iot-lakets-demo",
            )

        assert result["job_id"] == 42
        assert result["action"] == "created"
        assert result["notebook_path"].endswith("/retention")
        assert result["schedule"]["cron"].startswith("0 0 3")
        w.workspace.import_.assert_called_once()
        w.jobs.create.assert_called_once()
        w.jobs.reset.assert_not_called()

    def test_updates_job_when_exists(self) -> None:
        w = _fake_workspace_client()
        existing = MagicMock(
            job_id=99,
            settings=MagicMock(name="LakeTS Retention — iot-lakets-demo/production"),
        )
        # MagicMock intercepts .name attr access; set explicitly
        existing.settings.name = "LakeTS Retention — iot-lakets-demo/production"
        w.jobs.list.return_value = [existing]

        with patch("databricks.sdk.WorkspaceClient", return_value=w), patch.object(
            lakets_deploy_service,
            "_notebook_source_bytes",
            return_value=b"# body\n",
        ):
            result = lakets_deploy_service.deploy_retention_job(
                project_id="iot-lakets-demo",
            )

        assert result["job_id"] == 99
        assert result["action"] == "updated"
        w.jobs.reset.assert_called_once()
        w.jobs.create.assert_not_called()

    def test_passes_params_to_notebook_task(self) -> None:
        w = _fake_workspace_client()
        with patch("databricks.sdk.WorkspaceClient", return_value=w), patch.object(
            lakets_deploy_service,
            "_notebook_source_bytes",
            return_value=b"# body\n",
        ):
            lakets_deploy_service.deploy_retention_job(
                project_id="my-proj",
                branch_id="staging",
                database="custom_db",
            )

        _, kwargs = w.jobs.create.call_args
        task = kwargs["tasks"][0]
        assert task.notebook_task.base_parameters == {
            "project_id": "my-proj",
            "branch_id": "staging",
            "database": "custom_db",
        }
