"""Tests for provisioning_service.ensure_shared_streaming_sp.

Covers the idempotent SHARED streaming service principal ("iot-sss-industries")
that every industry pipeline reuses, instead of minting one SP per pipeline.

Real SDK accessor: the codebase never stores a Databricks SDK ``WorkspaceClient``
on ``AppState`` (see core/state.py — no such field). Every existing provisioning
helper (``ensure_streaming_sp``, ``provision_lakebase_role``, etc.) either takes
``w`` as a parameter or instantiates ``WorkspaceClient()`` fresh inline. To keep
the ``ensure_shared_streaming_sp(state)`` signature required by the brief testable
without hitting a real workspace, the implementation resolves
``getattr(state, "workspace_client", None)`` and falls back to a fresh
``WorkspaceClient()`` otherwise — production code never sets that attribute today,
so production behaviour is unaffected; these tests inject a mock through it.
"""

from __future__ import annotations

import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services import provisioning_service

SHARED_SP_NAME = provisioning_service.SHARED_STREAMING_SP_DISPLAY_NAME


def _state_with_ws(w: MagicMock) -> types.SimpleNamespace:
    return types.SimpleNamespace(workspace_client=w)


def _existing_sp(app_id: str = "abc-123", name: str = SHARED_SP_NAME) -> MagicMock:
    sp = MagicMock()
    sp.application_id = app_id
    sp.display_name = name
    return sp


class TestEnsureSharedStreamingSP:
    """ensure_shared_streaming_sp resolves-or-creates ONE fixed-name SP."""

    @pytest.mark.asyncio
    async def test_returns_existing_sp_without_creating(self) -> None:
        w = MagicMock()
        w.service_principals.list.return_value = [_existing_sp()]

        with patch(
            "services.provisioning_service.grant_app_can_use"
        ) as grant_mock, patch(
            "services.provisioning_service._app_name", return_value="iot-digital-twin"
        ):
            out = await provisioning_service.ensure_shared_streaming_sp(
                _state_with_ws(w)
            )

        assert out == {
            "client_id": "abc-123",
            "display_name": SHARED_SP_NAME,
            "created": False,
        }
        w.service_principals.create.assert_not_called()
        # Grant is re-applied idempotently even on the reuse path.
        grant_mock.assert_called_once()
        assert grant_mock.call_args.args[2] == "abc-123"

    @pytest.mark.asyncio
    async def test_creates_shared_sp_when_absent(self) -> None:
        w = MagicMock()
        w.service_principals.list.return_value = []
        created_sp = MagicMock()
        created_sp.id = "sp-id-1"
        created_sp.application_id = "new-app-id"
        w.service_principals.create.return_value = created_sp
        w.api_client.do.return_value = {"secret": "shh"}

        with patch(
            "services.provisioning_service.provision_secrets_scope",
            new=AsyncMock(return_value={"status": "created"}),
        ) as scope_mock, patch(
            "services.provisioning_service.provision_lakebase_role",
            new=AsyncMock(return_value=[]),
        ) as role_mock, patch(
            "services.provisioning_service.grant_app_can_use"
        ) as grant_mock, patch(
            "services.provisioning_service._app_name", return_value="iot-digital-twin"
        ), patch.dict(
            "os.environ",
            {
                "LAKEBASE_ENDPOINT": (
                    "projects/iot-lakets-demo/branches/production/endpoints/primary"
                )
            },
        ):
            out = await provisioning_service.ensure_shared_streaming_sp(
                _state_with_ws(w)
            )

        assert out == {
            "client_id": "new-app-id",
            "display_name": SHARED_SP_NAME,
            "created": True,
        }
        w.service_principals.create.assert_called_once_with(
            display_name=SHARED_SP_NAME, active=True,
        )
        scope_mock.assert_called_once_with("iot-secrets")
        grant_mock.assert_called_once()
        assert grant_mock.call_args.args[2] == "new-app-id"
        role_mock.assert_called_once()
        assert role_mock.call_args.kwargs["sp_client_id"] == "new-app-id"
        assert role_mock.call_args.kwargs["project_id"] == "iot-lakets-demo"
        assert role_mock.call_args.kwargs["branch_id"] == "production"

    @pytest.mark.asyncio
    async def test_missing_lakebase_endpoint_skips_role_registration(self) -> None:
        w = MagicMock()
        w.service_principals.list.return_value = []
        created_sp = MagicMock()
        created_sp.id = "sp-id-2"
        created_sp.application_id = "new-app-id-2"
        w.service_principals.create.return_value = created_sp
        w.api_client.do.return_value = {"secret": "shh"}

        with patch(
            "services.provisioning_service.provision_secrets_scope",
            new=AsyncMock(return_value={"status": "created"}),
        ), patch(
            "services.provisioning_service.provision_lakebase_role",
            new=AsyncMock(return_value=[]),
        ) as role_mock, patch(
            "services.provisioning_service.grant_app_can_use"
        ), patch(
            "services.provisioning_service._app_name", return_value="iot-digital-twin"
        ), patch.dict(
            "os.environ", {"LAKEBASE_ENDPOINT": ""}, clear=False,
        ):
            out = await provisioning_service.ensure_shared_streaming_sp(
                _state_with_ws(w)
            )

        assert out["created"] is True
        assert out["client_id"] == "new-app-id-2"
        role_mock.assert_not_called()
