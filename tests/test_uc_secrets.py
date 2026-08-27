"""Tests for the UC-secret read/write helper (Task 3).

The app SP's own OAuth secret is stored as a Unity Catalog secret so the
Zerobus producer notebooks can read it at runtime.  These tests mock the SDK
api-client call and assert the exact ``/api/2.1/unity-catalog/secrets`` shape.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from services import uc_secrets


def _make_ws(do_return=None):
    ws = MagicMock()
    ws.api_client.do = MagicMock(return_value=do_return if do_return is not None else {})
    return ws


class TestPutSecret:
    @pytest.mark.asyncio
    async def test_posts_to_uc_secrets_api_with_secret_info(self) -> None:
        ws = _make_ws({})
        res = await uc_secrets.put_secret(
            "classic_stable_realtime_demos_catalog", "rtm_lakets_zb",
            "app_sp_oauth", "top-secret", ws=ws,
        )
        method = ws.api_client.do.call_args.args[0]
        path = ws.api_client.do.call_args.args[1]
        body = ws.api_client.do.call_args.kwargs["body"]
        assert method == "POST"
        assert path == "/api/2.1/unity-catalog/secrets"
        info = body["secret_info"]
        assert info["catalog_name"] == "classic_stable_realtime_demos_catalog"
        assert info["schema_name"] == "rtm_lakets_zb"
        assert info["name"] == "app_sp_oauth"
        assert info["value"] == "top-secret"
        assert res["status"] in ("created", "updated", "ok")

    @pytest.mark.asyncio
    async def test_already_exists_falls_back_to_update(self) -> None:
        ws = MagicMock()
        # First POST raises "already exists", the PATCH update then succeeds.
        ws.api_client.do = MagicMock(side_effect=[Exception("secret already exists"), {}])
        res = await uc_secrets.put_secret(
            "cat", "sch", "app_sp_oauth", "v2", ws=ws,
        )
        # Two calls: the failed POST and the update.
        assert ws.api_client.do.call_count == 2
        update_call = ws.api_client.do.call_args_list[1]
        assert update_call.args[0] in ("PATCH", "PUT")
        assert "cat.sch.app_sp_oauth" in update_call.args[1]
        assert res["status"] in ("updated", "created", "ok")


class TestGetSecret:
    @pytest.mark.asyncio
    async def test_gets_from_uc_secrets_api_and_returns_value(self) -> None:
        ws = _make_ws({"secret_info": {"value": "the-secret"}})
        val = await uc_secrets.get_secret("cat", "sch", "app_sp_oauth", ws=ws)
        method = ws.api_client.do.call_args.args[0]
        path = ws.api_client.do.call_args.args[1]
        assert method == "GET"
        assert path == "/api/2.1/unity-catalog/secrets/cat.sch.app_sp_oauth"
        assert val == "the-secret"

    @pytest.mark.asyncio
    async def test_returns_value_when_flat_shape(self) -> None:
        ws = _make_ws({"value": "flat-secret"})
        val = await uc_secrets.get_secret("cat", "sch", "app_sp_oauth", ws=ws)
        assert val == "flat-secret"
