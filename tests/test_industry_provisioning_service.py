"""Tests for the industry provisioning orchestrator (plan Phase 2)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services import industry_provisioning_service as ips


def _patch_all(*, preflight_pass=True):
    """Return a context-manager stack patching every collaborator of the
    orchestrator with mocks; yields the mocks dict."""
    checks = [{"check": "x", "status": "pass" if preflight_pass else "fail", "detail": "d"}]
    write_mgr = MagicMock()
    write_mgr.close = AsyncMock()

    patches = {
        "preflight_checks": patch.object(ips.cat, "preflight_checks", return_value=checks),
        "ensure_shared_streaming_sp": patch.object(
            ips.provisioning_service, "ensure_shared_streaming_sp",
            new=AsyncMock(return_value={"client_id": "app-xyz", "created": False}),
        ),
        "ensure_secret_scope": patch.object(
            ips.cat, "ensure_secret_scope",
            new=AsyncMock(return_value={"resource": "scope", "status": "created"}),
        ),
        "provision_industry_catalog": patch.object(
            ips.cat, "provision_industry_catalog",
            new=AsyncMock(return_value={
                "catalog": "iot_spiketest",
                "results": [],
                "metrics_table": "iot_spiketest.metrics.streaming_metrics",
                "metrics_location": "s3://x/",
            }),
        ),
        "provision_industry_branch": patch.object(
            ips.pps, "provision_industry_branch",
            new=AsyncMock(return_value={
                "project": "iot-pipelines", "branch": "spiketest",
                "primary_host": "p", "replica_host": "r",
            }),
        ),
        "get_write_conn": patch.object(
            ips.pps, "get_write_conn", new=AsyncMock(return_value=write_mgr),
        ),
        "setup_lakets_on_branch": patch.object(
            ips.setup, "setup_lakets_on_branch",
            new=AsyncMock(return_value={"branch": "spiketest", "steps": []}),
        ),
        "grant_streaming_sp": patch.object(
            ips.cat, "grant_streaming_sp",
            new=AsyncMock(return_value={"catalog": "iot_spiketest", "results": []}),
        ),
        "grant_app_backend_read": patch.object(
            ips.cat, "grant_app_backend_read",
            new=AsyncMock(return_value={"catalog": "iot_spiketest", "principal": "app-sp", "results": []}),
        ),
    }
    return patches, write_mgr


@pytest.mark.asyncio
async def test_orchestrator_runs_all_phases_and_records_sp() -> None:
    patches, write_mgr = _patch_all()
    recorded: list[dict] = []

    async def _rec(_db, **kw):
        recorded.append(kw)

    with patches["preflight_checks"], patches["ensure_shared_streaming_sp"], \
         patches["ensure_secret_scope"], patches["provision_industry_catalog"], \
         patches["provision_industry_branch"], patches["get_write_conn"], \
         patches["setup_lakets_on_branch"], patches["grant_streaming_sp"], \
         patches["grant_app_backend_read"], \
         patch.object(ips, "record_resource", side_effect=_rec):
        summary = await ips.provision_industry(
            "spiketest", "setup-1", db=MagicMock(), ws=MagicMock(),
        )

    phases = [s["phase"] for s in summary["sections"]]
    for expected in (
        "preflight", "shared_sp", "secret_scope", "uc_catalog",
        "lakebase_branch", "lakets_setup", "grants",
    ):
        assert expected in phases, f"missing phase {expected}"

    assert summary["catalog"] == "iot_spiketest"
    assert summary["branch"] == "spiketest"
    assert summary["sp_principal"] == "app-xyz"
    # SP recorded as a shared resource.
    sp_rows = [r for r in recorded if r["resource_type"] == "service_principal"]
    assert sp_rows and sp_rows[0]["metadata"]["shared"] is True
    write_mgr.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_orchestrator_raises_on_failed_preflight() -> None:
    patches, _ = _patch_all(preflight_pass=False)
    with patches["preflight_checks"], patches["ensure_shared_streaming_sp"]:
        with pytest.raises(RuntimeError, match="Pre-flight"):
            await ips.provision_industry("spiketest", "s1", ws=MagicMock())


@pytest.mark.asyncio
async def test_orchestrator_skips_realtime_when_disabled() -> None:
    patches, _ = _patch_all()
    with patches["preflight_checks"], patches["ensure_shared_streaming_sp"], \
         patches["ensure_secret_scope"], patches["provision_industry_catalog"], \
         patches["provision_industry_branch"] as branch_mock, \
         patches["get_write_conn"], patches["setup_lakets_on_branch"], \
         patches["grant_streaming_sp"], patches["grant_app_backend_read"]:
        summary = await ips.provision_industry(
            "spiketest", "s1", ws=MagicMock(), enable_realtime=False,
        )
    branch_mock.assert_not_called()
    phases = [s["phase"] for s in summary["sections"]]
    assert "lakebase_branch" not in phases
    assert "grants" in phases
