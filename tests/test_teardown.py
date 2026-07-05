"""Tests for the teardown orchestrator (Phase 1, Task 1.4).

Unit tests use a mocked DB to assert the orchestrator dispatches per-resource
deleters in DESCENDING teardown_order, marks each ledger row deleted, and then
deletes the setup (FK cascade removes metadata).  An integration test against
the live iot-app-backend project proves nothing is orphaned after teardown.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from models import app_schema
from services import (
    deployment_repo,
    pipeline_setup_repo,
    resource_ledger,
    teardown_service,
)

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Unit — mocked DB
# ---------------------------------------------------------------------------
async def test_teardown_dispatches_in_descending_order(mock_db) -> None:
    # resources_in_teardown_order() maps db.fetch() results; feed descending rows.
    mock_db.fetch = AsyncMock(
        return_value=[
            {"id": "r3", "resource_type": "job", "identifier": "j", "metadata": {},
             "teardown_order": 90},
            {"id": "r2", "resource_type": "kafka_topic", "identifier": "t",
             "metadata": {}, "teardown_order": 50},
            {"id": "r1", "resource_type": "uc_catalog", "identifier": "c",
             "metadata": {}, "teardown_order": 10},
        ]
    )
    mock_db.fetchrow = AsyncMock(return_value={"id": "r", "status": "deleted",
                                               "metadata": {}})
    mock_db.execute = AsyncMock(return_value="DELETE 1")

    calls: list[str] = []

    async def _rec(res):
        calls.append(res["resource_type"])
        return {"status": "deleted"}

    deleters = {
        "job": AsyncMock(side_effect=_rec),
        "kafka_topic": AsyncMock(side_effect=_rec),
        "uc_catalog": AsyncMock(side_effect=_rec),
    }

    result = await teardown_service.teardown_setup(
        mock_db, "setup-1", deleters=deleters
    )

    assert calls == ["job", "kafka_topic", "uc_catalog"]
    assert result["success"] is True
    assert result["deleted_setup"] is True
    assert len(result["steps"]) == 3
    # every ledger row marked deleted (mark_resource_status → db.fetchrow UPDATE)
    assert mock_db.fetchrow.await_count == 3


async def test_teardown_skips_unknown_resource_type(mock_db) -> None:
    mock_db.fetch = AsyncMock(
        return_value=[
            {"id": "r1", "resource_type": "mystery_type", "identifier": "x",
             "metadata": {}, "teardown_order": 5},
        ]
    )
    mock_db.fetchrow = AsyncMock(return_value={"id": "r1", "status": "deleted",
                                               "metadata": {}})
    mock_db.execute = AsyncMock(return_value="DELETE 1")

    result = await teardown_service.teardown_setup(mock_db, "setup-1", deleters={})

    assert result["steps"][0]["action"] == "skipped"
    # unknown type still marked deleted and does not fail the teardown
    assert result["success"] is True
    assert mock_db.fetchrow.await_count == 1


async def test_teardown_reports_deleter_failure(mock_db) -> None:
    mock_db.fetch = AsyncMock(
        return_value=[
            {"id": "r1", "resource_type": "job", "identifier": "j", "metadata": {},
             "teardown_order": 5},
        ]
    )
    mock_db.fetchrow = AsyncMock(return_value={"id": "r1", "metadata": {}})
    mock_db.execute = AsyncMock(return_value="DELETE 1")

    async def _boom(res):
        raise RuntimeError("delete failed")

    result = await teardown_service.teardown_setup(
        mock_db, "setup-1", deleters={"job": AsyncMock(side_effect=_boom)}
    )

    assert result["success"] is False
    assert result["steps"][0]["action"] == "failed"
    assert "delete failed" in result["steps"][0]["error"]


async def test_teardown_failure_does_not_cascade_orphan(mock_db) -> None:
    # A failed deleter must NOT drop the setup row — keeping the ledger row
    # (marked ``failed``) is the only record that the cloud resource still
    # exists, so it is surfaced for retry instead of silently orphaned.
    mock_db.fetch = AsyncMock(
        return_value=[
            {"id": "r1", "resource_type": "uc_catalog", "identifier": "c",
             "metadata": {}, "teardown_order": 10},
        ]
    )
    mock_db.fetchrow = AsyncMock(return_value={"id": "r1", "metadata": {}})
    mock_db.execute = AsyncMock(return_value="DELETE 1")

    async def _boom(res):
        raise RuntimeError("boom")

    with patch.object(
        teardown_service.pipeline_setup_repo, "delete_setup", AsyncMock()
    ) as del_mock:
        result = await teardown_service.teardown_setup(
            mock_db, "setup-1", deleters={"uc_catalog": AsyncMock(side_effect=_boom)}
        )

    del_mock.assert_not_awaited()
    assert result["success"] is False
    assert result["deleted_setup"] is False
    # the failed row is marked ``failed`` (not ``deleted``) before we bail out
    assert mock_db.fetchrow.await_args.args[1] == "failed"


async def test_teardown_skips_shared_resources(mock_db) -> None:
    # Shared SP / secret-scope / iot-pipelines project rows carry
    # ``metadata.shared`` and must never be deleted per-industry — even when a
    # deleter is registered for their type.
    mock_db.fetch = AsyncMock(
        return_value=[
            {"id": "r1", "resource_type": "service_principal",
             "identifier": "iot-sss-industries", "metadata": {"shared": True},
             "teardown_order": 1},
        ]
    )
    mock_db.fetchrow = AsyncMock(return_value={"id": "r1", "metadata": {}})
    mock_db.execute = AsyncMock(return_value="DELETE 1")

    sp_deleter = AsyncMock()
    result = await teardown_service.teardown_setup(
        mock_db, "setup-1", deleters={"service_principal": sp_deleter}
    )

    sp_deleter.assert_not_awaited()  # shared guard fires before dispatch
    assert result["steps"][0]["action"] == "skipped"
    assert "shared" in result["steps"][0]["reason"].lower()
    assert result["success"] is True
    assert mock_db.fetchrow.await_args.args[1] == "skipped"


# ---------------------------------------------------------------------------
# Integration — live iot-app-backend, nothing orphaned
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture()
async def schema_ready(app_backend_conn):
    await app_schema.ensure_app_schema(app_backend_conn)
    return app_backend_conn


async def test_teardown_leaves_nothing_orphaned(schema_ready) -> None:
    db = schema_ready
    setup = await pipeline_setup_repo.create_setup(db, industry="pharmaceutical")
    dep = await deployment_repo.create_deployment(
        db, setup_id=setup["id"], mode="realtime"
    )
    await resource_ledger.record_resource(
        db, setup_id=setup["id"], resource_type="uc_catalog",
        identifier="iot_pharmaceutical", teardown_order=10,
    )
    await resource_ledger.record_resource(
        db, setup_id=setup["id"], deployment_id=dep["id"],
        resource_type="kafka_topic", identifier="iot.pharmaceutical.realtime",
        teardown_order=50,
    )

    order: list[str] = []

    async def _rec(res):
        order.append(res["resource_type"])
        return {"status": "deleted"}

    result = await teardown_service.teardown_setup(
        db, setup["id"],
        deleters={"uc_catalog": _rec, "kafka_topic": _rec},
    )

    assert result["success"] is True
    # descending teardown_order
    assert order == ["kafka_topic", "uc_catalog"]
    # setup gone + cascade wiped deployments and resources — nothing orphaned
    assert await pipeline_setup_repo.get_setup(db, setup["id"]) is None
    assert await deployment_repo.list_deployments(db, setup["id"]) == []
    assert await resource_ledger.list_resources(db, setup["id"]) == []
