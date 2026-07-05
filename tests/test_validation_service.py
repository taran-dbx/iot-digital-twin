"""Unit tests for the validation driver (plan Phase 4).

Exercises the driver end-to-end with **fake executors** and an in-memory repo —
no cluster, no Kafka, no real DB — covering: full run to an open deploy gate,
halt-on-failure + retry-from-failure, the ``cdf_manual`` waiting_admin park +
release, resume after a crash, and top-level data merge (``cluster_id``).
"""

from __future__ import annotations

import copy

import pytest

from services import validation_service as vs
from services import validation_state_machine as sm

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# In-memory repo double
# ---------------------------------------------------------------------------
class FakeRepo:
    """Minimal stand-in for ``pipeline_setup_repo`` backed by one dict."""

    def __init__(self, setup: dict) -> None:
        self.setup = setup
        self.update_calls = 0

    async def get_setup_by_industry(self, db, industry):
        if self.setup.get("industry") != industry:
            return None
        return copy.deepcopy(self.setup)

    async def update_setup(self, db, setup_id, **fields):
        self.update_calls += 1
        self.setup.update(fields)
        return copy.deepcopy(self.setup)


@pytest.fixture()
def repo(monkeypatch):
    """Patch the service's repo with an in-memory double; return it."""
    setup = {
        "id": "setup-1",
        "industry": "pharmaceutical",
        "enabled_modes": ["microbatch"],
        "validation_state": {},
        "warehouse_id": "wh-1",
    }
    fake = FakeRepo(setup)
    monkeypatch.setattr(vs.pipeline_setup_repo, "get_setup_by_industry",
                        fake.get_setup_by_industry)
    monkeypatch.setattr(vs.pipeline_setup_repo, "update_setup", fake.update_setup)
    return fake


# ---------------------------------------------------------------------------
# Fake executors
# ---------------------------------------------------------------------------
def _pass(**data):
    async def _fn(ctx):
        return vs.StepResult(sm.PASSED, data=data)
    return _fn


def _fail(msg):
    async def _fn(ctx):
        return vs.StepResult(sm.FAILED, error=msg)
    return _fn


async def _wait_admin(ctx):
    return vs.StepResult(sm.WAITING_ADMIN)


def _all_pass_executors():
    # Bare-name keyed, mirroring DEFAULT_EXECUTORS.
    names = [
        "provision_cluster", "source_connectivity", "topics_created",
        "schema_conformance", "partitions_precreate", "write_test",
        "cdf_manual", "cdf_verify", "read_back", "dbsql_read",
        "metrics_check", "cleanup", "create_table", "auth_probe", "produce_test",
    ]
    return {n: _pass() for n in names}


# ---------------------------------------------------------------------------
# Full run → open gate
# ---------------------------------------------------------------------------
async def test_full_run_opens_gate(repo):
    execs = _all_pass_executors()
    state = await vs.start_validation(None, "pharmaceutical", executors=execs)

    assert sm.is_gate_open(state) is True
    # Persisted state matches.
    assert sm.is_gate_open(repo.setup["validation_state"]) is True
    # Terminal steps ran too (no-op passes since not in exec map → default pass).
    assert state["steps"]["done"]["status"] == sm.PASSED


async def test_start_missing_setup_raises(repo):
    with pytest.raises(vs.ValidationError):
        await vs.start_validation(None, "unknown-industry", executors={})


# ---------------------------------------------------------------------------
# Halt on failure + retry from failure point
# ---------------------------------------------------------------------------
async def test_run_halts_on_failure(repo):
    execs = _all_pass_executors()
    execs["source_connectivity"] = _fail("dns down")
    state = await vs.start_validation(None, "pharmaceutical", executors=execs)

    assert state["steps"]["source_connectivity"]["status"] == sm.FAILED
    assert state["steps"]["source_connectivity"]["error"] == "dns down"
    # Downstream never ran.
    assert state["steps"]["topics_created"]["status"] == sm.PENDING
    assert sm.is_gate_open(state) is False


async def test_retry_from_failure_resumes(repo):
    execs = _all_pass_executors()
    execs["source_connectivity"] = _fail("dns down")
    await vs.start_validation(None, "pharmaceutical", executors=execs)

    # Fix the backend cause → executor now passes; retry that step.
    execs["source_connectivity"] = _pass()
    state = await vs.retry_step(
        None, "pharmaceutical", "source_connectivity", executors=execs
    )
    assert sm.is_gate_open(state) is True


async def test_retry_unknown_step_raises(repo):
    await vs.start_validation(None, "pharmaceutical", executors=_all_pass_executors())
    with pytest.raises(vs.ValidationError):
        await vs.retry_step(None, "pharmaceutical", "realtime:write_test", executors={})


# ---------------------------------------------------------------------------
# cdf_manual park + release (realtime lane)
# ---------------------------------------------------------------------------
@pytest.fixture()
def rt_repo(monkeypatch):
    setup = {
        "id": "setup-rt",
        "industry": "pharmaceutical",
        "enabled_modes": ["realtime"],
        "validation_state": {},
        "warehouse_id": "wh-1",
    }
    fake = FakeRepo(setup)
    monkeypatch.setattr(vs.pipeline_setup_repo, "get_setup_by_industry",
                        fake.get_setup_by_industry)
    monkeypatch.setattr(vs.pipeline_setup_repo, "update_setup", fake.update_setup)
    return fake


async def test_realtime_parks_on_cdf_manual(rt_repo):
    execs = _all_pass_executors()
    execs["cdf_manual"] = _wait_admin
    state = await vs.start_validation(None, "pharmaceutical", executors=execs)

    assert state["steps"]["realtime:cdf_manual"]["status"] == sm.WAITING_ADMIN
    # Parked — cdf_verify and downstream have not run.
    assert state["steps"]["realtime:cdf_verify"]["status"] == sm.PENDING
    assert sm.next_runnable_step(state) is None


async def test_cdf_enabled_releases_and_completes(rt_repo):
    execs = _all_pass_executors()
    execs["cdf_manual"] = _wait_admin
    await vs.start_validation(None, "pharmaceutical", executors=execs)

    # Admin enables managed sync → release the park (cdf_manual now passes).
    state = await vs.set_cdf_enabled(None, "pharmaceutical", executors=execs)
    assert state["steps"]["realtime:cdf_manual"]["status"] == sm.PASSED
    assert sm.is_gate_open(state) is True


# ---------------------------------------------------------------------------
# Resume after interruption
# ---------------------------------------------------------------------------
async def test_resume_continues_from_persisted_state(repo):
    execs = _all_pass_executors()
    execs["topics_created"] = _fail("kafka perms")
    await vs.start_validation(None, "pharmaceutical", executors=execs)
    assert repo.setup["validation_state"]["steps"]["topics_created"]["status"] == sm.FAILED

    # Simulate a reload: resume rehydrates the persisted state. Without a retry
    # the failed step is the next runnable, so a fixed executor lets it proceed.
    execs["topics_created"] = _pass()
    state = await vs.resume_validation(None, "pharmaceutical", executors=execs)
    assert sm.is_gate_open(state) is True


# ---------------------------------------------------------------------------
# cluster_id / run_id merge
# ---------------------------------------------------------------------------
async def test_cluster_id_merged_to_top_level(repo):
    execs = _all_pass_executors()
    execs["provision_cluster"] = _pass(cluster_id="0705-abc")

    async def _rec_run(ctx):
        return vs.StepResult(sm.PASSED, data={"run_id": "run-xyz"})

    execs["write_test"] = _rec_run
    state = await vs.start_validation(None, "pharmaceutical", executors=execs)

    assert state["cluster_id"] == "0705-abc"
    assert state["run_ids"]["microbatch:write_test"] == "run-xyz"


# ---------------------------------------------------------------------------
# can_deploy gate
# ---------------------------------------------------------------------------
async def test_can_deploy_reflects_gate(repo):
    execs = _all_pass_executors()
    execs["metrics_check"] = _fail("no metrics")
    await vs.start_validation(None, "pharmaceutical", executors=execs)

    gate = await vs.can_deploy(None, "pharmaceutical")
    assert gate["can_deploy"] is False
    assert "microbatch:metrics_check" in gate["outstanding"]
    # Per-mode verdicts are exposed so the frontend can gate Deploy per lane.
    assert gate["modes"]["microbatch"]["can_deploy"] is False
    assert "microbatch:metrics_check" in gate["modes"]["microbatch"]["outstanding"]

    # A never-started setup still reports a closed gate with full outstanding list.
    repo.setup["validation_state"] = {}
    gate2 = await vs.can_deploy(None, "pharmaceutical")
    assert gate2["can_deploy"] is False
    assert "provision_cluster" in gate2["outstanding"]
