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
        "bootstrap_servers": "b-1.kafka.us-east-1.amazonaws.com:9098",
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
        "bootstrap_servers": "b-1.kafka.us-east-1.amazonaws.com:9098",
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


# ---------------------------------------------------------------------------
# get_state enrichment — cluster_state / current_step / phase / updated_at
# (spec §1.12 — live progress for the validate/state poll)
# ---------------------------------------------------------------------------
class _FakeClusterState:
    """Stand-in for the SDK ``ClusterState`` enum (has a ``.value``)."""

    def __init__(self, value):
        self.value = value


class _FakeClusterInfo:
    def __init__(self, state):
        self.state = state


class _FakeClusters:
    """Records the cluster_id asked for; returns a canned state (or raises)."""

    def __init__(self, *, state_value=None, raises=False):
        self._state_value = state_value
        self._raises = raises
        self.asked = None

    def get(self, cluster_id=None):
        self.asked = cluster_id
        if self._raises:
            raise RuntimeError("cluster gone")
        return _FakeClusterInfo(_FakeClusterState(self._state_value))


class _FakeWs:
    def __init__(self, clusters):
        self.clusters = clusters


def _started_setup(**extra):
    """A setup whose validation_state has a real (mid-run) sequence."""
    state = sm.initial_state(["microbatch"])
    state = sm.set_step(state, "provision_cluster", sm.PASSED, updated_at="2026-07-06T00:00:00+00:00")
    state = sm.set_step(state, "source_connectivity", sm.RUNNING, updated_at="2026-07-06T00:01:00+00:00")
    state["cluster_id"] = "0705-abc"
    return {
        "id": "setup-1",
        "industry": "pharmaceutical",
        "enabled_modes": ["microbatch"],
        "validation_state": state,
        **extra,
    }


def _repo_with(monkeypatch, setup):
    fake = FakeRepo(setup)
    monkeypatch.setattr(vs.pipeline_setup_repo, "get_setup_by_industry",
                        fake.get_setup_by_industry)
    monkeypatch.setattr(vs.pipeline_setup_repo, "update_setup", fake.update_setup)
    return fake


async def test_get_state_surfaces_cluster_state(monkeypatch):
    _repo_with(monkeypatch, _started_setup())
    clusters = _FakeClusters(state_value="RUNNING")
    out = await vs.get_state(None, "pharmaceutical", ws=_FakeWs(clusters))
    assert clusters.asked == "0705-abc"
    assert out["cluster_state"] == "RUNNING"
    assert out["cluster_id"] == "0705-abc"


async def test_get_state_current_step_and_phase(monkeypatch):
    _repo_with(monkeypatch, _started_setup())
    out = await vs.get_state(None, "pharmaceutical",
                             ws=_FakeWs(_FakeClusters(state_value="RUNNING")))
    # source_connectivity is the running (first non-passed) step.
    assert out["current_step"] == "source_connectivity"
    # Cluster RUNNING + past provision_cluster → validating.
    assert out["phase"] == "validating"


async def test_get_state_phase_provisioning_when_cluster_pending(monkeypatch):
    setup = _started_setup()
    # Roll back to the provision_cluster step still running.
    state = sm.initial_state(["microbatch"])
    state = sm.set_step(state, "provision_cluster", sm.RUNNING)
    state["cluster_id"] = "0705-abc"
    setup["validation_state"] = state
    _repo_with(monkeypatch, setup)
    out = await vs.get_state(None, "pharmaceutical",
                             ws=_FakeWs(_FakeClusters(state_value="PENDING")))
    assert out["phase"] == "provisioning_cluster"
    assert out["cluster_state"] == "PENDING"


async def test_get_state_preserves_per_step_updated_at(monkeypatch):
    _repo_with(monkeypatch, _started_setup())
    out = await vs.get_state(None, "pharmaceutical",
                             ws=_FakeWs(_FakeClusters(state_value="RUNNING")))
    steps = out["steps"]
    assert steps["provision_cluster"]["updated_at"] == "2026-07-06T00:00:00+00:00"
    assert steps["source_connectivity"]["updated_at"] == "2026-07-06T00:01:00+00:00"
    # Existing fields kept intact (backwards-compatible additive shape).
    assert out["sequence"] == sm.build_sequence(["microbatch"])
    assert "run_ids" in out


async def test_get_state_null_cluster_state_when_no_cluster(monkeypatch):
    setup = _started_setup()
    setup["validation_state"]["cluster_id"] = None
    _repo_with(monkeypatch, setup)
    clusters = _FakeClusters(state_value="RUNNING")
    out = await vs.get_state(None, "pharmaceutical", ws=_FakeWs(clusters))
    assert out["cluster_state"] is None
    assert out["cluster_id"] is None
    assert clusters.asked is None  # no lookup attempted


async def test_get_state_unknown_when_lookup_fails(monkeypatch):
    _repo_with(monkeypatch, _started_setup())
    out = await vs.get_state(None, "pharmaceutical",
                             ws=_FakeWs(_FakeClusters(raises=True)))
    assert out["cluster_state"] == "UNKNOWN"


async def test_get_state_default_workspace_client_lookup_is_graceful(monkeypatch):
    # No ws injected → default client path; a failing client yields UNKNOWN,
    # never errors the endpoint.
    _repo_with(monkeypatch, _started_setup())
    monkeypatch.setattr(vs, "_workspace_client",
                        lambda: _FakeWs(_FakeClusters(raises=True)))
    out = await vs.get_state(None, "pharmaceutical")
    assert out["cluster_state"] == "UNKNOWN"


async def test_get_state_none_when_no_state(monkeypatch):
    _repo_with(monkeypatch, {"id": "s", "industry": "pharmaceutical",
                             "enabled_modes": ["microbatch"], "validation_state": {}})
    assert await vs.get_state(None, "pharmaceutical") is None


async def test_get_state_none_when_no_setup(monkeypatch):
    _repo_with(monkeypatch, {"id": "s", "industry": "other", "validation_state": {}})
    assert await vs.get_state(None, "pharmaceutical") is None


# ---------------------------------------------------------------------------
# prepare_start readiness guard (spec §1.10 — never 500; 4xx on incomplete draft)
# ---------------------------------------------------------------------------
def _ready_setup(**extra):
    """A setup that has every input an interactive validation run needs."""
    base = {
        "id": "setup-ready",
        "industry": "automotive",
        "enabled_modes": ["realtime", "microbatch", "zerobus"],
        "bootstrap_servers": "b-1.kafka.us-east-1.amazonaws.com:9098",
        "warehouse_id": "wh-1",
        "validation_state": {},
    }
    base.update(extra)
    return base


async def test_incomplete_setup_error_is_validation_error():
    # The route relies on the subclass relationship (catch 400 before 404).
    assert issubclass(vs.IncompleteSetupError, vs.ValidationError)


async def test_prepare_start_valid_returns_initial_state(monkeypatch):
    fake = _repo_with(monkeypatch, _ready_setup())
    out = await vs.prepare_start(None, "automotive")
    state = out["validation_state"]
    # A fresh, fully-pending state was built and persisted synchronously.
    assert state["sequence"] == sm.build_sequence(
        ["realtime", "microbatch", "zerobus"]
    )
    assert state["steps"]["provision_cluster"]["status"] == sm.PENDING
    assert fake.setup["validation_state"]["sequence"]  # persisted


async def test_prepare_start_only_adds_new_mode_when_enabled_modes_changed(monkeypatch):
    previous = sm.initial_state(["realtime", "microbatch"])
    for key in previous["sequence"]:
        previous = sm.set_step(previous, key, sm.PASSED)
    _repo_with(
        monkeypatch,
        _ready_setup(
            enabled_modes=["realtime", "microbatch", "zerobus"],
            validation_state=previous,
        ),
    )

    out = await vs.prepare_start(None, "automotive")
    state = out["validation_state"]

    assert state["steps"]["realtime:write_test"]["status"] == sm.PASSED
    assert state["steps"]["microbatch:write_test"]["status"] == sm.PASSED
    assert state["steps"]["zerobus:create_table"]["status"] == sm.PENDING


async def test_prepare_start_same_modes_is_explicit_full_rerun(monkeypatch):
    previous = sm.initial_state(["zerobus"])
    for key in previous["sequence"]:
        previous = sm.set_step(previous, key, sm.PASSED)
    _repo_with(
        monkeypatch,
        _ready_setup(enabled_modes=["zerobus"], validation_state=previous),
    )

    out = await vs.prepare_start(None, "automotive")

    assert all(
        record["status"] == sm.PENDING
        for record in out["validation_state"]["steps"].values()
    )


async def test_prepare_start_resumes_reconciled_state_with_new_pending_lane(monkeypatch):
    previous = sm.initial_state(["realtime"])
    for key in previous["sequence"]:
        previous = sm.set_step(previous, key, sm.PASSED)
    reconciled = sm.reconcile_modes(previous, ["realtime", "zerobus"])
    _repo_with(
        monkeypatch,
        _ready_setup(
            enabled_modes=["realtime", "zerobus"],
            validation_state=reconciled,
        ),
    )

    out = await vs.prepare_start(None, "automotive")

    assert out["validation_state"]["steps"]["realtime:write_test"]["status"] == sm.PASSED
    assert out["validation_state"]["steps"]["zerobus:create_table"]["status"] == sm.PENDING


async def test_prepare_start_incomplete_missing_warehouse_raises(monkeypatch):
    _repo_with(monkeypatch, _ready_setup(warehouse_id=None))
    with pytest.raises(vs.IncompleteSetupError) as ei:
        await vs.prepare_start(None, "automotive")
    assert "warehouse_id" in str(ei.value)
    assert "complete the source config first" in str(ei.value)


async def test_prepare_start_incomplete_missing_bootstrap_raises(monkeypatch):
    _repo_with(monkeypatch, _ready_setup(bootstrap_servers="   "))
    with pytest.raises(vs.IncompleteSetupError) as ei:
        await vs.prepare_start(None, "automotive")
    assert "bootstrap_servers" in str(ei.value)


async def test_prepare_start_incomplete_no_enabled_modes_raises(monkeypatch):
    _repo_with(monkeypatch, _ready_setup(enabled_modes=[]))
    with pytest.raises(vs.IncompleteSetupError) as ei:
        await vs.prepare_start(None, "automotive")
    assert "enabled_modes" in str(ei.value)


async def test_prepare_start_zerobus_only_needs_no_bootstrap(monkeypatch):
    # Zerobus is serverless (no Kafka), so a missing broker list is fine.
    _repo_with(monkeypatch, _ready_setup(enabled_modes=["zerobus"],
                                         bootstrap_servers=None))
    out = await vs.prepare_start(None, "automotive")
    assert out["validation_state"]["sequence"] == sm.build_sequence(["zerobus"])


async def test_prepare_start_missing_setup_raises_plain_validation_error(monkeypatch):
    # No setup at all → base ValidationError (route maps to 404), NOT incomplete.
    _repo_with(monkeypatch, {"id": "s", "industry": "other", "validation_state": {}})
    with pytest.raises(vs.ValidationError) as ei:
        await vs.prepare_start(None, "automotive")
    assert not isinstance(ei.value, vs.IncompleteSetupError)
