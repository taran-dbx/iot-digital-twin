"""Unit tests for the pure validation state machine (plan Phase 4, spec §1.12).

Exhaustively covers sequence construction, transitions, retry-from-failure,
resume (next-runnable), the ``waiting_admin`` park, and the hard deploy gate —
no mocks required (the module is side-effect-free).
"""

from __future__ import annotations

import pytest

from services import validation_state_machine as sm


# ---------------------------------------------------------------------------
# Key helpers
# ---------------------------------------------------------------------------
def test_step_key_roundtrip():
    assert sm.step_key(None, "provision_cluster") == "provision_cluster"
    assert sm.step_key("realtime", "write_test") == "realtime:write_test"
    assert sm.bare_step("realtime:write_test") == "write_test"
    assert sm.bare_step("provision_cluster") == "provision_cluster"
    assert sm.mode_of("realtime:write_test") == "realtime"
    assert sm.mode_of("provision_cluster") is None


# ---------------------------------------------------------------------------
# Sequence construction
# ---------------------------------------------------------------------------
def test_build_sequence_all_modes_order():
    seq = sm.build_sequence(["zerobus", "realtime", "microbatch"])
    # Common first regardless of input order.
    assert seq[:4] == list(sm.COMMON_STEPS)
    # Lanes in canonical MODE_ORDER (realtime, microbatch, zerobus).
    assert seq.index("realtime:write_test") < seq.index("microbatch:write_test")
    assert seq.index("microbatch:write_test") < seq.index("zerobus:create_table")
    # Terminal last.
    assert seq[-2:] == list(sm.TERMINAL_STEPS)


def test_build_sequence_single_mode_excludes_others():
    seq = sm.build_sequence(["microbatch"])
    assert not any(k.startswith("realtime:") for k in seq)
    assert not any(k.startswith("zerobus:") for k in seq)
    assert "microbatch:cleanup" in seq


def test_build_sequence_realtime_lane_order():
    seq = sm.build_sequence(["realtime"])
    rt = [sm.bare_step(k) for k in seq if k.startswith("realtime:")]
    assert rt == list(sm.MODE_STEPS["realtime"])


def test_initial_state_all_pending():
    state = sm.initial_state(["realtime", "microbatch"])
    assert state["enabled_modes"] == ["realtime", "microbatch"]
    assert all(s["status"] == sm.PENDING for s in state["steps"].values())
    assert state["cluster_id"] is None
    assert state["run_ids"] == {}
    assert set(state["steps"]) == set(state["sequence"])


# ---------------------------------------------------------------------------
# Transitions + immutability
# ---------------------------------------------------------------------------
def test_set_step_is_immutable():
    state = sm.initial_state(["microbatch"])
    new = sm.set_step(state, "provision_cluster", sm.PASSED)
    assert new["steps"]["provision_cluster"]["status"] == sm.PASSED
    # original untouched
    assert state["steps"]["provision_cluster"]["status"] == sm.PENDING


def test_set_step_rejects_unknown_key_and_status():
    state = sm.initial_state(["microbatch"])
    with pytest.raises(KeyError):
        sm.set_step(state, "nope", sm.PASSED)
    with pytest.raises(ValueError):
        sm.set_step(state, "provision_cluster", "banana")


# ---------------------------------------------------------------------------
# Resume — next_runnable_step
# ---------------------------------------------------------------------------
def test_next_runnable_is_first_pending():
    state = sm.initial_state(["microbatch"])
    assert sm.next_runnable_step(state) == "provision_cluster"


def test_next_runnable_skips_passed():
    state = sm.initial_state(["microbatch"])
    state = sm.set_step(state, "provision_cluster", sm.PASSED)
    state = sm.set_step(state, "source_connectivity", sm.PASSED)
    assert sm.next_runnable_step(state) == "topics_created"


def test_next_runnable_returns_failed_step():
    state = sm.initial_state(["microbatch"])
    state = sm.set_step(state, "provision_cluster", sm.FAILED, error="boom")
    assert sm.next_runnable_step(state) == "provision_cluster"


def test_next_runnable_blocks_on_waiting_admin():
    state = sm.initial_state(["realtime"])
    for k in ("provision_cluster", "source_connectivity", "topics_created",
              "schema_conformance", "realtime:partitions_precreate",
              "realtime:write_test"):
        state = sm.set_step(state, k, sm.PASSED)
    state = sm.set_step(state, "realtime:cdf_manual", sm.WAITING_ADMIN)
    # Parked — nothing auto-runs until the admin releases cdf_manual.
    assert sm.next_runnable_step(state) is None


def test_next_runnable_none_when_all_passed():
    state = sm.initial_state(["microbatch"])
    for k in state["sequence"]:
        state = sm.set_step(state, k, sm.PASSED)
    assert sm.next_runnable_step(state) is None


# ---------------------------------------------------------------------------
# Retry from failure point
# ---------------------------------------------------------------------------
def test_retry_from_resets_step_and_downstream_only():
    state = sm.initial_state(["microbatch"])
    # Pretend everything through dbsql_read passed, then a later step failed.
    for k in state["sequence"]:
        state = sm.set_step(state, k, sm.PASSED)
    state = sm.set_step(state, "microbatch:dbsql_read", sm.FAILED, error="x")

    retried = sm.retry_from(state, "microbatch:dbsql_read")
    seq = retried["sequence"]
    idx = seq.index("microbatch:dbsql_read")
    # Upstream still passed.
    for k in seq[:idx]:
        assert retried["steps"][k]["status"] == sm.PASSED
    # Target + downstream reset to pending.
    for k in seq[idx:]:
        assert retried["steps"][k]["status"] == sm.PENDING


def test_retry_from_unknown_step_raises():
    state = sm.initial_state(["microbatch"])
    with pytest.raises(KeyError):
        sm.retry_from(state, "realtime:write_test")


def test_retry_after_fix_advances_past_failure():
    state = sm.initial_state(["microbatch"])
    state = sm.set_step(state, "provision_cluster", sm.PASSED)
    state = sm.set_step(state, "source_connectivity", sm.FAILED, error="dns")
    # Retry that failed step; it and downstream go pending → runnable again.
    state = sm.retry_from(state, "source_connectivity")
    assert sm.next_runnable_step(state) == "source_connectivity"
    # Fix + pass it → next runnable advances.
    state = sm.set_step(state, "source_connectivity", sm.PASSED)
    assert sm.next_runnable_step(state) == "topics_created"


# ---------------------------------------------------------------------------
# CDF manual release
# ---------------------------------------------------------------------------
def test_mark_cdf_enabled_releases_park():
    state = sm.initial_state(["realtime"])
    state = sm.set_step(state, "realtime:cdf_manual", sm.WAITING_ADMIN)
    state = sm.mark_cdf_enabled(state, "realtime")
    assert state["steps"]["realtime:cdf_manual"]["status"] == sm.PASSED


def test_mark_cdf_enabled_no_realtime_lane_raises():
    state = sm.initial_state(["microbatch"])
    with pytest.raises(KeyError):
        sm.mark_cdf_enabled(state, "realtime")


# ---------------------------------------------------------------------------
# Hard deploy gate
# ---------------------------------------------------------------------------
def test_gate_closed_until_all_steps_pass():
    state = sm.initial_state(["realtime", "microbatch"])
    assert sm.is_gate_open(state) is False
    status = sm.gate_status(state)
    assert status["can_deploy"] is False
    assert "provision_cluster" in status["outstanding"]


def test_gate_ignores_terminal_steps():
    state = sm.initial_state(["microbatch"])
    # Pass every non-terminal step; leave terminal steps pending.
    for k in sm.validation_step_keys(state):
        state = sm.set_step(state, k, sm.PASSED)
    assert sm.is_gate_open(state) is True
    assert sm.gate_status(state)["outstanding"] == []
    # Terminal steps still pending but the gate is open.
    assert state["steps"]["done"]["status"] == sm.PENDING


def test_gate_closed_if_cleanup_not_passed():
    state = sm.initial_state(["microbatch"])
    for k in sm.validation_step_keys(state):
        state = sm.set_step(state, k, sm.PASSED)
    # Reopen a cleanup step (data not purged) → gate closes.
    state = sm.set_step(state, "microbatch:cleanup", sm.PENDING)
    assert sm.data_purged(state) is False
    assert sm.is_gate_open(state) is False


def test_data_purged_true_when_all_cleanups_pass():
    state = sm.initial_state(["realtime", "microbatch", "zerobus"])
    cleanups = [k for k in state["sequence"] if sm.bare_step(k) == "cleanup"]
    assert len(cleanups) == 3
    for k in cleanups:
        state = sm.set_step(state, k, sm.PASSED)
    assert sm.data_purged(state) is True
