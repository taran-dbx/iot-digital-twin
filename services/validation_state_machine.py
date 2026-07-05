"""Pure, persisted validation state machine (spec §1.10, §1.12; plan Phase 4).

This module contains **only** side-effect-free logic that operates on a plain
``dict`` — the value stored in ``pipeline_setup.validation_state`` (JSONB).  It
knows nothing about clusters, Kafka, or the database; the effectful step
executors live in ``services/validation_steps.py`` and the driver that ties
them together lives in ``services/validation_service.py``.  Keeping the state
machine pure makes the transition / retry-from-failure / resume / deploy-gate
logic exhaustively unit-testable with no mocks.

State shape::

    {
      "version": 1,
      "enabled_modes": ["realtime", "microbatch", "zerobus"],
      "sequence": ["provision_cluster", ..., "realtime:write_test", ..., "done"],
      "steps": {"<key>": {"status", "error", "updated_at"}, ...},
      "cluster_id": "0705-...",           # set by provision_cluster
      "run_ids": {"<key>": "run-123"},    # per-step bounded-run ids
    }

Step flow (spec §1.12)::

    provision_cluster → source_connectivity → topics_created → schema_conformance
      ├─ realtime:  partitions_precreate → write_test → cdf_manual(waiting_admin)
      │             → cdf_verify → read_back → dbsql_read → metrics_check → cleanup
      ├─ microbatch: write_test → dbsql_read → metrics_check → cleanup
      └─ zerobus:   create_table → auth_probe → produce_test → dbsql_read → cleanup
    → show_final_notebook → done

Common steps are stored under their bare name (``"provision_cluster"``); per-mode
steps under a ``"<mode>:<step>"`` key (``"realtime:write_test"``) so the same
step name in different lanes stays distinct.

All mutating helpers return a **new** dict (deep-copied) — the input is never
mutated, per the immutability rule.
"""

from __future__ import annotations

import copy
from typing import Iterable

__all__ = [
    "PENDING", "RUNNING", "PASSED", "FAILED", "WAITING_ADMIN", "STEP_STATUSES",
    "COMMON_STEPS", "MODE_STEPS", "MODE_ORDER", "TERMINAL_STEPS",
    "WAITING_ADMIN_STEPS", "CLEANUP_STEP", "SEP",
    "step_key", "bare_step", "mode_of", "is_terminal",
    "build_sequence", "initial_state", "get_step", "set_step",
    "mark_running", "next_runnable_step", "retry_from", "mark_cdf_enabled",
    "validation_step_keys", "outstanding_steps", "is_gate_open",
    "data_purged", "gate_status",
]

# --- Step statuses ---------------------------------------------------------
PENDING = "pending"
RUNNING = "running"
PASSED = "passed"
FAILED = "failed"
WAITING_ADMIN = "waiting_admin"

STEP_STATUSES = (PENDING, RUNNING, PASSED, FAILED, WAITING_ADMIN)

# --- Step catalogue --------------------------------------------------------
COMMON_STEPS: tuple[str, ...] = (
    "provision_cluster",
    "source_connectivity",
    "topics_created",
    "schema_conformance",
)

MODE_STEPS: dict[str, tuple[str, ...]] = {
    "realtime": (
        "partitions_precreate",
        "write_test",
        "cdf_manual",
        "cdf_verify",
        "read_back",
        "dbsql_read",
        "metrics_check",
        "cleanup",
    ),
    "microbatch": (
        "write_test",
        "dbsql_read",
        "metrics_check",
        "cleanup",
    ),
    "zerobus": (
        "create_table",
        "auth_probe",
        "produce_test",
        "dbsql_read",
        "cleanup",
    ),
}

# Deterministic ordering of the per-mode lanes in the flat sequence.
MODE_ORDER: tuple[str, ...] = ("realtime", "microbatch", "zerobus")

# Terminal steps after every lane — not counted toward the deploy gate.
TERMINAL_STEPS: tuple[str, ...] = ("show_final_notebook", "done")

# Steps that park in ``waiting_admin`` for a manual admin action.
WAITING_ADMIN_STEPS: frozenset[str] = frozenset({"cdf_manual"})

# The data-purge step (its success proves test data was cleaned up — §1.13).
CLEANUP_STEP = "cleanup"

SEP = ":"


# --- Key helpers -----------------------------------------------------------
def step_key(mode: str | None, step: str) -> str:
    """Return the flat state key for ``step`` in ``mode`` (``None`` = common)."""
    return f"{mode}{SEP}{step}" if mode else step


def bare_step(key: str) -> str:
    """Return the bare step name for a flat key (strips any ``<mode>:`` prefix)."""
    return key.split(SEP, 1)[1] if SEP in key else key


def mode_of(key: str) -> str | None:
    """Return the mode for a flat key, or ``None`` for a common/terminal step."""
    return key.split(SEP, 1)[0] if SEP in key else None


def is_terminal(key: str) -> bool:
    """True when ``key`` is a terminal (non-gated) step."""
    return key in TERMINAL_STEPS


# --- Construction ----------------------------------------------------------
def _blank_step() -> dict:
    return {"status": PENDING, "error": None, "updated_at": None}


def build_sequence(enabled_modes: Iterable[str]) -> list[str]:
    """Return the ordered flat step sequence for the enabled modes.

    Common steps first, then each enabled lane in :data:`MODE_ORDER`, then the
    terminal steps.  Unknown / disabled modes are ignored.
    """
    enabled = set(enabled_modes or ())
    seq = list(COMMON_STEPS)
    for mode in MODE_ORDER:
        if mode in enabled:
            seq += [step_key(mode, s) for s in MODE_STEPS[mode]]
    seq += list(TERMINAL_STEPS)
    return seq


def initial_state(enabled_modes: Iterable[str]) -> dict:
    """Return a fresh validation state with every step ``pending``."""
    seq = build_sequence(enabled_modes)
    return {
        "version": 1,
        "enabled_modes": [m for m in MODE_ORDER if m in set(enabled_modes or ())],
        "sequence": seq,
        "steps": {key: _blank_step() for key in seq},
        "cluster_id": None,
        "run_ids": {},
    }


# --- Accessors -------------------------------------------------------------
def get_step(state: dict, key: str) -> dict:
    """Return the step record for ``key`` (raises ``KeyError`` if absent)."""
    return state["steps"][key]


# --- Transitions (all return a new state) ----------------------------------
def set_step(
    state: dict,
    key: str,
    status: str,
    *,
    error: str | None = None,
    updated_at: str | None = None,
) -> dict:
    """Return a copy of ``state`` with ``key`` set to ``status`` (+ error/ts)."""
    if key not in state.get("steps", {}):
        raise KeyError(f"unknown step: {key!r}")
    if status not in STEP_STATUSES:
        raise ValueError(f"invalid status: {status!r}")
    new = copy.deepcopy(state)
    new["steps"][key] = {"status": status, "error": error, "updated_at": updated_at}
    return new


def mark_running(state: dict, key: str, *, updated_at: str | None = None) -> dict:
    """Return a copy with ``key`` marked ``running``."""
    return set_step(state, key, RUNNING, updated_at=updated_at)


def next_runnable_step(state: dict) -> str | None:
    """Return the next step to execute, or ``None`` when blocked/complete.

    Walks the sequence: skips ``passed`` steps; returns the first
    ``pending`` / ``failed`` / ``running`` step (running = interrupted, safe to
    re-run on resume).  Returns ``None`` if the first non-passed step is
    ``waiting_admin`` (parked on a manual action) or if every step is passed.
    """
    for key in state["sequence"]:
        status = state["steps"][key]["status"]
        if status == PASSED:
            continue
        if status == WAITING_ADMIN:
            return None
        return key
    return None


def retry_from(state: dict, key: str) -> dict:
    """Reset ``key`` and every downstream step to ``pending`` (retry-from-failure).

    Upstream ``passed`` steps are left untouched, so a retry re-runs only the
    failed step and its dependents (spec §1.12).
    """
    seq = state["sequence"]
    if key not in seq:
        raise KeyError(f"unknown step: {key!r}")
    idx = seq.index(key)
    new = copy.deepcopy(state)
    for downstream in seq[idx:]:
        new["steps"][downstream] = _blank_step()
    return new


def mark_cdf_enabled(state: dict, mode: str = "realtime") -> dict:
    """Mark the ``<mode>:cdf_manual`` step ``passed`` (admin clicked "enabled").

    Used by the ``/validate/cdf-enabled`` endpoint to release the
    ``waiting_admin`` park so the driver can run ``cdf_verify`` downstream.
    """
    key = step_key(mode, "cdf_manual")
    if key not in state.get("steps", {}):
        raise KeyError(f"no cdf_manual step for mode {mode!r}")
    return set_step(state, key, PASSED)


# --- Deploy gate (spec §1.12) ----------------------------------------------
def validation_step_keys(state: dict) -> list[str]:
    """Return every non-terminal step key (the gate-relevant steps)."""
    return [k for k in state["sequence"] if not is_terminal(k)]


def outstanding_steps(state: dict) -> list[str]:
    """Return non-terminal steps that are not yet ``passed``."""
    return [
        k for k in validation_step_keys(state)
        if state["steps"][k]["status"] != PASSED
    ]


def is_gate_open(state: dict) -> bool:
    """True when every non-terminal step is ``passed`` (hard deploy gate).

    Because every mode lane ends in a ``cleanup`` step, an open gate implies
    test data has been purged as well.
    """
    return not outstanding_steps(state)


def data_purged(state: dict) -> bool:
    """True when every mode's ``cleanup`` step has ``passed`` (test data purged)."""
    cleanup_keys = [k for k in state["sequence"] if bare_step(k) == CLEANUP_STEP]
    return all(state["steps"][k]["status"] == PASSED for k in cleanup_keys)


def gate_status(state: dict) -> dict:
    """Return the deploy-gate verdict + what is still outstanding."""
    return {
        "can_deploy": is_gate_open(state),
        "data_purged": data_purged(state),
        "outstanding": outstanding_steps(state),
    }
