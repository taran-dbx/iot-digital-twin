"""Validation harness orchestration (plan Phase 4; spec §1.10–1.13).

Ties the pure state machine (``services/validation_state_machine.py``) to the
persisted ``pipeline_setup.validation_state`` column and the effectful step
executors (``services/validation_steps.py``).  Public API used by
``routes/validation_admin.py``:

- :func:`start_validation`   — build a fresh state, persist, run to the first
                               block/failure/park.
- :func:`resume_validation`  — rehydrate the persisted state and keep running
                               (survives tab/browser close — spec §1.12).
- :func:`get_state`          — return the persisted state (poll for live status).
- :func:`retry_step`         — reset a failed step + downstream, run from there.
- :func:`set_cdf_enabled`    — release the ``cdf_manual`` park, run ``cdf_verify``+.
- :func:`can_deploy`         — the hard deploy gate verdict.

The **driver** (:func:`run_machine`) executes one step at a time: mark
``running`` → persist → run the executor → apply the result → persist → stop on
failure or ``waiting_admin``.  Persisting before and after every step is what
makes the flow resumable if the app restarts mid-run.

Executors are injected (``executors=`` / :data:`validation_steps.DEFAULT_EXECUTORS`)
so the driver is unit-tested with fakes and the live cluster/Kafka/DB work is
exercised only by the skippable integration test.
"""

from __future__ import annotations

import copy
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from services import pipeline_setup_repo
from services import validation_state_machine as sm

logger = logging.getLogger("iot_digital_twin.services.validation")

# Safety bound on driver iterations (a full 3-mode run is ~25 steps).
_MAX_DRIVER_STEPS = 200


class ValidationError(RuntimeError):
    """Raised for caller-facing validation flow errors (mapped to HTTP 4xx)."""


@dataclass(frozen=True)
class StepResult:
    """The outcome of running one step executor.

    ``status`` is one of :data:`sm.PASSED` / :data:`sm.FAILED` /
    :data:`sm.WAITING_ADMIN`.  ``data`` may carry ``cluster_id`` (merged to the
    top-level state) and ``run_id`` (recorded per-step under ``run_ids``).
    """

    status: str
    error: str | None = None
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StepContext:
    """Everything a step executor needs — no global state."""

    db: Any
    setup: dict
    state: dict
    key: str
    step: str
    mode: str | None
    ws: Any | None = None


# A step executor: takes a context, returns a StepResult.
Executor = Callable[[StepContext], Awaitable[StepResult]]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
async def _persist(db: Any, setup_id: Any, state: dict) -> None:
    """Write ``state`` back to ``pipeline_setup.validation_state``."""
    await pipeline_setup_repo.update_setup(db, setup_id, validation_state=state)


async def _require_setup(db: Any, industry: str) -> dict:
    setup = await pipeline_setup_repo.get_setup_by_industry(db, industry)
    if setup is None:
        raise ValidationError(f"no pipeline setup for industry {industry!r}")
    return setup


def _apply_result(state: dict, key: str, result: StepResult) -> dict:
    """Return a new state with ``key`` set to ``result`` and any data merged."""
    state = sm.set_step(state, key, result.status, error=result.error,
                        updated_at=_now_iso())
    data = result.data or {}
    if data:
        state = copy.deepcopy(state)
        cluster_id = data.get("cluster_id")
        if cluster_id:
            state["cluster_id"] = cluster_id
        run_id = data.get("run_id")
        if run_id:
            state.setdefault("run_ids", {})[key] = run_id
    return state


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
async def run_machine(
    db: Any,
    setup: dict,
    *,
    executors: dict[str, Executor] | None = None,
    ws: Any | None = None,
    max_steps: int = _MAX_DRIVER_STEPS,
) -> dict:
    """Run the state machine from its current state until it blocks or completes.

    Stops at the first ``failed`` or ``waiting_admin`` step (so the admin can fix
    the backend cause and retry — §1.12), or when every step is ``passed``.
    Persists before and after each step for resumability.  Returns the final
    state.
    """
    if executors is None:
        from services import validation_steps
        executors = validation_steps.DEFAULT_EXECUTORS

    state = setup.get("validation_state")
    if not state or "sequence" not in state:
        raise ValidationError("setup has no initialized validation_state")
    setup_id = setup["id"]

    for _ in range(max_steps):
        key = sm.next_runnable_step(state)
        if key is None:
            break  # complete or parked on waiting_admin

        # Mark running + persist (so a crash mid-step is visible on resume).
        state = sm.mark_running(state, key, updated_at=_now_iso())
        await _persist(db, setup_id, state)

        bare = sm.bare_step(key)
        executor = executors.get(bare)
        if executor is None:
            # No executor registered (e.g. terminal steps) → treat as a no-op pass.
            result = StepResult(sm.PASSED)
        else:
            ctx = StepContext(
                db=db, setup=setup, state=state, key=key, step=bare,
                mode=sm.mode_of(key), ws=ws,
            )
            try:
                result = await executor(ctx)
            except Exception as exc:  # any executor error → failed step, halt
                logger.warning("validation step %s failed: %s", key, exc)
                result = StepResult(sm.FAILED, error=str(exc)[:500])

        state = _apply_result(state, key, result)
        await _persist(db, setup_id, state)

        if result.status != sm.PASSED:
            break  # failed or waiting_admin — stop and wait for admin action

    return state


# ---------------------------------------------------------------------------
# Prepare helpers — persist the pre-run state synchronously, return the setup.
# The routes call these, return the state to the client, then run the driver in
# the background (a full run can take minutes provisioning the cluster).
# ---------------------------------------------------------------------------
async def prepare_start(db: Any, industry: str) -> dict:
    """Build a fresh validation state, persist it, return the setup (with state)."""
    setup = await _require_setup(db, industry)
    state = sm.initial_state(setup.get("enabled_modes") or [])
    await _persist(db, setup["id"], state)
    return {**setup, "validation_state": state}


async def prepare_retry(db: Any, industry: str, step_key: str) -> dict:
    """Reset ``step_key`` + downstream to pending, persist, return the setup."""
    setup = await _require_setup(db, industry)
    state = setup.get("validation_state") or {}
    if step_key not in state.get("steps", {}):
        raise ValidationError(f"unknown validation step: {step_key!r}")
    state = sm.retry_from(state, step_key)
    await _persist(db, setup["id"], state)
    return {**setup, "validation_state": state}


async def prepare_cdf_enabled(db: Any, industry: str, mode: str = "realtime") -> dict:
    """Release the ``<mode>:cdf_manual`` park, persist, return the setup."""
    setup = await _require_setup(db, industry)
    state = setup.get("validation_state") or {}
    if sm.step_key(mode, "cdf_manual") not in state.get("steps", {}):
        raise ValidationError(f"no cdf_manual step for mode {mode!r}")
    state = sm.mark_cdf_enabled(state, mode)
    await _persist(db, setup["id"], state)
    return {**setup, "validation_state": state}


# ---------------------------------------------------------------------------
# Public API — prepare + run (awaited).  Tests + the integration harness use
# these; the routes use the prepare_* helpers above plus a background run.
# ---------------------------------------------------------------------------
async def start_validation(
    db: Any,
    industry: str,
    *,
    executors: dict[str, Executor] | None = None,
    ws: Any | None = None,
) -> dict:
    """Initialize a fresh validation state from the setup's enabled modes and run.

    Overwrites any prior ``validation_state`` (a restart-from-scratch).  Use
    :func:`resume_validation` to continue an existing run.
    """
    setup = await prepare_start(db, industry)
    return await run_machine(db, setup, executors=executors, ws=ws)


async def resume_validation(
    db: Any,
    industry: str,
    *,
    executors: dict[str, Executor] | None = None,
    ws: Any | None = None,
) -> dict:
    """Rehydrate the persisted state and continue running where it left off."""
    setup = await _require_setup(db, industry)
    if not (setup.get("validation_state") or {}).get("sequence"):
        # Nothing started yet → start fresh.
        return await start_validation(db, industry, executors=executors, ws=ws)
    return await run_machine(db, setup, executors=executors, ws=ws)


async def get_state(db: Any, industry: str) -> dict | None:
    """Return the persisted ``validation_state`` for an industry (or ``None``)."""
    setup = await pipeline_setup_repo.get_setup_by_industry(db, industry)
    if setup is None:
        return None
    return setup.get("validation_state") or None


async def retry_step(
    db: Any,
    industry: str,
    step_key: str,
    *,
    executors: dict[str, Executor] | None = None,
    ws: Any | None = None,
) -> dict:
    """Reset ``step_key`` + downstream to pending, then run from there (§1.12)."""
    setup = await prepare_retry(db, industry, step_key)
    return await run_machine(db, setup, executors=executors, ws=ws)


async def set_cdf_enabled(
    db: Any,
    industry: str,
    *,
    mode: str = "realtime",
    executors: dict[str, Executor] | None = None,
    ws: Any | None = None,
) -> dict:
    """Release the ``<mode>:cdf_manual`` park and run ``cdf_verify`` downstream."""
    setup = await prepare_cdf_enabled(db, industry, mode)
    return await run_machine(db, setup, executors=executors, ws=ws)


async def can_deploy(db: Any, industry: str) -> dict:
    """Return the **per-mode** deploy-gate verdict for an industry.

    Aligns with the per-mode ``deployment_gate`` used by ``deploy_pipeline`` so
    the frontend can enable Deploy per mode.  Returns an aggregate
    (``can_deploy`` / ``data_purged`` / ``outstanding`` over all enabled modes)
    plus a ``modes`` map of per-mode verdicts.

    A setup with no started validation reports ``can_deploy=False`` with the
    full outstanding step list derived from its enabled modes.
    """
    setup = await _require_setup(db, industry)
    state = setup.get("validation_state") or {}
    enabled = setup.get("enabled_modes") or []
    if not state.get("sequence"):
        state = sm.initial_state(enabled)
    return sm.multi_mode_gate_status(state, enabled)
