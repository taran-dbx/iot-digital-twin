"""Pure derivation of the admin pipeline API contract (status / health / startability).

The admin surface (``GET /api/admin/pipelines`` and the per-pipeline health
endpoint) must report *real* per-mode state — never a fabricated "idle/running"
or a hardcoded count.  This module contains **only** side-effect-free logic that
turns the persisted rows (``pipeline_setup`` + its ``pipeline_deployment`` rows +
``validation_state``) plus a *job-running lookup* (the caller resolves live job
state via the Jobs API) into the frontend contract:

    {
      "setup_id", "industry",
      "status",   # draft | running | stopped   (pipeline rollup)
      "health",   # draft | healthy | unhealthy  (pipeline rollup)
      "modes": [{"mode", "status", "health", "startable"}, ...],
      "startable", "ready",
    }

Derivation rules (spec §1.10, §1.12):

* **status** (per mode) — ``draft`` is the DEFAULT when there is no real data:
    - ``draft``    — the mode is not deployed (no deployment row / no ``job_id``).
    - ``running``  — the mode is deployed AND its Jobs-API run is RUNNING.
    - ``stopped``  — the mode is deployed but not running.
* **health** (per mode) — ``healthy | unhealthy | draft``:
    - ``draft``     — not deployed.
    - ``healthy``   — deployed AND the mode's validation gate is open AND (for
      realtime) managed CDF is confirmed.
    - ``unhealthy`` — deployed but the gate/CDF is incomplete (this is the case
      for every current realtime pipeline, since CDF is a manual step nobody has
      done yet).
* **startable** (per mode) — ``True`` ONLY when the mode is deployed AND its
  validation gate is open AND (for realtime) CDF is confirmed.  End users only
  see startable pipelines; admins see all.

Keeping this pure makes the whole contract exhaustively unit-testable with the
Jobs API / repos mocked, and guarantees a partial / empty row degrades to
``draft`` rather than raising.
"""

from __future__ import annotations

from typing import Iterable, Mapping

from services import validation_state_machine as sm

# --- Per-mode status ---------------------------------------------------------
STATUS_DRAFT = "draft"
STATUS_RUNNING = "running"
STATUS_STOPPED = "stopped"

# --- Health ------------------------------------------------------------------
HEALTH_DRAFT = "draft"
HEALTH_HEALTHY = "healthy"
HEALTH_UNHEALTHY = "unhealthy"

# ``pipeline_deployment.status`` values that (together with a ``job_id``) mean the
# mode's job was actually created in the workspace.
_NOT_DEPLOYED_STATUSES = frozenset({"pending", "deploying", "failed"})


def is_deployed(deployment: Mapping | None) -> bool:
    """True when a mode's job actually exists (has a ``job_id`` and is past deploy).

    A ``job_id`` is written only after the workspace Job is created, so its
    presence is the authoritative "deployed" signal; a row still ``pending`` /
    ``deploying`` / ``failed`` is not deployed even if it somehow carries an id.
    """
    if not deployment:
        return False
    if not deployment.get("job_id"):
        return False
    return deployment.get("status") not in _NOT_DEPLOYED_STATUSES


def cdf_confirmed(setup: Mapping | None, deployment: Mapping | None, mode: str) -> bool:
    """True when managed CDF is confirmed for ``mode`` (only realtime needs it).

    Non-realtime modes never require CDF.  For realtime, CDF is confirmed when
    the validation ``realtime:cdf_verify`` step passed OR the deployment row's
    ``cdf_enabled`` flag is set.  Since CDF is a **manual** admin step that is not
    done for current pipelines, this returns ``False`` for them.
    """
    if mode != "realtime":
        return True
    vstate = (setup or {}).get("validation_state") or {}
    if sm._key_status(vstate, sm.step_key("realtime", "cdf_verify")) == sm.PASSED:
        return True
    return bool(deployment and deployment.get("cdf_enabled"))


def gate_open(setup: Mapping | None, mode: str) -> bool:
    """True when the mode's validation deploy-gate is open (all required steps passed).

    Delegates to the single source of truth in ``validation_state_machine``.
    An unknown mode or empty state degrades to ``False`` (not open).
    """
    vstate = (setup or {}).get("validation_state") or {}
    try:
        return sm.is_mode_gate_open(vstate, mode)
    except ValueError:
        return False


def mode_status(deployment: Mapping | None, job_running: bool) -> str:
    """Return ``draft`` / ``running`` / ``stopped`` for a mode (draft is the default)."""
    if not is_deployed(deployment):
        return STATUS_DRAFT
    return STATUS_RUNNING if job_running else STATUS_STOPPED


def mode_startable(setup: Mapping | None, deployment: Mapping | None, mode: str) -> bool:
    """True only when the mode is deployed, its gate is open, and CDF is confirmed."""
    return (
        is_deployed(deployment)
        and gate_open(setup, mode)
        and cdf_confirmed(setup, deployment, mode)
    )


def mode_health(setup: Mapping | None, deployment: Mapping | None, mode: str) -> str:
    """Return ``draft`` / ``healthy`` / ``unhealthy`` for a mode."""
    if not is_deployed(deployment):
        return HEALTH_DRAFT
    if gate_open(setup, mode) and cdf_confirmed(setup, deployment, mode):
        return HEALTH_HEALTHY
    return HEALTH_UNHEALTHY


def build_mode_view(
    setup: Mapping | None,
    deployment: Mapping | None,
    mode: str,
    job_running: bool,
) -> dict:
    """Return the per-mode contract object ``{mode, status, health, startable}``."""
    return {
        "mode": mode,
        "status": mode_status(deployment, job_running),
        "health": mode_health(setup, deployment, mode),
        "startable": mode_startable(setup, deployment, mode),
    }


# --- Rollups -----------------------------------------------------------------
def rollup_status(mode_views: list[dict]) -> str:
    """Pipeline status: running if any mode runs, else stopped if any deployed, else draft."""
    statuses = {m["status"] for m in mode_views}
    if STATUS_RUNNING in statuses:
        return STATUS_RUNNING
    if STATUS_STOPPED in statuses:
        return STATUS_STOPPED
    return STATUS_DRAFT


def rollup_health(mode_views: list[dict]) -> str:
    """Pipeline health: unhealthy if any deployed mode is unhealthy, healthy if any
    healthy and none unhealthy, else draft (nothing deployed yet)."""
    healths = {m["health"] for m in mode_views}
    if HEALTH_UNHEALTHY in healths:
        return HEALTH_UNHEALTHY
    if HEALTH_HEALTHY in healths:
        return HEALTH_HEALTHY
    return HEALTH_DRAFT


def _ordered_modes(setup: Mapping, deployments: Iterable[Mapping]) -> list[str]:
    """Return the modes to report, ordered by :data:`sm.MODE_ORDER`.

    Union of the setup's ``enabled_modes`` and any mode that has a deployment
    row (so a deployed-but-not-enabled mode is never hidden), filtered to known
    modes and ordered deterministically.
    """
    known = set(sm.MODE_ORDER)
    seen: set[str] = set()
    for m in setup.get("enabled_modes") or []:
        if m in known:
            seen.add(m)
    for dep in deployments or []:
        m = dep.get("mode")
        if m in known:
            seen.add(m)
    return [m for m in sm.MODE_ORDER if m in seen]


def build_pipeline_view(
    setup: Mapping,
    deployments: Iterable[Mapping],
    job_running: Mapping[str, bool] | None = None,
) -> dict:
    """Return the full pipeline contract object for one setup.

    ``job_running`` maps ``job_id`` (as ``str``) → whether its Jobs-API run is
    RUNNING; the caller resolves this via the Jobs API and passes it in so this
    stays pure.  A missing / empty map means no mode is running (default draft /
    stopped, never a fabricated "running").
    """
    deployments = list(deployments or [])
    job_running = job_running or {}
    dep_by_mode = {d.get("mode"): d for d in deployments}

    mode_views: list[dict] = []
    for mode in _ordered_modes(setup, deployments):
        dep = dep_by_mode.get(mode)
        job_id = dep.get("job_id") if dep else None
        running = bool(job_running.get(str(job_id))) if job_id else False
        mode_views.append(build_mode_view(setup, dep, mode, running))

    startable = any(m["startable"] for m in mode_views)
    return {
        "setup_id": setup.get("id"),
        "industry": setup.get("industry"),
        "status": rollup_status(mode_views),
        "health": rollup_health(mode_views),
        "modes": mode_views,
        "startable": startable,
        "ready": startable,
    }


def collect_job_ids(deployments: Iterable[Mapping]) -> list[str]:
    """Return the distinct ``job_id`` strings across deployed modes (for a Jobs-API sweep)."""
    ids: list[str] = []
    for dep in deployments or []:
        if is_deployed(dep):
            jid = str(dep.get("job_id"))
            if jid not in ids:
                ids.append(jid)
    return ids
