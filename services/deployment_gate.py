"""Validation hard-gate for per-mode pipeline deploy (spec §1.12).

The admin cannot deploy a mode until **every** required validation step for
that mode is ``passed`` (and test data purged).  This module is the single
source of truth for *which* steps a mode requires and whether the gate is open.

Step ids mirror the frontend state machine
(``client/src/pages/setup-validation/types.ts``): common steps are shared,
per-lane steps are prefixed ``rt_`` / ``mb_`` / ``zb_``.
"""

from __future__ import annotations

COMMON_STEPS: tuple[str, ...] = (
    "provision_cluster",
    "source_connectivity",
    "topics_created",
    "schema_conformance",
)
REALTIME_STEPS: tuple[str, ...] = (
    "rt_partitions_precreate",
    "rt_write_test",
    "rt_cdf_manual",
    "rt_cdf_verify",
    "rt_read_back",
    "rt_dbsql_read",
    "rt_metrics_check",
    "rt_cleanup",
)
MICROBATCH_STEPS: tuple[str, ...] = (
    "mb_write_test",
    "mb_dbsql_read",
    "mb_metrics_check",
    "mb_cleanup",
)
# Zerobus is non-Kafka / serverless (no cluster, no topics) so it does not
# require the common Kafka-cluster steps — only its own lane.
ZEROBUS_STEPS: tuple[str, ...] = (
    "zb_create_table",
    "zb_auth_probe",
    "zb_produce_test",
    "zb_dbsql_read",
    "zb_cleanup",
)

REQUIRED_STEPS: dict[str, tuple[str, ...]] = {
    "realtime": COMMON_STEPS + REALTIME_STEPS,
    "microbatch": COMMON_STEPS + MICROBATCH_STEPS,
    "zerobus": ZEROBUS_STEPS,
}

VALID_MODES: tuple[str, ...] = tuple(REQUIRED_STEPS.keys())


class DeployGateError(RuntimeError):
    """Raised when a deploy is attempted before validation passes."""

    def __init__(self, mode: str, outstanding: list[str]) -> None:
        self.mode = mode
        self.outstanding = outstanding
        super().__init__(
            f"Deploy gate for mode '{mode}' not satisfied — "
            f"{len(outstanding)} step(s) outstanding: {', '.join(outstanding)}"
        )


def _step_status(validation_state: dict | None, step: str) -> str:
    """Return the status of ``step`` from ``validation_state`` ('pending' if unset).

    Tolerates two shapes: ``{step: "passed"}`` and ``{step: {"status": "passed"}}``.
    """
    value = (validation_state or {}).get(step)
    if isinstance(value, dict):
        return value.get("status", "pending")
    return value or "pending"


def outstanding_steps(validation_state: dict | None, mode: str) -> list[str]:
    """Return the required steps for ``mode`` that are not yet ``passed``."""
    if mode not in REQUIRED_STEPS:
        raise ValueError(f"unknown mode: {mode!r}")
    return [
        step
        for step in REQUIRED_STEPS[mode]
        if _step_status(validation_state, step) != "passed"
    ]


def gate_passed(validation_state: dict | None, mode: str) -> bool:
    """True when every required validation step for ``mode`` is ``passed``."""
    return not outstanding_steps(validation_state, mode)


def assert_gate_passed(validation_state: dict | None, mode: str) -> None:
    """Raise :class:`DeployGateError` unless the mode's validation gate passed."""
    outstanding = outstanding_steps(validation_state, mode)
    if outstanding:
        raise DeployGateError(mode, outstanding)
