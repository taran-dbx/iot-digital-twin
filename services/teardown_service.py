"""Teardown orchestrator for a pipeline setup (app-backend, spec §2.4).

Delete flow: read the ``provisioned_resource`` ledger in DESCENDING
``teardown_order`` (reverse dependency order) → dispatch a per-``resource_type``
delete → mark each ledger row ``deleted`` → finally delete the ``pipeline_setup``
row, whose ``ON DELETE CASCADE`` FKs remove the ``pipeline_deployment`` and
``provisioned_resource`` metadata.  Result: nothing orphaned.

The concrete cloud-resource deleters (UC drop, Kafka topic delete, Lakebase
branch/endpoint delete, job delete, …) are provided as an injected registry so
they are mockable in tests and can be filled in by later phases.  A resource
whose type has no registered deleter is recorded as ``skipped`` (e.g. the Kafka
topic-delete permission gap noted in spec §1.8) rather than failing the whole
teardown.
"""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable

from services import pipeline_setup_repo, resource_ledger
from services.app_repo_common import DBExecutor

logger = logging.getLogger("iot_digital_twin.services.teardown")

# A deleter takes the ledger row (dict) and performs the real delete.
Deleter = Callable[[dict], Awaitable[Any]]

# No concrete deleters yet — Phase 2+ registers UC / MSK / Lakebase / job
# deleters here.  Until then every type is ``skipped`` (recorded, not failed).
DEFAULT_DELETERS: dict[str, Deleter] = {}


async def teardown_setup(
    db: DBExecutor,
    setup_id: Any,
    deleters: dict[str, Deleter] | None = None,
) -> dict:
    """Tear down every resource for a setup, then delete the setup row.

    Args:
        db: async DB executor bound to ``iot-app-backend``.
        setup_id: the ``pipeline_setup`` id to tear down.
        deleters: optional registry mapping ``resource_type`` → async deleter.
            Falls back to :data:`DEFAULT_DELETERS`.

    Returns:
        ``{"success", "setup_id", "deleted_setup", "steps": [...]}``.  ``steps``
        records each resource with an ``action`` of ``deleted`` / ``skipped`` /
        ``failed`` (+ ``error``).  ``success`` is False if any deleter raised.
    """
    registry = DEFAULT_DELETERS if deleters is None else deleters
    steps: list[dict] = []

    resources = await resource_ledger.resources_in_teardown_order(db, setup_id)
    for res in resources:
        rtype = res.get("resource_type")
        identifier = res.get("identifier")
        deleter = registry.get(rtype)
        step: dict[str, Any] = {
            "resource_id": res.get("id"),
            "resource_type": rtype,
            "identifier": identifier,
        }
        if deleter is None:
            step["action"] = "skipped"
            step["reason"] = "no deleter registered"
            logger.info(
                "teardown: no deleter for %s '%s' — skipping (recorded)",
                rtype, identifier,
            )
        else:
            try:
                await deleter(res)
                step["action"] = "deleted"
            except Exception as exc:  # concrete delete failure — non-fatal per step
                step["action"] = "failed"
                step["error"] = str(exc)[:300]
                logger.warning(
                    "teardown: deleter for %s '%s' failed: %s",
                    rtype, identifier, exc,
                )

        # Mark the ledger row deleted regardless (the FK cascade below removes
        # the row entirely; the status flip keeps intermediate state coherent
        # if the cascade delete is deferred or fails).
        try:
            await resource_ledger.mark_resource_status(db, res.get("id"), "deleted")
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("teardown: could not mark %s deleted: %s",
                           res.get("id"), exc)

        steps.append(step)

    # Delete the setup — FK ON DELETE CASCADE removes deployments + resources.
    deleted_setup = await pipeline_setup_repo.delete_setup(db, setup_id)

    success = all(s.get("action") != "failed" for s in steps)
    return {
        "success": success,
        "setup_id": setup_id,
        "deleted_setup": deleted_setup,
        "steps": steps,
    }
