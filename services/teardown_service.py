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


class SkipResource(Exception):
    """Raised by a deleter to signal an intentional skip (NOT a failure).

    Use it for resources that are deliberately not deleted here — e.g. a Kafka
    topic (``DeleteTopic`` denied, spec §1.8), a Lakebase primary endpoint that
    is auto-removed when its branch is deleted, or a shared resource.  The
    orchestrator records the row as ``skipped`` with the exception message as
    the reason; ``success`` stays True.
    """


def _is_shared(res: dict) -> bool:
    """Return True when the ledger row is flagged shared (never torn down here).

    The shared SP, secret scope, and ``iot-pipelines`` Lakebase project are
    recorded per-setup with ``metadata.shared`` so a per-industry teardown never
    deletes the resource other industries still depend on.
    """
    return bool((res.get("metadata") or {}).get("shared"))


async def teardown_setup(
    db: DBExecutor,
    setup_id: Any,
    deleters: dict[str, Deleter] | None = None,
) -> dict:
    """Tear down every resource for a setup, then delete the setup row.

    Walks the ledger in DESCENDING ``teardown_order`` (reverse dependency order)
    and, per row: skips ``shared`` resources, dispatches the matching deleter,
    and marks the ledger row with its outcome (``deleted`` / ``skipped`` /
    ``failed``) BEFORE any cascade delete — so a failed delete is never silently
    orphaned.

    Only when **every** resource succeeded (``deleted``/``skipped``) is the
    ``pipeline_setup`` row dropped (its ``ON DELETE CASCADE`` FKs remove the
    deployments + ledger rows).  If any deleter failed, the setup is **kept** so
    the failed rows survive for inspection/retry, and the failures are surfaced.

    Args:
        db: async DB executor bound to ``iot-app-backend``.
        setup_id: the ``pipeline_setup`` id to tear down.
        deleters: optional registry mapping ``resource_type`` → async deleter.
            Falls back to :data:`DEFAULT_DELETERS`.

    Returns:
        ``{"success", "setup_id", "deleted_setup", "steps", "summary"}``.
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

        if _is_shared(res):
            step["action"] = "skipped"
            step["reason"] = "shared resource — not torn down per-industry"
            logger.info("teardown: shared %s '%s' — skipping", rtype, identifier)
        elif deleter is None:
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
            except SkipResource as skip:  # intentional skip, not a failure
                step["action"] = "skipped"
                step["reason"] = str(skip)
                logger.info(
                    "teardown: deleter for %s '%s' skipped: %s",
                    rtype, identifier, skip,
                )
            except Exception as exc:  # concrete delete failure — non-fatal per step
                step["action"] = "failed"
                step["error"] = str(exc)[:300]
                logger.warning(
                    "teardown: deleter for %s '%s' failed: %s",
                    rtype, identifier, exc,
                )

        # Mark the ledger row with its actual outcome BEFORE any cascade.  A
        # ``failed`` row is thus never lost: if we bail on the cascade below it
        # survives in the ledger as the record that the cloud resource remains.
        try:
            await resource_ledger.mark_resource_status(
                db, res.get("id"), step["action"]
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("teardown: could not mark %s %s: %s",
                           res.get("id"), step["action"], exc)

        steps.append(step)

    failed = [s for s in steps if s.get("action") == "failed"]
    success = not failed

    # Only cascade-delete the setup when nothing failed — otherwise keep it so
    # the failed rows are not silently orphaned and the admin can retry.
    if success:
        deleted_setup = await pipeline_setup_repo.delete_setup(db, setup_id)
    else:
        deleted_setup = False
        logger.warning(
            "teardown: %d resource(s) failed for setup %s — keeping setup row "
            "(nothing cascaded) so failures are not orphaned", len(failed), setup_id,
        )

    summary = {
        "deleted": sum(1 for s in steps if s.get("action") == "deleted"),
        "skipped": sum(1 for s in steps if s.get("action") == "skipped"),
        "failed": len(failed),
        "total": len(steps),
    }
    return {
        "success": success,
        "setup_id": setup_id,
        "deleted_setup": deleted_setup,
        "steps": steps,
        "summary": summary,
    }
