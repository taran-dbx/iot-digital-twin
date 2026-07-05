"""Databricks App permission helpers.

A deployed Databricks App authenticates incoming requests against its own
access-control list. When the streaming pipeline mints a *new* service
principal (SP) for the Lakebase sink, that SP has no permission on the app, so
its OAuth token is rejected with a 401 the moment it tries to call back into
the app. The fix is to grant the SP ``CAN_USE`` on the app via the apps
permissions API.

``grant_app_can_use`` uses *merge* semantics (``update_permissions``, not
``set_permissions``) so existing grants — including the app owner — are left
untouched. The call is best-effort: a failure here must never break the
provisioning / heal flow, so we log and swallow.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger("iot_digital_twin.services.app_permissions")

DEFAULT_APP_NAME = "iot-digital-twin"


def _app_name() -> str:
    """Return the Databricks App name from env, falling back to the constant."""
    name = (os.getenv("DATABRICKS_APP_NAME") or "").strip()
    return name or DEFAULT_APP_NAME


def grant_app_can_use(w: Any, app_name: str, sp_application_id: str) -> None:
    """Grant ``CAN_USE`` on the app to a service principal (merge semantics).

    Non-fatal: any failure is logged and swallowed so callers (provisioning,
    heal) keep running. ``sp_application_id`` is the SP's *application id*
    (a.k.a. client id), which is what the apps ACL keys on.
    """
    if not app_name or not sp_application_id:
        logger.warning(
            "grant_app_can_use: missing app_name=%r or sp_application_id=%r — skipping",
            app_name, sp_application_id,
        )
        return

    try:
        from databricks.sdk.service.apps import (
            AppAccessControlRequest,
            AppPermissionLevel,
        )

        w.apps.update_permissions(
            app_name=app_name,
            access_control_list=[
                AppAccessControlRequest(
                    service_principal_name=sp_application_id,
                    permission_level=AppPermissionLevel.CAN_USE,
                )
            ],
        )
        logger.info(
            "Granted CAN_USE on app '%s' to service principal '%s'",
            app_name, sp_application_id,
        )
    except Exception as exc:
        logger.warning(
            "Failed to grant CAN_USE on app '%s' to SP '%s' (non-fatal): %s",
            app_name, sp_application_id, exc,
        )
