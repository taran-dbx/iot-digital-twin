"""Second, additive Lakebase connection manager for the ``iot-app-backend`` project.

The app-backend metadata model (``pipeline_setup`` / ``pipeline_deployment`` /
``provisioned_resource`` — see ``models/app_schema.py``) lives in a *separate*
Lakebase project (``iot-app-backend``) from the legacy demo/streaming project
(``iot-lakets-demo``) that the existing ``backend.LakebaseConnectionManager``
serves via ``PGHOST`` / ``LAKEBASE_ENDPOINT``.

This module is **purely additive**: it constructs a second
``LakebaseConnectionManager`` bound to the app-backend endpoint and never touches
``PGHOST``, ``LAKEBASE_ENDPOINT``, ``app.yaml``, or the existing connection.

Credentials always go through the Databricks SDK OAuth path
(``w.postgres.generate_database_credential`` for the app-backend *endpoint*), so
a static ``PGUSER``/``PGPASSWORD`` pair injected for the *legacy* project can
never accidentally pin this pool to the wrong host.
"""

from __future__ import annotations

import logging
import os

from backend import LakebaseConnectionManager, _get_lakebase_credentials

logger = logging.getLogger("iot_digital_twin.services.app_backend_db")

# ---------------------------------------------------------------------------
# app-backend project coordinates (overridable via env, but never via PGHOST)
# ---------------------------------------------------------------------------
APP_BACKEND_PROJECT = "iot-app-backend"
APP_BACKEND_ENDPOINT = os.getenv(
    "APP_BACKEND_LAKEBASE_ENDPOINT",
    "projects/iot-app-backend/branches/production/endpoints/primary",
)
APP_BACKEND_HOST = os.getenv(
    "APP_BACKEND_PGHOST",
    "ep-mute-scene-d2cvnr0v.database.us-east-1.cloud.databricks.com",
)
APP_BACKEND_DATABASE = os.getenv("APP_BACKEND_PGDATABASE", "databricks_postgres")


class AppBackendConnectionManager(LakebaseConnectionManager):
    """A ``LakebaseConnectionManager`` pinned to the ``iot-app-backend`` project.

    Overrides credential resolution to *always* use SDK OAuth against the
    app-backend endpoint, ignoring any ``PGUSER``/``PGPASSWORD`` static creds
    that Databricks may inject for the legacy (``iot-lakets-demo``) project.
    """

    async def initialize(self) -> None:  # type: ignore[override]
        if not self._pg_host:
            raise RuntimeError("app-backend Lakebase host not configured")

        import asyncpg

        self._pg_user, self._pg_password = _get_lakebase_credentials(self._endpoint_name)
        self._uses_static_creds = False

        ssl_mode = os.getenv("PGSSLMODE", "require")
        self._pool = await asyncpg.create_pool(
            host=self._pg_host,
            database=self._pg_database,
            user=self._pg_user,
            password=self._pg_password,
            port=self._pg_port,
            min_size=self._pool_min,
            max_size=self._pool_max,
            ssl=ssl_mode,
        )
        logger.info(
            "app-backend Lakebase pool created — host=%s db=%s user=%s",
            self._pg_host, self._pg_database, self._pg_user,
        )
        import asyncio

        self._refresh_task = asyncio.create_task(self._token_refresh_loop())


def build_app_backend_manager() -> AppBackendConnectionManager:
    """Construct (but do not initialize) the app-backend connection manager.

    Host, database, and endpoint are pinned to the ``iot-app-backend`` project
    constants above (env-overridable) — never resolved from ``PGHOST`` /
    ``LAKEBASE_ENDPOINT``, so the legacy connection is untouched.
    """
    return AppBackendConnectionManager(
        pg_host=APP_BACKEND_HOST,
        pg_database=APP_BACKEND_DATABASE,
        endpoint_name=APP_BACKEND_ENDPOINT,
    )
