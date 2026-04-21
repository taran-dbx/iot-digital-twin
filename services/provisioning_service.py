"""Provisioning helper functions for settings-page resource creation.

These functions create UC catalogs, Lakebase projects/branches/roles/tables,
UC secrets, and metrics tables.  They are used by the settings route handlers.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from models.sql_ddl import (
    SENSOR_READINGS_DDL,
    MATVIEW_1M_DDL,
    MATVIEW_5M_DDL,
    MATVIEW_INDEX_DDL,
    LAKEBASE_ROLE_NAME,
    LAKEBASE_ROLE_ID,
)
from services import lakets_service

logger = logging.getLogger("iot_digital_twin.services.provisioning")


async def provision_secrets_scope(scope_name: str) -> dict:
    """Create a UC secret scope (idempotent)."""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        try:
            w.secrets.create_scope(scope=scope_name)
            return {"resource": f"Secret scope '{scope_name}'", "status": "created"}
        except Exception as inner:
            if "already exists" in str(inner).lower():
                return {"resource": f"Secret scope '{scope_name}'", "status": "already_exists"}
            raise
    except Exception as exc:
        return {"resource": f"Secret scope '{scope_name}'", "status": "failed", "error": str(exc)[:200]}


async def provision_uc_catalog(uc: dict) -> list[dict]:
    """Create UC catalog, schemas, and checkpoint volume (idempotent)."""
    results: list[dict] = []
    pattern = uc.get("catalog_pattern", "iot_{name}")
    if "{name}" in pattern:
        catalog_name = pattern.replace("{name}", "default")
    else:
        catalog_name = pattern or "iot_default"

    schemas = uc.get("schemas", ["raw", "metrics", "checkpoints"])
    volume_name = uc.get("checkpoint_volume", "stream_checkpoints")
    storage_root_base = uc.get("storage_root", "")

    def _is_already_exists(exc: Exception) -> bool:
        return "already exists" in str(exc).lower()

    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.catalog import VolumeType

        w = WorkspaceClient()

        try:
            create_kwargs: dict[str, Any] = {"name": catalog_name}
            if storage_root_base:
                create_kwargs["storage_root"] = f"{storage_root_base}/{catalog_name}"
            w.catalogs.create(**create_kwargs)
            results.append({"resource": f"UC catalog '{catalog_name}'", "status": "created"})
        except Exception as e:
            if _is_already_exists(e):
                results.append({"resource": f"UC catalog '{catalog_name}'", "status": "already_exists"})
            else:
                raise

        for schema in schemas:
            try:
                w.schemas.create(name=schema, catalog_name=catalog_name)
                results.append({"resource": f"Schema '{catalog_name}.{schema}'", "status": "created"})
            except Exception as e:
                if _is_already_exists(e):
                    results.append({"resource": f"Schema '{catalog_name}.{schema}'", "status": "already_exists"})
                else:
                    results.append({"resource": f"Schema '{catalog_name}.{schema}'", "status": "failed", "error": str(e)[:200]})

        if "checkpoints" in schemas:
            try:
                w.volumes.create(
                    catalog_name=catalog_name,
                    schema_name="checkpoints",
                    name=volume_name,
                    volume_type=VolumeType.MANAGED,
                )
                results.append({"resource": f"Volume '{catalog_name}.checkpoints.{volume_name}'", "status": "created"})
            except Exception as e:
                if _is_already_exists(e):
                    results.append({"resource": f"Volume '{catalog_name}.checkpoints.{volume_name}'", "status": "already_exists"})
                else:
                    results.append({"resource": f"Volume '{catalog_name}.checkpoints.{volume_name}'", "status": "failed", "error": str(e)[:200]})

    except Exception as exc:
        results.append({"resource": f"UC catalog '{catalog_name}'", "status": "failed", "error": str(exc)[:200]})

    return results


async def provision_lakebase_table(lb: dict) -> dict:
    """Create the sensor_readings table in Lakebase (idempotent)."""
    mode = lb.get("mode", "autoscale")
    table_name = lb.get("pg_table", "sensor_readings")
    database = lb.get("database", "databricks_postgres")

    try:
        import asyncpg

        if mode == "autoscale":
            project_id = lb.get("project_id", "")
            branch_id = lb.get("branch_id", "production")
            if not project_id:
                return {"resource": f"Lakebase table '{table_name}'", "status": "failed", "error": "project_id is empty"}

            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            parent = f"projects/{project_id}/branches/{branch_id}"
            endpoints = list(w.postgres.list_endpoints(parent=parent))
            if not endpoints:
                return {"resource": f"Lakebase table '{table_name}'", "status": "failed", "error": f"No endpoints for {parent}"}

            endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
            pg_host = endpoint.status.hosts.host
            pg_user = w.current_user.me().user_name
            cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)
            pg_password = cred.token
            pg_port = 5432
        else:
            pg_host = lb.get("host", "")
            pg_user = lb.get("user", "")
            pg_password = lb.get("password", "")
            pg_port = int(lb.get("port", 5432))
            if not pg_host:
                return {"resource": f"Lakebase table '{table_name}'", "status": "failed", "error": "host is empty"}

        conn = await asyncpg.connect(
            host=pg_host, database=database, user=pg_user,
            password=pg_password, port=pg_port, ssl="require",
        )
        try:
            # Create the main table
            ddl = SENSOR_READINGS_DDL.format(table_name=table_name)
            await conn.execute(ddl)

            # Matviews are superseded by LakeTS RollUps when tiering is in use.
            # Creating them would later block the ChronoTable conversion (they
            # would hold references to the renamed original).
            tiering_requested = bool(lb.get("tiering"))
            matview_errors: list[str] = []
            if not tiering_requested:
                for mv_ddl in [MATVIEW_1M_DDL, MATVIEW_5M_DDL, *MATVIEW_INDEX_DDL]:
                    try:
                        await conn.execute(mv_ddl)
                    except Exception as mv_exc:
                        mv_err = str(mv_exc)
                        if "already exists" not in mv_err.lower():
                            matview_errors.append(mv_err[:80])

            result: dict = {"resource": f"Lakebase table '{table_name}'", "status": "created"}
            if matview_errors:
                result["error"] = f"Table OK; matview warnings: {'; '.join(matview_errors)}"

            # Install LakeTS on the same connection.  A failure here must not
            # fail the overall provisioning — it is surfaced as a warning.
            if lb.get("install_lakets", True):
                try:
                    sql_path = lakets_service.resolve_lakets_source(lb.get("lakets_sql_path"))
                    version = await lakets_service.install_lakets(
                        conn, sql_path, skip_if_installed=True,
                    )
                    result["lakets_version"] = version
                except Exception as lts_exc:
                    logger.warning("LakeTS install failed: %s", lts_exc)
                    result["lakets_error"] = str(lts_exc)[:200]

            # ChronoTable conversion + tier policy (Phase 2).  Only when the caller
            # supplied a `tiering` block.  Failures are warnings, not hard fails.
            tiering = lb.get("tiering")
            if tiering and result.get("lakets_version"):
                try:
                    # Pre-clean before ChronoTable conversion.  LakeTS'
                    # create_chronotable renames the source table and drops the
                    # rename at the end; any dependency holding a reference
                    # blocks the drop and forces a rollback.  Dependencies to
                    # clear:
                    #   1. Legacy matviews (sensor_1m, sensor_5m) — superseded by
                    #      LakeTS RollUps once tiering is active.
                    #   2. BIGSERIAL sequence ownership — renaming the table
                    #      carries sequence ownership with it, and the new
                    #      partitions still reference the sequence, so DROP
                    #      of the rename fails on OWNED BY.
                    #   3. Leftover ``_lakets_orig`` from a failed previous run.
                    await conn.execute("DROP MATERIALIZED VIEW IF EXISTS sensor_1m CASCADE")
                    await conn.execute("DROP MATERIALIZED VIEW IF EXISTS sensor_5m CASCADE")
                    await conn.execute(
                        f"ALTER SEQUENCE IF EXISTS {table_name}_id_seq OWNED BY NONE"
                    )
                    await conn.execute(
                        f"DROP TABLE IF EXISTS {table_name}_lakets_orig CASCADE"
                    )

                    await lakets_service.create_chronotable(
                        conn,
                        table=table_name,
                        time_col=tiering.get("time_column", "event_time"),
                        chunk=tiering.get("chunk", "15 minutes"),
                    )
                    await lakets_service.apply_tier_policy(
                        conn,
                        table=table_name,
                        hot=tiering["hot"],
                        drop=tiering["drop"],
                    )
                    result["tiering_applied"] = {
                        "hot": tiering["hot"],
                        "drop": tiering["drop"],
                        "chunk": tiering.get("chunk", "15 minutes"),
                    }
                except Exception as tier_exc:
                    logger.warning("Tier policy application failed: %s", tier_exc)
                    result["tiering_error"] = str(tier_exc)[:200]

            return result
        except Exception as inner_exc:
            err = str(inner_exc)
            if "already exists" in err.lower() or "DuplicateTable" in err:
                return {"resource": f"Lakebase table '{table_name}'", "status": "already_exists"}
            return {"resource": f"Lakebase table '{table_name}'", "status": "failed", "error": err[:200]}
        finally:
            await conn.close()

    except Exception as exc:
        return {"resource": f"Lakebase table '{table_name}'", "status": "failed", "error": str(exc)[:200]}


async def provision_lakebase_project_and_branch(project_id: str, branch_id: str) -> list[dict]:
    """Create Lakebase autoscale project and branch if they don't exist."""
    results: list[dict] = []

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.postgres import Project, Branch, BranchSpec
    w = WorkspaceClient()

    # Step 1: Check if project exists
    project_exists = False
    try:
        w.postgres.get_project(name=f"projects/{project_id}")
        project_exists = True
        results.append({"resource": f"Lakebase project '{project_id}'", "status": "already_exists"})
    except Exception as e:
        err_str = str(e).lower()
        if "not_found" in err_str or "not found" in err_str or "does not exist" in err_str:
            project_exists = False
        else:
            results.append({"resource": f"Lakebase project '{project_id}'", "status": "failed", "error": str(e)[:200]})
            return results

    # Step 2: Create project if needed
    if not project_exists:
        try:
            w.postgres.create_project(project=Project(), project_id=project_id)
            results.append({"resource": f"Lakebase project '{project_id}'", "status": "created"})
        except Exception as exc:
            if "already exists" in str(exc).lower():
                results.append({"resource": f"Lakebase project '{project_id}'", "status": "already_exists"})
            else:
                results.append({"resource": f"Lakebase project '{project_id}'", "status": "failed", "error": str(exc)[:200]})
                return results

    # Step 3: Check if branch exists
    branch_exists = False
    parent = f"projects/{project_id}/branches/{branch_id}"
    try:
        w.postgres.get_branch(name=parent)
        branch_exists = True
        results.append({"resource": f"Lakebase branch '{branch_id}'", "status": "already_exists"})
    except Exception as e:
        err_str = str(e).lower()
        if "not_found" in err_str or "not found" in err_str or "does not exist" in err_str:
            branch_exists = False
        else:
            results.append({"resource": f"Lakebase branch '{branch_id}'", "status": "failed", "error": str(e)[:200]})
            return results

    # Step 4: Create branch if needed
    if not branch_exists:
        try:
            # ``no_expiry=True`` is required by newer Lakebase API versions —
            # otherwise "Expiration must be specified when creating a branch".
            # Lives inside BranchSpec, not on Branch itself.
            w.postgres.create_branch(
                parent=f"projects/{project_id}",
                branch=Branch(spec=BranchSpec(no_expiry=True)),
                branch_id=branch_id,
            )
            results.append({"resource": f"Lakebase branch '{branch_id}'", "status": "created"})
        except Exception as exc:
            if "already exists" in str(exc).lower():
                results.append({"resource": f"Lakebase branch '{branch_id}'", "status": "already_exists"})
            else:
                results.append({"resource": f"Lakebase branch '{branch_id}'", "status": "failed", "error": str(exc)[:200]})
                return results

    # Step 5: Ensure a primary read-write endpoint exists on the branch.
    # Project creation auto-creates a `production` branch with a `primary`
    # endpoint, but creating a branch on an existing project does NOT.
    try:
        endpoints = list(w.postgres.list_endpoints(parent=parent))
    except Exception:
        endpoints = []

    if not endpoints:
        try:
            from databricks.sdk.service.postgres import (
                Endpoint,
                EndpointSpec,
                EndpointType,
            )
            w.postgres.create_endpoint(
                parent=parent,
                endpoint=Endpoint(
                    spec=EndpointSpec(
                        endpoint_type=EndpointType.READ_WRITE,
                        autoscaling_limit_min_cu=0.5,
                        autoscaling_limit_max_cu=2.0,
                    ),
                ),
                endpoint_id="primary",
            )
            results.append({"resource": f"Lakebase endpoint 'primary'", "status": "created"})
        except Exception as exc:
            if "already exists" in str(exc).lower():
                results.append({"resource": f"Lakebase endpoint 'primary'", "status": "already_exists"})
            else:
                results.append({
                    "resource": f"Lakebase endpoint 'primary'",
                    "status": "failed",
                    "error": str(exc)[:200],
                })

    # Step 6: Wait for endpoint to be ACTIVE (up to 90s)
    if not branch_exists or not project_exists or not endpoints:
        for _ in range(18):  # 18 * 5s = 90s
            try:
                current = list(w.postgres.list_endpoints(parent=parent))
                if current:
                    break
            except Exception:
                pass
            await asyncio.sleep(5)

    return results


def _is_permission_error(exc: Exception) -> bool:
    """Return True when an SDK exception is the Lakebase project-admin gate."""
    s = str(exc).lower()
    return (
        "not authorized" in s
        or "permission denied" in s
        or "unauthorized" in s
        or "permission_denied" in s
        or "forbidden" in s
    )


async def provision_lakebase_role(
    project_id: str, branch_id: str, password: str, database: str = "databricks_postgres"
) -> list[dict]:
    """Create a password-authenticated Postgres role in Lakebase, set its password,
    and grant full privileges on the target database and public schema.

    When the calling identity lacks Lakebase project-admin (typical for a
    deployed Databricks App running as its own SP), the role creation is
    skipped gracefully — autoscale OAuth already provides the runtime
    credential via ``LAKEBASE_ENDPOINT`` so this named role is redundant."""
    results: list[dict] = []
    role_name = LAKEBASE_ROLE_NAME

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.postgres import Role, RoleRoleSpec, RoleAuthMethod, RoleMembershipRole
    w = WorkspaceClient()

    parent = f"projects/{project_id}/branches/{branch_id}"

    # Step 1: Check if role already exists
    role_exists = False
    try:
        w.postgres.get_role(name=f"{parent}/roles/{LAKEBASE_ROLE_ID}")
        role_exists = True
        results.append({"resource": f"Postgres role '{role_name}'", "status": "already_exists"})
    except Exception as e:
        err_str = str(e).lower()
        if "not_found" in err_str or "not found" in err_str or "does not exist" in err_str:
            role_exists = False
        elif _is_permission_error(e):
            results.append({
                "resource": f"Postgres role '{role_name}'",
                "status": "skipped",
                "error": "Runtime identity lacks Lakebase project-admin. "
                         "Autoscale OAuth is used instead — this named role is not required.",
            })
            return results
        else:
            results.append({"resource": f"Postgres role '{role_name}'", "status": "failed", "error": str(e)[:200]})
            return results

    # Step 2: Create role if needed
    if not role_exists:
        try:
            w.postgres.create_role(
                parent=parent,
                role=Role(spec=RoleRoleSpec(
                    postgres_role=role_name,
                    auth_method=RoleAuthMethod.PG_PASSWORD_SCRAM_SHA_256,
                    membership_roles=[RoleMembershipRole.DATABRICKS_SUPERUSER],
                )),
                role_id=LAKEBASE_ROLE_ID,
            )
            results.append({"resource": f"Postgres role '{role_name}'", "status": "created"})
        except Exception as exc:
            if "already exists" in str(exc).lower():
                results.append({"resource": f"Postgres role '{role_name}'", "status": "already_exists"})
            elif _is_permission_error(exc):
                results.append({
                    "resource": f"Postgres role '{role_name}'",
                    "status": "skipped",
                    "error": "Runtime identity lacks Lakebase project-admin. "
                             "Autoscale OAuth is used instead — this named role is not required.",
                })
                return results
            else:
                results.append({"resource": f"Postgres role '{role_name}'", "status": "failed", "error": str(exc)[:200]})
                return results

    # Step 3: Set password and grant full privileges via SQL
    try:
        import asyncpg

        endpoints = list(w.postgres.list_endpoints(parent=parent))
        if not endpoints:
            results.append({"resource": "Set role password", "status": "failed", "error": "No endpoints available yet"})
            return results

        endpoint = w.postgres.get_endpoint(name=endpoints[0].name)
        pg_host = endpoint.status.hosts.host
        pg_user = w.current_user.me().user_name
        cred = w.postgres.generate_database_credential(endpoint=endpoints[0].name)

        # Connect to the target database to set grants there
        conn = await asyncpg.connect(
            host=pg_host, database=database,
            user=pg_user, password=cred.token, port=5432, ssl="require",
        )
        try:
            # Quote identifiers to prevent SQL injection (defense-in-depth)
            qrole = f'"{role_name}"'
            qdb = f'"{database}"'

            # Step 3a: Set password (separate try — SP may lack ALTER ROLE permission)
            # Lakebase OAuth roles (e.g. app service principals) often cannot ALTER
            # another role's password. This is non-fatal: the role was already created
            # via SDK with PG_PASSWORD_SCRAM_SHA_256 auth, and notebooks in autoscale
            # mode can use OAuth via generate_database_credential() instead.
            try:
                escaped_pw = password.replace("'", "''")
                await conn.execute(f"ALTER ROLE {qrole} WITH PASSWORD '{escaped_pw}'")
                results.append({"resource": f"Role '{role_name}' password", "status": "created"})
            except Exception as pw_exc:
                pw_err = str(pw_exc)
                if "permission denied" in pw_err.lower():
                    results.append({
                        "resource": f"Role '{role_name}' password",
                        "status": "skipped",
                        "error": "Connected identity lacks ALTER ROLE permission. "
                                 "For autoscale mode, notebooks use OAuth authentication "
                                 "instead of password-based credentials.",
                    })
                else:
                    results.append({"resource": f"Role '{role_name}' password", "status": "failed", "error": pw_err[:200]})

            # Step 3b: Grant privileges (should work even if password-set failed)
            await conn.execute(f"GRANT CONNECT ON DATABASE {qdb} TO {qrole}")
            await conn.execute(f"GRANT USAGE ON SCHEMA public TO {qrole}")
            await conn.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA public TO {qrole}")
            await conn.execute(f"GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO {qrole}")
            await conn.execute(
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {qrole}"
            )
            await conn.execute(
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {qrole}"
            )
            await conn.execute(f"GRANT CREATE ON SCHEMA public TO {qrole}")
            results.append({"resource": f"Role '{role_name}' grants on '{database}'", "status": "created"})
        finally:
            await conn.close()
    except Exception as exc:
        results.append({"resource": "Set role password & grants", "status": "failed", "error": str(exc)[:200]})

    return results


async def save_direct_creds_to_secrets(secrets: dict, lb_user: str, lb_password: str) -> list[dict]:
    """Save Lakebase credentials to a workspace secret scope (created if needed)."""
    results: list[dict] = []
    scope = (secrets.get("scope") or "").strip()
    user_key = secrets.get("lb_user_key", "lb-user")
    pass_key = secrets.get("lb_password_key", "lb-password")

    if not scope:
        results.append({"resource": "Save credentials to secrets", "status": "failed", "error": "Scope name is empty"})
        return results

    # Ensure scope exists before putting secrets
    scope_result = await provision_secrets_scope(scope)
    results.append(scope_result)

    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()

        w.secrets.put_secret(scope=scope, key=user_key, string_value=lb_user)
        results.append({"resource": f"Secret '{scope}/{user_key}'", "status": "created"})

        w.secrets.put_secret(scope=scope, key=pass_key, string_value=lb_password)
        results.append({"resource": f"Secret '{scope}/{pass_key}'", "status": "created"})
    except Exception as exc:
        results.append({"resource": "Save credentials to secrets", "status": "failed", "error": str(exc)[:200]})

    return results


# ---------------------------------------------------------------------------
# UC Secrets (catalog-scoped)
# ---------------------------------------------------------------------------

async def provision_uc_secret(
    catalog: str, schema: str, name: str, value: str,
) -> dict:
    """Create or update a UC Secret (catalog-scoped).

    Uses the REST API: POST /api/2.1/unity-catalog/secrets
    """
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    host = w.config.host.rstrip("/")
    headers = w.config.authenticate()
    fqn = f"{catalog}.{schema}.{name}"

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{host}/api/2.1/unity-catalog/secrets",
            headers=headers,
            json={"secret_info": {
                "catalog_name": catalog,
                "schema_name": schema,
                "name": name,
                "value": value,
            }},
        )
        if resp.status_code == 200:
            return {"resource": f"UC secret '{fqn}'", "status": "created"}

        if resp.status_code == 409 or "already exists" in resp.text.lower():
            resp2 = await client.patch(
                f"{host}/api/2.1/unity-catalog/secrets/{fqn}?update_mask=*",
                headers=headers,
                json={"secret_info": {"value": value}},
            )
            if resp2.status_code == 200:
                return {"resource": f"UC secret '{fqn}'", "status": "updated"}
            return {"resource": f"UC secret '{fqn}'", "status": "failed", "error": resp2.text[:200]}

        return {"resource": f"UC secret '{fqn}'", "status": "failed", "error": resp.text[:200]}


async def grant_uc_secret_permissions(
    catalog: str, schema: str, principal: str,
) -> dict:
    """Grant READ_SECRET and REFERENCE_SECRET on a UC schema to a principal."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    host = w.config.host.rstrip("/")
    headers = w.config.authenticate()
    fqn = f"{catalog}.{schema}"

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.patch(
            f"{host}/api/2.1/unity-catalog/permissions/schema/{fqn}",
            headers=headers,
            json={"changes": [{
                "principal": principal,
                "add": ["READ_SECRET", "REFERENCE_SECRET"],
            }]},
        )
        if resp.status_code == 200:
            return {"resource": f"UC secret grants on '{fqn}' for {principal}", "status": "created"}
        return {"resource": f"UC secret grants on '{fqn}'", "status": "failed", "error": resp.text[:200]}


async def save_creds_as_uc_secrets(
    catalog: str, schema: str,
    lb_user: str, lb_password: str,
    zb_client_id: str | None = None,
    zb_client_secret: str | None = None,
) -> list[dict]:
    """Save Lakebase and Zerobus credentials as UC secrets (catalog-scoped)."""
    results: list[dict] = []

    results.append(await provision_uc_secret(catalog, schema, "lb-user", lb_user))
    results.append(await provision_uc_secret(catalog, schema, "lb-password", lb_password))

    if zb_client_id:
        results.append(await provision_uc_secret(catalog, schema, "zerobus-client-id", zb_client_id))
    if zb_client_secret:
        results.append(await provision_uc_secret(catalog, schema, "zerobus-client-secret", zb_client_secret))

    return results


# ---------------------------------------------------------------------------
# Metrics table provisioning
# ---------------------------------------------------------------------------

STREAMING_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    query_id STRING,
    run_id STRING,
    query_name STRING,
    batch_id LONG,
    timestamp STRING,
    progress STRING
)
"""


async def provision_metrics_table(
    catalog: str, schema: str = "metrics",
    table_name: str = "streaming_metrics",
    grant_to: str | None = None,
) -> list[dict]:
    """Create the streaming_metrics table and optionally grant access.

    Uses the Databricks SQL Statement Execution API.
    """
    results: list[dict] = []
    fqn = f"{catalog}.{schema}.{table_name}"

    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()

        warehouses = list(w.warehouses.list())
        if not warehouses:
            return [{"resource": f"Metrics table '{fqn}'", "status": "failed",
                     "error": "No SQL warehouse available"}]

        wh_id = warehouses[0].id

        stmt = w.statement_execution.execute_statement(
            warehouse_id=wh_id,
            statement=STREAMING_METRICS_DDL.format(table=fqn),
            wait_timeout="50s",
        )
        if stmt.status.error:
            results.append({"resource": f"Metrics table '{fqn}'", "status": "failed",
                           "error": str(stmt.status.error)[:200]})
        else:
            results.append({"resource": f"Metrics table '{fqn}'", "status": "created"})

        if grant_to:
            for sql in [
                f'GRANT MODIFY, SELECT ON TABLE {fqn} TO `{grant_to}`',
                f'GRANT USE CATALOG ON CATALOG {catalog} TO `{grant_to}`',
                f'GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{grant_to}`',
            ]:
                try:
                    w.statement_execution.execute_statement(
                        warehouse_id=wh_id, statement=sql, wait_timeout="50s",
                    )
                except Exception as ge:
                    logger.warning("Grant failed: %s — %s", sql, ge)

            results.append({"resource": f"Grants on '{fqn}' to {grant_to}", "status": "created"})

    except Exception as exc:
        results.append({"resource": f"Metrics table '{fqn}'", "status": "failed",
                       "error": str(exc)[:200]})

    return results
