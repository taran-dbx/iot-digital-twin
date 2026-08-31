"""Unit tests for the effectful realtime validation executors (spec §1.10).

These exercise the *branching / wiring* of ``services/validation_steps.py`` with
the cluster command execution and Lakebase connections mocked out — the live
streaming path stays behind ``tests/test_integration_validation.py`` (opt-in).

Focus (the Phase-7 design-gap fix):
  * realtime ``write_test`` PRODUCES test data then runs the RTM consumer that
    lands rows via a ForeachWriter (not a noop sink);
  * ``read_back`` verifies rows via Lakebase primary + replica;
  * ``cleanup`` empties the test rows and deletes the validation checkpoint;
  * ``cdf_verify`` keys on this pipeline's specific shadow table.
"""

from __future__ import annotations

import pytest

from services import validation_state_machine as sm
from services import validation_steps as vs
from services.validation_service import StepContext, StepResult

UC_CATALOG = "test_catalog"
UC_SCHEMA = "test_schema"
PIPELINES_LAKEBASE_PROJECT = "test_pipelines"


@pytest.fixture(autouse=True)
def _one_schema_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin one-schema UC + pipelines project env for validation_steps._cfg()."""
    monkeypatch.setenv("UC_CATALOG", UC_CATALOG)
    monkeypatch.setenv("UC_SCHEMA", UC_SCHEMA)
    monkeypatch.setenv("PIPELINES_LAKEBASE_PROJECT", PIPELINES_LAKEBASE_PROJECT)


_SETUP = {
    "industry": "pharmaceutical",
    "bootstrap_servers": "b-1.kafka.us-east-1.amazonaws.com:9198",
    "derived": {"region": "us-east-1", "broker_count": 3},
    "service_credential": "iot-rtm-sc",
    "cluster_spec": {"partition_count": 3},
    "warehouse_id": "wh1",
}


def _ctx(mode: str) -> StepContext:
    return StepContext(
        db=None, setup=_SETUP, state={"cluster_id": "c-1"},
        key=f"{mode}:x", step="x", mode=mode, ws=object(),
    )


@pytest.mark.asyncio
async def test_realtime_write_test_produces_then_consumes(monkeypatch):
    """realtime write_test must first produce sample data, then run the RTM
    consumer that lands rows (mirrors the proven MSK→RTM→Lakebase path)."""
    calls: list[str] = []

    async def fake_run(ctx, code, *, timeout_s=300):
        calls.append(code)
        return StepResult(sm.PASSED, data={})

    produced = {}

    def fake_produce(topic, bootstrap, region, cred, **kw):
        produced["topic"] = topic
        return f"PRODUCE::{topic}"

    def fake_write(topic, bootstrap, cred, **kw):
        produced["consumer_kw"] = kw
        return f"CONSUME::{topic}"

    monkeypatch.setattr(vs, "_run_on_cluster", fake_run)
    monkeypatch.setattr(vs.snip, "rtm_produce", fake_produce)
    monkeypatch.setattr(vs.snip, "rtm_write_test", fake_write)
    monkeypatch.setattr(
        vs, "_resolve_lakebase_write_conn", lambda ctx, branch: ("pg-host", "pg-user", "pg-token")
    )

    result = await vs.write_test(_ctx("realtime"))

    assert result.status == sm.PASSED
    # Producer runs before the consumer.
    assert len(calls) == 2
    assert calls[0].startswith("PRODUCE::")
    assert calls[1].startswith("CONSUME::")
    # The consumer is wired to the ChronoTable + a distinct validation checkpoint
    # + the buffered metrics listener target.
    kw = produced["consumer_kw"]
    assert kw["table"] == "sensor_readings"
    assert "/validation/realtime" in kw["checkpoint_path"]
    assert kw["metrics_table_fqn"] == f"{UC_CATALOG}.{UC_SCHEMA}.pharmaceutical_metrics"
    assert "streaming_metrics_realtime/" in kw["metrics_table_location"]


@pytest.mark.asyncio
async def test_realtime_write_test_stops_if_producer_fails(monkeypatch):
    calls: list[str] = []

    async def fake_run(ctx, code, *, timeout_s=300):
        calls.append(code)
        return StepResult(sm.FAILED, error="produce boom")

    monkeypatch.setattr(vs, "_run_on_cluster", fake_run)
    monkeypatch.setattr(vs.snip, "rtm_produce", lambda *a, **k: "PRODUCE")
    monkeypatch.setattr(vs.snip, "rtm_write_test", lambda *a, **k: "CONSUME")

    result = await vs.write_test(_ctx("realtime"))
    assert result.status == sm.FAILED
    assert len(calls) == 1  # consumer never runs if produce fails


@pytest.mark.asyncio
async def test_microbatch_write_test_unchanged(monkeypatch):
    captured = {}

    async def fake_run(ctx, code, *, timeout_s=300):
        captured["code"] = code
        return StepResult(sm.PASSED, data={})

    monkeypatch.setattr(vs, "_run_on_cluster", fake_run)
    monkeypatch.setattr(
        vs.snip, "microbatch_write_test",
        lambda *a, **k: "MICROBATCH",
    )
    result = await vs.write_test(_ctx("microbatch"))
    assert result.status == sm.PASSED
    assert captured["code"] == "MICROBATCH"


@pytest.mark.asyncio
async def test_read_back_checks_primary_and_replica(monkeypatch):
    class FakeConn:
        def __init__(self, n):
            self.n = n
            self.queries: list = []

        async def fetchval(self, sql, *args):
            self.queries.append((sql, args))
            return self.n

        async def close(self):
            pass

    primary = FakeConn(100)
    replica = FakeConn(97)

    async def fake_write_conn(branch, *, ws=None):
        return primary

    async def fake_read_conn(branch, *, ws=None):
        return replica

    monkeypatch.setattr(vs.pps, "get_write_conn", fake_write_conn)
    monkeypatch.setattr(vs.pps, "get_read_conn", fake_read_conn)

    result = await vs.read_back(_ctx("realtime"))
    assert result.status == sm.PASSED
    assert result.data["rows_primary"] == 100
    assert result.data["rows_replica"] == 97
    # Only the test rows are counted (source-scoped isolation).
    assert any("source" in q[0] for q in primary.queries)


@pytest.mark.asyncio
async def test_read_back_fails_when_no_rows(monkeypatch):
    class FakeConn:
        async def fetchval(self, sql, *args):
            return 0

        async def close(self):
            pass

    async def fake_conn(branch, *, ws=None):
        return FakeConn()

    monkeypatch.setattr(vs.pps, "get_write_conn", fake_conn)
    monkeypatch.setattr(vs.pps, "get_read_conn", fake_conn)

    result = await vs.read_back(_ctx("realtime"))
    assert result.status == sm.FAILED


@pytest.mark.asyncio
async def test_cleanup_purges_rows_and_deletes_checkpoint(monkeypatch):
    deleted = {}

    class FakeConn:
        async def execute(self, sql, *args):
            deleted["sql"] = sql
            deleted["args"] = args
            return "DELETE 100"

        async def close(self):
            pass

    async def fake_write_conn(branch, *, ws=None):
        return FakeConn()

    removed = {}

    async def fake_run(ctx, code, *, timeout_s=300):
        removed["code"] = code
        return StepResult(sm.PASSED, data={})

    monkeypatch.setattr(vs.pps, "get_write_conn", fake_write_conn)
    monkeypatch.setattr(vs, "_run_on_cluster", fake_run)
    monkeypatch.setattr(vs.snip, "remove_path", lambda p: f"RM::{p}")

    result = await vs.cleanup(_ctx("realtime"))
    assert result.status == sm.PASSED
    # Test rows purged (source-scoped) + checkpoint removed.
    assert "DELETE" in deleted["sql"].upper()
    assert "source" in deleted["sql"]
    assert removed["code"].startswith("RM::")
    assert "/validation/realtime" in removed["code"]
    assert result.data["checkpoint_deleted"] is True


@pytest.mark.asyncio
async def test_cdf_manual_ensures_shadow_then_parks(monkeypatch):
    """cdf_manual must call enable_sync on sensor_readings (prep the CDC shadow)
    before parking for the admin — the managed sync has nothing to sync otherwise."""
    called = {}

    class FakeConn:
        async def close(self):
            called["closed"] = True

    async def fake_write_conn(branch, *, ws=None):
        called["branch"] = branch
        return FakeConn()

    async def fake_enable_sync(conn, table, *, schema="public"):
        called["table"] = table
        called["schema"] = schema
        return {"status": "created"}

    monkeypatch.setattr(vs.pps, "get_write_conn", fake_write_conn)
    monkeypatch.setattr(vs.lakets_svc, "enable_sync", fake_enable_sync)

    result = await vs.cdf_manual(_ctx("realtime"))
    assert result.status == sm.WAITING_ADMIN
    assert called["table"] == "sensor_readings"
    assert called["schema"] == "public"
    assert result.data["cdf_shadow"] == "created"
    assert result.data["shadow_table"] == "lakets_cdf._shadow_sensor_readings"
    assert called.get("closed") is True


@pytest.mark.asyncio
async def test_cdf_manual_parks_even_if_shadow_prep_fails(monkeypatch):
    """A shadow-prep failure is non-fatal — cdf_manual must still park for the
    admin (surfacing the error) rather than failing the whole lane."""
    async def boom_conn(branch, *, ws=None):
        raise RuntimeError("branch auth failed")

    monkeypatch.setattr(vs.pps, "get_write_conn", boom_conn)

    result = await vs.cdf_manual(_ctx("realtime"))
    assert result.status == sm.WAITING_ADMIN
    assert "error" in str(result.data["cdf_shadow"])


@pytest.mark.asyncio
async def test_dbsql_read_realtime_targets_cdf_synced_table(monkeypatch):
    """Realtime dbsql_read must query the one-schema {industry}_realtime Delta
    table — NOT the retired per-industry realtime.sensor_readings layout."""
    captured = {}

    async def fake_dbsql(sql, wh):
        captured["sql"] = sql
        return {"data": [{"n": 3}]}

    monkeypatch.setattr(vs, "execute_dbsql", fake_dbsql)
    result = await vs.dbsql_read(_ctx("realtime"))
    assert result.status == sm.PASSED
    assert f"{UC_CATALOG}.{UC_SCHEMA}.pharmaceutical_realtime" in captured["sql"]
    assert ".realtime.sensor_readings" not in captured["sql"]
    assert "lb__shadow_sensor_readings_history" not in captured["sql"]


@pytest.mark.asyncio
async def test_dbsql_read_microbatch_targets_sensor_data(monkeypatch):
    captured = {}

    async def fake_dbsql(sql, wh):
        captured["sql"] = sql
        return {"data": [{"n": 9}]}

    monkeypatch.setattr(vs, "execute_dbsql", fake_dbsql)
    result = await vs.dbsql_read(_ctx("microbatch"))
    assert result.status == sm.PASSED
    assert f"{UC_CATALOG}.{UC_SCHEMA}.pharmaceutical_microbatch" in captured["sql"]
    assert ".microbatch.sensor_data" not in captured["sql"]


@pytest.mark.asyncio
async def test_dbsql_read_fails_when_zero_rows(monkeypatch):
    """No-data must FAIL, not pass — the old check returned PASSED regardless."""
    async def fake_dbsql(sql, wh):
        return {"data": [{"n": 0}]}

    monkeypatch.setattr(vs, "execute_dbsql", fake_dbsql)
    result = await vs.dbsql_read(_ctx("microbatch"))
    assert result.status == sm.FAILED
    assert "0 rows" in str(result.error)


@pytest.mark.asyncio
async def test_metrics_check_fails_when_empty(monkeypatch):
    async def fake_dbsql(sql, wh):
        return {"data": [{"n": 0, "have_e2e": 0, "have_proc": 0}]}

    monkeypatch.setattr(vs, "execute_dbsql", fake_dbsql)
    result = await vs.metrics_check(_ctx("realtime"))
    assert result.status == sm.FAILED
    assert "no metrics rows" in str(result.error)


@pytest.mark.asyncio
async def test_metrics_check_reads_per_mode_table_and_passes(monkeypatch):
    captured = {}

    async def fake_dbsql(sql, wh):
        captured["sql"] = sql
        return {"data": [{"n": 7, "have_e2e": 7, "have_proc": 7}]}

    monkeypatch.setattr(vs, "execute_dbsql", fake_dbsql)
    result = await vs.metrics_check(_ctx("microbatch"))
    assert result.status == sm.PASSED
    assert result.data["rows"] == 7
    assert f"{UC_CATALOG}.{UC_SCHEMA}.pharmaceutical_metrics" in captured["sql"]
    assert "mode = 'microbatch'" in captured["sql"]
    assert "streaming_metrics_microbatch" not in captured["sql"]


@pytest.mark.asyncio
async def test_write_test_microbatch_passes_metrics_params(monkeypatch):
    captured = {}

    async def fake_run(ctx, code, *, timeout_s=300):
        return StepResult(sm.PASSED, data={})

    def fake_mb(topic, bootstrap, cred, delta, checkpoint, **kw):
        captured.update(kw)
        return "MB"

    monkeypatch.setattr(vs, "_run_on_cluster", fake_run)
    monkeypatch.setattr(vs.snip, "microbatch_write_test", fake_mb)
    result = await vs.write_test(_ctx("microbatch"))
    assert result.status == sm.PASSED
    # Micro-batch metrics land in the shared {industry}_metrics table (mode column).
    assert captured["metrics_table_fqn"] == f"{UC_CATALOG}.{UC_SCHEMA}.pharmaceutical_metrics"
    assert "streaming_metrics_microbatch/" in captured["metrics_table_location"]


@pytest.mark.asyncio
async def test_zerobus_produce_test_driver_side_no_cluster(monkeypatch):
    """produce_test must call zbv.produce driver-side (NOT _run_on_cluster).

    Zerobus pipelines have no interactive cluster — the SDK produces in-process
    via asyncio.to_thread, authenticated the same way auth_probe does.
    """
    # Pin UC env so industry_table() resolves without raising RuntimeError.
    monkeypatch.setenv("UC_CATALOG", "iot_test_catalog")
    monkeypatch.setenv("UC_SCHEMA", "rtm_test_schema")
    monkeypatch.setenv("DATABRICKS_HOST", "https://x.databricks.com")
    monkeypatch.setenv("ZEROBUS_SERVER_ENDPOINT", "x.zerobus.us-east-1.cloud.databricks.com")

    cluster_calls: list = []

    async def fake_run_on_cluster(ctx, code, *, timeout_s=300):
        cluster_calls.append(code)
        return StepResult(sm.PASSED, data={})

    produce_calls: list = []

    def fake_produce(**kw):
        produce_calls.append(kw)
        return (True, {"produced": 100, "table": kw.get("table_fqn", "")})

    monkeypatch.setattr(vs, "_run_on_cluster", fake_run_on_cluster)
    monkeypatch.setattr(vs.zbv, "produce", fake_produce)

    result = await vs.produce_test(_ctx("zerobus"))

    assert result.status == sm.PASSED
    # The cluster path must NOT have been touched — no cluster for zerobus.
    assert cluster_calls == [], "produce_test must not call _run_on_cluster for zerobus"
    # The driver-side producer must have been called exactly once.
    assert len(produce_calls) == 1
    call_kw = produce_calls[0]
    assert call_kw["workspace_url"] == "https://x.databricks.com"
    assert call_kw["server_endpoint"] == "x.zerobus.us-east-1.cloud.databricks.com"
    assert "pharmaceutical" in call_kw["table_fqn"]
    assert call_kw["num_records"] == 100
    assert call_kw["source"] == vs.VALIDATION_SOURCE


@pytest.mark.asyncio
async def test_zerobus_produce_test_fails_without_endpoint(monkeypatch):
    monkeypatch.delenv("ZEROBUS_SERVER_ENDPOINT", raising=False)
    result = await vs.produce_test(_ctx("zerobus"))
    assert result.status == sm.FAILED
    assert "ZEROBUS_SERVER_ENDPOINT" in str(result.error)


@pytest.mark.asyncio
async def test_zerobus_auth_probe_delegates_to_module(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://x.databricks.com")
    monkeypatch.setenv("ZEROBUS_SERVER_ENDPOINT", "x.zerobus.us-east-1.cloud.databricks.com")
    monkeypatch.setattr(vs.zbv, "auth_probe", lambda *a, **k: (True, {"ok": True}))
    result = await vs.auth_probe(_ctx("zerobus"))
    assert result.status == sm.PASSED

    monkeypatch.setattr(vs.zbv, "auth_probe", lambda *a, **k: (False, {"token_error": "401"}))
    result = await vs.auth_probe(_ctx("zerobus"))
    assert result.status == sm.FAILED


@pytest.mark.asyncio
async def test_cdf_verify_checks_lakebase_not_cluster(monkeypatch):
    """cdf_verify must query CDF status on the branch Lakebase (wal2delta.tables
    lives in Postgres, not Spark) via the Tier-1 check keyed on THIS shadow — a
    spark.sql query on the cluster can never see wal2delta.tables."""
    import services.health_checks as hc

    called = {}

    class FakeConn:
        async def close(self):
            called["closed"] = True

    async def fake_conn(branch, *, ws=None):
        called["branch"] = branch
        return FakeConn()

    async def fake_check(conn, *, table="sensor_readings", **kw):
        called["table"] = table
        return {"name": "cdf_sync", "status": hc.PASS, "detail": "STREAMING + advancing"}

    monkeypatch.setattr(vs.pps, "get_write_conn", fake_conn)
    monkeypatch.setattr(hc, "check_cdf_sync", fake_check)

    result = await vs.cdf_verify(_ctx("realtime"))
    assert result.status == sm.PASSED
    assert called["branch"] == "pharmaceutical"
    assert called["table"] == "sensor_readings"
    assert called.get("closed") is True


@pytest.mark.asyncio
async def test_cdf_verify_fails_when_not_streaming(monkeypatch):
    import services.health_checks as hc

    class FakeConn:
        async def close(self):
            pass

    async def fake_conn(branch, *, ws=None):
        return FakeConn()

    async def fake_check(conn, *, table="sensor_readings", **kw):
        return {"name": "cdf_sync", "status": hc.FAIL,
                "detail": "lakets_cdf._shadow_sensor_readings not STREAMING — enable managed sync"}

    monkeypatch.setattr(vs.pps, "get_write_conn", fake_conn)
    monkeypatch.setattr(hc, "check_cdf_sync", fake_check)

    result = await vs.cdf_verify(_ctx("realtime"))
    assert result.status == sm.FAILED
    assert "STREAMING" in str(result.error)


# ---------------------------------------------------------------------------
# provision_cluster must NOT leak a cluster when bring-up / library install fails
# ---------------------------------------------------------------------------
class _FakeMe:
    user_name = "sp-app-id"


class _RecordingClusters:
    def __init__(self):
        self.deleted = None

    def delete(self, cluster_id=None):
        self.deleted = cluster_id


class _ProvisionWs:
    def __init__(self, clusters):
        self.clusters = clusters
        self.current_user = type("U", (), {"me": staticmethod(lambda: _FakeMe())})()


class _VersionedClusters(_RecordingClusters):
    """Recording clusters that also serve a latest-LTS spark version."""

    def __init__(self, version="99.9.x-scala2.13", raises=False):
        super().__init__()
        self._version = version
        self._raises = raises

    def select_spark_version(self, long_term_support=False, latest=False):
        if self._raises:
            raise RuntimeError("versions API down")
        return self._version


@pytest.mark.asyncio
async def test_provision_cluster_terminates_created_cluster_on_failure(monkeypatch):
    clusters = _RecordingClusters()
    ws = _ProvisionWs(clusters)

    monkeypatch.setattr(vs, "_pick_gp_node", lambda w: ("m5.xlarge", 4))
    monkeypatch.setattr(vs.vc, "worker_sizing", lambda parts, cores: {"workers": 1})
    monkeypatch.setattr(vs.vc, "build_validation_cluster_spec", lambda **k: {"cluster_name": "iot-validate-x"})
    monkeypatch.setattr(vs, "_create_validation_cluster", lambda w, spec: "0706-created")

    async def boom(w, cluster_id):
        raise RuntimeError("cluster 0706-created entered PENDING timeout")

    monkeypatch.setattr(vs, "_wait_cluster_running", boom)

    ctx = StepContext(db=None, setup=_SETUP, state={}, key="provision_cluster",
                      step="provision_cluster", mode=None, ws=ws)
    with pytest.raises(RuntimeError):
        await vs.provision_cluster(ctx)
    # The just-created cluster was terminated rather than leaked.
    assert clusters.deleted == "0706-created"


@pytest.mark.asyncio
async def test_provision_cluster_pins_verified_dbr(monkeypatch):
    # Workspace "latest LTS" is 16.4 (Spark 3.5, no realTime trigger) — provision
    # must IGNORE it and pin the verified DBR 18.2 (RTM + service creds).
    ws = _ProvisionWs(_VersionedClusters(version="16.4.x-scala2.12"))
    captured: dict = {}

    monkeypatch.setattr(vs, "_pick_gp_node", lambda w: ("m5.xlarge", 4))
    monkeypatch.setattr(vs.vc, "worker_sizing", lambda parts, cores: {"workers": 1})

    def _spec(**kwargs):
        captured.update(kwargs)
        return {"cluster_name": "iot-validate-x"}

    monkeypatch.setattr(vs.vc, "build_validation_cluster_spec", _spec)
    monkeypatch.setattr(vs, "_create_validation_cluster", lambda w, spec: "0706-ok")

    async def _ok(w, cluster_id):
        return None

    monkeypatch.setattr(vs, "_wait_cluster_running", _ok)

    async def _libs(w, cluster_id, packages):
        return None

    monkeypatch.setattr(vs, "_install_cluster_libraries", _libs)

    ctx = StepContext(db=None, setup=_SETUP, state={}, key="provision_cluster",
                      step="provision_cluster", mode=None, ws=ws)
    result = await vs.provision_cluster(ctx)
    assert result.status == sm.PASSED
    # Pins the VERIFIED DBR (18.2), NOT the workspace's latest LTS (16.4).
    assert captured["spark_version"] == vs.vc.VALIDATION_DBR
    assert captured["spark_version"] == "18.2.x-scala2.13"


@pytest.mark.asyncio
async def test_latest_lts_falls_back_to_pinned_dbr_on_error():
    ws = _ProvisionWs(_VersionedClusters(raises=True))
    assert vs._latest_lts_spark_version(ws) == vs.vc.VALIDATION_DBR
