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
    assert kw["metrics_table_fqn"] == "iot_pharmaceutical.metrics.streaming_metrics"
    assert "streaming_metrics/" in kw["metrics_table_location"]


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
async def test_cdf_verify_passes_specific_shadow_table(monkeypatch):
    captured = {}

    async def fake_run(ctx, code, *, timeout_s=300):
        captured["code"] = code
        return StepResult(sm.PASSED, data={})

    monkeypatch.setattr(vs, "_run_on_cluster", fake_run)
    monkeypatch.setattr(vs.snip, "cdf_verify", lambda table, **k: f"CDF::{table}")

    result = await vs.cdf_verify(_ctx("realtime"))
    assert result.status == sm.PASSED
    assert captured["code"] == "CDF::sensor_readings"


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
async def test_provision_cluster_uses_latest_lts_runtime(monkeypatch):
    ws = _ProvisionWs(_VersionedClusters(version="99.9.x-scala2.13"))
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
    # The interactive cluster runs the workspace's latest LTS runtime, not a pin.
    assert captured["spark_version"] == "99.9.x-scala2.13"


@pytest.mark.asyncio
async def test_latest_lts_falls_back_to_pinned_dbr_on_error():
    ws = _ProvisionWs(_VersionedClusters(raises=True))
    assert vs._latest_lts_spark_version(ws) == vs.vc.VALIDATION_DBR
