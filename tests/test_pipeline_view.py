"""Unit tests for services/pipeline_view.py — the admin pipeline contract derivation.

Pure logic, no I/O: the Jobs-API result is passed in as a ``job_running`` map so
these tests fully cover the status / health / startability rules with no mocks.
"""

from __future__ import annotations

from services import pipeline_view as pv
from services import validation_state_machine as sm


# --- helpers -----------------------------------------------------------------
def _all_passed_state(mode: str) -> dict:
    """A validation_state with every required step for ``mode`` marked passed."""
    keys = sm.mode_required_keys(mode)
    return {"steps": {k: {"status": sm.PASSED} for k in keys}}


def _deployed(mode: str = "realtime", **extra) -> dict:
    d = {"mode": mode, "status": "deployed", "job_id": "123"}
    d.update(extra)
    return d


# --- is_deployed -------------------------------------------------------------
class TestIsDeployed:
    def test_none_is_not_deployed(self):
        assert pv.is_deployed(None) is False

    def test_no_job_id_is_not_deployed(self):
        assert pv.is_deployed({"mode": "realtime", "status": "deployed"}) is False

    def test_pending_status_is_not_deployed(self):
        assert pv.is_deployed({"job_id": "1", "status": "pending"}) is False

    def test_deployed_with_job_id(self):
        assert pv.is_deployed({"job_id": "1", "status": "deployed"}) is True


# --- status ------------------------------------------------------------------
class TestModeStatus:
    def test_draft_is_default_when_no_deployment(self):
        assert pv.mode_status(None, job_running=True) == pv.STATUS_DRAFT

    def test_draft_when_not_deployed(self):
        assert pv.mode_status({"job_id": None}, job_running=True) == pv.STATUS_DRAFT

    def test_running_only_when_deployed_and_job_running(self):
        assert pv.mode_status(_deployed(), job_running=True) == pv.STATUS_RUNNING

    def test_stopped_when_deployed_but_not_running(self):
        assert pv.mode_status(_deployed(), job_running=False) == pv.STATUS_STOPPED


# --- cdf / gate --------------------------------------------------------------
class TestCdfConfirmed:
    def test_non_realtime_always_confirmed(self):
        assert pv.cdf_confirmed({}, None, "microbatch") is True
        assert pv.cdf_confirmed({}, None, "zerobus") is True

    def test_realtime_not_confirmed_by_default(self):
        assert pv.cdf_confirmed({}, _deployed(), "realtime") is False

    def test_realtime_confirmed_via_cdf_verify_passed(self):
        state = {"steps": {"realtime:cdf_verify": {"status": sm.PASSED}}}
        assert pv.cdf_confirmed({"validation_state": state}, None, "realtime") is True

    def test_realtime_confirmed_via_deployment_flag(self):
        dep = _deployed(cdf_enabled=True)
        assert pv.cdf_confirmed({}, dep, "realtime") is True


class TestGateOpen:
    def test_empty_state_not_open(self):
        assert pv.gate_open({}, "realtime") is False

    def test_all_passed_is_open(self):
        setup = {"validation_state": _all_passed_state("microbatch")}
        assert pv.gate_open(setup, "microbatch") is True


# --- startable + health ------------------------------------------------------
class TestStartableAndHealth:
    def test_not_deployed_is_draft_health_and_not_startable(self):
        setup = {"validation_state": _all_passed_state("microbatch")}
        assert pv.mode_health(setup, None, "microbatch") == pv.HEALTH_DRAFT
        assert pv.mode_startable(setup, None, "microbatch") is False

    def test_realtime_cdf_incomplete_is_unhealthy_and_not_startable(self):
        # Everything passed EXCEPT cdf (manual step): gate not open -> unhealthy.
        keys = [k for k in sm.mode_required_keys("realtime") if "cdf" not in k]
        setup = {"validation_state": {"steps": {k: {"status": sm.PASSED} for k in keys}}}
        dep = _deployed("realtime")
        assert pv.mode_health(setup, dep, "realtime") == pv.HEALTH_UNHEALTHY
        assert pv.mode_startable(setup, dep, "realtime") is False

    def test_microbatch_fully_validated_and_deployed_is_healthy_startable(self):
        setup = {"validation_state": _all_passed_state("microbatch")}
        dep = _deployed("microbatch")
        assert pv.mode_health(setup, dep, "microbatch") == pv.HEALTH_HEALTHY
        assert pv.mode_startable(setup, dep, "microbatch") is True

    def test_realtime_full_gate_open_is_healthy_startable(self):
        # Gate open for realtime already implies cdf_verify passed.
        setup = {"validation_state": _all_passed_state("realtime")}
        dep = _deployed("realtime")
        assert pv.mode_startable(setup, dep, "realtime") is True
        assert pv.mode_health(setup, dep, "realtime") == pv.HEALTH_HEALTHY

    def test_deployed_but_gate_not_open_is_unhealthy(self):
        dep = _deployed("microbatch")
        assert pv.mode_health({}, dep, "microbatch") == pv.HEALTH_UNHEALTHY
        assert pv.mode_startable({}, dep, "microbatch") is False


# --- rollups + build_pipeline_view -------------------------------------------
class TestBuildPipelineView:
    def test_no_deployments_is_all_draft(self):
        setup = {"id": "s1", "industry": "pharma", "enabled_modes": ["realtime"]}
        view = pv.build_pipeline_view(setup, [], {})
        assert view["status"] == pv.STATUS_DRAFT
        assert view["health"] == pv.HEALTH_DRAFT
        assert view["startable"] is False
        assert view["ready"] is False
        assert view["modes"][0]["mode"] == "realtime"
        assert view["modes"][0]["status"] == pv.STATUS_DRAFT

    def test_current_realtime_pipeline_unhealthy_not_startable(self):
        # Deployed realtime with CDF not done -> unhealthy + not startable (the
        # real-world state of current pipelines).
        setup = {
            "id": "s1", "industry": "pharma", "enabled_modes": ["realtime"],
            "validation_state": {"steps": {
                k: {"status": sm.PASSED}
                for k in sm.mode_required_keys("realtime") if "cdf" not in k
            }},
        }
        deployments = [_deployed("realtime")]
        view = pv.build_pipeline_view(setup, deployments, {"123": False})
        assert view["status"] == pv.STATUS_STOPPED
        assert view["health"] == pv.HEALTH_UNHEALTHY
        assert view["startable"] is False

    def test_running_reported_only_from_jobs_api(self):
        setup = {"id": "s1", "industry": "pharma", "enabled_modes": ["microbatch"],
                 "validation_state": _all_passed_state("microbatch")}
        deployments = [_deployed("microbatch")]
        view = pv.build_pipeline_view(setup, deployments, {"123": True})
        assert view["status"] == pv.STATUS_RUNNING
        assert view["health"] == pv.HEALTH_HEALTHY
        assert view["startable"] is True

    def test_mode_union_orders_by_mode_order(self):
        setup = {"id": "s1", "industry": "x", "enabled_modes": ["zerobus"]}
        deployments = [{"mode": "realtime", "status": "deployed", "job_id": "9"}]
        view = pv.build_pipeline_view(setup, deployments, {})
        assert [m["mode"] for m in view["modes"]] == ["realtime", "zerobus"]

    def test_rollup_status_prefers_running_then_stopped(self):
        assert pv.rollup_status(
            [{"status": "stopped"}, {"status": "running"}]
        ) == pv.STATUS_RUNNING
        assert pv.rollup_status(
            [{"status": "draft"}, {"status": "stopped"}]
        ) == pv.STATUS_STOPPED
        assert pv.rollup_status([{"status": "draft"}]) == pv.STATUS_DRAFT

    def test_rollup_health_unhealthy_wins(self):
        assert pv.rollup_health(
            [{"health": "healthy"}, {"health": "unhealthy"}]
        ) == pv.HEALTH_UNHEALTHY
        assert pv.rollup_health([{"health": "healthy"}]) == pv.HEALTH_HEALTHY
        assert pv.rollup_health([{"health": "draft"}]) == pv.HEALTH_DRAFT


class TestRollupApiHealth:
    def _c(self, tier, status):
        return {"name": "x", "tier": tier, "status": status, "detail": ""}

    def test_unhealthy_when_any_api_check_fails(self):
        checks = [self._c("api", "pass"), self._c("api", "fail")]
        assert pv.rollup_api_health(checks) == pv.HEALTH_UNHEALTHY

    def test_healthy_when_pass_and_none_failed(self):
        checks = [self._c("api", "pass"), self._c("api", "skip")]
        assert pv.rollup_api_health(checks) == pv.HEALTH_HEALTHY

    def test_unknown_when_all_skipped(self):
        checks = [self._c("api", "skip"), self._c("api", "skip")]
        assert pv.rollup_api_health(checks) == pv.HEALTH_UNKNOWN

    def test_e2e_checks_never_gate_health(self):
        # A failing Tier-2 check must NOT make the pipeline unhealthy.
        checks = [self._c("api", "pass"), self._c("e2e", "fail")]
        assert pv.rollup_api_health(checks) == pv.HEALTH_HEALTHY


class TestCollectJobIds:
    def test_collects_only_deployed_distinct(self):
        deployments = [
            _deployed("realtime", job_id="1"),
            {"mode": "microbatch", "status": "pending", "job_id": None},
            _deployed("zerobus", job_id="1"),  # duplicate id
        ]
        assert pv.collect_job_ids(deployments) == ["1"]
