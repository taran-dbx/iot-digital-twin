import asyncio

import services.industry_service as isvc


def test_naming_helpers():
    assert isvc.pipeline_name("energy", "microbatch") == "iot-energy-microbatch"
    assert isvc.catalog_name("energy") == "iot_energy"
    assert isvc.topic_name("energy", "microbatch") == "iot.energy.microbatch"


def test_get_catalog_marks_running(monkeypatch):
    async def fake_list(_state):
        return [{"name": "iot-pharmaceutical-realtime", "status": "running"}]

    monkeypatch.setattr(isvc, "_list_pipelines", fake_list)
    cat = asyncio.get_event_loop().run_until_complete(isvc.get_catalog(None))
    pharma = next(c for c in cat if c["id"] == "pharmaceutical")
    rt = next(m for m in pharma["modes"] if m["id"] == "realtime")
    assert rt["status"] == "running"
    assert pharma["running_count"] == 1
    energy = next(c for c in cat if c["id"] == "energy")
    assert all(m["status"] == "idle" for m in energy["modes"])


def test_infer_mode_zerobus():
    row = {"source_type": "zerobus", "source_config": {}}
    assert isvc._infer_mode(row) == "zerobus"


def test_infer_mode_sss_lakebase_sink_is_realtime():
    row = {"source_type": "sss", "source_config": {"rtm_sink": "lakebase"}}
    assert isvc._infer_mode(row) == "realtime"


def test_infer_mode_sss_delta_sink_is_microbatch():
    row = {"source_type": "sss", "source_config": {"rtm_sink": "delta"}}
    assert isvc._infer_mode(row) == "microbatch"


def test_infer_mode_unknown_source_type_is_none():
    row = {"source_type": "dbsql", "source_config": {}}
    assert isvc._infer_mode(row) is None


def test_get_catalog_reconciles_legacy_pharma_demo_pipeline(monkeypatch):
    """The real live-workspace row: named "Demo", not the deterministic name."""

    async def fake_list(_state):
        return [
            {
                "name": "Demo",
                "status": "step2_complete",
                "industry": "pharmaceutical",
                "source_type": "sss",
                "source_config": {"source": "kafka", "rtm_sink": "lakebase"},
            }
        ]

    monkeypatch.setattr(isvc, "_list_pipelines", fake_list)
    cat = asyncio.get_event_loop().run_until_complete(isvc.get_catalog(None))
    pharma = next(c for c in cat if c["id"] == "pharmaceutical")
    rt = next(m for m in pharma["modes"] if m["id"] == "realtime")
    assert rt["status"] == "running"
    assert pharma["running_count"] >= 1


def test_get_catalog_reconciles_legacy_zerobus_pipeline(monkeypatch):
    async def fake_list(_state):
        return [
            {
                "name": "Auto Ingest",
                "status": "step2_complete",
                "industry": "automotive",
                "source_type": "zerobus",
                "source_config": {},
            }
        ]

    monkeypatch.setattr(isvc, "_list_pipelines", fake_list)
    cat = asyncio.get_event_loop().run_until_complete(isvc.get_catalog(None))
    auto = next(c for c in cat if c["id"] == "automotive")
    zb = next(m for m in auto["modes"] if m["id"] == "zerobus")
    assert zb["status"] == "running"


def test_get_catalog_draft_row_does_not_reconcile(monkeypatch):
    async def fake_list(_state):
        return [
            {
                "name": "Some Draft",
                "status": "draft",
                "industry": "pharmaceutical",
                "source_type": "sss",
                "source_config": {"rtm_sink": "lakebase"},
            }
        ]

    monkeypatch.setattr(isvc, "_list_pipelines", fake_list)
    cat = asyncio.get_event_loop().run_until_complete(isvc.get_catalog(None))
    pharma = next(c for c in cat if c["id"] == "pharmaceutical")
    assert all(m["status"] == "idle" for m in pharma["modes"])
    assert pharma["running_count"] == 0


def test_get_catalog_exact_name_match_still_wins(monkeypatch):
    async def fake_list(_state):
        return [{"name": "iot-energy-microbatch", "status": "running"}]

    monkeypatch.setattr(isvc, "_list_pipelines", fake_list)
    cat = asyncio.get_event_loop().run_until_complete(isvc.get_catalog(None))
    energy = next(c for c in cat if c["id"] == "energy")
    mb = next(m for m in energy["modes"] if m["id"] == "microbatch")
    assert mb["status"] == "running"
    assert energy["running_count"] == 1
