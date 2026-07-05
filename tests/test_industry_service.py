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
