"""Industry listing and sample generation endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Request, Depends
from fastapi.responses import JSONResponse

from core.state import AppState, get_app_state

router = APIRouter()


@router.get("/api/industries")
async def list_industries():
    """Return all built-in industry types with sensor counts."""
    from iot_simulator import DataGenerator, IndustryType

    result = []
    for industry in IndustryType:
        try:
            gen = DataGenerator(industries=[industry.value])
            result.append({
                "id": industry.value,
                "label": industry.value.replace("_", " ").title(),
                "sensor_count": gen.sensor_count,
            })
        except Exception:
            result.append({
                "id": industry.value,
                "label": industry.value.replace("_", " ").title(),
                "sensor_count": 0,
            })
    return result


@router.get("/api/industries/{name}/sensors")
async def get_industry_sensors(name: str):
    """Generate one tick and return the SensorRecord fields as config-like dicts."""
    from iot_simulator import DataGenerator

    try:
        gen = DataGenerator(industries=[name])
        records = gen.tick()

        sensor_cfgs = {s.name: s for s in gen._sensors} if hasattr(gen, "_sensors") else {}

        configs = []
        for r in records:
            freq = 1.0
            sc = sensor_cfgs.get(r.sensor_name)
            if sc and hasattr(sc, "config") and hasattr(sc.config, "update_frequency_hz"):
                freq = sc.config.update_frequency_hz

            configs.append({
                "name": r.sensor_name,
                "sensor_type": r.sensor_type,
                "unit": r.unit,
                "min_value": r.min_value,
                "max_value": r.max_value,
                "nominal_value": r.nominal_value,
                "noise_std": 0,
                "anomaly_probability": 0,
                "anomaly_magnitude": 0,
                "update_frequency_hz": freq,
                "cyclic": False,
                "cycle_period_seconds": 0,
                "cycle_amplitude": 0,
            })
        return configs
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})


@router.post("/api/generate-sample")
async def generate_sample(request: Request, state: AppState = Depends(get_app_state)):
    """Generate sample SensorRecords via DataGenerator.tick()."""
    from iot_simulator import DataGenerator

    body = await request.json()
    industry = body.get("industry", "pharmaceutical")
    num_ticks = min(int(body.get("ticks", 3)), 10)
    rate_hz = float(body.get("update_rate_hz", state.update_rate_hz))

    try:
        gen = DataGenerator(industries=[industry])
        collected: list[dict] = []
        for _ in range(num_ticks):
            for r in gen.tick():
                collected.append({
                    "sensor_name": r.sensor_name,
                    "industry": r.industry,
                    "value": round(r.value, 3),
                    "unit": r.unit,
                    "sensor_type": r.sensor_type,
                    "timestamp": r.timestamp,
                    "fault_active": r.fault_active,
                })

        sensor_count = len(gen._sensors) if hasattr(gen, "_sensors") else 0
        est_record_bytes = 175
        payload_info = {
            "sensor_count": sensor_count,
            "record_size_bytes": est_record_bytes,
            "per_tick_bytes": sensor_count * est_record_bytes,
            "per_second_bytes": int(sensor_count * est_record_bytes * rate_hz),
        }

        return {
            "industry": industry,
            "count": len(collected),
            "records": collected[:100],
            "payload_info": payload_info,
        }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})
