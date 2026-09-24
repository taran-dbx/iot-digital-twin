"""Tests for services.industry_templates — curated industry data ported from
the design reference (design-ref/Digital Twin Platform.dc.html)."""

from __future__ import annotations

from services.industry_templates import (
    CURATED_INDUSTRIES,
    MODES,
    get_template,
    mode_config,
)


def test_four_curated_industries():
    assert CURATED_INDUSTRIES == ["manufacturing", "pharmaceutical", "energy", "automotive"]


def test_get_template_pharma_has_kpis_and_twin():
    t = get_template("pharmaceutical")
    assert t is not None
    assert t.twin_type == "vessel"
    assert len(t.kpis) >= 3
    assert "reactor" in " ".join(k["k"].lower() for k in t.kpis)


def test_manufacturing_business_kpis_have_stable_fallbacks():
    """[C7230][defect][regression] Manufacturing KPIs retain startup fallbacks."""
    template = get_template("manufacturing")

    assert template is not None
    assert template.kpis == [
        {"k": "Production", "v": "1,204", "u": "units/hr"},
        {"k": "OEE", "v": "87.3", "u": "%"},
        {"k": "Utilization", "v": "72", "u": "%"},
        {"k": "Temperature", "v": "64", "u": "°C", "s": "warn"},
        {"k": "Active alerts", "v": "2", "u": "", "s": "crit"},
    ]


def test_get_template_unknown_returns_none():
    assert get_template("banking") is None


def test_mode_config_flags():
    realtime_desc = mode_config("realtime")["desc"]
    assert isinstance(realtime_desc, str) and realtime_desc
    assert realtime_desc.startswith("Spark Structured Streaming")
    assert mode_config("realtime") == {
        "id": "realtime", "name": "Real-time", "to": "Lakebase + LakeTS",
        "desc": realtime_desc, "uses_kafka": True,
        "uses_lakebase": True, "has_metrics": True,
    }
    assert mode_config("zerobus")["uses_kafka"] is False
    assert mode_config("zerobus")["uses_lakebase"] is False
    assert mode_config("zerobus")["has_metrics"] is False
    assert mode_config("microbatch")["has_metrics"] is True
    assert mode_config("microbatch")["uses_lakebase"] is False
