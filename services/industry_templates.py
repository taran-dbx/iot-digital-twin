"""Curated industry templates and streaming-mode metadata.

This is a straight Python port of the ``IND``, ``MODES``, and ``MODEMETA``
data objects embedded in the vendored design reference
(``design-ref/Digital Twin Platform.dc.html``, ``<script type="text/x-dc">``
block). It is the single source of truth for the four curated industries the
gallery/setup UI presents, and for the three streaming modes (realtime,
microbatch, zerobus) a pipeline can run in.

Values here are transcribed verbatim from the design where the design gives
exact strings (``name``, ``accent``, ``tag``, ``lede``, ``kpis``, mode
``desc``/``to``). ``sensors`` is derived from each industry's ``stream``
array in the design — the first element of every ``[name, value, unit,
alert]`` tuple.
"""

from __future__ import annotations

from dataclasses import dataclass, field


CURATED_INDUSTRIES: list[str] = ["manufacturing", "pharmaceutical", "energy", "automotive"]

MODES: list[str] = ["realtime", "microbatch", "zerobus"]


@dataclass(frozen=True)
class IndustryTemplate:
    id: str
    name: str
    accent: str
    icon: str
    twin_type: str
    tag: str
    lede: str
    sensors: list[str] = field(default_factory=list)
    kpis: list[dict] = field(default_factory=list)
    default_hz: int = 0


# Ported verbatim from IND in design-ref/Digital Twin Platform.dc.html.
_TEMPLATES: dict[str, IndustryTemplate] = {
    "manufacturing": IndustryTemplate(
        id="manufacturing",
        name="Manufacturing",
        accent="#e86247",
        icon="factory",
        twin_type="line",
        tag="Assembly line · CNC · robotics",
        lede=(
            "Real-time visibility into your production line — station "
            "throughput, spindle load, OEE and fault detection across the "
            "floor."
        ),
        sensors=[
            "Station 3 · spindle",
            "Conveyor speed",
            "Motor vibration",
            "Coolant temp",
            "Torque",
            "Air pressure",
        ],
        kpis=[
            {"k": "Line throughput", "v": "1,204", "u": "units/hr"},
            {"k": "OEE", "v": "87.3", "u": "%"},
            {"k": "Spindle load", "v": "72", "u": "%"},
            {"k": "Station temp", "v": "64", "u": "°C", "s": "warn"},
            {"k": "Active faults", "v": "2", "u": "", "s": "crit"},
        ],
        default_hz=40,
    ),
    "pharmaceutical": IndustryTemplate(
        id="pharmaceutical",
        name="Pharmaceutical",
        accent="#2db0a0",
        icon="pharma",
        twin_type="vessel",
        tag="Bioreactor · WFI · batch yield",
        lede=(
            "Process integrity for regulated production — reactor "
            "temperature, pressure, WFI flow and batch yield with "
            "tamper-evident history."
        ),
        sensors=[
            "Reactor A · temp",
            "Vessel pressure",
            "WFI flow",
            "pH",
            "Dissolved O₂",
            "Agitator RPM",
        ],
        kpis=[
            {"k": "Reactor temp", "v": "37.2", "u": "°C"},
            {"k": "Vessel pressure", "v": "1.8", "u": "bar"},
            {"k": "WFI flow", "v": "42", "u": "L/min"},
            {"k": "Batch yield", "v": "96.1", "u": "%"},
            {"k": "Deviations", "v": "0", "u": ""},
        ],
        default_hz=25,
    ),
    "energy": IndustryTemplate(
        id="energy",
        name="Energy",
        accent="#6cbf3c",
        icon="energy",
        twin_type="grid",
        tag="Turbines · grid load · substations",
        lede=(
            "Grid and generation health — turbine output, frequency "
            "stability, transformer load and demand across substations."
        ),
        sensors=[
            "Turbine 1 · output",
            "Grid frequency",
            "Xfmr T4 load",
            "Voltage L1",
            "Reactive power",
            "Bearing temp",
        ],
        kpis=[
            {"k": "Grid output", "v": "842", "u": "MW"},
            {"k": "Frequency", "v": "50.01", "u": "Hz"},
            {"k": "Turbine RPM", "v": "3,000", "u": ""},
            {"k": "Xfmr load", "v": "78", "u": "%", "s": "warn"},
            {"k": "Faults", "v": "0", "u": ""},
        ],
        default_hz=60,
    ),
    "automotive": IndustryTemplate(
        id="automotive",
        name="Automotive",
        accent="#5b7be8",
        icon="auto",
        twin_type="auto",
        tag="Body shop · paint · powertrain test",
        lede=(
            "Connected assembly and test — weld quality, paint-booth "
            "climate, torque verification and end-of-line diagnostics per "
            "vehicle."
        ),
        sensors=[
            "Weld cell · current",
            "Torque station 7",
            "Paint booth RH",
            "Booth temp",
            "Conveyor index",
            "Battery test V",
        ],
        kpis=[
            {"k": "Vehicles/hr", "v": "58", "u": ""},
            {"k": "First-pass yield", "v": "94.2", "u": "%"},
            {"k": "Weld current", "v": "11.4", "u": "kA"},
            {"k": "Booth humidity", "v": "61", "u": "%", "s": "warn"},
            {"k": "Rework flags", "v": "1", "u": ""},
        ],
        default_hz=50,
    ),
}


# Ported verbatim from MODES in design-ref/Digital Twin Platform.dc.html.
_MODES: dict[str, dict] = {
    "realtime": {
        "id": "realtime",
        "name": "Real-time",
        "to": "Lakebase + LakeTS",
        "desc": (
            "Spark Structured Streaming → Lakebase, served as time-series "
            "(hot) with CDF sync to Delta for cold history."
        ),
        "uses_kafka": True,
        "uses_lakebase": True,
        "has_metrics": True,
    },
    "microbatch": {
        "id": "microbatch",
        "name": "Micro-batch",
        "to": "Delta table",
        "desc": (
            "Spark Structured Streaming in micro-batches → a Delta table "
            "in Unity Catalog."
        ),
        "uses_kafka": True,
        "uses_lakebase": False,
        "has_metrics": True,
    },
    "zerobus": {
        "id": "zerobus",
        "name": "Zerobus",
        "to": "Delta (direct)",
        "desc": "Zerobus SDK writes straight to a Delta table — no Spark streaming job.",
        "uses_kafka": False,
        "uses_lakebase": False,
        "has_metrics": False,
    },
}


def get_template(industry: str) -> IndustryTemplate | None:
    """Return the curated template for ``industry``, or None if unknown."""
    return _TEMPLATES.get(industry)


def mode_config(mode: str) -> dict:
    """Return the streaming-mode metadata for ``mode`` (realtime/microbatch/zerobus)."""
    return dict(_MODES[mode])
