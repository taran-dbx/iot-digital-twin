"""Static regression coverage for the vendored manufacturing dashboard."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from routes.static_files import legacy_dashboard, legacy_settings_page


DIST = Path(__file__).parents[1] / "static" / "dist"


def _bundle() -> str:
    index = (DIST / "index.html").read_text(encoding="utf-8")
    match = re.search(r'src="/assets/(index-[^"]+\.js)"', index)
    assert match is not None
    return (DIST / "assets" / match.group(1)).read_text(encoding="utf-8")


def _manufacturing_derivation(bundle: str) -> str:
    # Minifier renames the React import (`T`/`E`) and the local binding (`Le`);
    # anchor on the industry-gated useMemo body instead.
    start = bundle.index('useMemo(()=>{if(t!=="manufacturing")')
    end = bundle.index(".useEffect(()=>", start)
    return bundle[start:end]


def test_manufacturing_kpis_use_live_sensor_derivations():
    """[C7230][defect][regression] Business KPIs update from manufacturing SSE data."""
    bundle = _bundle()

    for marker in (
        'Production:"throughput"',
        'OEE:"oee"',
        'Utilization:"utilization"',
        'Temperature:"temperature"',
        '"Active alerts":"faults"',
        '"assy_line_1_throughput"',
        '"assy_line_1_speed"',
        '"cnc_1_spindle_power"',
        '"assy_line_1_motor_current"',
        '"cnc_1_spindle_temp"',
    ):
        assert marker in bundle
    assert "/1250)*100" in bundle


def test_manufacturing_business_kpis_stay_separate_from_pipeline_metrics():
    """[C7230][regression] Do not map operational throughput into business KPIs."""
    bundle = _bundle()
    derivation = _manufacturing_derivation(bundle)

    assert "input_rows_per_second" not in derivation
    assert "processed_rows_per_second" not in derivation
    assert ".sensor_name===" in derivation


def test_manufacturing_realtime_data_is_industry_scoped():
    """[C7232][defect][regression] Mixed SSE data cannot contaminate the factory."""
    bundle = _bundle()
    derivation = _manufacturing_derivation(bundle)

    assert "latest_readings" in derivation
    assert "Object.values(" in derivation
    assert '.industry==="manufacturing"' in derivation
    assert ".filter(" in derivation
    assert ".fault_active===!0" in derivation
    assert 'e==="microbatch"' not in derivation
    assert 'aria-label":"Interactive 3D manufacturing factory"' in bundle
    assert '"bioreactor_1_temp"' in bundle
    assert '.fault_active==="1"' in bundle
    assert '.fault_active==="true"' in bundle
    assert 'value:"—",unit:"",alert:!1' in bundle


def test_manufacturing_overlay_cards_are_compact_and_accessible():
    """[C7231][defect][smoke][regression] 3D labels remain readable but unobtrusive."""
    index = (DIST / "index.html").read_text(encoding="utf-8")
    css = (DIST / "manufacturing-dashboard.css").read_text(encoding="utf-8")
    bundle = _bundle()

    assert 'href="/static/dist/manufacturing-dashboard.css"' in index
    assert index.index('href="/assets/index-') < index.index(
        'href="/static/dist/manufacturing-dashboard.css"'
    )
    label_rule = css[css.index(".manufacturing-factory__label {") :]
    label_rule = label_rule[: label_rule.index("}") + 1]
    assert "width: 132px" in label_rule
    assert "max-width: 132px" in label_rule
    assert "padding: 4px 6px" in label_rule
    for selector in (
        ".manufacturing-factory__label > span",
        ".manufacturing-factory__kpis small",
        ".manufacturing-factory__kpis b",
        ".manufacturing-factory__kpis i",
    ):
        rule = css[css.index(f"{selector} {{") :]
        rule = rule[: rule.index("}") + 1]
        assert "font-size: 10px" in rule
    title_rule = css[css.index(".manufacturing-factory__label strong {") :]
    title_rule = title_rule[: title_rule.index("}") + 1]
    assert "font-size: 12px" in title_rule
    assert "distanceFactor:8" in bundle
    assert 'aria-label":"Interactive 3D manufacturing factory"' in bundle
    assert 'aria-label":"Factory stations"' in bundle


def test_tier_summary_uses_latest_timestamps_and_delay_c7265():
    """[C7265][defect][regression] Tier summary avoids expensive row counts."""
    component = (
        Path(__file__).parents[1]
        / "client"
        / "src"
        / "components"
        / "dashboard"
        / "historical-dbsql.tsx"
    ).read_text(encoding="utf-8")

    assert "Latest Hot Event" in component
    assert "Latest Cold Event" in component
    assert "Replication Delay" in component
    assert "hot_latest_at" in component
    assert "latest_event_at" in component
    assert "hot_rows" not in component
    assert "total_rows" not in component
    assert "Checking Lakebase Sync status" in component


def test_streaming_summary_tiles_use_pipeline_scoped_spark_metrics():
    """[C7247][defect][regression] Summary tiles cannot use global app counters."""
    bundle = _bundle()
    component = (
        Path(__file__).parents[1]
        / "client"
        / "src"
        / "components"
        / "dashboard"
        / "streaming-source-metrics.tsx"
    ).read_text(encoding="utf-8")

    for label in (
        "Records/s",
        "Processed rows/s",
        "Ingest Latency (avg)",
        "Processing p50",
        "Latency p95",
        "Processing p99",
        "Buffer Len",
        "Spark E2E p99",
    ):
        assert label in bundle
    assert "const sparkProcessedRps = latency?.processed_rps" in component
    assert "const sparkProcessingP50 = latency?.processing_p50" in component
    assert "const sparkProcessingP99 = latency?.processing_p99" in component
    assert "const sparkE2eP99 = latency?.e2e_p99" in component

    # Process-global source values remain only for Zerobus; Spark tiles resolve
    # from the industry/mode-scoped latency response.
    assert re.search(r"isZerobus\s*\?\s*rolling", component)
    assert "sparkProcessedRps != null" in component
    assert "sparkProcessingP50 != null" in component
    assert "sparkProcessingP99 != null" in component
    assert "industry" in component and "mode" in component


def test_event_to_insert_latency_is_hidden_for_all_pipelines():
    """[C7248][defect][regression] Untrusted insert-lag values remain blank."""
    bundle = _bundle()
    streaming_source = (
        Path(__file__).parents[1]
        / "client"
        / "src"
        / "components"
        / "dashboard"
        / "streaming-source-metrics.tsx"
    ).read_text(encoding="utf-8")
    assert 'label:"Insert Lag p50",value:"---",sub:void 0' in bundle
    assert 'label:"Insert Lag p95",value:"---",sub:void 0' in bundle

    e2e_label = bundle.index('"Spark E2E p99"')
    e2e_widget = bundle[e2e_label : e2e_label + 300]
    assert "avg_e2e_latency_ms" not in e2e_widget
    assert '"---"' in e2e_widget
    assert "const SHOW_STREAMING_E2E_LATENCY = false" in streaming_source
    assert "SHOW_STREAMING_E2E_LATENCY && sparkE2eP99 != null" in streaming_source
    assert '"Ingestion Latency"' not in bundle


def test_industry_pages_wait_for_parallel_runtime_state():
    """[C7250][defect][regression] Runtime loading never flashes Start pipeline."""
    bundle = _bundle()

    assert "Loading industries…" in bundle
    assert "Loading industry…" in bundle
    assert '("/industries/runtime")' in bundle
    assert 'new Set(["starting","running","degraded","stopped"])' in bundle
    assert 'case"starting":return{label:"Starting",color:"#3b82f6"' in bundle


def test_gallery_renders_from_catalog_without_admin_fallback_c7262():
    """[C7262][performance][regression] Industries page never awaits /admin/pipelines.

    Catalog cards must appear from industry-first ``deploymentId`` without waiting
    for the Jobs/Lakebase runtime refresh. The public runtime endpoint is the only
    live-status source — falling back to ``/admin/pipelines`` on 503 re-runs the
    expensive path and is what made ``/industries`` hang.
    """
    bundle = _bundle()
    admin_api = (
        Path(__file__).parents[1] / "client" / "src" / "lib" / "admin-api.ts"
    ).read_text(encoding="utf-8")
    gallery = (
        Path(__file__).parents[1]
        / "client"
        / "src"
        / "pages"
        / "industry-gallery-page.tsx"
    ).read_text(encoding="utf-8")
    dashboard = (
        Path(__file__).parents[1]
        / "client"
        / "src"
        / "pages"
        / "mode-dashboard-page.tsx"
    ).read_text(encoding="utf-8")

    assert "listPipelineRuntime" in gallery
    assert "deploymentId" in gallery
    assert "galleryReady = !!catalog" in gallery or "galleryReady=!!catalog" in bundle.replace(
        " ", ""
    )
    assert "return request(`/admin/pipelines`)" not in admin_api
    assert "listAdminPipelines" not in dashboard
    assert "listPipelineRuntime" in dashboard


def test_failure_stopped_pipeline_offers_restart_c7260():
    """[C7260][defect][regression] Fail-stop recovery is actionable in the UI."""
    bundle = _bundle()

    assert "Pipeline stopped after task failure" in bundle
    assert "Restart pipeline" in bundle
    assert "restartable" in bundle


def test_legacy_pipeline_pages_are_unreachable_c7263():
    """[C7263][defect][regression] Industry UI never opens legacy pipeline pages."""
    root = Path(__file__).parents[1]
    bundle = _bundle()
    app = (root / "client" / "src" / "app.tsx").read_text(encoding="utf-8")
    pipeline_card = (
        root / "client" / "src" / "components" / "industry" / "pipeline-card.tsx"
    ).read_text(encoding="utf-8")
    setup_page = (
        root / "client" / "src" / "pages" / "industry-setup-page.tsx"
    ).read_text(encoding="utf-8")
    gallery = (
        root / "client" / "src" / "pages" / "industry-gallery-page.tsx"
    ).read_text(encoding="utf-8")

    retired_modules = (
        "pipeline-detail-page",
        "dashboard-page",
        "pipeline-wizard-page",
        "home-page",
    )
    for module in retired_modules:
        assert not re.search(rf'from\s+["\']@/pages/{module}["\']', app)

    source_redirects = {
        "/legacy": "/industries",
        "/pipelines/new": "/setup-validate",
        "/pipelines/:id": "/industries",
        "/pipelines/:id/dashboard": "/industries",
    }
    for path, target in source_redirects.items():
        assert re.search(
            rf'path="{re.escape(path)}".{{0,180}}'
            rf'<Navigate\s+to="{re.escape(target)}"\s+replace',
            app,
            re.DOTALL,
        )
        assert re.search(
            rf'path:"{re.escape(path)}".{{0,140}}'
            rf'to:"{re.escape(target)}".{{0,40}}replace:!0',
            bundle,
            re.DOTALL,
        )

    for legacy_copy in (
        "View Dashboard",
        "View Pipeline Dashboard",
        "Go to Dashboard",
        "NEURAL OBSERVATORY",
    ):
        assert legacy_copy not in bundle

    assert "/pipelines/" not in pipeline_card
    assert "navigate(`/i/${resolvedIndustryId}/${mode.id}/dashboard`)" in setup_page
    assert "/pipelines/${industryId}/dashboard" not in gallery
    assert 'navigate(`/setup-validate?industry=${encodeURIComponent(industry.id)}`)' in pipeline_card


@pytest.mark.asyncio
async def test_server_legacy_routes_redirect_to_industry_first_ui_c7263():
    """[C7263][defect][regression] Server routes cannot bypass SPA redirects."""
    legacy = await legacy_dashboard()
    settings = await legacy_settings_page()

    assert legacy.status_code == 307
    assert legacy.headers["location"] == "/industries"
    assert settings.status_code == 307
    assert settings.headers["location"] == "/setup-validate"


def test_runtime_is_loaded_once_then_polled_only_while_starting_c7264():
    """[C7264][defect][regression] Settled pipeline cards do not keep polling."""
    root = Path(__file__).parents[1]
    setup_page = (
        root / "client" / "src" / "pages" / "industry-setup-page.tsx"
    ).read_text(encoding="utf-8")
    pipeline_card = (
        root / "client" / "src" / "components" / "industry" / "pipeline-card.tsx"
    ).read_text(encoding="utf-8")

    assert "void refreshAdmin();" in setup_page
    assert 'if (effective !== "starting") return;' in pipeline_card
    assert "timer = setTimeout(tick, POLL_FAST_MS)" in pipeline_card
    assert 'realStatus === "stopped" || realStatus === "draft"' in pipeline_card
    assert "requestAge >= STARTING_SETTLE_GRACE_MS" in pipeline_card
    assert "setJustStarted(false)" in pipeline_card
    assert "return () => clearTimeout(timer)" in pipeline_card
