"""
IoT Digital Twin — Databricks Architecture Diagram
Generated with the Python `diagrams` library (graphviz backend).
"""

from diagrams import Diagram, Cluster, Edge
from diagrams.programming.framework import React, FastAPI
from diagrams.programming.language import TypeScript, Python
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.analytics import Spark
from diagrams.onprem.queue import Kafka
from diagrams.aws.analytics import Kinesis
from diagrams.generic.storage import Storage
from diagrams.generic.network import Firewall
from diagrams.onprem.client import User
from diagrams.onprem.network import Nginx
from diagrams.saas.chat import Slack      # placeholder icon – we'll use custom labels
from diagrams.programming.flowchart import StartEnd, Action, Decision

OUTPUT = "/Users/taranpreetsingh.grover/Documents/Claude Projects/Apps/iot-digital-twin/docs/architecture-diagram"

graph_attrs = {
    "fontsize": "22",
    "bgcolor": "#0f172a",
    "fontcolor": "#f1f5f9",
    "fontname": "Inter, Helvetica, sans-serif",
    "pad": "0.8",
    "nodesep": "0.5",
    "ranksep": "1.2",
    "splines": "ortho",
    "rankdir": "LR",
}

node_attrs = {
    "fontsize": "13",
    "fontname": "Inter, Helvetica, sans-serif",
    "fontcolor": "#f1f5f9",
    "style": "filled",
    "fillcolor": "#1e293b",
    "color": "#475569",
}

edge_attrs = {
    "color": "#64748b",
    "fontcolor": "#94a3b8",
    "fontsize": "11",
    "fontname": "Inter, Helvetica, sans-serif",
    "penwidth": "1.5",
    "arrowsize": "0.8",
}

with Diagram(
    "IoT Digital Twin — Databricks Architecture",
    filename=OUTPUT,
    outformat="png",
    show=False,
    direction="LR",
    graph_attr=graph_attrs,
    node_attr=node_attrs,
    edge_attr=edge_attrs,
):
    # ─────────────────────────────────────────────────────
    # USER
    # ─────────────────────────────────────────────────────
    user = User("Browser User")

    # ─────────────────────────────────────────────────────
    # FRONTEND
    # ─────────────────────────────────────────────────────
    with Cluster(
        "FRONTEND — React + TypeScript  (Vite · Tailwind · Radix UI)",
        graph_attr={
            "bgcolor": "#1e3a5f",
            "fontcolor": "#bfdbfe",
            "fontsize": "16",
            "style": "filled,rounded",
            "pencolor": "#3b82f6",
            "penwidth": "2",
        },
    ):
        with Cluster("Pages", graph_attr={"bgcolor": "#1d4ed8", "fontcolor": "#dbeafe", "pencolor": "#60a5fa"}):
            pages = React("Home · Dashboard\nPipeline Wizard · Detail\nData Hub · Source Settings")

        with Cluster("State & Hooks", graph_attr={"bgcolor": "#1d4ed8", "fontcolor": "#dbeafe", "pencolor": "#60a5fa"}):
            ctx     = TypeScript("UserProvider\nPipelineProvider\nSensorProvider")
            ws_hook = TypeScript("useWebSocket\n(auto-reconnect)")
            charts  = TypeScript("Recharts\nreal-time viz")

    # ─────────────────────────────────────────────────────
    # BACKEND
    # ─────────────────────────────────────────────────────
    with Cluster(
        "BACKEND — FastAPI on Databricks Apps  (uvicorn · async)",
        graph_attr={
            "bgcolor": "#14532d",
            "fontcolor": "#bbf7d0",
            "fontsize": "16",
            "style": "filled,rounded",
            "pencolor": "#22c55e",
            "penwidth": "2",
        },
    ):
        auth = FastAPI("Auth Middleware\nx-forwarded-access-token\nOAuth via Databricks")

        with Cluster("REST API Endpoints", graph_attr={"bgcolor": "#166534", "fontcolor": "#dcfce7", "pencolor": "#4ade80"}):
            api_pip = FastAPI("/api/pipelines\nPipeline CRUD")
            api_sen = FastAPI("/api/sensor\nSensor Queries")
            api_pro = FastAPI("/api/provision\nProvisioning")
            api_clu = FastAPI("/api/clusters\nCluster Mgmt")
            api_sss = FastAPI("/api/sss/ingest\nSSS Ingest")

        with Cluster("Real-time Layer", graph_attr={"bgcolor": "#166534", "fontcolor": "#dcfce7", "pencolor": "#4ade80"}):
            ws_ep   = FastAPI("/ws  WebSocket Endpoint")
            bcast   = FastAPI("Broadcast Manager\nconnected clients")

        with Cluster("IoT Simulator  (asyncio background)", graph_attr={"bgcolor": "#166534", "fontcolor": "#dcfce7", "pencolor": "#4ade80"}):
            sim     = Python("Sensor Event\nGenerator")
            metrics = Python("Source Metrics\nwebsocket · lakebase\nzerobus · sss")

    # ─────────────────────────────────────────────────────
    # DATA SINKS
    # ─────────────────────────────────────────────────────
    with Cluster(
        "DATA SINKS — 3 Parallel Outputs from IoT Simulator",
        graph_attr={
            "bgcolor": "#164e63",
            "fontcolor": "#cffafe",
            "fontsize": "16",
            "style": "filled,rounded",
            "pencolor": "#06b6d4",
            "penwidth": "2",
        },
    ):
        sk_ws = Storage("WebSocket Sink\n2 Hz broadcast")
        sk_lb = Storage("Lakebase Sink\nasyncpg · batch 50 · 1 Hz")
        sk_zb = Storage("Zerobus Sink\ngRPC stream · batch 10")

    # ─────────────────────────────────────────────────────
    # STORAGE
    # ─────────────────────────────────────────────────────
    with Cluster(
        "DATA STORAGE",
        graph_attr={
            "bgcolor": "#431407",
            "fontcolor": "#fed7aa",
            "fontsize": "16",
            "style": "filled,rounded",
            "pencolor": "#f97316",
            "penwidth": "2",
        },
    ):
        with Cluster("Lakebase — PostgreSQL", graph_attr={"bgcolor": "#7c2d12", "fontcolor": "#ffedd5", "pencolor": "#fb923c"}):
            pg_pip = PostgreSQL("pipelines\nJSONB config · per-user")
            pg_sen = PostgreSQL("sensor_readings\ntime-series")
            pg_mv1 = PostgreSQL("sensor_1m\n1-min materialized view")
            pg_mv2 = PostgreSQL("sensor_5m\n5-min materialized view")

        with Cluster("Delta + Unity Catalog", graph_attr={"bgcolor": "#7c2d12", "fontcolor": "#ffedd5", "pencolor": "#fb923c"}):
            delta = Storage("Delta Table\ntaran_explore.iot_simulator\n.iot_sensor_data")
            uc    = Storage("Unity Catalog\ncatalogs · schemas\nvolumes · secrets")

    # ─────────────────────────────────────────────────────
    # EXTERNAL — DATABRICKS PLATFORM
    # ─────────────────────────────────────────────────────
    with Cluster(
        "EXTERNAL INTEGRATIONS — Databricks Platform",
        graph_attr={
            "bgcolor": "#2e1065",
            "fontcolor": "#e9d5ff",
            "fontsize": "16",
            "style": "filled,rounded",
            "pencolor": "#a855f7",
            "penwidth": "2",
        },
    ):
        with Cluster("Spark Structured Streaming", graph_attr={"bgcolor": "#4c1d95", "fontcolor": "#ede9fe", "pencolor": "#c084fc"}):
            sss_src = Spark("Sources\nKafka · Kinesis · EventHub")
            sss_job = Spark("SSS Notebook Job\nserverless Spark")
            sss_fw  = Spark("ForeachWriter\n→ Lakebase")

        with Cluster("Databricks Workspace", graph_attr={"bgcolor": "#4c1d95", "fontcolor": "#ede9fe", "pencolor": "#c084fc"}):
            dbk_sdk = Python("Databricks SDK\nOAuth · cluster mgmt")
            dbk_api = Nginx("Workspace API\nclusters · notebooks · jobs")
            dbk_kaf = Kafka("Kafka Producer\nNotebook Job")

    # ─────────────────────────────────────────────────────
    # EDGES
    # ─────────────────────────────────────────────────────
    blue  = Edge(color="#60a5fa", style="bold", penwidth="2")
    green = Edge(color="#4ade80", style="bold", penwidth="2")
    cyan  = Edge(color="#22d3ee", style="bold", penwidth="2")
    orange= Edge(color="#fb923c", style="bold", penwidth="2")
    purple= Edge(color="#c084fc", style="bold", penwidth="2")
    gray  = Edge(color="#64748b", style="dashed", penwidth="1.5")
    amber = Edge(color="#fbbf24", style="dashed", penwidth="1.5")

    # User ↔ Frontend
    user >> Edge(color="#60a5fa", label="HTTPS", penwidth="2") >> pages
    bcast >> Edge(color="#22d3ee", label="real-time sensor frames", penwidth="2") >> ws_hook

    # Frontend ↔ Backend REST
    pages >> Edge(color="#4ade80", label="GET sensor data", penwidth="2") >> api_sen
    pages >> Edge(color="#4ade80", label="pipeline CRUD", penwidth="2") >> api_pip
    pages >> Edge(color="#4ade80", label="provision / cluster ops", penwidth="2") >> api_pro
    ctx   >> Edge(color="#fbbf24", label="OAuth token", style="dashed") >> auth
    auth  >> Edge(color="#fbbf24", label="validates", style="dashed") >> api_pip
    auth  >> Edge(color="#fbbf24", label="validates", style="dashed") >> ws_ep

    # Frontend ↔ WebSocket
    ws_hook >> Edge(color="#22d3ee", label="WebSocket frames", penwidth="2") >> ws_ep
    ws_ep >> Edge(color="#22d3ee", penwidth="2") >> bcast

    # IoT Simulator → Sinks
    sim >> Edge(color="#22d3ee", label="sensor events") >> sk_ws
    sim >> Edge(color="#22d3ee", label="sensor events") >> sk_lb
    sim >> Edge(color="#22d3ee", label="sensor events") >> sk_zb
    sim >> metrics

    # Sinks → Storage / Broadcast
    sk_ws >> Edge(color="#22d3ee", label="broadcast 2 Hz") >> bcast
    sk_lb >> Edge(color="#fb923c", label="batch write") >> pg_sen
    sk_zb >> Edge(color="#fb923c", label="gRPC stream") >> delta

    # API → Storage
    api_pip >> Edge(color="#fb923c", label="CRUD") >> pg_pip
    api_sen >> Edge(color="#fb923c", label="SELECT") >> pg_sen
    api_sen >> Edge(color="#fb923c", label="SELECT") >> pg_mv1
    api_sen >> Edge(color="#fb923c", label="SELECT") >> pg_mv2

    # SSS pipeline
    sss_src >> Edge(color="#c084fc", label="read stream") >> sss_job
    sss_job >> Edge(color="#c084fc", label="HTTP POST\nwindowed aggregates") >> api_sss
    sss_job >> sss_fw
    sss_fw  >> Edge(color="#fb923c", label="write raw rows") >> pg_sen
    api_sss >> Edge(color="#22d3ee", label="WS broadcast") >> bcast

    # Provisioning / Cluster mgmt
    api_pro >> Edge(color="#c084fc", label="provision") >> dbk_sdk
    api_clu >> Edge(color="#c084fc", label="cluster ops") >> dbk_sdk
    dbk_sdk >> Edge(color="#c084fc", label="REST calls") >> dbk_api
    dbk_api >> Edge(color="#c084fc", label="submit job") >> dbk_kaf
    api_pro >> Edge(color="#fb923c", label="catalog·schema\nvolume·secret") >> uc
    delta   >> Edge(color="#64748b", label="governed by", style="dashed") >> uc

print("Diagram generated successfully.")
