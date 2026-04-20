"""
IoT Digital Twin — Databricks Architecture Diagram  (v5 final)
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
import warnings, math
warnings.filterwarnings("ignore")

# ── Canvas ────────────────────────────────────────────────────────────────────
FW, FH = 52, 30
fig, ax = plt.subplots(figsize=(FW, FH))
ax.set_xlim(0, FW); ax.set_ylim(0, FH); ax.axis("off")
fig.patch.set_facecolor("#0f172a")
BG = "#0f172a"

# ── Palette ───────────────────────────────────────────────────────────────────
FE_BG, FE_ND, FE_BR, FE_TX = "#172554", "#1d4ed8", "#60a5fa", "#bfdbfe"
BE_BG, BE_ND, BE_BR, BE_TX = "#052e16", "#166534", "#4ade80", "#bbf7d0"
AU_ND, AU_BR, AU_TX         = "#78350f", "#fcd34d", "#fef3c7"
SK_BG, SK_ND, SK_BR, SK_TX  = "#082f49", "#0369a1", "#38bdf8", "#e0f2fe"
ST_BG, ST_ND, ST_BR, ST_TX  = "#431407", "#9a3412", "#fb923c", "#ffedd5"
EX_BG, EX_ND, EX_BR, EX_TX = "#2e1065", "#6b21a8", "#c084fc", "#ede9fe"
TX_H, TX_D = "#f1f5f9", "#94a3b8"

A_BLUE, A_GREEN, A_CYAN     = "#60a5fa", "#4ade80", "#22d3ee"
A_ORANGE, A_PURPLE, A_AMBER = "#fb923c", "#c084fc", "#fbbf24"
A_GRAY                      = "#64748b"


# ── Drawing helpers ───────────────────────────────────────────────────────────

def grp(x, y, w, h, bg, br, title, fs=9.5, lw=2.2, r=0.35):
    ax.add_patch(FancyBboxPatch((x,y),w,h,
        boxstyle=f"round,pad=0,rounding_size={r}",
        lw=lw, edgecolor=br, facecolor=bg, zorder=2))
    ax.text(x+w/2, y+h-0.28, title, ha="center", va="top",
            fontsize=fs, fontweight="bold", color=TX_H, zorder=6,
            multialignment="center", linespacing=1.4)


def sgrp(x, y, w, h, bg, br, title, fs=8, r=0.2):
    ax.add_patch(FancyBboxPatch((x,y),w,h,
        boxstyle=f"round,pad=0,rounding_size={r}",
        lw=1.5, edgecolor=br, facecolor=bg, zorder=3))
    ax.text(x+w/2, y+h-0.2, title, ha="center", va="top",
            fontsize=fs, fontweight="bold", color=TX_H, zorder=7,
            multialignment="center", linespacing=1.35)


def nd(x, y, w, h, label, fill, br, txc=TX_H, fs=8):
    ax.add_patch(FancyBboxPatch((x,y),w,h,
        boxstyle="round,pad=0,rounding_size=0.13",
        lw=1.4, edgecolor=br, facecolor=fill, zorder=8))
    ax.text(x+w/2, y+h/2, label, ha="center", va="center",
            fontsize=fs, color=txc, multialignment="center",
            linespacing=1.4, zorder=9)
    return (x+w/2, y+h/2)


def arr(x0,y0,x1,y1, col, lbl="", lw=1.7, dash=False, rad=0.0, zl=4):
    ls = "--" if dash else "-"
    ap = dict(arrowstyle="->", color=col, lw=lw,
              connectionstyle=f"arc3,rad={rad}", linestyle=ls)
    ax.annotate("", xy=(x1,y1), xytext=(x0,y0), arrowprops=ap, zorder=zl)
    if lbl:
        mx,my = x0*0.45+x1*0.55, y0*0.45+y1*0.55
        ax.text(mx, my, lbl, ha="center", va="center", fontsize=6.2,
                color=col, zorder=zl+2,
                bbox=dict(boxstyle="round,pad=0.13",fc=BG,ec="none",alpha=0.92))


def bi(x0,y0,x1,y1, col, lbl="", lw=1.7, off=0.09):
    dx,dy = x1-x0, y1-y0
    L = math.hypot(dx,dy)
    if L < 1e-6: return
    nx,ny = -dy/L*off, dx/L*off
    arr(x0+nx,y0+ny, x1+nx,y1+ny, col, lw=lw)
    arr(x1-nx,y1-ny, x0-nx,y0-ny, col, lw=lw)
    if lbl:
        mx,my = (x0+x1)/2, (y0+y1)/2
        ax.text(mx,my,lbl, ha="center",va="center", fontsize=6.2,
                color=col, zorder=10,
                bbox=dict(boxstyle="round,pad=0.13",fc=BG,ec="none",alpha=0.92))


# ═══════════════════════════════════════════════════════════════════════════════
# COLUMN GEOMETRY
#   User | Frontend | Backend | Sinks | Storage | External
# ═══════════════════════════════════════════════════════════════════════════════
BOT = 2.8          # bottom y
TOP = 28.4         # top y
H   = TOP - BOT    # common height 25.6

# Column left-edge X positions
X_USR = 0.2
X_FE  = 2.1
X_BE  = 9.4
X_SK  = 22.2
X_ST  = 27.4
X_EX  = 37.2

# Column widths
W_FE  = 6.8
W_BE  = 12.2
W_SK  = 4.6
W_ST  = 9.1
W_EX  = 14.4

# ─────────────────────────────────────────────────────────────────────────────
# TITLE
# ─────────────────────────────────────────────────────────────────────────────
ax.text(FW/2, FH-0.45,
        "IoT Digital Twin  —  Databricks Application Architecture",
        ha="center", va="top", fontsize=18, fontweight="bold",
        color=TX_H, zorder=20)

C = {}   # node centres keyed by name

# ═══════════════════════════════════════════════════════════════════════════════
# FRONTEND
# ═══════════════════════════════════════════════════════════════════════════════
fx, fw = X_FE, W_FE
grp(fx, BOT, fw, H, FE_BG, FE_BR,
    "FRONTEND\nReact + TypeScript\n(Vite · Tailwind · Radix UI)", fs=9)
NW = fw - 0.55

# Pages sub-group  (top 38%)
pgy = BOT + 15.6
pgh = 9.4
sgrp(fx+0.15, pgy, fw-0.3, pgh, "#1e3a8a", FE_BR, "Pages", fs=8)
pages = ["Home","Dashboard","Pipeline\nWizard","Pipeline\nDetail","Data Hub","Source\nSettings"]
for i,lbl in enumerate(pages):
    r,c = divmod(i,3)
    px = fx + 0.28 + c*2.2
    py = pgy + pgh - 0.55 - 2.4 - r*3.0
    nd(px, py, 2.1, 2.6, lbl, FE_ND, FE_BR, FE_TX, fs=8)
C["pages"] = (fx+fw/2, pgy+pgh/2)

# Contexts sub-group
cgy = BOT + 11.0
cgh = 4.2
sgrp(fx+0.15, cgy, fw-0.3, cgh, "#1e3a8a", FE_BR, "Contexts", fs=8)
C["ctx"] = nd(fx+0.28, cgy+0.55, NW, 3.3,
              "UserProvider\nPipelineProvider\nSensorProvider",
              FE_ND, FE_BR, FE_TX, fs=8.5)

# Hooks & Charts sub-group
hgy = BOT + 0.5
hgh = 10.2
sgrp(fx+0.15, hgy, fw-0.3, hgh, "#1e3a8a", FE_BR, "Hooks & Charts", fs=8)
C["ws_hook"] = nd(fx+0.28, hgy+5.3, NW, 4.5,
                  "useWebSocket\n(auto-reconnecting)", FE_ND, FE_BR, FE_TX, fs=9)
C["charts"]  = nd(fx+0.28, hgy+0.55, NW, 4.4,
                  "Recharts\nreal-time visualization", FE_ND, FE_BR, FE_TX, fs=9)

# ── USER ──────────────────────────────────────────────────────────────────────
uy = BOT + H/2
ax.add_patch(FancyBboxPatch((X_USR, uy-1.0), 1.65, 2.0,
    boxstyle="round,pad=0,rounding_size=0.22", lw=2,
    edgecolor="#94a3b8", facecolor="#1e293b", zorder=8))
ax.text(X_USR+0.825, uy+0.45, "USER",
        ha="center", va="center", fontsize=9.5, fontweight="bold",
        color=TX_H, zorder=9)
ax.text(X_USR+0.825, uy-0.35, "Browser",
        ha="center", va="center", fontsize=8, color=TX_D, zorder=9)
C["user"] = (X_USR+0.825, uy)


# ═══════════════════════════════════════════════════════════════════════════════
# BACKEND
# ═══════════════════════════════════════════════════════════════════════════════
bx, bw = X_BE, W_BE
grp(bx, BOT, bw, H, BE_BG, BE_BR,
    "BACKEND  —  FastAPI on Databricks Apps  (uvicorn · async)", fs=9.5)
BNW = bw - 0.55

# Auth Middleware
C["auth"] = nd(bx+0.28, BOT+22.2, BNW, 2.6,
               "Auth Middleware\nx-forwarded-access-token  ·  OAuth via Databricks",
               AU_ND, AU_BR, AU_TX, fs=8.5)

# REST API sub-group
rgy = BOT + 12.0
rgh = 9.8
sgrp(bx+0.15, rgy, bw-0.3, rgh, "#0a2e15", BE_BR, "REST API Endpoints", fs=8.5)
rest_items = [
    ("/api/pipelines  —  Pipeline CRUD",  "api_pip"),
    ("/api/sensor  —  Sensor Queries",    "api_sen"),
    ("/api/provision  —  Provisioning",   "api_pro"),
    ("/api/clusters  —  Cluster Mgmt",    "api_clu"),
    ("/api/sss/ingest  —  SSS Ingest",    "api_sss"),
]
for i,(lbl,key) in enumerate(rest_items):
    ny = rgy + 0.55 + i*1.72
    C[key] = nd(bx+0.28, ny, BNW, 1.45, lbl, BE_ND, BE_BR, BE_TX, fs=8)

# Real-time Layer sub-group
rtgy = BOT + 6.8
rtgh = 4.9
sgrp(bx+0.15, rtgy, bw-0.3, rtgh, "#0a2e15", BE_BR, "Real-time Layer", fs=8.5)
C["ws_ep"] = nd(bx+0.28, rtgy+2.3, BNW, 2.3, "/ws  —  WebSocket Endpoint",
                BE_ND, BE_BR, BE_TX, fs=9)
C["bcast"] = nd(bx+0.28, rtgy+0.55, BNW, 1.45,
                "Broadcast Manager  (connected clients)", BE_ND, BE_BR, BE_TX, fs=8.5)

# IoT Simulator sub-group
iogt = BOT + 0.5
iogh = 6.0
sgrp(bx+0.15, iogt, bw-0.3, iogh, "#0a2e15", BE_BR,
     "IoT Simulator  (asyncio background task)", fs=8.5)
C["sim"]     = nd(bx+0.28, iogt+3.0, BNW, 2.7,
                  "Sensor Event Generator", BE_ND, BE_BR, BE_TX, fs=9.5)
C["metrics"] = nd(bx+0.28, iogt+0.55, BNW, 2.2,
                  "Source Metrics\nwebsocket · lakebase · zerobus · sss",
                  BE_ND, BE_BR, BE_TX, fs=8)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA SINKS
# ═══════════════════════════════════════════════════════════════════════════════
skx, skw = X_SK, W_SK
grp(skx, BOT, skw, H, SK_BG, SK_BR,
    "DATA SINKS\n(3 Parallel\nOutputs)", fs=8.5)
SKNW = skw - 0.5
sinks = [
    ("WebSocket Sink\n2 Hz broadcast",           "sk_ws"),
    ("Lakebase Sink\nasyncpg · batch 50 · 1 Hz", "sk_lb"),
    ("Zerobus Sink\ngRPC stream · batch 10",       "sk_zb"),
]
for i,(lbl,key) in enumerate(sinks):
    sy = BOT + 1.0 + i*8.1
    C[key] = nd(skx+0.25, sy, SKNW, 7.1, lbl, SK_ND, SK_BR, SK_TX, fs=9)


# ═══════════════════════════════════════════════════════════════════════════════
# STORAGE
# ═══════════════════════════════════════════════════════════════════════════════
stx, stw = X_ST, W_ST
grp(stx, BOT, stw, H, ST_BG, ST_BR, "DATA STORAGE", fs=10.5)
STNW = stw - 0.6

# Lakebase sub-group (top half)
lbgy = BOT + 13.2
lbgh = 11.3
sgrp(stx+0.2, lbgy, stw-0.4, lbgh, "#5c1a07", ST_BR, "Lakebase — PostgreSQL", fs=8.5)
pg_items = [
    ("pipelines\nJSONB config · per-user",  "pg_pip"),
    ("sensor_readings\ntime-series",         "pg_sen"),
    ("sensor_1m\n1-min materialized view",   "pg_mv1"),
    ("sensor_5m\n5-min materialized view",   "pg_mv2"),
]
for i,(lbl,key) in enumerate(pg_items):
    py = lbgy + 0.55 + i*2.6
    C[key] = nd(stx+0.4, py, STNW, 2.2, lbl, ST_ND, ST_BR, ST_TX, fs=8.5)

# Delta + Unity Catalog sub-group (bottom half)
dcgy = BOT + 0.5
dcgh = 12.4
sgrp(stx+0.2, dcgy, stw-0.4, dcgh, "#5c1a07", ST_BR, "Delta + Unity Catalog", fs=8.5)
C["delta"] = nd(stx+0.4, dcgy+6.5, STNW, 5.7,
                "Delta Table\ntaran_explore.iot_simulator\n.iot_sensor_data",
                ST_ND, ST_BR, ST_TX, fs=9)
C["uc"]    = nd(stx+0.4, dcgy+0.65, STNW, 5.5,
                "Unity Catalog\ncatalogs · schemas\nvolumes · secrets",
                ST_ND, ST_BR, ST_TX, fs=9)


# ═══════════════════════════════════════════════════════════════════════════════
# EXTERNAL
# ═══════════════════════════════════════════════════════════════════════════════
exx, exw = X_EX, W_EX
grp(exx, BOT, exw, H, EX_BG, EX_BR,
    "EXTERNAL INTEGRATIONS  —  Databricks Platform", fs=9.5)

half = (exw - 0.9) / 2

# Left sub-group: Spark Structured Streaming
sgrp(exx+0.2, BOT+0.5, half+0.1, H-1.8, "#3b0764", EX_BR,
     "Spark Structured Streaming", fs=8.5)
sss_items = [
    ("Sources\nKafka · Kinesis\nEventHub",        "sss_src"),
    ("SSS Notebook Job\nserverless Spark",         "sss_job"),
    ("ForeachWriter\n→ Lakebase\nsensor_readings", "sss_fw"),
]
for i,(lbl,key) in enumerate(sss_items):
    sy = BOT + 1.1 + i*8.1
    C[key] = nd(exx+0.35, sy, half-0.15, 7.1, lbl, EX_ND, EX_BR, EX_TX, fs=8.5)

# Right sub-group: Databricks Workspace
dbx = exx + 0.5 + half + 0.2
sgrp(dbx, BOT+0.5, half+0.3, H-1.8, "#3b0764", EX_BR, "Databricks Workspace", fs=8.5)
dbk_items = [
    ("Databricks SDK\nOAuth · cluster mgmt",         "dbk_sdk"),
    ("Workspace API\nclusters · notebooks · jobs",   "dbk_api"),
    ("Kafka Producer\nNotebook Job\nserverless Spark","dbk_kaf"),
]
for i,(lbl,key) in enumerate(dbk_items):
    dy = BOT + 1.1 + i*8.1
    C[key] = nd(dbx+0.15, dy, half+0.0, 7.1, lbl, EX_ND, EX_BR, EX_TX, fs=8.5)


# ═══════════════════════════════════════════════════════════════════════════════
# LEGEND
# ═══════════════════════════════════════════════════════════════════════════════
legend = [
    (FE_ND, FE_BR, "Frontend  (React + TypeScript)"),
    (BE_ND, BE_BR, "Backend  (FastAPI / Databricks Apps)"),
    (AU_ND, AU_BR, "Auth Middleware  (OAuth)"),
    (SK_ND, SK_BR, "Data Sinks"),
    (ST_ND, ST_BR, "Data Storage"),
    (EX_ND, EX_BR, "External / Databricks Platform"),
]
for i,(fc,ec,lbl) in enumerate(legend):
    rx = 0.5 + i * 8.7
    ax.add_patch(FancyBboxPatch((rx, 0.9), 0.6, 0.55,
        boxstyle="round,pad=0,rounding_size=0.08",
        lw=1.2, edgecolor=ec, facecolor=fc, zorder=12))
    ax.text(rx+0.76, 1.18, lbl, va="center", fontsize=8, color=TX_D, zorder=13)


# ═══════════════════════════════════════════════════════════════════════════════
# ARROWS
# ═══════════════════════════════════════════════════════════════════════════════
def cx(k): return C[k][0]
def cy(k): return C[k][1]

fe_r = fx + fw      # right edge of FE
be_l = bx           # left edge of BE
be_r = bx + bw      # right edge of BE
sk_r = skx + skw    # right edge of Sinks
st_l = stx          # left edge of Storage
st_r = stx + stw    # right edge of Storage
ex_l = exx          # left edge of External

# ── 1. User ↔ Frontend (HTTPS) ────────────────────────────────────────────────
bi(C["user"][0]+0.83, C["user"][1],
   fx, BOT + H*0.62,
   A_BLUE, "HTTPS", lw=2.0)

# ── 2. Frontend ↔ Backend REST ────────────────────────────────────────────────
bi(fe_r, BOT+21.5,  be_l, cy("api_pip"), A_GREEN, "/api/pipelines  CRUD")
bi(fe_r, BOT+19.8,  be_l, cy("api_sen"), A_GREEN, "/api/sensor")
arr(fe_r, BOT+18.1, be_l, cy("api_pro"), A_GREEN, "/api/provision")
arr(fe_r, BOT+16.5, be_l, cy("api_clu"), A_GREEN, "/api/clusters")

# ctx → auth (dashed amber)
arr(fe_r, cy("ctx"), be_l+BNW*0.5, cy("auth"), A_AMBER, "OAuth token", dash=True)
arr(cx("auth"), cy("auth")-0.7, cx("api_pip"), cy("api_pip")+0.5, A_AMBER, "validates", dash=True)
arr(cx("auth"), cy("auth")-0.7, cx("ws_ep"),   cy("ws_ep")+0.5,   A_AMBER, "validates", dash=True)

# ── 3. Frontend ↔ WebSocket ───────────────────────────────────────────────────
bi(fe_r, cy("ws_hook"), be_l, cy("ws_ep"), A_CYAN, "WebSocket frames")
# bcast → ws_hook (real-time back to browser)
arr(be_l, cy("bcast")+0.3, fe_r, cy("ws_hook")+0.4,
    A_CYAN, "real-time sensor frames", rad=-0.12)
# ws_ep → bcast (internal)
arr(cx("ws_ep"), cy("ws_ep")-0.7, cx("bcast"), cy("bcast")+0.55, A_CYAN)

# ── 4. IoT Simulator → 3 Sinks ────────────────────────────────────────────────
sim_r = cx("sim") + BNW/2
arr(sim_r, cy("sim")+0.6, skx, cy("sk_ws"), A_CYAN, "sensor events")
arr(sim_r, cy("sim"),     skx, cy("sk_lb"), A_CYAN, "sensor events")
arr(sim_r, cy("sim")-0.6, skx, cy("sk_zb"), A_CYAN, "sensor events")
# sim → metrics (internal)
arr(cx("sim"), cy("sim")-0.8, cx("metrics"), cy("metrics")+0.65, A_GRAY)

# ── 5. Sinks → Storage / Broadcast ────────────────────────────────────────────
# sk_ws → bcast (back-left, curved)
arr(sk_r, cy("sk_ws"), cx("bcast")+BNW/2, cy("bcast"),
    A_CYAN, "broadcast 2 Hz", rad=0.35, lw=1.8)
# sk_lb → pg_sen
arr(sk_r, cy("sk_lb"), st_l, cy("pg_sen"), A_ORANGE, "batch write")
# sk_zb → delta
arr(sk_r, cy("sk_zb"), st_l, cy("delta"),  A_ORANGE, "gRPC stream")

# ── 6. API → Storage ──────────────────────────────────────────────────────────
api_r = cx("api_pip") + BNW/2
bi(api_r, cy("api_pip"), st_l, cy("pg_pip"), A_ORANGE, "CRUD")
arr(api_r, cy("api_sen"),       st_l, cy("pg_sen"), A_ORANGE, "SELECT")
arr(api_r, cy("api_sen")-0.25,  st_l, cy("pg_mv1"), A_ORANGE)
arr(api_r, cy("api_sen")-0.50,  st_l, cy("pg_mv2"), A_ORANGE)
arr(api_r, cy("api_pro")+0.3,   st_l, cy("uc"), A_ORANGE, "catalog·schema\nvolume·secret")

# ── 7. SSS pipeline ───────────────────────────────────────────────────────────
arr(cx("sss_src"), cy("sss_src")+0.7, cx("sss_job"), cy("sss_job")-0.7,
    A_PURPLE, "read stream")
arr(cx("sss_job"), cy("sss_job")+0.7, cx("sss_fw"),  cy("sss_fw")-0.7, A_PURPLE)

# sss_fw → pg_sen (ForeachWriter, crosses Storage right edge)
arr(ex_l, cy("sss_fw"), st_r, cy("pg_sen")-0.3, A_ORANGE,
    "ForeachWriter\n(raw rows → Lakebase)", rad=-0.1)

# sss_job → api_sss (HTTP POST back to FastAPI)
arr(ex_l, cy("sss_job"), cx("api_sss")+BNW/2, cy("api_sss"),
    A_PURPLE, "HTTP POST\nwindowed aggregates", rad=-0.1)

# api_sss → bcast (WS broadcast)
arr(cx("api_sss"), cy("api_sss")+0.5, cx("bcast"), cy("bcast")+0.5,
    A_CYAN, "WS broadcast")

# ── 8. Provisioning → Databricks SDK ─────────────────────────────────────────
arr(api_r, cy("api_pro"),      ex_l, cy("dbk_sdk"),      A_PURPLE, "provision")
arr(api_r, cy("api_clu")+0.3,  ex_l, cy("dbk_sdk")-0.4,  A_PURPLE, "cluster ops")
arr(cx("dbk_sdk"), cy("dbk_sdk")+0.7, cx("dbk_api"), cy("dbk_api")-0.7,
    A_PURPLE, "REST calls")
arr(cx("dbk_api"), cy("dbk_api")+0.7, cx("dbk_kaf"), cy("dbk_kaf")-0.7,
    A_PURPLE, "submit job")

# ── 9. Delta governed by UC ───────────────────────────────────────────────────
arr(cx("delta"), cy("delta")-0.7, cx("uc"), cy("uc")+0.7, A_GRAY, "governed by", dash=True)


# ─────────────────────────────────────────────────────────────────────────────
# Save
# ─────────────────────────────────────────────────────────────────────────────
OUT = "/Users/taranpreetsingh.grover/Documents/Claude Projects/Apps/iot-digital-twin/docs/architecture-diagram.png"
plt.savefig(OUT, dpi=160, bbox_inches="tight", facecolor=BG, edgecolor="none")
plt.close()
print(f"Saved: {OUT}")
