"""WebSocket and Server-Sent Events endpoints."""

from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import JSONResponse, StreamingResponse

from core.auth import get_current_user
from core.state import AppState, get_app_state

logger = logging.getLogger("iot_digital_twin.routes.websocket")
router = APIRouter()


# ---------------------------------------------------------------------------
# SSE endpoint (preferred — works through Databricks App proxy)
# ---------------------------------------------------------------------------

@router.get("/api/events")
async def sse_events(request: Request, state: AppState = Depends(get_app_state)):
    """Server-Sent Events endpoint for real-time sensor data."""
    queue: asyncio.Queue = asyncio.Queue(maxsize=100)
    state.sse_clients.append(queue)
    logger.info("SSE client connected (%d total)", len(state.sse_clients))

    async def event_stream():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=30)
                    yield f"data: {json.dumps(data, default=str)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            try:
                state.sse_clients.remove(queue)
            except ValueError:
                pass
            logger.info("SSE client disconnected (%d remaining)", len(state.sse_clients))

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# WebSocket endpoint (fallback — blocked by Databricks App proxy)
# ---------------------------------------------------------------------------

@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket, state: AppState = Depends(get_app_state)):
    await ws.accept()
    state.connected_clients.append(ws)
    logger.info("WebSocket client connected (%d total)", len(state.connected_clients))
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        try:
            state.connected_clients.remove(ws)
        except ValueError:
            pass
        logger.info("WebSocket client disconnected (%d remaining)", len(state.connected_clients))


# ---------------------------------------------------------------------------
# SSS threshold-alert ingest endpoint
# ---------------------------------------------------------------------------

@router.post("/api/sss/alerts")
async def sss_alerts_ingest(request: Request, user_email: str = Depends(get_current_user)):
    """Accept threshold alerts from the SSS notebook and fan out to all SSE clients."""
    state: AppState = request.app.state.app_state
    body = await request.json()
    alerts = body.get("alerts", [])

    if not alerts:
        return JSONResponse(content={"status": "ok", "count": 0})

    dispatched = 0
    for alert in alerts:
        event_data = {
            "type": "threshold_alert",
            "pipeline_id": alert.get("pipeline_id"),
            "sensor_id": alert.get("sensor_id"),
            "sensor_type": alert.get("sensor_type"),
            "value": alert.get("value"),
            "threshold_min": alert.get("threshold_min"),
            "threshold_max": alert.get("threshold_max"),
            "timestamp": alert.get("timestamp"),
        }
        for client_queue in state.sse_clients:
            try:
                client_queue.put_nowait(event_data)
                dispatched += 1
            except Exception:
                pass

    logger.info(
        "sss_alerts_ingest: %d alerts received, %d events dispatched to %d clients",
        len(alerts),
        dispatched,
        len(state.sse_clients),
    )
    return JSONResponse(content={"status": "ok", "count": len(alerts)})
