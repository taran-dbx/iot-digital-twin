"""User identity and settings endpoints."""

from __future__ import annotations

import json

from fastapi import APIRouter, Request, Depends
from fastapi.responses import JSONResponse

from core.auth import get_current_user
from core.state import AppState, get_app_state

router = APIRouter()


@router.get("/api/me")
async def get_me(user_email: str = Depends(get_current_user)):
    return {"email": user_email}


@router.get("/api/settings")
async def get_user_settings(
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    row = await state.db.fetchrow(
        "SELECT settings FROM user_settings WHERE user_email = $1", user_email
    )
    if row:
        return json.loads(row["settings"]) if isinstance(row["settings"], str) else row["settings"]
    return {}


@router.put("/api/settings")
async def save_user_settings(
    request: Request,
    user_email: str = Depends(get_current_user),
    state: AppState = Depends(get_app_state),
):
    if not state.db_available or not state.db:
        return JSONResponse(status_code=503, content={"error": "Database not configured"})
    try:
        body = await request.json()
    except Exception as exc:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {exc}"})

    await state.db.execute(
        """
        INSERT INTO user_settings (user_email, settings, updated_at)
        VALUES ($1, $2::jsonb, NOW())
        ON CONFLICT (user_email) DO UPDATE
        SET settings = $2::jsonb, updated_at = NOW()
        """,
        user_email,
        json.dumps(body),
    )
    return body
