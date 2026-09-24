"""Static file serving for the React SPA."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import FileResponse, RedirectResponse

router = APIRouter()

SPA_INDEX = "static/dist/index.html"


@router.get("/legacy")
async def legacy_dashboard():
    return RedirectResponse("/industries")


@router.get("/legacy-settings")
async def legacy_settings_page():
    return RedirectResponse("/setup-validate")


@router.get("/")
async def root():
    return FileResponse(SPA_INDEX)
