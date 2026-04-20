"""Static file serving — React SPA + legacy dashboard."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter()

SPA_INDEX = "static/dist/index.html"


@router.get("/legacy")
async def legacy_dashboard():
    return FileResponse("static/index.html")


@router.get("/legacy-settings")
async def legacy_settings_page():
    return FileResponse("static/settings.html")


@router.get("/")
async def root():
    return FileResponse(SPA_INDEX)
