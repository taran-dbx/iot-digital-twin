"""Static file serving for the React SPA."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter()

SPA_INDEX = "static/dist/index.html"


@router.get("/")
async def root():
    return FileResponse(SPA_INDEX)
