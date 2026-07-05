"""API route modules — one file per domain."""

from __future__ import annotations

from fastapi import APIRouter

from routes.health import router as health_router
from routes.websocket import router as websocket_router
from routes.sources import router as sources_router
from routes.sensor_data import router as sensor_data_router
from routes.simulator import router as simulator_router
from routes.producers import router as producers_router
from routes.provisioning import router as provisioning_router
from routes.cluster import router as cluster_router
from routes.deployment import router as deployment_router
from routes.jobs import router as jobs_router
from routes.industries import router as industries_router
from routes.user import router as user_router
from routes.settings import router as settings_router
from routes.warehouses import router as warehouses_router
from routes.pipelines import router as pipelines_router
from routes.lakets import router as lakets_router
from routes.pipeline_deploy import router as pipeline_deploy_router


def create_api_router() -> APIRouter:
    """Combine all sub-routers into a single top-level router."""
    api = APIRouter()
    api.include_router(health_router)
    api.include_router(websocket_router)
    api.include_router(sources_router)
    api.include_router(sensor_data_router)
    api.include_router(simulator_router)
    api.include_router(producers_router)
    api.include_router(provisioning_router)
    api.include_router(cluster_router)
    api.include_router(deployment_router)
    api.include_router(jobs_router)
    api.include_router(industries_router)
    api.include_router(user_router)
    api.include_router(settings_router)
    api.include_router(warehouses_router)
    api.include_router(pipelines_router)
    api.include_router(lakets_router)
    api.include_router(pipeline_deploy_router)
    return api
