from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from minmodkg.api.internal import admin
from minmodkg.api.models.db import create_db_and_tables
from minmodkg.api.routers import (
    commodity,
    dedup_mineral_site,
    deposit_type,
    lod,
    mineral_site,
    stats,
    unit,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield


app = FastAPI(
    openapi_url="/api/v1/openapi.json", docs_url="/api/v1/docs", lifespan=lifespan
)

app.include_router(commodity.router, prefix="/api/v1")
app.include_router(dedup_mineral_site.router, prefix="/api/v1")
app.include_router(deposit_type.router, prefix="/api/v1")
app.include_router(lod.router)
app.include_router(mineral_site.router, prefix="/api/v1")
app.include_router(stats.router, prefix="/api/v1")
app.include_router(unit.router, prefix="/api/v1")
app.include_router(admin.router, prefix="/api/v1/admin")
