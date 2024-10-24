from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from minmodkg.api.internal import admin
from minmodkg.api.models.db import create_db_and_tables
from minmodkg.api.routers import (
    commodity,
    dedup_mineral_site,
    deposit_type,
    lod,
    login,
    mineral_site,
    stats,
    unit,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield


API_ENDPOINT = os.environ.get("MINMOD_API_ENDPOINT", "/api/v1")

app = FastAPI(
    openapi_url="/api/v1/openapi.json",
    docs_url=f"{API_ENDPOINT}/docs",
    lifespan=lifespan,
)

app.include_router(commodity.router, prefix=API_ENDPOINT)
app.include_router(dedup_mineral_site.router, prefix=API_ENDPOINT)
app.include_router(deposit_type.router, prefix=API_ENDPOINT)
app.include_router(lod.router)
app.include_router(login.router, prefix=API_ENDPOINT)
app.include_router(mineral_site.router, prefix=API_ENDPOINT)
app.include_router(stats.router, prefix=API_ENDPOINT)
app.include_router(unit.router, prefix=API_ENDPOINT)
app.include_router(admin.router, prefix=f"{API_ENDPOINT}/admin")
