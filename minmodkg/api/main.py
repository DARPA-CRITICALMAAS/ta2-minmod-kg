from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from minmodkg.api.internal import admin
from minmodkg.api.routers import (
    dedup_mineral_site,
    lod,
    login,
    mineral_site,
    predefined_entities,
    stats,
)
from minmodkg.config import API_PREFIX, LOD_PREFIX
from minmodkg.models_v2.kgrel.base import create_db_and_tables


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield


app = FastAPI(
    openapi_url=f"{API_PREFIX}/openapi.json",
    docs_url=f"{API_PREFIX}/docs",
    lifespan=lifespan,
)

app.include_router(predefined_entities.router, prefix=API_PREFIX)
app.include_router(stats.router, prefix=API_PREFIX)
app.include_router(dedup_mineral_site.router, prefix=API_PREFIX)
app.include_router(dedup_mineral_site.router, prefix=API_PREFIX)
app.include_router(lod.router, prefix=LOD_PREFIX)
app.include_router(login.router, prefix=API_PREFIX)
app.include_router(mineral_site.router, prefix=API_PREFIX)
app.include_router(admin.router, prefix=f"{API_PREFIX}/admin")
