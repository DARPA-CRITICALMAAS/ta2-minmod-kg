from __future__ import annotations

from typing import Optional

from fastapi import APIRouter
from minmodkg.services.kgrel_entity import EntityService

router = APIRouter(tags=["entities"])


@router.get("/commodities")
def commodities(is_critical: Optional[bool] = None):
    commodities = EntityService.get_instance().get_commodities()
    if is_critical is not None:
        if is_critical:
            commodities = [comm for comm in commodities if comm.is_critical]
        else:
            commodities = [comm for comm in commodities if not comm.is_critical]
    return [
        {
            "id": comm.id,
            "uri": comm.uri,
            "name": comm.name,
            "aliases": comm.aliases,
            "parent": comm.parent,
            "is_critical": comm.is_critical,
        }
        for comm in commodities
    ]


@router.get("/units")
def units():
    return [
        {"uri": u.uri, "name": u.name, "aliases": u.aliases}
        for u in EntityService.get_instance().get_units()
    ]


@router.get("/deposit-types")
def deposit_types():
    return [
        {
            "uri": deptype.uri,
            "name": deptype.name,
            "environment": deptype.environment,
            "group": deptype.group,
        }
        for deptype in EntityService.get_instance().get_deposit_types()
    ]


@router.get("/countries")
def countries():
    return [
        {"uri": r.uri, "name": r.name}
        for r in EntityService.get_instance().get_countries()
    ]


@router.get("/states-or-provinces")
def state_or_provinces():
    return [
        {"uri": r.uri, "name": r.name, "country": r.country}
        for r in EntityService.get_instance().get_state_or_provinces()
    ]


@router.get("/data-sources")
def data_sources():
    return [
        {"uri": r.id, "name": r.name, "score": r.score, "connection": r.connection}
        for r in EntityService.get_instance().get_data_sources().values()
    ]
