from __future__ import annotations

from functools import lru_cache
from typing import Optional

from fastapi import APIRouter
from minmodkg.api.dependencies import get_snapshot_id
from minmodkg.models.base import MINMOD_KG, MINMOD_NS
from minmodkg.models.crs import CRS
from minmodkg.models.material_form import MaterialForm
from minmodkg.models.source import Source

router = APIRouter(tags=["predefined-entities"])


@router.get("/commodities")
def commodities(is_critical: Optional[bool] = None):
    commodities = get_commodities(get_snapshot_id())
    if is_critical is not None:
        if is_critical:
            commodities = [comm for comm in commodities if comm["is_critical"]]
        else:
            commodities = [comm for comm in commodities if not comm["is_critical"]]
    return commodities


@router.get("/units")
def units():
    return get_units(get_snapshot_id())


@router.get("/deposit-types")
def deposit_types():
    return get_deposit_types(get_snapshot_id())


@router.get("/countries")
def countries():
    return get_countries(get_snapshot_id())


@router.get("/states-or-provinces")
def state_or_provinces():
    return get_state_or_provinces(get_snapshot_id())


@router.get("/sources")
def sources():
    return get_sources(get_snapshot_id())


@lru_cache(maxsize=1)
def get_commodities(snapshot_id: str):
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """
    SELECT ?uri ?name ?is_critical
    WHERE {
        ?uri a mo:Commodity ;
            rdfs:label ?name ;
            mo:is_critical_commodity ?is_critical
    }
    """
    qres = MINMOD_KG.query(query)
    return qres


@lru_cache(maxsize=1)
def get_units(snapshot_id: str):
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """
    SELECT ?uri ?name
    WHERE {
        ?uri a mo:Unit ;
            rdfs:label ?name .
    }
    """
    qres = MINMOD_KG.query(query)
    return qres


@lru_cache(maxsize=1)
def get_deposit_types(snapshot_id: str):
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """
    SELECT ?uri ?name ?environment ?group
    WHERE {
        ?uri a mo:DepositType ;
            rdfs:label ?name ;
            mo:environment ?environment ;
            mo:group ?group .
    }
    """
    qres = MINMOD_KG.query(query)
    return qres


@lru_cache(maxsize=1)
def get_countries(snapshot_id: str):
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """
    SELECT ?uri ?name
    WHERE {
        ?uri a mo:Country ;
            rdfs:label ?name .
    }
    """
    qres = MINMOD_KG.query(query)
    return qres


@lru_cache(maxsize=1)
def get_state_or_provinces(snapshot_id: str):
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """
    SELECT ?uri ?name
    WHERE {
        ?uri a mo:StateOrProvince ;
            rdfs:label ?name .
    }
    """
    qres = MINMOD_KG.query(query)
    return qres


@lru_cache(maxsize=1)
def get_material_forms(snapshot_id: str) -> list[MaterialForm]:
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """
    SELECT *
    WHERE {
        ?uri a mo:MaterialForm ;
            rdfs:label ?name ;
            mo:conversion ?conversion ;
            mo:formula ?formula ;
            mo:commodity ?commodity .
    }
    """
    qres = MINMOD_KG.query(query)
    return [
        MaterialForm(
            uri=x["uri"],
            name=x["name"],
            formula=x["formula"],
            commodity=x["commodity"],
            conversion=x["conversion"],
        )
        for x in qres
    ]


@lru_cache(maxsize=1)
def get_crs(snapshot_id: str) -> list[CRS]:
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """
    SELECT ?uri ?name
    WHERE {
        ?uri a mo:CoordinateReferenceSystem ;
            rdfs:label ?name .
    }
    """
    qres = MINMOD_KG.query(query)
    return [CRS(uri=x["uri"], name=x["name"]) for x in qres]


@lru_cache(maxsize=1)
def get_sources(snapshot_id: str) -> list[Source]:
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """
    SELECT ?uri ?id ?name ?score ?connection
    WHERE {
        ?uri rdf:type mo:Source ;
            rdfs:label ?name ;
            mo:id ?id ;
            mo:score ?score .
        
        OPTIONAL { ?uri mo:connection ?connection . }
    }
    """
    qres = MINMOD_KG.query(query)
    return [
        Source(
            uri=x["uri"],
            name=x["name"],
            id=x["id"],
            score=x["score"],
            connection=x["connection"],
        )
        for x in qres
    ]
