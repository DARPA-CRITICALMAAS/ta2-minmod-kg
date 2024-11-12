from __future__ import annotations

from functools import lru_cache
from typing import Optional

from fastapi import APIRouter
from minmodkg.api.dependencies import SPARQL_ENDPOINT, get_snapshot_id
from minmodkg.misc import sparql_query
from minmodkg.models.crs import CRS
from minmodkg.models.material_form import MaterialForm

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


@lru_cache(maxsize=1)
def get_commodities(snapshot_id: str, endpoint: str = SPARQL_ENDPOINT):
    query = """
    SELECT ?uri ?name ?is_critical
    WHERE {
        ?uri a :Commodity ;
            rdfs:label ?name ;
            :is_critical_commodity ?is_critical
    }
    """
    qres = sparql_query(query, endpoint)
    return qres


@lru_cache(maxsize=1)
def get_units(snapshot_id: str, endpoint: str = SPARQL_ENDPOINT):
    query = """
    SELECT ?uri ?name
    WHERE {
        ?uri a :Unit ;
            rdfs:label ?name .
    }
    """
    qres = sparql_query(query, endpoint)
    return qres


@lru_cache(maxsize=1)
def get_deposit_types(snapshot_id: str, endpoint: str = SPARQL_ENDPOINT):
    query = """
    SELECT ?uri ?name ?environment ?group
    WHERE {
        ?uri a :DepositType ;
            rdfs:label ?name ;
            :environment ?environment ;
            :group ?group .
    }
    """
    qres = sparql_query(query, endpoint)
    return qres


@lru_cache(maxsize=1)
def get_material_forms(
    snapshot_id: str, endpoint: str = SPARQL_ENDPOINT
) -> dict[str, MaterialForm]:
    query = """
    SELECT *
    WHERE {
        ?uri a :MaterialForm ;
            rdfs:label ?name ;
            :conversion ?conversion ;
            :formula ?formula ;
            :commodity ?commodity .
    }
    """
    qres = sparql_query(query, endpoint)
    return {
        x["uri"]: MaterialForm(
            uri=x["uri"],
            name=x["name"],
            formula=x["formula"],
            commodity=x["commodity"],
            conversion=x["conversion"],
        )
        for x in qres
    }


@lru_cache(maxsize=1)
def get_crs(snapshot_id: str, endpoint: str = SPARQL_ENDPOINT) -> dict[str, CRS]:
    query = """
    SELECT ?uri ?name
    WHERE {
        ?uri a :CoordinateReferenceSystem ;
            rdfs:label ?name .
    }
    """
    qres = sparql_query(query, endpoint)
    return {x["uri"]: CRS(uri=x["uri"], name=x["name"]) for x in qres}
