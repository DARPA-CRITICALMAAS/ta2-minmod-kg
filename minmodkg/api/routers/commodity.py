from __future__ import annotations

from functools import lru_cache
from typing import Optional

from fastapi import APIRouter
from minmodkg.api.dependencies import SPARQL_ENDPOINT, get_snapshot_id
from minmodkg.misc import sparql_query

router = APIRouter(tags=["deposit_types"])


@router.get("/commodities")
def commodities(is_critical: Optional[bool] = None):
    commodities = get_commodities(get_snapshot_id())
    if is_critical is not None:
        if is_critical:
            commodities = [comm for comm in commodities if comm["is_critical"]]
        else:
            commodities = [comm for comm in commodities if not comm["is_critical"]]
    return commodities


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
