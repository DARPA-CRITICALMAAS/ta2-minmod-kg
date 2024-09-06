from __future__ import annotations

from functools import lru_cache

from fastapi import APIRouter
from minmodkg.api.dependencies import DEFAULT_ENDPOINT, get_snapshot_id
from minmodkg.misc import run_sparql_query
from minmodkg.transformations import make_site_uri

router = APIRouter(tags=["deposit_types"])


@router.get("/commodities")
def commodities():
    return get_commodities(get_snapshot_id())


@lru_cache(maxsize=1)
def get_commodities(snapshot_id: str, endpoint: str = DEFAULT_ENDPOINT):
    query = """
    SELECT ?uri ?name
    WHERE {
        ?uri a :Commodity ;
            rdfs:label ?name .
    }
    """
    qres = run_sparql_query(query, endpoint)
    return qres
