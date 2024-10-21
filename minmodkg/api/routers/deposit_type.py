from __future__ import annotations

from functools import lru_cache

from fastapi import APIRouter
from minmodkg.api.dependencies import SPARQL_ENDPOINT, get_snapshot_id
from minmodkg.misc import sparql_query
from minmodkg.transformations import make_site_uri

router = APIRouter(tags=["deposit_types"])


@router.get("/deposit_types")
def deposit_types():
    return get_deposit_types(get_snapshot_id())


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
