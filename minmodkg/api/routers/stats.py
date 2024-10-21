from __future__ import annotations

from functools import lru_cache

from fastapi import APIRouter
from minmodkg.api.dependencies import SPARQL_ENDPOINT, get_snapshot_id
from minmodkg.misc import sparql_query

router = APIRouter(tags=["statistics"])


@router.get("/documents/count")
def document_count():
    return get_document_count(get_snapshot_id())


@router.get("/mineral-inventories/count")
def inventory_count():
    return get_inventory_count(get_snapshot_id())


@router.get("/mineral-sites/count")
def mineralsites_count():
    return get_mineralsites_count(get_snapshot_id())


@router.get("/mineral-inventories/count-by-commodity")
def inventory_by_commodity():
    return get_inventory_by_commodity(get_snapshot_id())


@router.get("/mineral-sites/count-by-commodity")
def mineralsites_by_commodity():
    return get_mineralsites_by_commodity(get_snapshot_id())


@router.get("/documents/count-by-commodity")
def documents_by_commodity():
    return get_documents_by_commodity(get_snapshot_id())


@lru_cache(maxsize=1)
def get_document_count(snapshot_id: str, endpoint: str = SPARQL_ENDPOINT):
    query = """
        SELECT (COUNT(?doc) AS ?total)
        WHERE {
            ?doc a :Document .
        }
    """
    qres = sparql_query(query, endpoint)
    return qres[0]


@lru_cache(maxsize=1)
def get_inventory_count(snapshot_id: str, endpoint: str = SPARQL_ENDPOINT):
    query = """
        SELECT (COUNT(?mi) AS ?total)
        WHERE {
            ?mi a :MineralInventory .
        }
    """
    qres = sparql_query(query, endpoint)
    return qres[0]


@lru_cache(maxsize=1)
def get_mineralsites_count(snapshot_id: str, endpoint: str = SPARQL_ENDPOINT):
    query = """
       SELECT (COUNT(?ms) AS ?total)
        WHERE {
            ?ms a :MineralSite .
        }
    """
    qres = sparql_query(query, endpoint)
    return qres[0]


@lru_cache(maxsize=1)
def get_inventory_by_commodity(snapshot_id: str, endpoint: str = SPARQL_ENDPOINT):
    query = """
        SELECT  ?commodity_uri ?commodity_label ?total
        WHERE {
            {
                SELECT ?commodity_uri (COUNT(DISTINCT ?mi) AS ?total)
                WHERE {
                    ?mi a :MineralInventory .
                    ?mi :commodity/:normalized_uri ?commodity_uri .
                }
                GROUP BY ?commodity_uri
            }
            ?commodity_uri rdfs:label ?commodity_label .
        }
    """
    qres = sparql_query(query, endpoint)
    return qres


@lru_cache(maxsize=1)
def get_mineralsites_by_commodity(snapshot_id: str, endpoint: str = SPARQL_ENDPOINT):
    query = """
        SELECT  ?commodity_uri ?commodity_label ?total
        WHERE {
            {
                SELECT ?commodity_uri (COUNT(DISTINCT ?ms) AS ?total)
                WHERE {
                    ?ms a :MineralSite .
                    ?ms :mineral_inventory ?mi .
                    ?mi :commodity/:normalized_uri ?commodity_uri .
                }
                GROUP BY ?commodity_uri
            }
            ?commodity_uri rdfs:label ?commodity_label .
        }
        
    """
    qres = sparql_query(query, endpoint)
    return qres


@lru_cache(maxsize=1)
def get_documents_by_commodity(snapshot_id: str, endpoint: str = SPARQL_ENDPOINT):
    query = """

        SELECT  ?commodity_uri ?commodity_label ?total
        WHERE {
            {
                SELECT ?commodity_uri (COUNT(DISTINCT ?doc) AS ?total)
                WHERE {
                    ?mi :reference/:document ?doc . 
                    ?mi :commodity/:normalized_uri ?commodity_uri .
                }
                GROUP BY ?commodity_uri
            }
            ?commodity_uri rdfs:label ?commodity_label .
        }
    """
    qres = sparql_query(query, endpoint)
    return qres
