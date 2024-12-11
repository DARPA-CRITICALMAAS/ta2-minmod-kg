from __future__ import annotations

from functools import lru_cache

from fastapi import APIRouter
from minmodkg.api.dependencies import get_snapshot_id
from minmodkg.models.base import MINMOD_KG

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
def get_document_count(snapshot_id: str):
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """
        SELECT (COUNT(?doc) AS ?total)
        WHERE {
            ?doc a mo:Document .
        }
    """
    qres = MINMOD_KG.query(query)
    return qres[0]


@lru_cache(maxsize=1)
def get_inventory_count(snapshot_id: str):
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """
        SELECT (COUNT(?mi) AS ?total)
        WHERE {
            ?mi a mo:MineralInventory .
        }
    """
    qres = MINMOD_KG.query(query)
    return qres[0]


@lru_cache(maxsize=1)
def get_mineralsites_count(snapshot_id: str):
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """
       SELECT (COUNT(?ms) AS ?total)
        WHERE {
            ?ms a mo:MineralSite .
        }
    """
    qres = MINMOD_KG.query(query)
    return qres[0]


@lru_cache(maxsize=1)
def get_inventory_by_commodity(snapshot_id: str):
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """
        SELECT  ?commodity_uri ?commodity_label ?total
        WHERE {
            {
                SELECT ?commodity_uri (COUNT(DISTINCT ?mi) AS ?total)
                WHERE {
                    ?mi a mo:MineralInventory .
                    ?mi mo:commodity/mo:normalized_uri ?commodity_uri .
                }
                GROUP BY ?commodity_uri
            }
            ?commodity_uri rdfs:label ?commodity_label .
        }
    """
    qres = MINMOD_KG.query(query)
    return qres


@lru_cache(maxsize=1)
def get_mineralsites_by_commodity(snapshot_id: str):
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """
        SELECT  ?commodity_uri ?commodity_label ?total
        WHERE {
            {
                SELECT ?commodity_uri (COUNT(DISTINCT ?ms) AS ?total)
                WHERE {
                    ?ms a mo:MineralSite .
                    ?ms mo:mineral_inventory ?mi .
                    ?mi mo:commodity/mo:normalized_uri ?commodity_uri .
                }
                GROUP BY ?commodity_uri
            }
            ?commodity_uri rdfs:label ?commodity_label .
        }
        
    """
    qres = MINMOD_KG.query(query)
    return qres


@lru_cache(maxsize=1)
def get_documents_by_commodity(snapshot_id: str):
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """

        SELECT  ?commodity_uri ?commodity_label ?total
        WHERE {
            {
                SELECT ?commodity_uri (COUNT(DISTINCT ?doc) AS ?total)
                WHERE {
                    ?mi mo:reference/mo:document ?doc . 
                    ?mi mo:commodity/mo:normalized_uri ?commodity_uri .
                }
                GROUP BY ?commodity_uri
            }
            ?commodity_uri rdfs:label ?commodity_label .
        }
    """
    qres = MINMOD_KG.query(query)
    return qres
