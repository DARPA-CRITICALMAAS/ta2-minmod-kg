from __future__ import annotations

from fastapi import APIRouter, Response
from minmodkg.misc.utils import CacheResponse
from minmodkg.models.kg.base import MINMOD_KG

router = APIRouter(tags=["statistics"])
cache_response = CacheResponse()


@router.get("/documents/count")
def document_count(response: Response):
    return cache_response(
        key="document_count",
        expired=60 * 60,
        response=response,
        compute_response=get_document_count,
    )


@router.get("/mineral-inventories/count")
def inventory_count(response: Response):
    return cache_response(
        key="inventory_count",
        expired=3 * 60 * 60,
        response=response,
        compute_response=get_inventory_count,
    )


@router.get("/mineral-sites/count")
def mineralsites_count(response: Response):
    return cache_response(
        key="mineralsites_count",
        expired=60 * 60,
        response=response,
        compute_response=get_mineralsites_count,
    )


@router.get("/mineral-inventories/count-by-commodity")
def inventory_by_commodity(response: Response):
    return cache_response(
        key="inventory_by_commodity",
        expired=3 * 60 * 60,
        response=response,
        compute_response=get_inventory_by_commodity,
    )


@router.get("/mineral-sites/count-by-commodity")
def mineralsites_by_commodity(response: Response):
    return cache_response(
        key="mineralsites_by_commodity",
        expired=3 * 60 * 60,
        response=response,
        compute_response=get_mineralsites_by_commodity,
    )


@router.get("/documents/count-by-commodity")
def documents_by_commodity(response: Response):
    return cache_response(
        key="documents_by_commodity",
        expired=3 * 60 * 60,
        response=response,
        compute_response=get_documents_by_commodity,
    )


def get_document_count():
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """
        SELECT (COUNT(?doc) AS ?total)
        WHERE {
            ?doc a mo:Document .
        }
    """
    qres = MINMOD_KG.query(query)
    return qres[0]


def get_inventory_count():
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """
        SELECT (COUNT(?mi) AS ?total)
        WHERE {
            ?mi a mo:MineralInventory .
        }
    """
    qres = MINMOD_KG.query(query)
    return qres[0]


def get_mineralsites_count():
    assert MINMOD_KG.ns.mo.alias == "mo"
    query = """
       SELECT (COUNT(?ms) AS ?total)
        WHERE {
            ?ms a mo:MineralSite .
        }
    """
    qres = MINMOD_KG.query(query)
    return qres[0]


def get_inventory_by_commodity():
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


def get_mineralsites_by_commodity():
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


def get_documents_by_commodity():
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
