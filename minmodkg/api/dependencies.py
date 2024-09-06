from __future__ import annotations

import os
from typing import Optional

from fastapi import APIRouter, HTTPException
from minmodkg.misc import run_sparql_query

DEFAULT_ENDPOINT = os.environ.get("SPARQL_ENDPOINT", "https://minmod.isi.edu/sparql")
MNR_NS = "https://minmod.isi.edu/resource/"
MNO_NS = "https://minmod.isi.edu/ontology/"


def get_snapshot_id(endpoint: str = DEFAULT_ENDPOINT):
    query = "SELECT ?snapshot_id WHERE { mnr:kg dcterms:hasVersion ?snapshot_id }"
    qres = run_sparql_query(query, endpoint)
    return qres[0]["snapshot_id"]


def norm_commodity(commodity: str) -> str:
    if commodity.startswith("http"):
        raise HTTPException(
            status_code=404,
            detail=f"Expect commodity to be either just an id (QXXX) or name. Get `{commodity}` instead",
        )
    if not is_minmod_id(commodity):
        uri = get_commodity_by_name(commodity)
        if uri is None:
            raise HTTPException(
                status_code=404, detail=f"Commodity `{commodity}` not found"
            )
        commodity = uri
    return commodity


def is_minmod_id(text: str) -> bool:
    return text.startswith("Q") and text[1:].isdigit()


def get_commodity_by_name(name: str) -> Optional[str]:
    query = (
        'SELECT ?uri WHERE { ?uri a :Commodity ; rdfs:label ?name . FILTER(LCASE(STR(?name)) = "%s") }'
        % name.lower()
    )
    qres = run_sparql_query(query, DEFAULT_ENDPOINT)
    if len(qres) == 0:
        return None
    uri = qres[0]["uri"]
    assert uri.startswith(MNR_NS)
    uri = uri[len(MNR_NS) :]
    return uri


def rank_source(source_id: str) -> int:
    """Get ranking of a source, higher is better"""
    default_score = 5
    order = [
        ("https://api.cdr.land/v1/docs/documents", 10),
        ("https://w3id.org/usgs", 10),
        ("https://doi.org/", 10),
        ("http://minmod.isi.edu/", 10),
        ("https://mrdata.usgs.gov/deposit", 7),
        ("https://mrdata.usgs.gov/mrds", 1),
    ]

    for prefix, score in order:
        if source_id.startswith(prefix):
            return score
    return default_score
