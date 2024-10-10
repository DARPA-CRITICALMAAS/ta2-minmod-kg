from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

from fastapi import APIRouter, HTTPException
from minmodkg.misc import LongestPrefixIndex, run_sparql_query

DEFAULT_ENDPOINT = os.environ.get("SPARQL_ENDPOINT", "https://minmod.isi.edu/sparql")
MNR_NS = "https://minmod.isi.edu/resource/"
MNO_NS = "https://minmod.isi.edu/ontology/"


def get_snapshot_id(endpoint: str = DEFAULT_ENDPOINT):
    query = "SELECT ?snapshot_id WHERE { mnr:kg dcterms:hasVersion ?snapshot_id }"
    qres = run_sparql_query(query, endpoint)
    return qres[0]["snapshot_id"]


def norm_commodity(commodity: str, endpoint: str = DEFAULT_ENDPOINT) -> str:
    if commodity.startswith("http"):
        raise HTTPException(
            status_code=404,
            detail=f"Expect commodity to be either just an id (QXXX) or name. Get `{commodity}` instead",
        )
    if not is_minmod_id(commodity):
        uri = get_commodity_by_name(commodity, endpoint)
        if uri is None:
            raise HTTPException(
                status_code=404, detail=f"Commodity `{commodity}` not found"
            )
        commodity = uri
    return commodity


def is_minmod_id(text: str) -> bool:
    return text.startswith("Q") and text[1:].isdigit()


def get_commodity_by_name(name: str, endpoint: str = DEFAULT_ENDPOINT) -> Optional[str]:
    query = (
        'SELECT ?uri WHERE { ?uri a :Commodity ; rdfs:label ?name . FILTER(LCASE(STR(?name)) = "%s") }'
        % name.lower()
    )
    qres = run_sparql_query(query, endpoint)
    if len(qres) == 0:
        return None
    uri = qres[0]["uri"]
    assert uri.startswith(MNR_NS)
    uri = uri[len(MNR_NS) :]
    return uri


def rank_source(
    source_id: str, snapshot_id: str, endpoint: str = DEFAULT_ENDPOINT
) -> int:
    """Get ranking of a source, higher is better"""
    default_score = 5
    score = get_source_scores(snapshot_id, endpoint).get_score(source_id)
    if score is None:
        print("Unknown source id:", source_id)
        return default_score
    return score


@dataclass
class SourceScore:
    source2score: dict[str, int]
    index: LongestPrefixIndex

    def get_score(self, source_id: str) -> Optional[int]:
        prefix = self.index.get(source_id)
        if prefix is not None:
            return self.source2score[prefix]
        return None


@lru_cache(maxsize=1)
def get_source_scores(snapshot_id: str, endpoint: str = DEFAULT_ENDPOINT):
    query = """
    SELECT ?uri ?prefixed_id ?score
    WHERE {
        ?uri a :SourceScore ;
            :prefixed_id ?prefixed_id ;
            :score ?score
    }
    """
    qres = run_sparql_query(query, endpoint)
    source2score = {record["prefixed_id"]: record["score"] for record in qres}
    index = LongestPrefixIndex.create(list(source2score.keys()))
    return SourceScore(source2score, index)
