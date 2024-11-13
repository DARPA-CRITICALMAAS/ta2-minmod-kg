from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Literal, Optional

import networkx as nx
import orjson
import pandas as pd
import shapely.wkt
from fastapi import APIRouter, Query, Response
from minmodkg.api.dependencies import (
    SPARQL_ENDPOINT,
    get_snapshot_id,
    norm_commodity,
    rank_source,
)
from minmodkg.config import MNR_NS
from minmodkg.grade_tonnage_model import GradeTonnageModel, SiteGradeTonnage
from minmodkg.misc import group_by_key, merge_wkts, reproject_wkt, sparql_query
from minmodkg.models.dedup_mineral_site import (
    DedupMineralSiteDepositType,
    DedupMineralSiteLocation,
    DedupMineralSitePublic,
)
from minmodkg.models.derived_mineral_site import GradeTonnage
from minmodkg.typing import IRI, InternalID
from pydantic import BaseModel

router = APIRouter(tags=["mineral_sites"])


@router.get("/dedup-mineral-sites")
def dedup_mineral_sites_v2(
    commodity: str,
    limit: int = -1,
    offset: int = 0,
):
    commodity = norm_commodity(commodity)
    output = get_dedup_mineral_sites(get_snapshot_id(), commodity, limit, offset)
    return [x.model_dump(exclude_none=True) for x in output]


@router.get("/dedup-mineral-sites/{dedup_site_id}")
def api_get_dedup_mineral_site(
    dedup_site_id: str,
    commodity: str,
    limit: int = -1,
    offset: int = 0,
):
    commodity = norm_commodity(commodity)
    output = get_dedup_mineral_site(
        get_snapshot_id(), MNR_NS + dedup_site_id, commodity
    )
    return output.model_dump(exclude_none=True)


def get_dedup_mineral_site(
    snapshot_id: str,
    dms: IRI,
    commodity: InternalID,
) -> DedupMineralSitePublic:
    query = """
    SELECT
        ?ms
        ?ms_source
        ?ms_name
        ?ms_type
        ?ms_rank
        ?created_by
        ?modified_at
        ?dt_id
        ?dt_source
        ?dt_confidence
        ?lat
        ?lon
        ?country
        ?state_or_province
        ?total_contained_metal
        ?total_tonnage
        ?total_grade
    WHERE {
        <%s> :site ?ms .

        OPTIONAL {
            ?ms :deposit_type_candidate [
                :source ?dt_source ;
                :confidence ?dt_confidence ;
                :normalized_uri ?dt_id ;
            ]
        }
        
        ?ms :source_id ?ms_source ;
            :created_by ?created_by ;
            :modified_at ?modified_at .

        OPTIONAL { ?ms rdfs:label ?ms_name . }
        OPTIONAL { ?ms :site_type ?ms_type . }
        OPTIONAL { ?ms :site_rank ?ms_rank . }

        OPTIONAL {
            ?ms :location_info ?loc .
            OPTIONAL { 
                ?loc :country/:normalized_uri ?country .
            }
            OPTIONAL {
                ?loc :state_or_province/:normalized_uri ?state_or_province . 
            }
        }

        OPTIONAL {
            ?ms :lat ?lat ;
                :lon ?lon .
        }

        OPTIONAL {
            ?ms :grade_tonnage [
                # when save grade tonnage, we convert commodity to full uri
                :commodity mnr:%s ;
                :total_contained_metal ?total_contained_metal ;
                :total_tonnage ?total_tonnage ;
                :total_grade ?total_grade ;
            ]
        }
    }
    """ % (
        dms,
        commodity,
    )
    qres = sparql_query(
        query,
        keys=[
            "ms",
            "ms_source",
            "ms_name",
            "ms_type",
            "ms_rank",
            "created_by",
            "modified_at",
            "dt_id",
            "dt_source",
            "dt_confidence",
            "lat",
            "lon",
            "country",
            "state_or_province",
            "total_contained_metal",
            "total_tonnage",
            "total_grade",
        ],
    )
    return make_dedup_site(dms, commodity, qres, snapshot_id)


def get_dedup_mineral_sites(
    snapshot_id: str,
    commodity: InternalID,
    limit: int = -1,
    offset: int = 0,
) -> list[DedupMineralSitePublic]:
    if limit > 0:
        dm_query_part = """
        {
            SELECT ?dms
            WHERE {
                ?dms a :DedupMineralSite ;
                    :commodity mnr:%s .
            }
            LIMIT %d OFFSET %d
        }
""" % (
            commodity,
            limit,
            offset,
        )
    else:
        dm_query_part = """
        ?dms a :DedupMineralSite ;
            :commodity mnr:%s .
""" % (
            commodity,
        )

    query = """
    SELECT
        ?dms
        ?ms
        ?ms_source
        ?ms_name
        ?ms_type
        ?ms_rank
        ?created_by
        ?modified_at
        ?dt_id
        ?dt_source
        ?dt_confidence
        ?lat
        ?lon
        ?country
        ?state_or_province
        ?total_contained_metal
        ?total_tonnage
        ?total_grade
    WHERE {
        %s
        ?dms :site ?ms .

        OPTIONAL {
            ?ms :deposit_type_candidate [
                :source ?dt_source ;
                :confidence ?dt_confidence ;
                :normalized_uri ?dt_id ;
            ]
        }
        
        ?ms :source_id ?ms_source ;
            :created_by ?created_by ;
            :modified_at ?modified_at .

        OPTIONAL { ?ms rdfs:label ?ms_name . }
        OPTIONAL { ?ms :site_type ?ms_type . }
        OPTIONAL { ?ms :site_rank ?ms_rank . }

        OPTIONAL {
            ?ms :location_info ?loc .
            OPTIONAL { 
                ?loc :country/:normalized_uri ?country .
            }
            OPTIONAL {
                ?loc :state_or_province/:normalized_uri ?state_or_province . 
            }
        }

        OPTIONAL {
            ?ms :lat ?lat ;
                :lon ?lon .
        }

        OPTIONAL {
            ?ms :grade_tonnage [
                # when save grade tonnage, we convert commodity to full uri
                :commodity mnr:%s ;
                :total_contained_metal ?total_contained_metal ;
                :total_tonnage ?total_tonnage ;
                :total_grade ?total_grade ;
            ]
        }
    }
    """ % (
        dm_query_part,
        commodity,
    )

    qres = sparql_query(
        query,
        keys=[
            "dms",
            "ms",
            "ms_source",
            "ms_name",
            "ms_type",
            "ms_rank",
            "created_by",
            "modified_at",
            "dt_id",
            "dt_source",
            "dt_confidence",
            "lat",
            "lon",
            "country",
            "state_or_province",
            "total_contained_metal",
            "total_tonnage",
            "total_grade",
        ],
    )

    if len(qres) == 0:
        return []

    dms2sites = group_by_key(qres, "dms")
    return [
        make_dedup_site(dms, commodity, dupsites, snapshot_id)
        for dms, dupsites in dms2sites.items()
    ]


def make_dedup_site(
    dms: IRI,
    commodity: InternalID,
    dupsites: list[dict],
    snapshot_id: str,
) -> DedupMineralSitePublic:
    sid2sites = group_by_key(dupsites, "ms")
    _tmp_deposit_types: dict[str, DedupMineralSiteDepositType] = {}
    for site in dupsites:
        if site["dt_id"] is not None and (
            site["dt_id"] not in _tmp_deposit_types
            or site["dt_confidence"] > _tmp_deposit_types[site["dt_id"]].confidence
        ):
            _tmp_deposit_types[site["dt_id"]] = DedupMineralSiteDepositType(
                uri=site["dt_id"],
                source=site["dt_source"],
                confidence=site["dt_confidence"],
            )
    deposit_types: list[DedupMineralSiteDepositType] = list(_tmp_deposit_types.values())
    ranked_site_ids = [
        site_id
        for site_id, _ in sorted(
            (
                (
                    site_id,
                    (
                        rank_source(
                            sites[0]["ms_source"],
                            sites[0]["created_by"],
                            snapshot_id,
                        ),
                        sites[0]["modified_at"],
                    ),
                )
                for site_id, sites in sid2sites.items()
            ),
            key=lambda x: x[1],
            reverse=True,
        )
    ]

    site_name: str = next(
        (
            (site := sid2sites[site_id][0])["ms_name"]
            for site_id in ranked_site_ids
            if site["ms_name"] is not None
        ),
        "",
    )
    site_type = next(
        (
            (site := sid2sites[site_id][0])["ms_type"]
            for site_id in ranked_site_ids
            if site["ms_type"] is not None
        ),
        "NotSpecified",
    )
    site_rank = next(
        (
            (site := sid2sites[site_id][0])["ms_rank"]
            for site_id in ranked_site_ids
            if site["ms_rank"] is not None
        ),
        "U",
    )
    country = []
    state_or_province = []
    lat = None
    long = None
    for site_id in ranked_site_ids:
        _tmp_country = {
            site["country"]
            for site in sid2sites[site_id]
            if site["country"] is not None
        }
        if len(_tmp_country) > 0:
            country = list(_tmp_country)
            break
    for site_id in ranked_site_ids:
        _tmp_province = {
            site["state_or_province"]
            for site in sid2sites[site_id]
            if site["state_or_province"] is not None
        }
        if len(_tmp_province) > 0:
            state_or_province = list(_tmp_province)
            break
    has_loc_site_id = next(
        (
            site_id
            for site_id in ranked_site_ids
            if sid2sites[site_id][0]["lat"] is not None
        ),
        None,
    )
    if has_loc_site_id is not None:
        lat = sid2sites[has_loc_site_id][0]["lat"]
        long = sid2sites[has_loc_site_id][0]["lon"]

    if (
        len(country) == 0
        and len(state_or_province) == 0
        and lat is None
        and long is None
    ):
        location = None
    else:
        location = DedupMineralSiteLocation(
            lat=lat,
            long=long,
            country=country,
            state_or_province=state_or_province,
        )

    gt_sites = [s for s in dupsites if s["total_contained_metal"] is not None]
    if len(gt_sites) > 0:
        gtsite = max(
            (s for s in dupsites if s["total_contained_metal"] is not None),
            key=lambda x: x["total_contained_metal"],
        )
        gt = GradeTonnage(
            commodity=commodity,
            total_contained_metal=gtsite["total_contained_metal"],
            total_tonnage=gtsite["total_tonnage"],
            total_grade=gtsite["total_grade"],
        )
    else:
        gt = GradeTonnage(
            commodity=commodity,
        )

    return DedupMineralSitePublic(
        uri=dms,
        name=site_name,
        type=site_type,
        rank=site_rank,
        sites=ranked_site_ids,
        deposit_types=sorted(deposit_types, key=lambda x: x.confidence, reverse=True)[
            :5
        ],
        location=location,
        grade_tonnage=gt,
    )
