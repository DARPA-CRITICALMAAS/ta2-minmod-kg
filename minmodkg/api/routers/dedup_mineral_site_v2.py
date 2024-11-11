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
from pydantic import BaseModel

router = APIRouter(tags=["mineral_sites"])


@router.get("/dedup-mineral-sites")
def dedup_mineral_sites_v2(
    commodity: str,
    limit: int = -1,
    offset: int = 0,
):
    commodity = norm_commodity(commodity)
    output = get_dedup_mineral_site_data_v2(get_snapshot_id(), commodity, limit, offset)
    return output


# @router.get("/dedup-mineral-sites/{dedup_site_id}")
# def dedup_mineral_sites_v2(
#     commodity: str,
#     limit: int = -1,
#     offset: int = 0,
# ):
#     commodity = norm_commodity(commodity)
#     output = get_dedup_mineral_site_data_v2(get_snapshot_id(), commodity, limit, offset)
#     return output


def get_dedup_mineral_site_data_v2(
    snapshot_id: str,
    commodity: str,
    limit: int = -1,
    offset: int = 0,
    endpoint: str = SPARQL_ENDPOINT,
):
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
        ?dt_name
        ?dt_source
        ?dt_confidence
        ?dt_group
        ?dt_env
        ?loc_wkt
        ?loc_crs
        ?country
        ?state_or_province
        ?total_contained_metal
        ?total_tonnage
        ?total_grade
    WHERE {
        %s
        ?dms :site ?ms .

        ?ms :grade_tonnage/:commodity mnr:%s .

        OPTIONAL {
            ?ms :deposit_type_candidate [
                :source ?dt_source ;
                :confidence ?dt_confidence ;
                :normalized_uri [
                    rdfs:label ?dt_name ;
                    :group ?dt_group ;
                    :environment ?dt_env ;
                ]
            ]
        }
        
        ?ms :source_id ?ms_source .

        OPTIONAL { ?ms rdfs:label ?ms_name . }
        OPTIONAL { ?ms :site_type ?ms_type . }
        OPTIONAL { ?ms :site_rank ?ms_rank . }

        OPTIONAL {
            ?ms :location_info ?loc .

            OPTIONAL { 
                ?loc :country/:normalized_uri/rdfs:label ?country .
            }
            OPTIONAL {
                ?loc :state_or_province/:normalized_uri/rdfs:label ?state_or_province . 
            }
            OPTIONAL {
                ?loc :crs/:normalized_uri/rdfs:label ?loc_crs .
            }
            OPTIONAL { ?loc :location ?loc_wkt . }
        }

        OPTIONAL {
            ?ms :grade_tonnage [
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
        commodity,
    )

    qres = sparql_query(
        query,
        endpoint,
        [
            "dms",
            "ms",
            "ms_source",
            "ms_name",
            "ms_type",
            "ms_rank",
            "dt_name",
            "dt_source",
            "dt_confidence",
            "dt_group",
            "dt_env",
            "country",
            "loc_crs",
            "loc_wkt",
            "state_or_province",
            "total_contained_metal",
            "total_tonnage",
            "total_grade",
        ],
    )

    if len(qres) == 0:
        return []

    dms2sites = group_by_key(qres, "dms")
    output = []

    for dms, dupsites in dms2sites.items():
        sid2sites = group_by_key(dupsites, "ms")
        deposit_types: list[DepositTypeResp] = []
        for site_id, sites in sid2sites.items():
            dt: dict[str, DepositTypeResp] = {}
            for site in sites:
                if site["dt_name"] is not None and (
                    site["dt_name"] not in dt
                    or site["dt_confidence"] > dt[site["dt_name"]].confidence
                ):
                    dt[site["dt_name"]] = DepositTypeResp(
                        name=site["dt_name"],
                        source=site["dt_source"],
                        confidence=site["dt_confidence"],
                        group=site["dt_group"],
                        environment=site["dt_env"],
                    )
            deposit_types.extend(dt.values())

        out_dedup_site = DedupMineralSiteResp(
            id=dms,
            commodity=commodity,
            sites=[
                MineralSiteResp(
                    id=sites[0]["ms"],
                    name=sites[0]["ms_name"],
                    type=sites[0].get("ms_type") or "NotSpecified",
                    rank=sites[0].get("ms_rank") or "U",
                    country=list({site["country"] for site in sites} - {None}),
                    state_or_province=list(
                        {site["state_or_province"] for site in sites} - {None}
                    ),
                )
                for site_id, sites in sid2sites.items()
            ],
            deposit_types=sorted(
                deposit_types, key=lambda x: x.confidence, reverse=True
            )[:5],
        )

        crs_wkts = [
            (
                rank_source(x["ms_source"], snapshot_id, endpoint),
                x["loc_crs"],
                x["loc_wkt"],
            )
            for x in dupsites
            if x["loc_wkt"] is not None
        ]

        if len(crs_wkts) > 0:
            best_crs, best_wkt = merge_wkts(crs_wkts)
            crs, wkt = merge_wkts(crs_wkts, min_rank=-1)
            out_dedup_site.loc_crs = crs
            out_dedup_site.loc_wkt = wkt
            out_dedup_site.best_loc_crs = best_crs
            out_dedup_site.best_loc_wkt = best_wkt

            try:
                geometry = shapely.wkt.loads(best_wkt)
                centroid = shapely.wkt.dumps(shapely.centroid(geometry))
                centroid = reproject_wkt(centroid, best_crs, "EPSG:4326")
                out_dedup_site.best_loc_centroid_epsg_4326 = centroid
            except shapely.errors.GEOSException:
                out_dedup_site.best_loc_centroid_epsg_4326 = None

        gt_sites = [s for s in dupsites if s["total_contained_metal"] is not None]
        if len(gt_sites) > 0:
            gtsite = max(
                (s for s in dupsites if s["total_contained_metal"] is not None),
                key=lambda x: x["total_contained_metal"],
            )
            out_dedup_site.total_contained_metal = gtsite["total_contained_metal"]
            out_dedup_site.total_tonnage = gtsite["total_tonnage"]
            out_dedup_site.total_grade = gtsite["total_grade"]

        output.append(out_dedup_site)

    return output


class MineralSiteResp(BaseModel):
    id: str
    name: Optional[str]
    type: str
    rank: str
    country: list[str]
    state_or_province: list[str]


class DepositTypeResp(BaseModel):
    name: str
    source: str
    confidence: float
    group: str
    environment: str


class DedupMineralSiteResp(BaseModel):
    id: str
    commodity: str
    sites: list[MineralSiteResp]
    deposit_types: list[DepositTypeResp]
    loc_crs: Optional[str] = None
    loc_wkt: Optional[str] = None
    best_loc_crs: Optional[str] = None
    best_loc_wkt: Optional[str] = None
    best_loc_centroid_epsg_4326: Optional[str] = None
    total_contained_metal: Optional[float] = None
    total_tonnage: Optional[float] = None
    total_grade: Optional[float] = None
