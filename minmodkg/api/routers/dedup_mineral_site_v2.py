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

router = APIRouter(tags=["mineral_sites"])


@router.get("/dedup-mineral-sites/{commodity}")
def dedup_mineral_sites_v2(
    commodity: str,
    limit: int = -1,
    offset: int = 0,
):
    commodity = norm_commodity(commodity)
    output = get_dedup_mineral_site_data_v2(get_snapshot_id(), commodity, limit, offset)
    return output


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
        record = {
            "id": dms,
            "commodity": MNR_NS + commodity,
            "sites": [
                {
                    "id": sites[0]["ms"],
                    "name": sites[0]["ms_name"],
                    "type": sites[0].get("ms_type") or "NotSpecified",
                    "rank": sites[0].get("ms_rank") or "U",
                    "country": list({site["country"] for site in sites}),
                    "state_or_province": list(
                        {site["state_or_province"] for site in sites}
                    ),
                }
                for site_id, sites in sid2sites.items()
            ],
        }

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
            record["loc_crs"] = crs
            record["loc_wkt"] = wkt
            record["best_loc_crs"] = best_crs
            record["best_loc_wkt"] = best_wkt

            try:
                geometry = shapely.wkt.loads(best_wkt)
                centroid = shapely.wkt.dumps(shapely.centroid(geometry))
                centroid = reproject_wkt(centroid, best_crs, "EPSG:4326")
                record["best_loc_centroid_epsg_4326"] = centroid
            except shapely.errors.GEOSException:
                record["best_loc_centroid_epsg_4326"] = None
        else:
            record["loc_crs"] = None
            record["loc_wkt"] = None
            record["best_loc_crs"] = None
            record["best_loc_wkt"] = None
            record["best_loc_centroid_epsg_4326"] = None

        gt_sites = [s for s in dupsites if s["total_contained_metal"] is not None]
        if len(gt_sites) == 0:
            record["total_contained_metal"] = None
            record["total_tonnage"] = None
            record["total_grade"] = None
        else:
            gtsite = max(
                (s for s in dupsites if s["total_contained_metal"] is not None),
                key=lambda x: x["total_contained_metal"],
            )
            record["total_contained_metal"] = gtsite["total_contained_metal"]
            record["total_tonnage"] = gtsite["total_tonnage"]
            record["total_grade"] = gtsite["total_grade"]

        deposit_types = []
        for site_id, sites in sid2sites.items():
            dt = {}
            for site in sites:
                if site["dt_name"] is not None and (
                    site["dt_name"] not in dt
                    or site["dt_confidence"] > dt[site["dt_name"]]["confidence"]
                ):
                    dt[site["dt_name"]] = {
                        "name": site["dt_name"],
                        "source": site["dt_source"],
                        "confidence": site["dt_confidence"],
                        "group": site["dt_group"],
                        "environment": site["dt_env"],
                    }
            deposit_types.extend(dt.values())

        record["deposit_types"] = sorted(
            deposit_types, key=lambda x: x["confidence"], reverse=True
        )[:5]
        output.append(record)

    return output
