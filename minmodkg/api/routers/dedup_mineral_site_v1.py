from __future__ import annotations

from typing import Annotated, Optional

from fastapi import APIRouter, Body, HTTPException
from minmodkg.api.dependencies import norm_commodity
from minmodkg.misc import group_by_key
from minmodkg.models.base import MINMOD_KG, MINMOD_NS
from minmodkg.models.dedup_mineral_site import (
    DedupMineralSite,
    DedupMineralSiteDepositType,
    DedupMineralSiteIdAndScore,
    DedupMineralSiteLocation,
    DedupMineralSitePublic,
)
from minmodkg.models.derived_mineral_site import GradeTonnage
from minmodkg.models_v2.kgrel.user import is_system_user
from minmodkg.typing import InternalID

router = APIRouter(tags=["mineral_sites"])


@router.get("/dedup-mineral-sites")
def dedup_mineral_sites_v2(
    commodity: Optional[str] = None,
    limit: int = -1,
    offset: int = 0,
):
    if commodity is None:
        output = get_dedup_mineral_sites(None, limit, offset)
    else:
        commodity = norm_commodity(commodity)
        output = get_dedup_mineral_sites_by_commodity(commodity, limit, offset)
    return [x.model_dump(exclude_none=True) for x in output]


@router.post("/dedup-mineral-sites/find_by_ids")
def api_get_dedup_mineral_sites(
    ids: Annotated[list[InternalID], Body(embed=True)],
    commodity: Annotated[InternalID, Body(embed=True)],
) -> dict[InternalID, dict]:
    return {
        id: site.model_dump(exclude_none=True)
        for id, site in get_dedup_mineral_sites_by_commodity_and_ids(
            ids, commodity
        ).items()
    }


@router.get("/dedup-mineral-sites/{dedup_site_id}")
def api_get_dedup_mineral_site(
    dedup_site_id: str,
    commodity: Optional[str] = None,
):
    if commodity is not None:
        commodity = norm_commodity(commodity)
        output = get_dedup_mineral_sites_by_commodity_and_ids(
            [dedup_site_id], commodity
        )
    else:
        tmp = get_dedup_mineral_sites([dedup_site_id])
        if len(tmp) > 0:
            output = {dedup_site_id: tmp[0]}

    if len(output) == 0:
        raise HTTPException(status_code=404, detail=f"{dedup_site_id} not found")
    return output[dedup_site_id].model_dump(exclude_none=True)


def get_dedup_mineral_sites_by_commodity_and_ids(
    lst_dms: list[InternalID],
    commodity: InternalID,
) -> dict[InternalID, DedupMineralSitePublic]:
    dedup_ns = DedupMineralSite.qbuilder.class_namespace
    mr = MINMOD_NS.mr
    md = MINMOD_NS.md
    mo = MINMOD_NS.mo
    rdfs = MINMOD_NS.rdfs

    assert md.alias == "md"
    assert mo.alias == "mo"
    assert mr.alias == "mr"
    assert rdfs.alias == "rdfs"

    query = """
    SELECT
        ?dms
        ?ms
        ?ms_source_score
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
        ?total_tonnage
        ?total_grade
    WHERE {
        ?dms md:site ?ms .

        OPTIONAL {
            ?ms mo:deposit_type_candidate [
                mo:source ?dt_source ;
                mo:confidence ?dt_confidence ;
                mo:normalized_uri ?dt_id ;
            ]
        }
        
        OPTIONAL {
            ?ms mo:source_uri/mo:score ?ms_source_score .
        }
        ?ms mo:created_by ?created_by ;
            mo:modified_at ?modified_at .

        OPTIONAL { ?ms rdfs:label ?ms_name . }
        OPTIONAL { ?ms mo:site_type ?ms_type . }
        OPTIONAL { ?ms mo:site_rank ?ms_rank . }

        OPTIONAL {
            ?ms mo:location_info ?loc .
            OPTIONAL { 
                ?loc mo:country/mo:normalized_uri ?country .
            }
            OPTIONAL {
                ?loc mo:state_or_province/mo:normalized_uri ?state_or_province . 
            }
        }
        
        %s

        OPTIONAL {
            ?derived_ms md:lat ?lat ;
                        md:lon ?lon .
        }

        OPTIONAL {
            ?derived_ms md:grade_tonnage [
                md:commodity mr:%s ;
                md:total_tonnage ?total_tonnage ;
                md:total_grade ?total_grade ;
            ]
        }

        VALUES ?dms { %s }
    }
    """ % (
        f'BIND (IRI(CONCAT("{md.namespace}", SUBSTR(STR(?ms), {len(mr.namespace)+1}))) as ?derived_ms)',
        commodity,
        " ".join(dedup_ns[dms] for dms in lst_dms),
    )

    qres = MINMOD_KG.query(
        query,
        keys=[
            "ms",
            "ms_source_score",
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
            "total_tonnage",
            "total_grade",
        ],
    )

    if len(lst_dms) == 1:
        if len(qres) == 0:
            return {}
        return {lst_dms[0]: make_dedup_site(lst_dms[0], qres, commodity=commodity)}
    dms2sites = group_by_key(qres, "dms")
    return {
        (dms_id := dedup_ns.id(dms)): make_dedup_site(
            dms_id, dupsites, commodity=commodity
        )
        for dms, dupsites in dms2sites.items()
        if len(dupsites) > 0
    }


def get_dedup_mineral_sites_by_commodity(
    commodity: InternalID,
    limit: int = -1,
    offset: int = 0,
) -> list[DedupMineralSitePublic]:
    dedup_ns = DedupMineralSite.qbuilder.class_namespace
    mr = MINMOD_NS.mr
    md = MINMOD_NS.md
    mo = MINMOD_NS.mo
    rdfs = MINMOD_NS.rdfs

    assert md.alias == "md"
    assert mo.alias == "mo"
    assert mr.alias == "mr"
    assert rdfs.alias == "rdfs"

    if limit > 0:
        dm_query_part = """
        {
            SELECT ?dms
            WHERE {
                ?dms a mo:DedupMineralSite ;
                    md:commodity mr:%s .
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
        ?dms a mo:DedupMineralSite ;
            md:commodity mr:%s .
""" % (
            commodity,
        )

    query = """
    SELECT
        ?dms
        ?ms
        ?ms_source_score
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
        ?total_tonnage
        ?total_grade
    WHERE {
        %s
        ?dms md:site ?ms .

        OPTIONAL {
            ?ms mo:deposit_type_candidate [
                mo:source ?dt_source ;
                mo:confidence ?dt_confidence ;
                mo:normalized_uri ?dt_id ;
            ]
        }
        
        OPTIONAL {
            ?ms mo:source_uri/mo:score ?ms_source_score .
        }
        ?ms mo:created_by ?created_by ;
            mo:modified_at ?modified_at .

        OPTIONAL { ?ms rdfs:label ?ms_name . }
        OPTIONAL { ?ms mo:site_type ?ms_type . }
        OPTIONAL { ?ms mo:site_rank ?ms_rank . }

        OPTIONAL {
            ?ms mo:location_info ?loc .
            OPTIONAL { 
                ?loc mo:country/mo:normalized_uri ?country .
            }
            OPTIONAL {
                ?loc mo:state_or_province/mo:normalized_uri ?state_or_province . 
            }
        }

        %s

        OPTIONAL {
            ?derived_ms md:lat ?lat ;
                        md:lon ?lon .
        }

        OPTIONAL {
            ?derived_ms md:grade_tonnage [
                md:commodity mr:%s ;
                md:total_tonnage ?total_tonnage ;
                md:total_grade ?total_grade ;
            ]
        }
    }
    """ % (
        dm_query_part,
        f'BIND (IRI(CONCAT("{md.namespace}", SUBSTR(STR(?ms), {len(mr.namespace)+1}))) as ?derived_ms)',
        commodity,
    )

    qres = MINMOD_KG.query(
        query,
        keys=[
            "dms",
            "ms",
            "ms_source_score",
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
            "total_tonnage",
            "total_grade",
        ],
    )

    if len(qres) == 0:
        return []

    dms2sites = group_by_key(qres, "dms")
    return [
        make_dedup_site(dedup_ns.id(dms), dupsites, commodity=commodity)
        for dms, dupsites in dms2sites.items()
    ]


def get_dedup_mineral_sites(
    lst_dms: Optional[list[InternalID]] = None,
    limit: int = -1,
    offset: int = 0,
):
    dedup_ns = DedupMineralSite.qbuilder.class_namespace
    mr = MINMOD_NS.mr
    md = MINMOD_NS.md
    mo = MINMOD_NS.mo
    rdfs = MINMOD_NS.rdfs

    assert md.alias == "md"
    assert mo.alias == "mo"
    assert mr.alias == "mr"
    assert rdfs.alias == "rdfs"

    if lst_dms is None:
        if limit > 0:
            dm_query_part = """
            {
                SELECT ?dms
                WHERE {
                    ?dms a mo:DedupMineralSite .
                }
                LIMIT %d OFFSET %d
            }
    """ % (
                limit,
                offset,
            )
        else:
            dm_query_part = "?dms a mo:DedupMineralSite ."
    else:
        dm_query_part = """
        VALUES ?dms { %s }
        """ % (
            " ".join(dedup_ns[dms] for dms in lst_dms)
        )

    query = """
    SELECT
        ?dms
        ?ms
        ?ms_source_score
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
        ?commodity
        ?total_tonnage
        ?total_grade
    WHERE {
        %s
        ?dms md:site ?ms .

        OPTIONAL {
            ?ms mo:deposit_type_candidate [
                mo:source ?dt_source ;
                mo:confidence ?dt_confidence ;
                mo:normalized_uri ?dt_id ;
            ]
        }
        
        OPTIONAL {
            ?ms mo:source_uri/mo:score ?ms_source_score ;
        }
        ?ms mo:created_by ?created_by ;
            mo:modified_at ?modified_at .

        OPTIONAL { ?ms rdfs:label ?ms_name . }
        OPTIONAL { ?ms mo:site_type ?ms_type . }
        OPTIONAL { ?ms mo:site_rank ?ms_rank . }

        OPTIONAL {
            ?ms mo:location_info ?loc .
            OPTIONAL { 
                ?loc mo:country/mo:normalized_uri ?country .
            }
            OPTIONAL {
                ?loc mo:state_or_province/mo:normalized_uri ?state_or_province . 
            }
        }

        %s

        OPTIONAL {
            ?derived_ms md:lat ?lat ;
                        md:lon ?lon .
        }

        ?derived_ms md:grade_tonnage ?derived_ms_gt .
        ?derived_ms_gt md:commodity ?commodity ;

        OPTIONAL {
            ?derived_ms_gt md:total_tonnage ?total_tonnage .
            ?derived_ms_gt md:total_grade ?total_grade .
        }
    }
    """ % (
        dm_query_part,
        f'BIND (IRI(CONCAT("{md.namespace}", SUBSTR(STR(?ms), {len(mr.namespace)+1}))) as ?derived_ms)',
    )

    qres = MINMOD_KG.query(
        query,
        keys=[
            "dms",
            "ms",
            "ms_source_score",
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
            "commodity",
            "total_tonnage",
            "total_grade",
        ],
    )

    if len(qres) == 0:
        return []

    for r in qres:
        r["commodity"] = mr.id(r["commodity"])

    dms2sites = group_by_key(qres, "dms")
    return [
        make_dedup_site(dedup_ns.id(dms), dupsites)
        for dms, dupsites in dms2sites.items()
    ]


def make_dedup_site(
    dms: InternalID,
    dupsites: list[dict],
    *,
    commodity: Optional[InternalID] = None,
    default_source_score: float = 0.5,
) -> DedupMineralSitePublic:
    mr = MINMOD_NS.mr

    sid2sites = group_by_key(dupsites, "ms")
    _tmp_deposit_types: dict[str, DedupMineralSiteDepositType] = {}
    for site in dupsites:
        if site["dt_id"] is not None and (
            site["dt_id"] not in _tmp_deposit_types
            or site["dt_confidence"] > _tmp_deposit_types[site["dt_id"]].confidence
        ):
            _tmp_deposit_types[site["dt_id"]] = DedupMineralSiteDepositType(
                id=mr.id(site["dt_id"]),
                source=site["dt_source"],
                confidence=site["dt_confidence"],
            )
    deposit_types: list[DedupMineralSiteDepositType] = list(_tmp_deposit_types.values())
    ranked_site_id_and_scores = [
        (site_id, min(site_score[0], 1.0))
        for site_id, site_score in sorted(
            (
                (
                    site_id,
                    get_ms_source_score(
                        sites[0]["ms_source_score"],
                        sites[0]["created_by"],
                        sites[0]["modified_at"],
                        default_source_score,
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
            _ms_name
            for site_id, site_score in ranked_site_id_and_scores
            if (_ms_name := sid2sites[site_id][0]["ms_name"]) is not None
        ),
        "",
    )
    site_type = next(
        (
            _ms_type
            for site_id, site_score in ranked_site_id_and_scores
            if (_ms_type := sid2sites[site_id][0]["ms_type"]) is not None
        ),
        "NotSpecified",
    )
    site_rank = next(
        (
            _ms_rank
            for site_id, site_score in ranked_site_id_and_scores
            if (_ms_rank := sid2sites[site_id][0]["ms_rank"]) is not None
        ),
        "U",
    )
    # TODO: fix me! we should normalize this rank & type at the beginning
    if site_type == "":
        site_type = "NotSpecified"
    if site_rank == "":
        site_rank = "U"
    country = []
    state_or_province = []
    lat = None
    lon = None
    for site_id, site_score in ranked_site_id_and_scores:
        _tmp_country = {
            mr.id(site["country"])
            for site in sid2sites[site_id]
            if site["country"] is not None
        }
        if len(_tmp_country) > 0:
            country = list(_tmp_country)
            break
    for site_id, site_score in ranked_site_id_and_scores:
        _tmp_province = {
            mr.id(site["state_or_province"])
            for site in sid2sites[site_id]
            if site["state_or_province"] is not None
        }
        if len(_tmp_province) > 0:
            state_or_province = list(_tmp_province)
            break
    has_loc_site_id = next(
        (
            site_id
            for site_id, site_score in ranked_site_id_and_scores
            if sid2sites[site_id][0]["lat"] is not None
        ),
        None,
    )
    if has_loc_site_id is not None:
        lat = sid2sites[has_loc_site_id][0]["lat"]
        lon = sid2sites[has_loc_site_id][0]["lon"]

    if (
        len(country) == 0
        and len(state_or_province) == 0
        and lat is None
        and lon is None
    ):
        location = None
    else:
        location = DedupMineralSiteLocation(
            lat=lat,
            lon=lon,
            country=country,
            state_or_province=state_or_province,
        )

    for s in dupsites:
        if s["total_tonnage"] is not None and s["total_grade"] is not None:
            s["total_contained_metal"] = s["total_tonnage"] * s["total_grade"]
        else:
            s["total_contained_metal"] = None

    gts = []

    if commodity is not None:
        commodity_gt_sites = {commodity: dupsites}
    else:
        commodity_gt_sites = group_by_key(dupsites, "commodity")

    for commodity, gt_sites in commodity_gt_sites.items():
        gt_sites = [s for s in gt_sites if s["total_contained_metal"] is not None]
        if len(gt_sites) > 0:
            # if there is grade & tonnage from the users, prefer it
            curated_gt_sites = [
                s for s in gt_sites if not is_system_user(s["created_by"])
            ]
            if len(curated_gt_sites) > 0:
                # choose based on the latest modified date
                gtsite = max(
                    curated_gt_sites,
                    key=lambda x: x["modified_at"],
                )
                gt = GradeTonnage(
                    commodity=commodity,
                    total_contained_metal=gtsite["total_contained_metal"],
                    total_tonnage=gtsite["total_tonnage"],
                    total_grade=gtsite["total_grade"],
                )
            else:
                # no curated grade & tonnage, choose the one with the highest contained metal
                # TODO: choose the one with the most recent date
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
        gts.append(gt)

    return DedupMineralSitePublic(
        id=dms,
        name=site_name,
        type=site_type,
        rank=site_rank,
        sites=[
            DedupMineralSiteIdAndScore(id=mr.id(sid), score=sscore)
            for sid, sscore in ranked_site_id_and_scores
        ],
        deposit_types=sorted(deposit_types, key=lambda x: x.confidence, reverse=True)[
            :5
        ],
        location=location,
        grade_tonnage=gts,
        modified_at=max(sites[0]["modified_at"] for sites in sid2sites.values()),
    )


def get_ms_source_score(
    source_score: Optional[float],
    created_by: str,
    modified_at: str,
    default_score: float,
):
    if source_score is None or source_score < 0:
        source_score = default_score
    if not is_system_user(created_by):
        # expert get the highest priority
        return (100.0, modified_at)
    return (min(source_score, 99.0), modified_at)
