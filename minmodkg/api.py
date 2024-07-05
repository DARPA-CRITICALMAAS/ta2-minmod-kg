from __future__ import annotations

import os
from functools import lru_cache
from typing import Annotated, Literal, Optional

import networkx as nx
import pandas as pd
from fastapi import APIRouter, FastAPI, Header, HTTPException, Response
from minmodkg.grade_tonnage_model import (
    GradeTonnageEstimate,
    GradeTonnageModel,
    ResourceCategory,
    SiteGradeTonnage,
)
from minmodkg.misc import group_by_key, merge_wkt, run_sparql_query

"""
An endpoint to allow querying derived data from the Minmod knowledge graph.
"""
app = FastAPI(openapi_url="/api/v1/openapi.json", docs_url="/api/v1/docs")
DEFAULT_ENDPOINT = os.environ.get("SPARQL_ENDPOINT", "https://minmod.isi.edu/sparql")
MNR_NS = "https://minmod.isi.edu/resource/"
router = APIRouter(
    prefix="/api/v1",
)


@router.get("/deposit_types")
def deposit_types():
    return get_deposit_types(get_snapshot_id())


@router.get("/commodities")
def commodities():
    return get_commodities(get_snapshot_id())


@router.get("/mineral_site_grade_and_tonnage/{commodity}")
def mineral_site_grade_and_tonnage(
    commodity: str,
    norm_tonnage_unit: Optional[str] = None,
    norm_grade_unit: Optional[str] = None,
    date_precision: Literal["year", "month", "day"] = "month",
    accept: Annotated[str | None, Header()] = None,
):
    commodity = norm_commodity(commodity)
    output = get_grade_tonnage_inventory(
        get_snapshot_id(),
        commodity,
        norm_tonnage_unit=norm_tonnage_unit,
        norm_grade_unit=norm_grade_unit,
        date_precision=date_precision,
    )
    if accept is not None and "text/csv" in accept:
        df = pd.DataFrame(output)
        return Response(
            df.to_csv(index=False, float_format="%.5f"), media_type="text/csv"
        )
    return output


@router.get("/mineral_site_deposit_types/{commodity}")
def mineral_site_deposit_types(
    commodity: str,
    accept: Annotated[str | None, Header()] = None,
):
    commodity = norm_commodity(commodity)
    output = get_deposit_type_classification(
        get_snapshot_id(),
        commodity,
    )
    if accept is not None and "text/csv" in accept:
        df = pd.DataFrame(output)
        return Response(
            df.to_csv(index=False, float_format="%.5f"), media_type="text/csv"
        )
    return output


@router.get("/mineral_site_location/{commodity}")
def mineral_site_location(
    commodity: str,
    accept: Annotated[str | None, Header()] = None,
):
    commodity = norm_commodity(commodity)
    output = get_mineral_site_location(
        get_snapshot_id(),
        commodity,
    )
    if accept is not None and "text/csv" in accept:
        df = pd.DataFrame(output)
        return Response(
            df.to_csv(index=False, float_format="%.5f"), media_type="text/csv"
        )
    return output


@router.get("/hyper_mineral_sites/{commodity}")
def hyper_mineral_sites(
    commodity: str,
    norm_tonnage_unit: Optional[str] = None,
    norm_grade_unit: Optional[str] = None,
    date_precision: Literal["year", "month", "day"] = "month",
    accept: Annotated[str | None, Header()] = None,
):
    commodity = norm_commodity(commodity)
    output = get_hyper_mineral_site_data(
        get_snapshot_id(),
        commodity,
        norm_tonnage_unit=norm_tonnage_unit,
        norm_grade_unit=norm_grade_unit,
        date_precision=date_precision,
    )
    if accept is not None and "text/csv" in accept:
        df = pd.DataFrame(output)
        return Response(
            df.to_csv(index=False, float_format="%.5f"), media_type="text/csv"
        )
    return output


def get_snapshot_id(endpoint=DEFAULT_ENDPOINT):
    query = "SELECT ?snapshot_id WHERE { mnr:kg dcterms:hasVersion ?snapshot_id }"
    qres = run_sparql_query(query, endpoint)
    return qres[0]["snapshot_id"]


def is_minmod_id(text: str) -> bool:
    return text.startswith("Q") and text[1:].isdigit()


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


@lru_cache(maxsize=1)
def get_commodities(snapshot_id: str, endpoint=DEFAULT_ENDPOINT):
    query = """
    SELECT ?uri ?name
    WHERE {
        ?uri a :Commodity ;
            rdfs:label ?name .
    }
    """
    qres = run_sparql_query(query, endpoint)
    return qres


@lru_cache(maxsize=1)
def get_deposit_types(snapshot_id: str, endpoint=DEFAULT_ENDPOINT):
    query = """
    SELECT ?uri ?name ?environment ?group
    WHERE {
        ?uri a :DepositType ;
            rdfs:label ?name ;
            :environment ?environment ;
            :group ?group .
    }
    """
    qres = run_sparql_query(query, endpoint)
    return qres


@lru_cache(maxsize=32)
def get_hyper_mineral_site_data(
    snapshot_id: str,
    commodity: str,
    norm_tonnage_unit=None,
    norm_grade_unit=None,
    date_precision: Literal["year", "month", "day"] = "month",
    endpoint=DEFAULT_ENDPOINT,
):
    site2group = get_site_group(snapshot_id, endpoint)
    sites_info = get_mineral_site_location(
        snapshot_id=snapshot_id, commodity=commodity, endpoint=endpoint
    )
    dpt_info = get_deposit_type_classification(
        snapshot_id=snapshot_id, commodity=commodity, endpoint=endpoint
    )
    grade_tonnage_info = get_grade_tonnage_inventory(
        snapshot_id=snapshot_id,
        commodity=commodity,
        norm_tonnage_unit=norm_tonnage_unit,
        norm_grade_unit=norm_grade_unit,
        date_precision=date_precision,
        endpoint=endpoint,
    )

    # ---- 1. find top grade-tonnage per each hypersite ----
    for record in grade_tonnage_info:
        record["group_id"] = site2group[record["ms"]]
    group2best_gt = {}
    for group_id, lst in group_by_key(grade_tonnage_info, "group_id").items():
        # group by 'group_id' and find the entry with the highest 'tot_contained_metal'
        lst = [x for x in lst if x["tot_contained_metal"] is not None]
        if len(lst) == 0:
            continue
        group2best_gt[group_id] = max(lst, key=lambda x: x["tot_contained_metal"])

    # ---- 2. find top deposit type per each hypersite ----
    for record in dpt_info:
        record["group_id"] = site2group[record["ms"]]
    group2best_dpt = {}
    for group_id, lst in group_by_key(dpt_info, "group_id").items():
        # group by 'group_id' and find the entry with the highest 'top1_deposit_classification_confidence'
        lst = [
            x for x in lst if x["top1_deposit_classification_confidence"] is not None
        ]
        if len(lst) == 0:
            continue
        group2best_dpt[group_id] = max(
            lst, key=lambda x: x["top1_deposit_classification_confidence"]
        )

    # ---- 3. squash into single-row hypersites ----
    output = []
    for group_id, lst in group_by_key(sites_info, "group_id").items():
        record: dict = {"group_id": group_id}

        for key in [
            "ms",
            "ms_name",
            "ms_type",
            "ms_rank",
            "country",
            "state_or_province",
        ]:
            record[key] = sorted({x[key] for x in lst if x[key] is not None})

        record["loc_crs"] = list(
            {x["loc_crs"] for x in lst if x["loc_crs"] is not None}
        )
        record["loc_wkt"] = sorted(
            {x["loc_wkt"] for x in lst if x["loc_wkt"] is not None}
        )
        if len(record["loc_wkt"]) > 1:
            record["loc_wkt"] = merge_wkt(record["loc_wkt"])

        for key in record:
            if isinstance(record[key], list):
                if len(record[key]) == 1:
                    record[key] = record[key][0]
                elif len(record[key]) == 0:
                    record[key] = None

        for key in ["tot_contained_metal", "total_tonnage", "total_grade"]:
            if group_id not in group2best_gt:
                record[key] = None
            else:
                record[key] = group2best_gt[group_id][key]

        for key in [
            "top1_deposit_type",
            "top1_deposit_group",
            "top1_deposit_environment",
            "top1_deposit_classification_confidence",
            "top1_deposit_classification_source",
        ]:
            if group_id not in group2best_dpt:
                record[key] = None
            else:
                record[key] = group2best_dpt[group_id][key]

        output.append(record)
    return output


@lru_cache(maxsize=32)
def get_mineral_site_location(
    snapshot_id: str,
    commodity: str,
    endpoint=DEFAULT_ENDPOINT,
) -> list[dict]:
    query = (
        """
    SELECT 
        ?ms 
        ?ms_name 
        ?ms_type 
        ?ms_rank
        ?state_or_province
        ?state_or_province_name
        ?country
        ?country_name
        ?loc_crs
        ?loc_crs_name
        ?loc_wkt 
    WHERE {
        { SELECT DISTINCT ?ms WHERE { ?ms :mineral_inventory/:commodity/:normalized_uri %s . } }
        
        OPTIONAL { ?ms rdfs:label ?ms_name . }
        OPTIONAL { ?ms :site_type ?ms_type . }
        OPTIONAL { ?ms :site_rank ?ms_rank . }
        OPTIONAL { 
            ?ms :location_info ?loc .
            
            OPTIONAL { 
                ?loc :country/:normalized_uri ?country .
                ?country rdfs:label ?country_name .
            }
            OPTIONAL {
                ?loc :state_or_province/:normalized_uri ?state_or_province . 
                ?state_or_province rdfs:label ?state_or_province_name .
            }
            OPTIONAL {
                ?loc :crs/:normalized_uri ?loc_crs .
                ?loc_crs rdfs:label ?loc_crs_name .
            }
            OPTIONAL { ?loc :location ?loc_wkt . }
        }
    }
    """
        % f"mnr:{commodity}"
    )
    qres = run_sparql_query(query, endpoint)

    site2group = get_site_group(snapshot_id, endpoint)
    output = []

    for row in qres:
        record = {
            "ms": row["ms"],
            "ms_name": row["ms_name"],
            "ms_type": row["ms_type"],
            "ms_rank": row["ms_rank"],
            "country": row.get("country_name", None),
            "state_or_province": row.get("state_or_province_name", None),
            "loc_crs": row.get("loc_crs_name", None),
            "loc_wkt": row.get("loc_wkt", None),
        }

        if all(
            record[key] is None
            for key in ["country", "state_or_province", "loc_crs", "loc_wkt"]
        ):
            continue
        record["group_id"] = site2group[row["ms"]]
        output.append(record)
    return output


@lru_cache(maxsize=32)
def get_deposit_type_classification(
    snapshot_id: str,
    commodity: str,
    endpoint=DEFAULT_ENDPOINT,
):
    query = """
    SELECT
        ?ms
        ?ms_name
        ?deposit_name
        ?deposit_source
        ?deposit_confidence
        ?deposit_group
        ?deposit_environment
        ?country
        ?country_name
        ?loc_crs
        ?loc_crs_name
        ?loc_wkt
        ?state_or_province
        ?state_or_province_name
    WHERE {
        # filter by commodity
        ?ms :mineral_inventory/:commodity/:normalized_uri %s .

        # get deposit type classification
        ?ms :deposit_type_candidate ?deposit_candidate_uri .
        ?deposit_candidate_uri :source ?deposit_source .
        ?deposit_candidate_uri :confidence ?deposit_confidence .
        ?deposit_candidate_uri :normalized_uri [
            rdfs:label ?deposit_name ;
            :group ?deposit_group ;
            :environment ?deposit_environment ] .

        OPTIONAL { ?ms rdfs:label ?ms_name . }
        OPTIONAL { 
            ?ms :location_info ?loc .
            
            OPTIONAL { 
                ?loc :country/:normalized_uri ?country .
                ?country rdfs:label ?country_name .
            }
            OPTIONAL {
                ?loc :state_or_province/:normalized_uri ?state_or_province . 
                ?state_or_province rdfs:label ?state_or_province_name .
            }
            OPTIONAL {
                ?loc :crs/:normalized_uri ?loc_crs .
                ?loc_crs rdfs:label ?loc_crs_name .
            }
            OPTIONAL { ?loc :location ?loc_wkt . }
        }
    }
    """ % (
        f"mnr:{commodity}"
    )
    qres = run_sparql_query(query, endpoint)
    # Old code copy from `generate_ta2_outputs.py`
    deposits_data = pd.DataFrame(
        [
            {
                "ms": row["ms"],
                "ms_name": row["ms_name"],
                "country": row.get("country_name", None),
                "state_or_province": row.get("state_or_province_name", None),
                "loc_crs": row.get("loc_crs_name", None),
                "loc_wkt": row.get("loc_wkt", None),
                "deposit_type": row.get("deposit_name", None),
                "deposit_group": row.get("deposit_group", None),
                "deposit_environment": row.get("deposit_environment", None),
                "deposit_classification_confidence": row.get(
                    "deposit_confidence", None
                ),
                "deposit_classification_source": row.get("deposit_source", None),
            }
            for row in qres
        ]
    )

    deposits_df = deposits_data.drop_duplicates()
    deposits_df.reset_index(drop=True, inplace=True)
    deposits_df.set_index(["ms", "deposit_type"], inplace=True)
    # deposits_df['info_count'] = deposits_df[['country', 'state_or_province', 'loc_crs', 'loc_wkt']].apply(lambda x: ((x != '') & (x.notna())).sum(), axis=1)
    deposits_df["info_count"] = deposits_df[
        [
            "country",
            "state_or_province",
            "loc_crs",
            "loc_wkt",
            "deposit_classification_confidence",
        ]
    ].apply(lambda x: (((x[:-1] != "") & (x[:-1].notna())).sum(), float(x[-1])), axis=1)
    deposits_df_ordered = deposits_df.sort_values(by="info_count", ascending=False)
    deposits_df_ordered = deposits_df_ordered[
        ~deposits_df_ordered.index.duplicated(keep="first")
    ]
    deposits_df_ordered.drop(columns=["info_count"], inplace=True)
    deposits_df_ordered["deposit_classification_confidence"] = deposits_df_ordered[
        "deposit_classification_confidence"
    ].astype(float)
    deposits_df_ordered.reset_index(inplace=True)

    grouped = deposits_df_ordered.groupby("ms")
    results = []
    for name, group in grouped:
        # Sort by 'deposit_classification_confidence'
        unique_group = group.drop_duplicates(
            subset=[
                "deposit_type",
                "deposit_group",
                "deposit_environment",
                "deposit_classification_confidence",
                "deposit_classification_source",
            ]
        )
        sorted_group = unique_group.sort_values(
            "deposit_classification_confidence", ascending=False
        )
        top_5 = sorted_group.head(5)

        result_row = {"ms": name}
        for i, row in enumerate(top_5.itertuples(index=False), start=1):
            result_row[f"top{i}_deposit_type"] = row.deposit_type
            result_row[f"top{i}_deposit_group"] = row.deposit_group
            result_row[f"top{i}_deposit_environment"] = row.deposit_environment
            result_row[f"top{i}_deposit_classification_confidence"] = (
                row.deposit_classification_confidence
            )
            result_row[f"top{i}_deposit_classification_source"] = (
                row.deposit_classification_source
            )

        results.append(result_row)

    final_df = pd.DataFrame(results)
    unique_ms_data = deposits_df_ordered[
        ["ms", "ms_name", "country", "state_or_province", "loc_crs", "loc_wkt"]
    ].drop_duplicates(subset=["ms"])
    merged_deposits_df = pd.merge(unique_ms_data, final_df, on="ms", how="left")

    # fix me: temporary code to deal with nan values
    output = []
    for record in merged_deposits_df.to_dict("records"):
        for k in record:
            if pd.isna(record[k]):
                record[k] = None
        output.append(record)
    return output


@lru_cache(maxsize=32)
def get_grade_tonnage_inventory(
    snapshot_id: str,
    commodity: str,
    norm_tonnage_unit=None,
    norm_grade_unit=None,
    date_precision: Literal["year", "month", "day"] = "month",
    endpoint=DEFAULT_ENDPOINT,
):
    query = """
    SELECT 
        ?ms                       # Mineral Site URI
        ?ms_name                  # Mineral Site Name
        ?country                  # Country
        ?country_name
        ?loc_crs                  # WKT CRS
        ?loc_crs_name
        ?loc_wkt                  # WKT Geometry
        ?state_or_province
        ?state_or_province_name
        ?doc
        
        ?mi
        ?mi_cat
        ?mi_date
        ?mi_zone

        ?mi_ore_value
        ?mi_ore_unit
        ?mi_grade_value
        ?mi_grade_unit
    WHERE {
        ?ms :mineral_inventory ?mi .

        ?mi :category/:normalized_uri ?mi_cat ;
            :commodity/:normalized_uri %s ;
            :grade [
                :value ?mi_grade_value ;
                :unit/:normalized_uri ?mi_grade_unit 
            ] ;
            :ore [
                :value ?mi_ore_value ;
                :unit/:normalized_uri ?mi_ore_unit 
            ] .

        OPTIONAL { ?ms rdfs:label ?ms_name . }
        OPTIONAL { 
            ?ms :location_info ?loc .
            
            OPTIONAL { 
                ?loc :country/:normalized_uri ?country .
                ?country rdfs:label ?country_name .
            }
            OPTIONAL {
                ?loc :state_or_province/:normalized_uri ?state_or_province . 
                ?state_or_province rdfs:label ?state_or_province_name .
            }
            OPTIONAL {
                ?loc :crs/:normalized_uri ?loc_crs .
                ?loc_crs rdfs:label ?loc_crs_name .
            }
            OPTIONAL { ?loc :location ?loc_wkt . }
        }

        OPTIONAL { ?mi :reference/:document/:uri ?doc . }
        OPTIONAL { ?mi :zone ?mi_zone . }
        OPTIONAL { ?mi :date ?mi_date . }
    }
    """ % (
        f"mnr:{commodity}"
    )
    qres = run_sparql_query(
        query,
        endpoint,
        [
            "ms_name",
            "loc",
            "country",
            "country_name",
            "state_or_province",
            "state_or_province_name",
            "loc_crs",
            "loc_crs_name",
            "loc_wkt",
            "doc",
            "mi_zone",
            "mi_date",
        ],
    )
    # compute grade & tonnage for each mineral site
    grade_tonnage_model = GradeTonnageModel()

    output = []
    for ms, invs in group_by_key(qres, "ms").items():
        grade_tonnage = grade_tonnage_model(
            [
                GradeTonnageModel.MineralInventory(
                    id=inv,
                    date=inv_props[0]["mi_date"],
                    zone=inv_props[0]["mi_zone"],
                    category=[x["mi_cat"] for x in inv_props],
                    ore_value=inv_props[0]["mi_ore_value"],
                    ore_unit=inv_props[0]["mi_ore_unit"],
                    grade_value=inv_props[0]["mi_grade_value"],
                    grade_unit=inv_props[0]["mi_grade_unit"],
                )
                for inv, inv_props in group_by_key(invs, "mi").items()
            ],
            norm_tonnage_unit=norm_tonnage_unit,
            norm_grade_unit=norm_grade_unit,
        )
        record = {
            "ms": ms,
            "ms_name": invs[0]["ms_name"],
            "country": invs[0]["country_name"],
            "state_or_province": invs[0]["state_or_province_name"],
            "loc_crs": invs[0]["loc_crs_name"],
            "loc_wkt": invs[0]["loc_wkt"],
            "doc_url": invs[0]["doc"],
        }

        record.update(**fmt_grade_tonnage(grade_tonnage, norm_grade_unit))
        output.append(record)
    return output


@lru_cache(maxsize=1)
def get_site_group(snapshot_id: str, endpoint=DEFAULT_ENDPOINT):
    query = "SELECT ?s1 ?s2 WHERE { ?s1 a :MineralSite . ?s2 a :MineralSite . ?s1 owl:sameAs ?s2 . }"
    output = run_sparql_query(query, endpoint)
    G = nx.from_edgelist([(row["s1"], row["s2"]) for row in output])
    groups = nx.connected_components(G)

    mapping = {}
    for gid, group in enumerate(groups, start=1):
        for node in group:
            mapping[node] = gid

    max_group_id = max(mapping.values())

    query = "SELECT ?s1 WHERE { ?s1 a :MineralSite . FILTER NOT EXISTS { ?s1 owl:sameAs ?s2 . } }"
    output = run_sparql_query(query, DEFAULT_ENDPOINT)
    unlinked_sites = sorted(row["s1"] for row in output)
    for i, site in enumerate(unlinked_sites, start=1):
        mapping[site] = max_group_id + i

    return mapping


def fmt_grade_tonnage(
    grade_tonnage: Optional[SiteGradeTonnage], norm_grade_unit: Optional[str] = None
) -> dict:
    if grade_tonnage is None:
        grade_tonnage = SiteGradeTonnage()

    record = {
        "tot_contained_metal": grade_tonnage.total_resource_contained_metal,
        "total_tonnage": grade_tonnage.total_resource_tonnage,
        "total_grade": (grade_tonnage.get_total_resource_grade(norm_grade_unit)),
    }

    # just in case we have more data about reserve than resource
    if grade_tonnage.total_reserve_tonnage is not None and (
        grade_tonnage.total_resource_tonnage is None
        or grade_tonnage.total_reserve_tonnage > grade_tonnage.total_resource_tonnage
    ):
        record["total_tonnage"] = grade_tonnage.total_reserve_tonnage
        record["tot_contained_metal"] = grade_tonnage.total_reserve_contained_metal
        record["total_grade"] = grade_tonnage.get_total_reserve_grade(norm_grade_unit)

    return record


app.include_router(router)
