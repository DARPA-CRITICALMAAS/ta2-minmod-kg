from __future__ import annotations

import json
from datetime import datetime
from functools import lru_cache
from os import name
from sys import maxsize
from typing import Annotated, Literal, Optional

import orjson
import serde.csv
from fastapi import APIRouter, Body, HTTPException, Query, Response, status
from htbuilder import H
from minmodkg.api.dependencies import (
    is_minmod_id,
    norm_commodity,
    norm_country,
    norm_deposit_type,
    norm_state_or_province,
)
from minmodkg.api.models.public_dedup_mineral_site import DedupMineralSitePublic
from minmodkg.models.kg.base import MINMOD_NS
from minmodkg.models.kgrel.entities.commodity import Commodity
from minmodkg.services.kgrel_entity import EntityService
from minmodkg.services.mineral_site import MineralSiteService
from minmodkg.typing import InternalID
from slugify import slugify

router = APIRouter(tags=["mineral_sites"])


@router.get("/dedup-mineral-sites")
def dedup_mineral_sites_v2(
    commodity: Optional[str] = None,
    deposit_type: Optional[InternalID] = None,
    country: Optional[InternalID] = None,
    state_or_province: Optional[InternalID] = None,
    has_grade_tonnage: Optional[bool] = None,
    limit: Annotated[int, Query(ge=0)] = 0,
    offset: Annotated[int, Query(ge=0)] = 0,
    return_count: Annotated[bool, Query()] = False,
    format: Annotated[Literal["json", "csv"], Query()] = "json",
):
    if commodity is not None:
        commodity = norm_commodity(commodity)
    if deposit_type is not None:
        deposit_type = norm_deposit_type(deposit_type)
    if country is not None:
        country = norm_country(country)
    if state_or_province is not None:
        state_or_province = norm_state_or_province(state_or_province)

    res = MineralSiteService().find_dedup_mineral_sites(
        commodity=commodity,
        deposit_type=deposit_type,
        country=country,
        state_or_province=state_or_province,
        has_grade_tonnage=has_grade_tonnage,
        limit=limit,
        offset=offset,
        return_count=return_count,
    )

    if format == "json":
        items = [
            DedupMineralSitePublic.from_kgrel(dmsi, commodity).to_dict()
            for dmsi in res["items"].values()
        ]

        if return_count:
            return {
                "items": items,
                "total": res["total"],
            }
        return items

    items = [
        DedupMineralSitePublic.from_kgrel(dmsi, commodity)
        for dmsi in res["items"].values()
    ]
    if format != "csv":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported format: {format}",
        )

    if commodity is not None:
        filename = f"{slugify(get_commodity_map()[commodity].name)}_{datetime.now().strftime(r'%Y%m%d')}.csv"
    else:
        filename = f"all_{datetime.now().strftime(r'%Y%m%d')}.csv"

    return Response(
        content=format_csv(items, commodity),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
        },
    )


@router.post("/dedup-mineral-sites/find_by_ids")
def api_get_dedup_mineral_sites(
    ids: Annotated[list[InternalID], Body(embed=True)],
    commodity: Annotated[InternalID, Body(embed=True)],
) -> dict[InternalID, dict]:
    res = MineralSiteService().find_dedup_mineral_sites(
        commodity=commodity, dedup_site_ids=ids
    )
    return {
        dms_id: DedupMineralSitePublic.from_kgrel(dmsi, commodity).to_dict()
        for dms_id, dmsi in res["items"].items()
    }


@router.get("/dedup-mineral-sites/{dedup_site_id}")
def api_get_dedup_mineral_site(
    dedup_site_id: str,
    commodity: Optional[str] = None,
):
    if commodity is not None:
        commodity = norm_commodity(commodity)
    qres = MineralSiteService().find_dedup_mineral_sites(
        commodity=commodity, dedup_site_ids=[dedup_site_id]
    )
    output = {
        dms_id: DedupMineralSitePublic.from_kgrel(dmsi, commodity)
        for dms_id, dmsi in qres["items"].items()
    }

    if len(output) == 0:
        raise HTTPException(status_code=404, detail=f"{dedup_site_id} not found")
    return output[dedup_site_id].to_dict()


def format_csv(
    lst_dms: list[DedupMineralSitePublic],
    commodity: Optional[InternalID],
) -> str:
    commodity_map = get_commodity_map()
    country_map = get_country_map()
    state_or_province_map = get_state_or_province_map()
    deposit_type_map = get_deposit_type_map()

    header = [
        "URI",
        "Name",
        "Type",
        "Rank",
        "Country",
        "State or Province",
        "Latitude",
        "Longitude",
        "Deposit Type Environment",
        "Deposit Type Group",
        "Deposit Type Name",
        "Deposit Type Confidence",
        "Commodity",
    ]
    if commodity is None:
        header.append("Is Critical Commodity")
    header.extend(
        [
            "Tonnage (Mt)",
            "Grade (%)",
            "Inventory Date",
            "Updated at",
        ]
    )

    name2idx = {n: i for i, n in enumerate(header)}
    rows = [header]

    for dms in lst_dms:
        row = [""] * len(header)
        row[name2idx["URI"]] = dms.uri
        row[name2idx["Name"]] = dms.name
        row[name2idx["Type"]] = dms.type
        row[name2idx["Rank"]] = dms.rank
        if dms.location is not None:
            row[name2idx["Country"]] = ", ".join(
                (country_map[id] for id in dms.location.country)
            )
            row[name2idx["State or Province"]] = ", ".join(
                (state_or_province_map[id] for id in dms.location.state_or_province)
            )

            if dms.location.lat is not None:
                row[name2idx["Latitude"]] = str(dms.location.lat)
            if dms.location.lon is not None:
                row[name2idx["Longitude"]] = str(dms.location.lon)

        if len(dms.deposit_types) > 0:
            dpt = deposit_type_map[dms.deposit_types[0].id]
            row[name2idx["Deposit Type Environment"]] = dpt.environment
            row[name2idx["Deposit Type Group"]] = dpt.group
            row[name2idx["Deposit Type Name"]] = dpt.name
            row[name2idx["Deposit Type Confidence"]] = str(
                dms.deposit_types[0].confidence
            )

        row[name2idx["Updated at"]] = dms.modified_at

        has_commodity = False
        for gt in dms.grade_tonnage:
            newrow = row.copy()
            comm = commodity_map[gt.commodity]
            newrow[name2idx["Commodity"]] = comm.name
            if commodity is None:
                newrow[name2idx["Is Critical Commodity"]] = str(int(comm.is_critical))
            if gt.total_tonnage is not None:
                newrow[name2idx["Tonnage (Mt)"]] = str(gt.total_tonnage)
            if gt.total_grade is not None:
                newrow[name2idx["Grade (%)"]] = str(gt.total_grade)
            if gt.date is not None:
                newrow[name2idx["Inventory Date"]] = gt.date
            rows.append(newrow)
    out = serde.csv.StringIO()
    serde.csv.ser(rows, out)
    return out.getvalue()


# def format_cdr(items: list[DedupMineralSitePublic], commodity: InternalID) -> str:
#     header = [
#         "id",
#         "record_ids",
#         "mineral_site_ids",
#         "names",
#         "type",
#         "rank",
#         "country",
#         "province",
#         "crs",
#         "centroid_epsg_4326",
#         "wkt",
#         "commodity",
#         "contained_metal",
#         "contained_metal_unit",
#         "tonnage",
#         "tonnage_unit",
#         "grade",
#         "grade_unit",
#         "top1_deposit_type",
#         "top1_deposit_group",
#         "top1_deposit_environment",
#         "top1_deposit_classification_confidence",
#         "top1_deposit_classification_source",
#     ]
#     rows = [header]
#     rows = []

#     country_map = get_country_map()

#     for item in items:
#         sites = orjson.dumps([site.id for site in item.sites]).decode()
#         country = ""
#         if item.location is not None:
#             if len(item.location.country) == 1:
#                 country = country_map[item.location.country[0]]
#             else:
#                 country = orjson.dumps(
#                     [country_map[x] for x in item.location.country]
#                 ).decode()
#         if item.location is not None:
#             pass

#         row = {
#             "id": str(item.uri),
#             "record_ids": sites,
#             "mineral_site_ids": sites,
#             "names": item.name,
#             "type": item.type,
#             "rank": item.rank,
#             # "country": item.location.country ,
#         }

#         rows.append(row)

#     out = serde.csv.StringIO()
#     serde.csv.ser(rows, out)
#     return out.getvalue()


@lru_cache(maxsize=None)
def get_country_map():
    lst = EntityService.get_instance().get_countries()
    return {record.id: record.name for record in lst}


@lru_cache(maxsize=None)
def get_state_or_province_map():
    lst = EntityService.get_instance().get_state_or_provinces()
    return {record.id: record.name for record in lst}


@lru_cache(maxsize=None)
def get_commodity_map():
    lst: list[Commodity] = EntityService.get_instance().get_commodities()
    return {record.id: record for record in lst}


@lru_cache(maxsize=None)
def get_deposit_type_map():
    lst = EntityService.get_instance().get_deposit_types()
    return {record.id: record for record in lst}
