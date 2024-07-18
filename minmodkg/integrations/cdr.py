from __future__ import annotations

import os
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import NamedTuple, Optional

import httpx
import orjson
import timer
from cdr_schemas.mineral import (
    DedupSite,
    DedupSiteRecord,
    DepositType,
    DepositTypeCandidate,
    GeoLocationInfo,
    MineralInventory,
    MineralSite,
)
from joblib import Parallel, delayed
from loguru import logger
from minmodkg.grade_tonnage_model import Mt_unit, percent_unit
from minmodkg.integrations.cdr_helper import (
    MINMOD_API,
    MINMOD_SYSTEM,
    CDRHelper,
    MinmodHelper,
)
from minmodkg.misc import MNR_NS, batch, run_sparql_query
from tqdm import tqdm


def replace_deposit_types():
    deposit_type_resp = httpx.get(
        f"{MINMOD_API}/deposit_types",
        verify=False,
        timeout=None,
    )
    deposit_type_resp.raise_for_status()

    deposit_types = []
    for record in deposit_type_resp.json():
        deposit_types.append(
            orjson.loads(
                DepositType(
                    id=record["uri"][len(MNR_NS) :],
                    name=record["name"],
                    group=record["group"],
                    environment=record["environment"],
                ).model_dump_json(exclude_none=True)
            )
        )

    CDRHelper.truncate(CDRHelper.DepositType)
    CDRHelper.upload_collection(CDRHelper.DepositType, deposit_types)


def upload_ta2_output(commodity: str, norm_tonnage_unit: str, norm_grade_unit: str):
    resp = httpx.get(
        f"{MINMOD_API}/dedup_mineral_sites/{commodity}",
        params={
            "norm_tonnage_unit": norm_tonnage_unit,
            "norm_grade_unit": norm_grade_unit,
        },
        verify=False,
        timeout=None,
    )
    resp.raise_for_status()

    dedup_sites = resp.json()

    dpt2id = MinmodHelper.get_deposit_type_to_id()
    commodity_id2name = MinmodHelper.get_commodity_id2name()
    unit_uri2name = MinmodHelper.get_unit_uri2name()

    inputs = []
    for group in dedup_sites:
        inputs.append(
            orjson.loads(
                DedupSite(
                    sites=[
                        DedupSiteRecord(
                            mineral_site_id=site["ms"],
                            name=site["ms_name"] or "",
                            country=site["country"] or "",
                            province=site["state_or_province"] or "",
                            site_rank=site["ms_rank"] or "",
                            site_type=site["ms_type"] or "",
                        )
                        for site in group["sites"]
                    ],
                    commodity=commodity_id2name[group["commodity"]],
                    contained_metal=group["total_contained_metal"],
                    contained_metal_units=unit_uri2name[
                        group["total_contained_metal_unit"]
                    ],
                    tonnage=group["total_tonnage"],
                    tonnage_units=unit_uri2name[group["total_tonnage_unit"]],
                    grade=group["total_grade"],
                    grade_units=unit_uri2name[group["total_grade_unit"]],
                    crs=group["loc_crs"] or "",
                    geom=group["loc_wkt"],
                    deposit_type_candidate=[
                        DepositTypeCandidate(
                            deposit_type_id=dpt2id[dt["name"]],
                            confidence=dt["confidence"],
                            source=dt["source"],
                        )
                        for dt in group["deposit_types"]
                    ],
                    system=MINMOD_SYSTEM,
                    system_version="1.1.3",
                    data_snapshot="",
                    data_snapshot_date="",
                ).model_dump_json(exclude_none=True)
            )
        )

    CDRHelper.upload_collection(CDRHelper.DedupSites, inputs)


if __name__ == "__main__":
    # replace_deposit_types()
    # CDRHelper.truncate(CDRHelper.DedupSites)
    # for commodity in ["zinc", "nickel", "copper", "cobalt"]:
    for commodity in ["cobalt"]:
        upload_ta2_output(
            commodity, norm_tonnage_unit=Mt_unit, norm_grade_unit=percent_unit
        )