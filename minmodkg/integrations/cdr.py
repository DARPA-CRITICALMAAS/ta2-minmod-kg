from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import httpx
import orjson
import serde.json
import shapely.wkt
import timer
from minmodkg.grade_tonnage_model import Mt_unit, percent_unit
from minmodkg.integrations.cdr_helper import (
    MINMOD_API,
    MINMOD_SYSTEM,
    CDRHelper,
    MinmodHelper,
)
from minmodkg.integrations.cdr_schemas import (
    DedupSite,
    DedupSiteRecord,
    DepositType,
    DepositTypeCandidate,
)
from minmodkg.models.base import MINMOD_NS
from minmodkg.models.dedup_mineral_site import DedupMineralSitePublic
from minmodkg.typing import InternalID
from tqdm import tqdm


def sync_deposit_types():
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
                    id=MINMOD_NS.mr.id(record["uri"]),
                    name=record["name"],
                    group=record["group"],
                    environment=record["environment"],
                ).model_dump_json(exclude_none=True)
            )
        )

    CDRHelper.truncate(CDRHelper.DepositType)
    CDRHelper.upload_collection(CDRHelper.DepositType, deposit_types)


def sync_commodities():
    pass


def format_dedup_site(
    dedup_site: DedupMineralSitePublic,
    commodity_id2name: dict[InternalID, str],
    country_id2name: dict[InternalID, str],
    province_id2name: dict[InternalID, str],
):
    output = []

    base = DedupSite(
        id=dedup_site.id,
        sites=[
            DedupSiteRecord(
                id=f"{dedup_site.id}___{site.id}",
                mineral_site_id=site.id,
            )
            for site in dedup_site.sites
        ],
        commodity="",
        contained_metal=None,
        contained_metal_units="million tonnes",
        tonnage=None,
        tonnage_units="million tonnes",
        grade=None,
        grade_units="percent",
        crs="EPSG:4326",
        centroid="",
        geom="",
        deposit_type_candidate=[
            DepositTypeCandidate(
                deposit_type_id=dt.id,
                confidence=dt.confidence,
                source=dt.source,
            )
            for dt in dedup_site.deposit_types
        ],
        system=MINMOD_SYSTEM,
        system_version="2.0.0a",
        data_snapshot="",
        data_snapshot_date=dedup_site.modified_at,
    )

    base.sites[0].name = dedup_site.name
    base.sites[0].site_type = dedup_site.type
    base.sites[0].site_rank = dedup_site.rank

    if dedup_site.location is not None:
        if dedup_site.location.lat is not None and dedup_site.location.lon is not None:
            base.centroid = (
                f"POINT ({dedup_site.location.lon} {dedup_site.location.lat})"
            )
            base.geom = base.centroid

        if len(dedup_site.location.country) > 0:
            base.sites[0].country = ", ".join(
                [country_id2name[id] for id in dedup_site.location.country]
            )
        if len(dedup_site.location.state_or_province) > 0:
            base.sites[0].province = ", ".join(
                [province_id2name[id] for id in dedup_site.location.state_or_province]
            )

    for gt in dedup_site.grade_tonnage:
        r = base.model_copy()
        r.id = f"{dedup_site.id}?commodity={gt.commodity}"
        r.sites = [s.model_copy() for s in base.sites]
        for site in r.sites:
            site.id = f"{r.id}___{site.id}"

        r.commodity = commodity_id2name[gt.commodity]
        r.contained_metal = gt.total_contained_metal
        r.tonnage = gt.total_tonnage
        r.grade = gt.total_grade

        output.append(r)

    return output


def sync_dedup_mineral_sites(cache_dir: Optional[Union[str, Path]] = None):
    commodity_id2name = MinmodHelper.get_commodity_id2name()
    country_id2name = MinmodHelper.get_country_id2name()
    province_id2name = MinmodHelper.get_province_id2name()

    dedup_sites = None
    if cache_dir is None:
        rerun = True
        outfile = None
    else:
        outfile = Path(cache_dir) / "data.json"
        outfile.parent.mkdir(parents=True, exist_ok=True)
        if outfile.exists():
            dedup_sites = serde.json.deser(outfile)
            rerun = False
        else:
            rerun = True

    if rerun:
        with timer.Timer().watch_and_report("Fetching dedup mineral sites"):
            resp = httpx.get(
                f"{MINMOD_API}/dedup-mineral-sites",
                params={},
                verify=False,
                timeout=None,
            )
            resp.raise_for_status()
        dedup_sites = resp.json()
        if outfile is not None:
            serde.json.ser(dedup_sites, outfile)

    assert dedup_sites is not None
    inputs = []
    for dedup_site in dedup_sites:
        for fmt_dedup_site in format_dedup_site(
            DedupMineralSitePublic.model_validate(dedup_site),
            commodity_id2name,
            country_id2name,
            province_id2name,
        ):
            inputs.append(
                orjson.loads(fmt_dedup_site.model_dump_json(exclude_none=True))
            )

    CDRHelper.truncate(CDRHelper.DedupSites)
    batch_size = 5000
    for i in tqdm(list(range(0, len(inputs), batch_size)), "Uploading dedup sites"):
        CDRHelper.upload_collection(CDRHelper.DedupSites, inputs[i : i + batch_size])


if __name__ == "__main__":
    # sync_deposit_types()
    sync_dedup_mineral_sites(f"data/ta2-output/{datetime.now().strftime('%Y-%m-%d')}")
