from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import httpx
import orjson
import serde.json
import timer
from loguru import logger
from minmodkg.api.models.public_dedup_mineral_site import DedupMineralSitePublic
from minmodkg.integrations.cdr.cdr_helper import (
    MINMOD_API,
    MINMOD_SYSTEM,
    CDRHelper,
    MinmodHelper,
    retry_request,
)
from minmodkg.integrations.cdr.cdr_schemas import (
    DedupSite,
    DedupSiteRecord,
    DepositType,
    DepositTypeCandidate,
    DocumentReference,
)
from minmodkg.integrations.cdr.cdr_schemas import (
    MineralInventory as CDRMineralInventory,
)
from minmodkg.integrations.cdr.cdr_schemas import MineralSite
from minmodkg.models.kg.base import MINMOD_NS
from minmodkg.models.kg.mineral_inventory import MineralInventory
from minmodkg.typing import InternalID
from tqdm import tqdm


def sync_deposit_types():
    deposit_type_resp = retry_request(
        lambda: httpx.get(
            f"{MINMOD_API}/deposit-types",
            verify=False,
            timeout=None,
        )
    )

    deposit_types = []
    for record in deposit_type_resp.json():
        deposit_types.append(
            DepositType(
                id=MINMOD_NS.mr.id(record["uri"]),
                name=record["name"],
                group=record["group"],
                environment=record["environment"],
            ).model_dump(exclude_none=True)
        )

    # fetch deposit types from CDR and check if they are the same first
    exist_dpts = {
        (r["id"], r["name"], r["group"], r["environment"])
        for r in CDRHelper.fetch(CDRHelper.DepositType)
    }
    if exist_dpts == {
        (r["id"], r["name"], r["group"], r["environment"]) for r in deposit_types
    }:
        logger.info("Deposit types are the same, skipping update")
        return

    CDRHelper.truncate(CDRHelper.DepositType)
    CDRHelper.upload_collection(CDRHelper.DepositType, deposit_types)
    logger.info("Deposit types updated!")


def sync_mineral_sites():
    commodity_uri2name = MinmodHelper.get_commodity_uri2name()

    ms_resp = httpx.get(
        f"{MINMOD_API}/cdr/mining-report", verify=False, timeout=None
    ).raise_for_status()

    mineral_sites = []
    for ms in ms_resp.json():
        invs = []
        for mi in ms["inventories"]:
            inv = MineralInventory.from_dict(mi)
            if inv.commodity.normalized_uri not in commodity_uri2name:
                continue
            if len(inv.reference.page_info) == 0:
                continue

            invs.append(
                CDRMineralInventory(
                    commodity=commodity_uri2name[inv.commodity.normalized_uri],
                    documents=[
                        DocumentReference(
                            cdr_id=ms["record_id"],
                            page=page.page,
                        )
                        for page in inv.reference.page_info
                    ],
                )
            )

        mineral_sites.append(
            MineralSite(
                id=ms["site_id"],
                source_id=ms["source_id"],
                record_id=ms["record_id"],
                name=ms["name"] or "",
                mineral_inventory=invs,
                validated=False,
                system=MINMOD_SYSTEM,
                system_version="2.0.0a",
            ).model_dump()
        )

    CDRHelper.truncate(CDRHelper.MineralSite)
    CDRHelper.upload_collection(CDRHelper.MineralSite, mineral_sites)


def format_dedup_site(
    dedup_site: DedupMineralSitePublic,
    commodity_id2name: dict[InternalID, str],
    country_id2name: dict[InternalID, str],
    province_id2name: dict[InternalID, str],
) -> list[DedupSite]:
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

    # commodity must be unique to each record to ensure unique id
    assert len({gt.commodity for gt in dedup_site.grade_tonnage}) == len(
        dedup_site.grade_tonnage
    ), dedup_site.id

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


def format_dedup_sites(
    dedup_sites: list[dict],
):
    commodity_id2name = MinmodHelper.get_commodity_id2name()
    country_id2name = MinmodHelper.get_country_id2name()
    province_id2name = MinmodHelper.get_province_id2name()

    output = []
    for dedup_site in dedup_sites:
        for fmt_dedup_site in format_dedup_site(
            DedupMineralSitePublic.from_dict(dedup_site),
            commodity_id2name,
            country_id2name,
            province_id2name,
        ):
            output.append(
                orjson.loads(fmt_dedup_site.model_dump_json(exclude_none=True))
            )
    return output


def sync_dedup_mineral_sites(
    cache_dir: Optional[Union[str, Path]] = None,
    prev_cache_dir: Optional[Union[str, Path]] = None,
):

    rerun_fetch_dedup_sites = True
    uploaded_mineral_sites = set()

    if cache_dir is not None:
        cache_dir = Path(cache_dir)
        cache_dedup_site_file = cache_dir / "dedup_sites.json"

        rerun_fetch_dedup_sites = not cache_dedup_site_file.exists()
        uploaded_mineral_sites = {
            msid
            for infile in cache_dir.glob("uploaded_sites_b*.json")
            for msid in serde.json.deser(infile)
        }

    if rerun_fetch_dedup_sites:
        with timer.Timer().watch_and_report(
            "Fetching dedup mineral sites", print_fn=logger.info
        ):
            resp = retry_request(
                lambda: httpx.get(
                    f"{MINMOD_API}/dedup-mineral-sites",
                    params={},
                    verify=False,
                    timeout=None,
                )
            )
            dedup_sites = resp.json()

            # check duplicated site ids
            id2site = {}
            for dms in dedup_sites:
                if dms["id"] in id2site:
                    raise ValueError(f"Duplicate site ID: {dms['id']}")
                id2site[dms["id"]] = dms

            # store the results
            if cache_dir is not None:
                serde.json.ser(dedup_sites, cache_dedup_site_file)
    else:
        dedup_sites = serde.json.deser(cache_dedup_site_file)

    with timer.Timer().watch_and_report(
        "Format dedup mineral sites", print_fn=logger.info
    ):
        cdr_dedup_sites = format_dedup_sites(dedup_sites)
    reset_cdr = True

    if prev_cache_dir is not None and (Path(prev_cache_dir) / "_SUCCESS").exists():
        # we can compare the previous sync and only upload the difference
        # if this exceed a threshold, we should just start from scratch
        with timer.Timer().watch_and_report(
            "Load previous CDR dedup mineral sites", print_fn=logger.info
        ):
            prev_dedup_sites = serde.json.deser(
                Path(prev_cache_dir) / "dedup_sites.json"
            )
            prev_cdr_dedup_sites = format_dedup_sites(prev_dedup_sites)

        with timer.Timer().watch_and_report(
            "Compute differences compared to the previous run", print_fn=logger.info
        ):
            # find the difference between cdr_dedup_sites and prev_cdr_dedup_sites
            id2prev = {dms["id"]: dms for dms in prev_cdr_dedup_sites}
            replace_dedup_sites = []
            add_dedup_sites = []

            for dms in cdr_dedup_sites:
                if dms["id"] in id2prev:
                    if dms["data_snapshot_date"] != id2prev[dms["id"]][
                        "data_snapshot_date"
                    ] or {ms["id"] for ms in dms["sites"]} != {
                        ms["id"] for ms in id2prev[dms["id"]]["sites"]
                    }:
                        replace_dedup_sites.append(dms)
                else:
                    add_dedup_sites.append(dms)

        # more than 10K sites to replace or add, we should start from scratch
        reset_cdr = (len(replace_dedup_sites) + len(add_dedup_sites)) > 10000
        if not reset_cdr:
            logger.info("Performing incremental update")
            # we can just replace the dedup sites
            CDRHelper.delete_collection(CDRHelper.DedupSites, replace_dedup_sites)
            CDRHelper.upload_collection(
                CDRHelper.DedupSites, replace_dedup_sites + add_dedup_sites
            )
            if cache_dir is not None:
                (cache_dir / "_SUCCESS").touch()
            logger.info("Sync dedup mineral sites done!")
            return

    logger.info("Performing full replacement")
    # we have to start from scratch
    if len(uploaded_mineral_sites) == 0:
        # first time uploading -- truncate the collection
        CDRHelper.truncate(CDRHelper.DedupSites)

    # filter out sites that have already been uploaded -- this only happens when there is an error from CDR.
    cdr_dedup_sites = [
        dms for dms in cdr_dedup_sites if dms["id"] not in uploaded_mineral_sites
    ]

    batch_size = 5000
    for i in tqdm(
        list(range(0, len(cdr_dedup_sites), batch_size)), "Uploading dedup sites"
    ):
        CDRHelper.upload_collection(
            CDRHelper.DedupSites, cdr_dedup_sites[i : i + batch_size]
        )
        if cache_dir is not None:
            serde.json.ser(
                [dms["id"] for dms in cdr_dedup_sites[i : i + batch_size]],
                cache_dir / f"uploaded_sites_b{i:03d}.json",
            )

    if cache_dir is not None:
        (cache_dir / "_SUCCESS").touch()
    logger.info("Sync dedup mineral sites done!")


if __name__ == "__main__":
    CDRHelper.truncate(CDRHelper.MineralSite)
    sync_mineral_sites()
    # sync_deposit_types()
    # sync_dedup_mineral_sites(f"data/ta2-output/{datetime.now().strftime('%Y-%m-%d')}")
