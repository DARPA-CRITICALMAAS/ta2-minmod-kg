from __future__ import annotations

import os
from functools import lru_cache

import httpx
import orjson
from cdr_schemas.mineral import (
    DepositTypeCandidate,
    GeoLocationInfo,
    MineralInventory,
    MineralSite,
)
from joblib import Parallel, delayed
from loguru import logger
from minmodkg.misc import MNR_NS, batch, run_sparql_query
from tqdm import tqdm

AUTH_TOKEN = os.environ.get("CDR_AUTH_TOKEN")
MINMOD_API = "https://minmod.isi.edu/api/v1"
CDR_API = "https://api.cdr.land/v1"

cdr_headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}


def create_mineral_site(sites: list[MineralSite]):
    """ """

    def create_site(site: MineralSite):
        r = httpx.delete(
            f"{CDR_API}/minerals/site/{site.id}", headers=cdr_headers, timeout=None
        )
        assert r.status_code == 404 or r.status_code == 204, r.text

        r = httpx.post(
            f"{CDR_API}/minerals/site",
            json=orjson.loads(site.model_dump_json(exclude_none=True)),
            headers=cdr_headers,
            timeout=None,
        )
        r.raise_for_status()

    it = Parallel(n_jobs=-1, return_as="generator_unordered")(
        delayed(create_site)(site) for site in sites
    )
    for _ in tqdm(it, total=len(sites), desc="creating mineral sites"):
        pass


@lru_cache(maxsize=1)
def get_deposit_type_to_id():
    deposit_type_resp = httpx.get(
        f"{MINMOD_API}/deposit_types",
        verify=False,
        timeout=None,
    )
    deposit_type_resp.raise_for_status()

    dpt2id = {}
    for record in deposit_type_resp.json():
        assert record["name"] not in dpt2id
        dpt2id[record["name"]] = record["uri"][len(MNR_NS) :]
    return dpt2id


@lru_cache(maxsize=1)
def get_commodity_name_to_id():
    resp = httpx.get(
        f"{MINMOD_API}/commodities",
        verify=False,
        timeout=None,
    )
    resp.raise_for_status()

    com2id = {}
    for record in resp.json():
        assert record["name"] not in com2id
        com2id[record["name"]] = record["uri"][len(MNR_NS) :]
    return com2id


def get_mineral_site_data_by_commodity(commodity: str):
    """Get mineral site data for a commodity"""
    dpt2id = get_deposit_type_to_id()
    # commodity2id = get_commodity_name_to_id()

    ms_deposity_type_resp = httpx.get(
        f"{MINMOD_API}/mineral_site_deposit_types/{commodity}",
        verify=False,
        timeout=None,
    )
    ms_deposity_type_resp.raise_for_status()

    grade_tonnage_resp = httpx.get(
        f"{MINMOD_API}/mineral_site_grade_and_tonnage/{commodity}",
        verify=False,
        timeout=None,
    )
    grade_tonnage_resp.raise_for_status()

    id2dpt = {
        record["ms"][len(MNR_NS) :]: record for record in ms_deposity_type_resp.json()
    }
    id2gat = {
        record["ms"][len(MNR_NS) :]: record for record in grade_tonnage_resp.json()
    }

    site_ids = sorted(set(id2dpt.keys()).union(id2gat.keys()))

    logger.info("Having total of {} sites", len(site_ids))
    id2siteinfo = {}
    for batch_ids in tqdm(
        batch(100, site_ids), desc="retrieving additional information of mineral sites"
    ):
        tmp = get_extra_mineral_site_data(batch_ids)
        assert len(tmp) == len(batch_ids), (len(tmp), len(batch_ids))
        id2siteinfo.update(tmp)

    sites = []
    for id in tqdm(site_ids, desc="reformat mineral sites"):
        site = MineralSite(
            id=id,
            source_id=id2siteinfo[id]["source_id"],
            record_id=str(id2siteinfo[id]["record_id"]),
            name=id2siteinfo[id]["name"],
            site_rank=id2siteinfo[id]["site_rank"],
            site_type=id2siteinfo[id]["site_type"],
            validated=False,
            system="minmodkg",
            system_version="0.1.0",
        )

        if id in id2dpt:
            record = id2dpt[id]
            if record["country"] is not None:
                site.country = [record["country"]]
            if record["state_or_province"] is not None:
                site.province = [record["state_or_province"]]
            if record["loc_wkt"] is not None:
                site.location = GeoLocationInfo(
                    crs=record["loc_crs"], geom=record["loc_wkt"]
                )
            for k in range(1, 5 + 1):
                if record[f"top{k}_deposit_type"] is not None:
                    site.deposit_type_candidate.append(
                        DepositTypeCandidate(
                            observed_name=record[f"top{k}_deposit_type"],
                            deposit_type_id=dpt2id[record[f"top{k}_deposit_type"]],
                            confidence=record[
                                f"top{k}_deposit_classification_confidence"
                            ],
                            source=record[f"top{k}_deposit_classification_source"],
                        )
                    )

        if id in id2gat:
            record = id2gat[id]
            if record["country"] is not None:
                site.country = [record["country"]]
            if record["state_or_province"] is not None:
                site.province = [record["state_or_province"]]
            if record["loc_wkt"] is not None:
                site.location = GeoLocationInfo(
                    crs=record["loc_crs"], geom=record["loc_wkt"]
                )

            if record["tot_contained_metal"] is not None:
                site.mineral_inventory.append(
                    MineralInventory(
                        commodity=commodity,
                        contained_metal=record["tot_contained_metal"],
                        ore_value=record["total_tonnage"],
                        grade_value=record["total_grade"],
                    )
                )

        sites.append(site)
    return sites


def get_extra_mineral_site_data(site_ids: list[str]) -> dict[str, dict]:
    qres = run_sparql_query(
        """
    SELECT 
        ?uri
        ?source_id 
        ?record_id 
        ?name 
        ?site_rank 
        ?site_type 
    WHERE {
        ?uri a :MineralSite .
        
        OPTIONAL { ?uri :source_id ?source_id . }
        OPTIONAL { ?uri :record_id ?record_id . }
        OPTIONAL { ?uri rdfs:label ?name . }
        OPTIONAL { ?uri :site_rank ?site_rank . }
        OPTIONAL { ?uri :site_type ?site_type . }

        VALUES ?uri { %s }
    }
        """
        % (" ".join(f"mnr:{site_id}" for site_id in site_ids))
    )

    output = {}
    for row in qres:
        newrow = {
            "uri": row["uri"],
            "source_id": row.get("source_id", ""),
            "record_id": row.get("record_id", ""),
            "name": row.get("name", ""),
            "site_rank": row.get("site_rank", ""),
            "site_type": row.get("site_type", ""),
        }
        for k, v in newrow.items():
            if v is None:
                newrow[k] = ""
        output[row["uri"][len(MNR_NS) :]] = newrow
    return output
