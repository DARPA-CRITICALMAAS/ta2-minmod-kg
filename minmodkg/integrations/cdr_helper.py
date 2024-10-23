from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional

import httpx
import timer
from joblib import Parallel, delayed
from minmodkg.config import MNR_NS
from tqdm import tqdm

if "CDR_AUTH_TOKEN" in os.environ:
    AUTH_TOKEN = os.environ.get("CDR_AUTH_TOKEN")
else:
    keyfile = Path(os.path.expanduser("~/.config/cdr.key"))
    assert keyfile.exists()
    AUTH_TOKEN = keyfile.read_text().strip()

MINMOD_API = os.environ.get("MINMOD_API", "https://minmod.isi.edu/api/v1")
MINMOD_SYSTEM = os.environ.get("MINMOD_SYSTEM", "minmod")
CDR_API = "https://api.cdr.land/v1"

cdr_headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}


@dataclass
class Endpoint:
    item: str
    collection: str
    bulk_upload: Optional[str] = None
    bulk_delete: Optional[str] = None
    count: Optional[str] = None


class MinmodHelper:
    @lru_cache(maxsize=1)
    @staticmethod
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
    @staticmethod
    def get_commodity_id2name():
        r = httpx.get(
            f"{MINMOD_API}/commodities",
            verify=False,
            timeout=None,
        )
        r.raise_for_status()

        id2name = {}
        for record in r.json():
            id = record["uri"][len(MNR_NS) :]
            name = record["name"]
            assert id not in id2name, (id, id2name)
            id2name[id] = name
        return id2name

    @lru_cache(maxsize=1)
    @staticmethod
    def get_unit_uri2name():
        r = httpx.get(
            f"{MINMOD_API}/units",
            verify=False,
            timeout=None,
        )
        r.raise_for_status()

        uri2name = {}
        for record in r.json():
            uri = record["uri"]
            name = record["name"]
            assert uri not in uri2name
            uri2name[uri] = name
        return uri2name


class CDRHelper:
    DedupSites = Endpoint(
        item="dedup-site",
        collection="dedup-sites",
        bulk_upload="dedup-sites",
        bulk_delete="dedup-site/bulk/sites",
        count="dedup-site/stats/count",
    )
    DepositType = Endpoint(item="deposit-type", collection="deposit-types")

    N_PARALLEL_JOBS = 16

    @staticmethod
    def upload_collection(endpoint: Endpoint, collection: list[dict]):
        with timer.Timer().watch_and_report(
            "Upload the whole collection to endpoint " + endpoint.collection
        ):
            if endpoint.bulk_upload is not None:
                r = httpx.post(
                    f"{CDR_API}/minerals/{endpoint.bulk_upload}",
                    json=collection,
                    headers=cdr_headers,
                    timeout=None,
                )
                r.raise_for_status()
            else:
                it = get_parallel(
                    n_jobs=CDRHelper.N_PARALLEL_JOBS, return_as="generator_unordered"
                )(delayed(CDRHelper.create)(endpoint, item) for item in collection)
                for _ in tqdm(it, total=len(collection), desc="uploading records"):
                    pass

    @staticmethod
    def truncate(endpoint: Endpoint):
        with timer.Timer().watch_and_report(
            "Truncating endpoint " + endpoint.collection
        ):
            if endpoint.bulk_upload is not None:
                r = httpx.delete(
                    f"{CDR_API}/minerals/{endpoint.bulk_delete}",
                    params={"system": MINMOD_SYSTEM},
                    headers=cdr_headers,
                    timeout=None,
                )
                assert r.status_code == 204 or r.status_code == 404, r.text
            else:
                r = httpx.get(
                    f"{CDR_API}/minerals/{endpoint.collection}",
                    headers=cdr_headers,
                    timeout=None,
                )
                r.raise_for_status()
                records = r.json()
                it = get_parallel(
                    n_jobs=CDRHelper.N_PARALLEL_JOBS, return_as="generator_unordered"
                )(
                    delayed(CDRHelper.delete_by_id)(endpoint, item["id"])
                    for item in records
                )
                for _ in tqdm(it, total=len(records), desc="deleting records"):
                    pass

            # double check the results
            if endpoint.count is None:
                r = httpx.get(
                    f"{CDR_API}/minerals/{endpoint.collection}",
                    headers=cdr_headers,
                    timeout=None,
                )
                r.raise_for_status()
                assert len(r.json()) == 0
            else:
                r = httpx.get(
                    f"{CDR_API}/minerals/{endpoint.count}",
                    headers=cdr_headers,
                    timeout=None,
                )
                r.raise_for_status()
                assert int(r.text.strip()) == 0

    @staticmethod
    def delete_by_id(endpoint: Endpoint, id: str):
        r = httpx.delete(
            f"{CDR_API}/minerals/{endpoint.item}/{id}",
            headers=cdr_headers,
            timeout=None,
        )
        assert r.status_code == 404 or r.status_code == 204, r.text

    @staticmethod
    def create(endpoint: Endpoint, item: dict):
        r = httpx.post(
            f"{CDR_API}/minerals/{endpoint.item}",
            json=item,
            headers=cdr_headers,
            timeout=None,
        )
        r.raise_for_status()


@lru_cache(maxsize=2)
def get_parallel(n_jobs: int = -1, return_as: str = "list"):
    return Parallel(n_jobs=n_jobs, return_as=return_as)
