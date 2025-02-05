from __future__ import annotations

import os
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Callable, Optional

import httpx
import timer
from joblib import Parallel, delayed
from loguru import logger
from minmodkg.models.kg.base import MINMOD_NS
from tqdm import tqdm

if "CDR_AUTH_TOKEN" in os.environ:
    AUTH_TOKEN = os.environ.get("CDR_AUTH_TOKEN")
else:
    keyfile = Path(os.path.expanduser("~/.config/cdr.key"))
    assert keyfile.exists()
    AUTH_TOKEN = keyfile.read_text().strip()

MINMOD_API = os.environ.get("MINMOD_API", "https://minmod.isi.edu/api/v1")
MINMOD_SYSTEM = os.environ.get("MINMOD_SYSTEM", "minmod")
CDR_API = os.environ.get("CDR_API", "https://api.cdr.land/v1")

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
            dpt2id[record["name"]] = MINMOD_NS.mr.id(record["uri"])
        return dpt2id

    @lru_cache(maxsize=1)
    @staticmethod
    def get_commodity_uri2name():
        r = httpx.get(
            f"{MINMOD_API}/commodities",
            verify=False,
            timeout=None,
        )
        r.raise_for_status()

        uri2name = {}
        for record in r.json():
            uri = record["uri"]
            name = record["name"]
            assert uri not in uri2name, (uri, uri2name)
            uri2name[uri] = name
        return uri2name

    @lru_cache(maxsize=1)
    @staticmethod
    def get_commodity_id2name():
        return {
            MINMOD_NS.mr.id(uri): name
            for uri, name in MinmodHelper.get_commodity_uri2name().items()
        }

    @lru_cache(maxsize=1)
    @staticmethod
    def get_country_id2name():
        r = httpx.get(
            f"{MINMOD_API}/countries",
            verify=False,
            timeout=None,
        )
        r.raise_for_status()

        id2name = {}
        for record in r.json():
            id = MINMOD_NS.mr.id(record["uri"])
            name = record["name"]
            assert id not in id2name, (id, id2name)
            id2name[id] = name
        return id2name

    @lru_cache(maxsize=1)
    @staticmethod
    def get_province_id2name():
        r = httpx.get(
            f"{MINMOD_API}/states-or-provinces",
            verify=False,
            timeout=None,
        )
        r.raise_for_status()

        id2name = {}
        for record in r.json():
            id = MINMOD_NS.mr.id(record["uri"])
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
    MineralSite = Endpoint(item="site", collection="sites", count="sites/count")

    N_PARALLEL_JOBS = 16

    @staticmethod
    def upload_collection(endpoint: Endpoint, collection: list[dict]):
        with timer.Timer().watch_and_report(
            f"Upload {len(collection)} records to endpoint {endpoint.collection}",
            print_fn=logger.info,
        ):
            if endpoint.bulk_upload is not None:
                retry_request(
                    lambda: httpx.post(
                        f"{CDR_API}/minerals/{endpoint.bulk_upload}",
                        json=collection,
                        headers=cdr_headers,
                        timeout=None,
                    )
                )
            else:
                it = get_parallel(
                    n_jobs=CDRHelper.N_PARALLEL_JOBS, return_as="generator_unordered"
                )(delayed(CDRHelper.create)(endpoint, item) for item in collection)
                for _ in tqdm(it, total=len(collection), desc="uploading records"):
                    pass

    @staticmethod
    def delete_collection(endpoint: Endpoint, collection: list[dict]):
        with timer.Timer().watch_and_report(
            f"Delete {len(collection)} records in endpoint {endpoint.collection}",
            print_fn=logger.info,
        ):
            it = get_parallel(
                n_jobs=CDRHelper.N_PARALLEL_JOBS, return_as="generator_unordered"
            )(
                delayed(CDRHelper.delete_by_id)(endpoint, item["id"])
                for item in collection
            )
            for _ in tqdm(it, total=len(collection), desc="delete records"):
                pass

    @staticmethod
    def truncate(endpoint: Endpoint):
        with timer.Timer().watch_and_report(
            f"Truncating endpoint {endpoint.collection}",
            print_fn=logger.info,
        ):
            if endpoint.bulk_upload is not None:
                r = retry_request(
                    lambda: httpx.delete(
                        f"{CDR_API}/minerals/{endpoint.bulk_delete}",
                        params={"system": MINMOD_SYSTEM},
                        headers=cdr_headers,
                        timeout=None,
                    ),
                    okay_status_code=(204, 404),
                )
            else:
                n_records = CDRHelper.count(endpoint)
                parallel = get_parallel(
                    n_jobs=CDRHelper.N_PARALLEL_JOBS, return_as="generator_unordered"
                )
                batch_size = 5000

                with tqdm(total=n_records, desc="deleting records") as pbar:
                    for i in range(0, n_records, batch_size):
                        records = CDRHelper.fetch(endpoint, limit=batch_size)
                        it = parallel(
                            delayed(CDRHelper.delete_by_id)(endpoint, item["id"])
                            for item in records
                        )
                        for _ in it:
                            pbar.update(1)

            # double check the results
            assert CDRHelper.count(endpoint) == 0

    @staticmethod
    def fetch(endpoint: Endpoint, limit: int = -1) -> list:
        r = retry_request(
            lambda: httpx.get(
                f"{CDR_API}/minerals/{endpoint.collection}",
                params={"limit": limit},
                headers=cdr_headers,
                timeout=None,
            )
        )
        return r.json()

    @staticmethod
    def count(endpoint: Endpoint):
        if endpoint.count is None:
            # the deposit type does not have count endpoint
            assert endpoint.item == "deposit-type"
            r = retry_request(
                lambda: httpx.get(
                    f"{CDR_API}/minerals/{endpoint.collection}",
                    headers=cdr_headers,
                    timeout=None,
                )
            )
            return len(r.json())
        else:
            r = retry_request(
                lambda: httpx.get(
                    f"{CDR_API}/minerals/{endpoint.count}",
                    headers=cdr_headers,
                    timeout=None,
                )
            )
            return int(r.text.strip())

    @staticmethod
    def delete_by_id(endpoint: Endpoint, id: str):
        retry_request(
            lambda: httpx.delete(
                f"{CDR_API}/minerals/{endpoint.item}/{id}",
                headers=cdr_headers,
                timeout=None,
            ),
            okay_status_code=(204, 404),
        )

    @staticmethod
    def create(endpoint: Endpoint, item: dict):
        retry_request(
            lambda: httpx.post(
                f"{CDR_API}/minerals/{endpoint.item}",
                json=item,
                headers=cdr_headers,
                timeout=None,
            ),
            msg="Fail to create item",
        )


def retry_request(
    req: Callable[[], httpx.Response],
    *,
    okay_status_code: tuple[int, ...] = (200, 201),
    msg: str = "Failed to make request",
    interval: float = 1,  # wait for 1 second before retry
    retry: int = 5,
) -> httpx.Response:
    for i in range(retry):
        r = req()
        if r.status_code in okay_status_code:
            return r
        print(f"F({r.status_code}).", end="", flush=True)
        time.sleep(interval)
    raise Exception(msg + f" {r.status_code} {r.text}")


@lru_cache(maxsize=2)
def get_parallel(n_jobs: int = -1, return_as: str = "list"):
    return Parallel(n_jobs=n_jobs, return_as=return_as)
