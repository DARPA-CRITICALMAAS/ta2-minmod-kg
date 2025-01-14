from __future__ import annotations

import json
import os
import time
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Callable

import httpx
from slugify import slugify

InternalID = Annotated[
    str,
    "internal MinMod ID (e.g., Q578) - together with `https://minmod.isi.edu/resource/` prefix, it creates the URI of a resource in the MinMod KG",
]


@dataclass
class SiteIdentification:
    site_id: InternalID
    snapshot_id: int


class MinModAPI:
    def __init__(self, endpoint: str):
        self.endpoint = MinModAPI.resolve_endpoint(endpoint)
        self.cfg_file = Path(
            os.path.expanduser("~/.config/minmod/" + slugify(self.endpoint) + ".json")
        )

    @property
    def auth_token(self) -> str:
        if self.cfg_file.exists():
            cfg = json.loads(self.cfg_file.read_bytes())
            resp = httpx.get(
                f"{self.endpoint}/api/v1/whoami",
                cookies={"session": cfg["token"]},
                **self.httpx_args(auth=False),
            )
            if resp.status_code == 401:
                raise ValueError("Auth token is expired. Please login again.")
            return cfg["token"]
        raise ValueError("Auth token is missing. Please login.")

    def upsert_mineral_site(
        self,
        site: dict,
        apply_update: Callable[[dict, dict], None],
        verbose: bool = False,
    ) -> SiteIdentification | None:
        """Upsert a mineral site, if the site exists, apply the update function to the existing site
        and retry the upsertion.

        Args:
            site: the site data
            apply_update: (signature: (existing_site, your_site)) the function to apply the update to the existing site
        """
        msid = self.make_site_id(site["source_id"], site["record_id"])
        if not self.has_site(msid):
            if verbose:
                print(f"Adding site {msid}")
            try:
                site_ident = self.add_site(msid, site)
                return site_ident
            except ConflictError as e:
                # the site has been added by someone else -- so we need to retry.
                ms = self.get_site(msid)
                site_ident = SiteIdentification(ms["id"], ms["snapshot_id"])
        else:
            ms = self.get_site(msid)
            site_ident = SiteIdentification(ms["id"], ms["snapshot_id"])

        if verbose:
            print(
                "Failed to add site. Fetching the existing site and retrying",
                end="",
                flush=True,
            )

        for _ in range(10):  # retry 10 times
            if verbose:
                print(".", end="", flush=True)

            apply_update(ms, deepcopy(site))
            try:
                site_ident = self.update_site(site_ident, ms)
            except ConflictError as e:
                # the site has been updated by someone else since the last time we fetch -- so we need to retry.
                ms = self.get_site(msid)
                site_ident = SiteIdentification(ms["id"], ms["snapshot_id"])

                time.sleep(0.5)
                continue

            if verbose:
                print(" Success!", flush=True)
            return site_ident

    def update_site(
        self, site_ident: SiteIdentification, site: dict
    ) -> SiteIdentification:
        """Update a mineral site"""
        resp = httpx.put(
            f"{self.endpoint}/api/v1/mineral-sites/{site_ident.site_id}",
            params={"snapshot_id": site_ident.snapshot_id},
            json=site,
            **self.httpx_args(),
        )
        if resp.status_code == 409:
            # some people might have updated the site before you
            raise ConflictError(
                "The site has been updated by someone else. You need to refetch and reapply your changes."
            )
        resp.raise_for_status()
        resp_data = resp.json()
        return SiteIdentification(resp_data["id"], resp_data["snapshot_id"])

    def add_site(self, site_id: InternalID, site: dict) -> SiteIdentification:
        """Add a mineral site and return the site ID (not URI) and its snapshot id."""
        resp = httpx.post(
            f"{self.endpoint}/api/v1/mineral-sites",
            json=site,
            **self.httpx_args(),
        )
        if resp.status_code == 409:
            # the site already exists --- fetch the site data
            raise ConflictError(
                "The site has been added by someone else. You need to fetch and apply your changes."
            )
        resp.raise_for_status()
        resp_data = resp.json()
        return SiteIdentification(resp_data["id"], resp_data["snapshot_id"])

    def has_site(self, site_id: InternalID) -> bool:
        """Check if a mineral site exists."""
        resp = httpx.head(
            f"{self.endpoint}/api/v1/mineral-sites/{site_id}",
            **self.httpx_args(),
        )
        if resp.status_code == 404:
            return False
        assert resp.status_code == 200
        return True

    def get_site(self, site_id: InternalID) -> dict:
        resp = httpx.head(
            f"{self.endpoint}/api/v1/mineral-sites/{site_id}",
            **self.httpx_args(),
        )
        if resp.status_code != 200:
            raise KeyError(f"The site `{site_id}` does not exist.")
        return resp.json()

    def make_site_id(self, source_id: str, record_id: str) -> InternalID:
        """Make a mineral site ID."""
        resp = httpx.get(
            f"{self.endpoint}/api/v1/mineral-sites/make-id",
            params={
                "source_id": source_id,
                "record_id": record_id,
                "return_uri": False,
            },
            **self.httpx_args(),
        )
        resp.raise_for_status()
        return resp.text.strip()

    def login(self, username: str, password: str):
        resp = httpx.post(
            f"{self.endpoint}/api/v1/login",
            json={"username": username, "password": password},
            **self.httpx_args(auth=False),
        )
        resp.raise_for_status()
        self.cfg_file.parent.mkdir(parents=True, exist_ok=True)
        self.cfg_file.write_text(json.dumps({"token": resp.cookies["session"]}))

    def whoami(self):
        resp = httpx.get(
            f"{self.endpoint}/api/v1/whoami",
            **self.httpx_args(),
        )
        resp.raise_for_status()
        data = resp.json()
        return f"Hello {data['name']} ({data['username']}) !"

    @classmethod
    def resolve_endpoint(cls, endpoint: str) -> str:
        resp = httpx.head(endpoint)
        if resp.status_code == 302:
            endpoint = resp.headers["Location"]
            if endpoint.endswith("/"):
                endpoint = endpoint[:-1]
        return endpoint

    def httpx_args(self, auth: bool = True) -> dict:
        args: dict = {
            "verify": False,
        }
        if auth:
            args["cookies"] = {"session": self.auth_token}
        return args


class ConflictError(Exception): ...
