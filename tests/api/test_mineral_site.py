from __future__ import annotations

import time
from pathlib import Path
from time import sleep

import pytest
import serde.json
from fastapi.testclient import TestClient
from minmodkg.api.models.public_mineral_site import InputPublicMineralSite
from minmodkg.api.routers.mineral_site import (
    crs_uri_to_name,
    material_form_uri_to_conversion,
    source_uri_to_score,
)
from minmodkg.misc.rdf_store import TripleStore
from minmodkg.models.inputs.candidate_entity import CandidateEntity
from minmodkg.models.inputs.location_info import LocationInfo
from minmodkg.models.inputs.mineral_inventory import MineralInventory
from minmodkg.models.inputs.reference import Document, Reference
from minmodkg.models.kg.base import MINMOD_KG
from minmodkg.models.kgrel.mineral_site import MineralSite, MineralSiteAndInventory
from minmodkg.models.kgrel.user import User
from minmodkg.services.mineral_site import MineralSiteService
from minmodkg.transformations import make_site_uri
from shapely import Point
from shapely.wkt import loads
from tests.utils import check_req


class TestMineralSiteData:
    @pytest.fixture(autouse=True)
    def site1_(self, user1: User):
        self.site1_commodity = "Q578"
        self.site1 = InputPublicMineralSite(
            source_id="database::https://mrdata.usgs.gov/mrds",
            record_id="10014570",
            name="Eagle Mine",
            location_info=LocationInfo(
                location="POINT(-87.099998474121 46.900001525879)",
            ),
            created_by=[user1.get_uri()],
            mineral_inventory=[
                MineralInventory(
                    commodity=CandidateEntity(
                        source="database::https://mrdata.usgs.gov/mrds",
                        confidence=1.0,
                        observed_name="Nickel",
                        normalized_uri=MINMOD_KG.ns.mr.uristr(self.site1_commodity),
                    ),
                    reference=Reference(
                        document=Document(uri="https://mrdata.usgs.gov/mrds")
                    ),
                )
            ],
        )
        self.site1_id = make_site_uri(
            self.site1.source_id, self.site1.record_id, namespace=""
        )
        self.site1_uri = make_site_uri(self.site1.source_id, self.site1.record_id)
        self.site1_dedup_uri = MINMOD_KG.ns.md.uristr(
            MineralSite.get_dedup_id([self.site1_id])
        )
        self.site1_dedup_id = MINMOD_KG.ns.md.id(self.site1_dedup_uri)
        self.site1_dump = {
            "id": self.site1_id,
            "source_id": self.site1.source_id,
            "record_id": self.site1.record_id,
            "name": self.site1.name,
            "location_info": {
                "location": "POINT(-87.099998474121 46.900001525879)",
            },
            "mineral_inventory": [
                inv.to_dict() for inv in self.site1.mineral_inventory
            ],
            "coordinates": {
                "lat": 46.900001525879,
                "lon": -87.099998474121,
            },
            "dedup_site_uri": self.site1_dedup_uri,
            "created_by": [user1.get_uri()],
            "grade_tonnage": [{"commodity": self.site1_commodity}],
        }

    @pytest.fixture(autouse=True)
    def site2_(self, user2: User):
        self.site2_commodity = "Q569"
        self.site2 = InputPublicMineralSite(
            source_id="database::https://mrdata.usgs.gov/mrds",
            record_id="10109359",
            name="Beaver Mine",
            location_info=LocationInfo(
                location="POINT(-118.7805 44.71207)",
            ),
            created_by=[user2.get_uri()],
            mineral_inventory=[
                MineralInventory(
                    commodity=CandidateEntity(
                        source="database::https://mrdata.usgs.gov/mrds",
                        confidence=1.0,
                        observed_name="Lithium",
                        normalized_uri=MINMOD_KG.ns.mr.uristr(self.site2_commodity),
                    ),
                    reference=Reference(
                        document=Document(uri="https://mrdata.usgs.gov/mrds")
                    ),
                )
            ],
            dedup_site_uri=self.site1_dedup_uri,
        )
        self.site2_id = make_site_uri(
            self.site2.source_id, self.site2.record_id, namespace=""
        )
        self.site2_uri = make_site_uri(self.site2.source_id, self.site2.record_id)
        self.site2_dump = self.site2.to_dict()
        self.site2_dump.update(
            {
                "coordinates": {
                    "lat": 44.71207,
                    "lon": -118.7805,
                },
                "dedup_site_uri": self.site2.dedup_site_uri,
                "created_by": [user2.get_uri()],
                "id": self.site2_id,
                "grade_tonnage": [{"commodity": self.site2_commodity}],
            }
        )
        del self.site2_dump["modified_at"]


class TestMineralSite(TestMineralSiteData):

    def test_create_first(self, auth_client: TestClient, kg: TripleStore, kgrel):
        # create a mineral site record
        resp = check_req(
            lambda: auth_client.post(
                "/api/v1/mineral-sites",
                json=self.site1.to_dict(),
            )
        ).json()
        self.site1.modified_at = resp["modified_at"]

        gold_resp = dict(
            **self.site1_dump,
            modified_at=resp["modified_at"],
            snapshot_id=resp["snapshot_id"],
        )
        norm_site_wkt(resp, gold_resp["location_info"]["location"])
        assert resp == gold_resp

        resp = check_req(
            lambda: auth_client.get(
                f"/api/v1/mineral-sites/{self.site1_id}",
            )
        ).json()
        norm_site_wkt(resp, gold_resp["location_info"]["location"])
        assert resp == gold_resp

        resp = check_req(
            lambda: auth_client.get(
                f"/api/v1/dedup-mineral-sites/{self.site1_dedup_id}",
                params={"commodity": self.site1_commodity},
            )
        ).json()
        assert resp == {
            "id": self.site1_dedup_id,
            "name": "Eagle Mine",
            "type": "NotSpecified",
            "rank": "U",
            "sites": [{"id": self.site1_id, "score": resp["sites"][0]["score"]}],
            "deposit_types": [],
            "location": {
                "lat": 46.900001525879,
                "lon": -87.099998474121,
                "country": [],
                "state_or_province": [],
            },
            "modified_at": self.site1.modified_at,
            "grade_tonnage": [{"commodity": "Q578"}],
        }

    def test_create_exist(self, auth_client, kg: TripleStore):
        resp = auth_client.post(
            "/api/v1/mineral-sites",
            json=self.site1.to_dict(),
        )
        assert resp.json() == {"detail": "The site already exists."}
        assert resp.status_code == 409

    def test_update_site(self, auth_client, kg: TripleStore):
        sleep(1.0)  # to ensure the modified_at is different
        self.site1.name = "Frog Mine"
        self.site1.dedup_site_uri = MINMOD_KG.ns.md.uristr(
            MineralSite.get_dedup_id([self.site1_id])
        )
        resp = check_req(
            lambda: auth_client.put(
                f"/api/v1/mineral-sites/{self.site1_id}",
                json=self.site1.to_dict(),
            )
        ).json()

        gold_resp = dict(
            **self.site1_dump,
            modified_at=resp["modified_at"],
            snapshot_id=resp["snapshot_id"],
        )
        gold_resp["name"] = "Frog Mine"
        norm_site_wkt(resp, gold_resp["location_info"]["location"])
        assert resp == gold_resp

        resp = check_req(
            lambda: auth_client.get(
                f"/api/v1/mineral-sites/{self.site1_id}",
            )
        ).json()
        assert resp == gold_resp

        resp = check_req(
            lambda: auth_client.get(
                f"/api/v1/dedup-mineral-sites/{self.site1_dedup_id}",
                params={"commodity": self.site1_commodity},
            )
        ).json()
        assert resp == {
            "id": self.site1_dedup_id,
            "name": "Frog Mine",
            "type": "NotSpecified",
            "rank": "U",
            "sites": [{"id": self.site1_id, "score": resp["sites"][0]["score"]}],
            "deposit_types": [],
            "location": {
                "lat": 46.900001525879,
                "lon": -87.099998474121,
                "country": [],
                "state_or_province": [],
            },
            "modified_at": gold_resp["modified_at"],
            "grade_tonnage": [{"commodity": "Q578"}],
        }

    def test_create_new_site(self, auth_client_2, kg: TripleStore):
        time.sleep(1.0)  # to ensure the modified_at is different
        resp = check_req(
            lambda: auth_client_2.post(
                "/api/v1/mineral-sites",
                json=self.site2.to_dict(),
            )
        ).json()
        gold_resp = dict(
            **self.site2_dump,
            modified_at=resp["modified_at"],
            snapshot_id=resp["snapshot_id"],
        )
        norm_site_wkt(resp, gold_resp["location_info"]["location"])

        assert resp == gold_resp

        for commodity in [self.site1_commodity, self.site2_commodity]:
            resp = check_req(
                lambda: auth_client_2.get(
                    f"/api/v1/dedup-mineral-sites/{self.site1_dedup_id}",
                    params={"commodity": commodity},
                )
            ).json()
            assert resp == {
                "id": self.site1_dedup_id,
                "name": "Beaver Mine",
                "type": "NotSpecified",
                "rank": "U",
                "sites": [
                    {
                        "id": self.site2_id,
                        "score": resp["sites"][0]["score"],
                    },
                    {
                        "id": self.site1_id,
                        "score": resp["sites"][1]["score"],
                    },
                ],
                "deposit_types": [],
                "location": {
                    "lat": 44.71207,
                    "lon": -118.7805,
                    "country": [],
                    "state_or_province": [],
                },
                "modified_at": gold_resp["modified_at"],
                "grade_tonnage": [{"commodity": commodity}],
            }


class TestMineralSiteLinking:
    def test_update_same_as(self, resource_dir: Path, user1, auth_client, kg, kgrel):
        time.sleep(1.0)  # to ensure the modified_at is different
        crss = crs_uri_to_name(None)
        material_form = material_form_uri_to_conversion(None)
        source_score = source_uri_to_score(None)

        mineral_site_service = MineralSiteService(kgrel)
        id2site = {}
        for file in (resource_dir / "kgdata/mineral-sites/json").iterdir():
            for raw_site in serde.json.deser(file):
                msi = MineralSiteAndInventory.from_raw_site(
                    raw_site,
                    material_form=material_form,
                    crs_names=crss,
                    source_score=source_score,
                )
                id2site[msi.ms.site_id] = msi.ms
                mineral_site_service.create(user1, msi)

        resp = check_req(
            lambda: auth_client.post(
                "/api/v1/same-as",
                json=[
                    {
                        "sites": [
                            "site__api-cdr-land-v1-docs-documents__02a0c7412e655ff0a9a4eb63cd1388ecb4aee96931f8bc4f98819e65cc83173755",
                            "site__api-cdr-land-v1-docs-documents__02a000a83e76360bec8f3fce2ff46cc8099f950cc1f757f8a16592062c49b3a5c5",
                        ]
                    }
                ],
            )
        )


def norm_site_wkt(resp: dict, wkt: str):
    gold_geometry = loads(wkt)
    pred_geometry = loads(resp["location_info"]["location"])

    assert isinstance(gold_geometry, Point)
    assert isinstance(pred_geometry, Point)

    assert gold_geometry.x == pytest.approx(pred_geometry.x, abs=1e-4)
    assert gold_geometry.y == pytest.approx(pred_geometry.y, abs=1e-4)

    resp["location_info"]["location"] = wkt
