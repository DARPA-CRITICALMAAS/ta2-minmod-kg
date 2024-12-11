from __future__ import annotations

import time
from time import sleep

import pytest
from fastapi.testclient import TestClient
from minmodkg.api.models.user import UserCreate
from minmodkg.api.routers.mineral_site import UpdateMineralSite, get_site_changes
from minmodkg.misc.rdf_store import RDFStore
from minmodkg.models.base import MINMOD_KG
from minmodkg.models.dedup_mineral_site import DedupMineralSite, DedupMineralSitePublic
from minmodkg.models.mineral_site import (
    CandidateEntity,
    LocationInfo,
    MineralInventory,
    MineralSite,
    Reference,
)
from minmodkg.models.reference import Document
from minmodkg.transformations import make_site_uri
from rdflib import RDF, RDFS
from rdflib import Literal as RDFLiteral
from rdflib import Namespace, URIRef
from tests.utils import check_req


class TestMineralSiteData:
    @pytest.fixture(autouse=True)
    def site1_(self, user1: UserCreate):
        self.site1_commodity = "Q578"
        self.site1 = MineralSite(
            source_id="database::https://mrdata.usgs.gov/mrds",
            record_id="10014570",
            name="Eagle Mine",
            location_info=LocationInfo(
                location="POINT (-87.1 46.9)",
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
        self.site1_dedup_uri = MINMOD_KG.ns.mr.uristr(
            DedupMineralSite.get_id([self.site1_id])
        )
        self.site1_dedup_id = MINMOD_KG.ns.mr.id(self.site1_dedup_uri)

        self.site1_dump = self.site1.model_dump(exclude_none=True)
        self.site1_dump.update(
            {
                "coordinates": {
                    "lat": 46.9,
                    "lon": -87.1,
                },
                "dedup_site_uri": self.site1_dedup_uri,
                "created_by": [user1.get_uri()],
                "id": self.site1_id,
                "grade_tonnage": [{"commodity": self.site1_commodity}],
            }
        )
        del self.site1_dump["modified_at"]

    @pytest.fixture(autouse=True)
    def site2_(self, user2: UserCreate):
        self.site2_commodity = "Q569"
        self.site2 = MineralSite(
            source_id="database::https://mrdata.usgs.gov/mrds",
            record_id="10109359",
            name="Beaver Mine",
            location_info=LocationInfo(
                location="POINT (-118.7805 44.71207)",
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
        self.site2_dump = self.site2.model_dump(exclude_none=True)
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

    def test_create_first(self, auth_client: TestClient, kg: RDFStore):
        # create a mineral site record
        resp = check_req(
            lambda: auth_client.post(
                "/api/v1/mineral-sites",
                json=self.site1.model_dump(exclude_none=True),
            )
        ).json()
        self.site1.modified_at = resp["modified_at"]
        print("Create first: ", resp["modified_at"])

        gold_resp = dict(**self.site1_dump, modified_at=resp["modified_at"])
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
            "name": "Eagle Mine",
            "type": "NotSpecified",
            "rank": "U",
            "sites": [self.site1_id],
            "deposit_types": [],
            "location": {
                "lat": 46.9,
                "lon": -87.1,
                "country": [],
                "state_or_province": [],
            },
            "grade_tonnage": {"commodity": "Q578"},
        }

    def test_create_exist(self, auth_client, kg: RDFStore):
        resp = auth_client.post(
            "/api/v1/mineral-sites",
            json=self.site1.model_dump(exclude_none=True),
        )
        assert resp.json() == {"detail": "The site already exists."}
        assert resp.status_code == 403

    def test_get_site_changes(self, auth_client, kg: RDFStore, user1: UserCreate):
        sleep(1.0)  # to ensure the modified_at is different
        print("Get site changes: ", self.site1.modified_at)
        self.site1.name = "Frog Mine"
        self.site1.dedup_site_uri = MINMOD_KG.ns.mr.uristr(
            DedupMineralSite.get_id([self.site1_id])
        )

        new_site1 = self.site1.model_copy()
        del_triples, add_triples = UpdateMineralSite.get_triples(
            self.site1_id, self.site1_uri, new_site1, user1
        )
        subj = MINMOD_KG.ns.mr[self.site1_id]
        print("Get site changes: ", self.site1.modified_at, new_site1.modified_at)
        assert set(del_triples) == {
            (
                subj,
                "mo:modified_at",
                [triple[2] for triple in del_triples if triple[1] == "mo:modified_at"][
                    0
                ],
            ),
            (
                subj,
                "rdfs:label",
                '"Eagle Mine"',
            ),
        }
        assert set(add_triples) == {
            (
                subj,
                "mo:modified_at",
                f'"{new_site1.modified_at}"',
            ),
            (
                subj,
                "rdfs:label",
                '"Frog Mine"',
            ),
        }

    def test_update_site(self, auth_client, kg: RDFStore):
        sleep(1.0)  # to ensure the modified_at is different
        self.site1.name = "Frog Mine"
        self.site1.dedup_site_uri = MINMOD_KG.ns.mr.uristr(
            DedupMineralSite.get_id([self.site1_id])
        )
        resp = check_req(
            lambda: auth_client.put(
                f"/api/v1/mineral-sites/{self.site1_id}",
                json=self.site1.model_dump(exclude_none=True),
            )
        ).json()

        gold_resp = dict(**self.site1_dump, modified_at=resp["modified_at"])
        gold_resp["name"] = "Frog Mine"
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
            "sites": [self.site1_id],
            "deposit_types": [],
            "location": {
                "lat": 46.9,
                "lon": -87.1,
                "country": [],
                "state_or_province": [],
            },
            "grade_tonnage": {"commodity": "Q578"},
        }

    def test_create_new_site(self, auth_client_2, kg: RDFStore):
        time.sleep(1.0)  # to ensure the modified_at is different
        resp = check_req(
            lambda: auth_client_2.post(
                "/api/v1/mineral-sites",
                json=self.site2.model_dump(exclude_none=True),
            )
        ).json()
        gold_resp = dict(**self.site2_dump, modified_at=resp["modified_at"])
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
                "sites": [self.site2_id, self.site1_id],
                "deposit_types": [],
                "location": {
                    "lat": 44.71207,
                    "lon": -118.7805,
                    "country": [],
                    "state_or_province": [],
                },
                "grade_tonnage": {"commodity": commodity},
            }


class TestMineralSiteLinking:
    def test_update_same_as(self, auth_client, kg):
        time.sleep(1.0)  # to ensure the modified_at is different
        resp = check_req(
            lambda: auth_client.post(
                "/api/v1/same-as",
                json=[
                    {
                        "sites": [
                            "site__doi-org-10-5066-p9htergk__29834",
                            "site__doi-org-10-5066-p9htergk__29328",
                        ]
                    }
                ],
            )
        )
