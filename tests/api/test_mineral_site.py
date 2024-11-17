from __future__ import annotations

from time import sleep

import pytest
from fastapi.testclient import TestClient
from minmodkg.config import MINMOD_KG
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


class TestMineralSite:

    @pytest.fixture(autouse=True)
    def site1_(self, user1_uri: str):
        self.site1_commodity = "Q578"
        self.site1 = MineralSite(
            source_id="database::https://mrdata.usgs.gov/mrds",
            record_id="10014570",
            name="Eagle Mine",
            location_info=LocationInfo(
                location="POINT (-87.1 46.9)",
            ),
            created_by=[user1_uri],
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

        self.site1_dump = self.site1.model_dump(exclude_none=True)
        self.site1_dump.update(
            {
                "coordinates": {
                    "lat": 46.9,
                    "lon": -87.1,
                },
                "dedup_site_uri": self.site1_dedup_uri,
                "created_by": [user1_uri],
                "uri": self.site1_uri,
                "grade_tonnage": [{"commodity": self.site1_commodity}],
            }
        )
        del self.site1_dump["modified_at"]

    @pytest.fixture(autouse=True)
    def site2_(self, user2_uri: str):
        self.site2_commodity = "Q569"
        self.site2 = MineralSite(
            source_id="database::https://mrdata.usgs.gov/mrds",
            record_id="10109359",
            name="Beaver Mine",
            location_info=LocationInfo(
                location="POINT (-118.7805 44.71207)",
            ),
            created_by=[user2_uri],
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
            same_as=[self.site1_uri],
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
                "created_by": [user2_uri],
                "uri": self.site2_uri,
                "grade_tonnage": [{"commodity": self.site2_commodity}],
            }
        )
        del self.site2_dump["modified_at"]

    def test_create_first(self, auth_client: TestClient, user1_uri: str, kg):
        # create a mineral site record
        resp = check_req(
            lambda: auth_client.post(
                "/api/v1/mineral-sites",
                json=self.site1.model_dump(exclude_none=True),
            )
        ).json()

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
                f"/api/v1/dedup-mineral-sites/{MINMOD_KG.ns.mr.id(self.site1_dedup_uri)}",
                params={"commodity": self.site1_commodity},
            )
        ).json()
        assert resp == {
            "uri": self.site1_dedup_uri,
            "name": "Eagle Mine",
            "type": "NotSpecified",
            "rank": "U",
            "sites": [self.site1_uri],
            "deposit_types": [],
            "grade_tonnage": {"commodity": "Q578"},
        }

    def test_create_exist(self, auth_client, kg):
        resp = auth_client.post(
            "/api/v1/mineral-sites",
            json=self.site1.model_dump(exclude_none=True),
        )
        assert resp.json() == {"detail": "The site already exists."}
        assert resp.status_code == 403

    def test_update_site(self, auth_client, user1_uri, kg):
        sleep(1.0)  # to ensure the modified_at is different
        self.site1.name = "Frog Mine"
        self.site1.dedup_site_uri = MINMOD_KG.ns.mr.uristr(
            DedupMineralSite.get_id([self.site1_id])
        )
        resp = check_req(
            lambda: auth_client.post(
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
                f"/api/v1/dedup-mineral-sites/{MINMOD_KG.ns.mr.id(self.site1_dedup_uri)}",
                params={"commodity": self.site1_commodity},
            )
        ).json()
        assert resp == {
            "uri": self.site1_dedup_uri,
            "name": "Frog Mine",
            "type": "NotSpecified",
            "rank": "U",
            "sites": [self.site1_uri],
            "deposit_types": [],
            "grade_tonnage": {"commodity": "Q578"},
        }

    def test_create_new_site(self, auth_client_2, user2_uri, kg):
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
                    f"/api/v1/dedup-mineral-sites/{MINMOD_KG.ns.mr.id(self.site1_dedup_uri)}",
                    params={"commodity": commodity},
                )
            ).json()
            assert resp == {
                "uri": self.site1_dedup_uri,
                "name": "Frog Mine",
                "type": "NotSpecified",
                "rank": "U",
                "sites": [self.site1_uri, self.site2_uri],
                "deposit_types": [],
                "grade_tonnage": {"commodity": commodity},
            }
