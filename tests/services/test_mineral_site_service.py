from __future__ import annotations

import time
from pathlib import Path

import pytest
from minmodkg.api.models.public_mineral_site import (
    Coordinates,
    GradeTonnage,
    InputPublicMineralSite,
    OutputPublicMineralSite,
)
from minmodkg.libraries.rdf import TripleStore
from minmodkg.misc.utils import assert_not_none
from minmodkg.models.kg.base import NS_MR
from minmodkg.models.kg.candidate_entity import CandidateEntity
from minmodkg.models.kg.location_info import LocationInfo
from minmodkg.models.kg.mineral_inventory import MineralInventory
from minmodkg.models.kg.reference import Document, Reference
from minmodkg.models.kgrel.user import User
from minmodkg.services.mineral_site import MineralSiteService
from sqlalchemy import Engine


class TestMSData:
    @pytest.fixture(autouse=True)
    def site1_(self, user1: User):
        self.site1_commodity = "Q578"
        self.site1 = InputPublicMineralSite(
            source_id="https://mrdata.usgs.gov/mrds",
            record_id="10014570",
            name="Eagle Mine",
            location_info=LocationInfo(
                location="POINT(-87.099998474121 46.900001525879)",
            ),
            created_by=[user1.get_uri()],
            mineral_inventory=[
                MineralInventory(
                    commodity=CandidateEntity(
                        source="https://mrdata.usgs.gov/mrds",
                        confidence=1.0,
                        observed_name="Nickel",
                        normalized_uri=NS_MR.uristr(self.site1_commodity),
                    ),
                    reference=Reference(
                        document=Document(uri="https://mrdata.usgs.gov/mrds")
                    ),
                )
            ],
        )
        self.site1_output = OutputPublicMineralSite(
            id=self.site1.id,
            source_id=self.site1.source_id,
            record_id=self.site1.record_id,
            dedup_site_uri=self.site1.dedup_site_uri,
            name=self.site1.name,
            created_by=[user1.get_uri()],
            aliases=self.site1.aliases,
            site_rank=self.site1.site_rank,
            site_type=self.site1.site_type,
            location_info=self.site1.location_info,
            deposit_type_candidate=self.site1.deposit_type_candidate,
            mineral_inventory=self.site1.mineral_inventory,
            reference=self.site1.reference,
            coordinates=Coordinates(
                lat=46.900001525879,
                lon=-87.099998474121,
            ),
            grade_tonnage=[GradeTonnage(commodity=self.site1_commodity)],
        )

    # @pytest.fixture(autouse=True)
    # def site2_(self, user2: User):
    #     self.site2_commodity = "Q569"
    #     self.site2 = InputPublicMineralSite(
    #         source_id="https://mrdata.usgs.gov/mrds",
    #         record_id="10109359",
    #         name="Beaver Mine",
    #         location_info=LocationInfo(
    #             location="POINT(-118.7805 44.71207)",
    #         ),
    #         created_by=[user2.get_uri()],
    #         mineral_inventory=[
    #             MineralInventory(
    #                 commodity=CandidateEntity(
    #                     source="https://mrdata.usgs.gov/mrds",
    #                     confidence=1.0,
    #                     observed_name="Lithium",
    #                     normalized_uri=MINMOD_KG.ns.mr.uristr(self.site2_commodity),
    #                 ),
    #                 reference=Reference(
    #                     document=Document(uri="https://mrdata.usgs.gov/mrds")
    #                 ),
    #             )
    #         ],
    #         dedup_site_uri=self.site1_dedup_uri,
    #     )
    #     self.site2_id = make_site_uri(
    #         self.site2.source_id, self.site2.record_id, namespace=""
    #     )
    #     self.site2_uri = make_site_uri(self.site2.source_id, self.site2.record_id)
    #     self.site2_dump = self.site2.to_dict()
    #     self.site2_dump.update(
    #         {
    #             "coordinates": {
    #                 "lat": 44.71207,
    #                 "lon": -118.7805,
    #             },
    #             "dedup_site_uri": self.site2.dedup_site_uri,
    #             "created_by": [user2.get_uri()],
    #             "id": self.site2_id,
    #             "grade_tonnage": [{"commodity": self.site2_commodity}],
    #         }
    #     )
    #     del self.site2_dump["modified_at"]


class TestCreateMineralSite(TestMSData):

    def test_create_mineral_site_first(
        self, user1: User, kg: TripleStore, kgrel: Engine
    ):
        service = MineralSiteService(kgrel)
        service.create(self.site1.to_kgrel(user1.get_uri()))

        out_ms = OutputPublicMineralSite.from_kgrel(
            assert_not_none(service.find_by_id(self.site1.id))
        )
        assert out_ms.to_dict() == dict(
            self.site1_output.to_dict(),
            modified_at=out_ms.modified_at,
            snapshot_id=out_ms.snapshot_id,
        )


class TestLinkMineralSite(TestMSData):
    def test_update_same_as(self, resource_dir: Path, user1: User, kgrel_with_data):
        time.sleep(1.0)  # to ensure the modified_at is different
        mineral_site_service = MineralSiteService(kgrel_with_data)
        mineral_site_service.update_same_as(
            user1.get_uri(),
            [
                [
                    "site__api-cdr-land-v1-docs-documents__02a0c7412e655ff0a9a4eb63cd1388ecb4aee96931f8bc4f98819e65cc83173755",
                    "site__api-cdr-land-v1-docs-documents__02a000a83e76360bec8f3fce2ff46cc8099f950cc1f757f8a16592062c49b3a5c5",
                ]
            ],
        )
