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
from minmodkg.models.kg.base import MINMOD_NS, NS_MR
from minmodkg.models.kg.candidate_entity import CandidateEntity
from minmodkg.models.kg.location_info import LocationInfo
from minmodkg.models.kg.mineral_inventory import MineralInventory
from minmodkg.models.kg.reference import Document, Reference
from minmodkg.models.kgrel.mineral_site import MineralSite
from minmodkg.models.kgrel.user import User
from minmodkg.services.kgrel_entity import EntityService
from minmodkg.services.mineral_site import ArgumentError, MineralSiteService
from sqlalchemy import Engine
from tests.utils import load_mineral_sites


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
            created_by=user1.get_uri(),
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
            dedup_site_uri=MINMOD_NS.md.uristr(
                MineralSite.get_dedup_id([self.site1.id])
            ),
            name=self.site1.name,
            created_by=user1.get_uri(),
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


class TestUpsertMineralSite(TestMSData):

    def test_upsert_mineral_site_first(
        self, user1: User, kg: TripleStore, kgrel: Engine
    ):
        service = MineralSiteService(kgrel)
        service.upsert([self.site1.to_kgrel(user1.get_uri())])

        out_ms = OutputPublicMineralSite.from_kgrel(
            assert_not_none(service.find_by_id(self.site1.id))
        )
        assert out_ms.to_dict() == dict(
            self.site1_output.to_dict(),
            modified_at=out_ms.modified_at,
            snapshot_id=out_ms.snapshot_id,
        )


class TestLinkMineralSite(TestMSData):
    def test_update_same_as(self, resource_dir: Path, user1: User, kgrel: Engine):
        time.sleep(1.0)  # to ensure the modified_at is different

        load_mineral_sites(
            kgrel,
            user1,
            [
                resource_dir
                / "kgdata/mineral-sites/json/Forrestania_Nickel_Project.json"
            ],
        )

        ms_service = MineralSiteService(kgrel)

        # from sqlalchemy import select
        # from sqlalchemy.orm import Session
        # from minmodkg.models.kgrel.dedup_mineral_site import DedupMineralSite
        # from minmodkg.models.kgrel.views.mineral_inventory_view import MineralInventoryView
        # with Session(kgrel) as session:
        #     dedup_sites = session.execute(select(DedupMineralSite)).scalars().all()
        #     print([dms.id for dms in dedup_sites])
        #     print(session.execute(select(MineralInventoryView).where(MineralInventoryView.dedup_site_id == dedup_sites[0].id)).scalars().all())

        # verify that the dedup site is correct
        dedup_res = ms_service.find_dedup_mineral_sites(
            commodity="Q578", return_count=True
        )
        assert dedup_res["total"] == 1 and len(dedup_res["items"]) == 1
        assert [
            (ss.site_id, ss.score.score)
            for ss in list(dedup_res["items"].values())[0].dms.ranked_sites
        ] == [
            (
                "site__api-cdr-land-v1-docs-documents__029ae7b78f5f44212f0088966cf17f38531c93dbb8b43ed8989f4975eaffc8c1d7__inferlink",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__029ae7b78f5f44212f0088966cf17f38531c93dbb8b43ed8989f4975eaffc8c1d7__sri",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__02965ca0428670b436cb460c27c19c41e53fe0d354e5eba41157a2f6c2e6cb91a0__inferlink",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__02965ca0428670b436cb460c27c19c41e53fe0d354e5eba41157a2f6c2e6cb91a0__sri",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__028df78e01cc5ee3b5982ac3063c7872ef73abae26c04db2df8f2ff1ccd58d7f88__inferlink",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__028df78e01cc5ee3b5982ac3063c7872ef73abae26c04db2df8f2ff1ccd58d7f88__sri",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__021924522fa0068d40d1d20c24d7a0c667709640802db026aa30eb94608fca28cf__inferlink",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__021924522fa0068d40d1d20c24d7a0c667709640802db026aa30eb94608fca28cf__sri",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__02026fd483f35adec27e48d1011a68135f93ab7d2b10d2d9507cb1412a5211275c__inferlink",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__02026fd483f35adec27e48d1011a68135f93ab7d2b10d2d9507cb1412a5211275c__sri",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__02021070bee9adf63d0d61ff45e07de2c2c128272c6209e1fa4ad6849c68505ed1__inferlink",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__02021070bee9adf63d0d61ff45e07de2c2c128272c6209e1fa4ad6849c68505ed1__sri",
                0.8,
            ),
            ("site__mrdata-usgs-gov-mrds__10280772__umn", 0.1),
            ("site__mrdata-usgs-gov-mrds__10280772__sri", 0.1),
        ]

        # cannot split sites that belong to the same record of a data source
        with pytest.raises(ArgumentError):
            ms_service.update_same_as(
                user1.get_uri(),
                [
                    [
                        "site__api-cdr-land-v1-docs-documents__02021070bee9adf63d0d61ff45e07de2c2c128272c6209e1fa4ad6849c68505ed1__inferlink",
                    ],
                    [
                        "site__api-cdr-land-v1-docs-documents__029ae7b78f5f44212f0088966cf17f38531c93dbb8b43ed8989f4975eaffc8c1d7__inferlink",
                        "site__api-cdr-land-v1-docs-documents__029ae7b78f5f44212f0088966cf17f38531c93dbb8b43ed8989f4975eaffc8c1d7__sri",
                        "site__api-cdr-land-v1-docs-documents__02965ca0428670b436cb460c27c19c41e53fe0d354e5eba41157a2f6c2e6cb91a0__inferlink",
                        "site__api-cdr-land-v1-docs-documents__02965ca0428670b436cb460c27c19c41e53fe0d354e5eba41157a2f6c2e6cb91a0__sri",
                        "site__api-cdr-land-v1-docs-documents__028df78e01cc5ee3b5982ac3063c7872ef73abae26c04db2df8f2ff1ccd58d7f88__inferlink",
                        "site__api-cdr-land-v1-docs-documents__028df78e01cc5ee3b5982ac3063c7872ef73abae26c04db2df8f2ff1ccd58d7f88__sri",
                        "site__api-cdr-land-v1-docs-documents__021924522fa0068d40d1d20c24d7a0c667709640802db026aa30eb94608fca28cf__inferlink",
                        "site__api-cdr-land-v1-docs-documents__021924522fa0068d40d1d20c24d7a0c667709640802db026aa30eb94608fca28cf__sri",
                        "site__api-cdr-land-v1-docs-documents__02026fd483f35adec27e48d1011a68135f93ab7d2b10d2d9507cb1412a5211275c__inferlink",
                        "site__api-cdr-land-v1-docs-documents__02026fd483f35adec27e48d1011a68135f93ab7d2b10d2d9507cb1412a5211275c__sri",
                        "site__api-cdr-land-v1-docs-documents__02021070bee9adf63d0d61ff45e07de2c2c128272c6209e1fa4ad6849c68505ed1__sri",
                        "site__mrdata-usgs-gov-mrds__10280772__umn",
                        "site__mrdata-usgs-gov-mrds__10280772__sri",
                    ],
                ],
            )

        new_dedup_ids = ms_service.update_same_as(
            user1.get_uri(),
            [
                [
                    "site__api-cdr-land-v1-docs-documents__02021070bee9adf63d0d61ff45e07de2c2c128272c6209e1fa4ad6849c68505ed1__inferlink",
                    "site__api-cdr-land-v1-docs-documents__02021070bee9adf63d0d61ff45e07de2c2c128272c6209e1fa4ad6849c68505ed1__sri",
                ],
                [
                    "site__api-cdr-land-v1-docs-documents__029ae7b78f5f44212f0088966cf17f38531c93dbb8b43ed8989f4975eaffc8c1d7__inferlink",
                    "site__api-cdr-land-v1-docs-documents__029ae7b78f5f44212f0088966cf17f38531c93dbb8b43ed8989f4975eaffc8c1d7__sri",
                    "site__api-cdr-land-v1-docs-documents__02965ca0428670b436cb460c27c19c41e53fe0d354e5eba41157a2f6c2e6cb91a0__inferlink",
                    "site__api-cdr-land-v1-docs-documents__02965ca0428670b436cb460c27c19c41e53fe0d354e5eba41157a2f6c2e6cb91a0__sri",
                    "site__api-cdr-land-v1-docs-documents__028df78e01cc5ee3b5982ac3063c7872ef73abae26c04db2df8f2ff1ccd58d7f88__inferlink",
                    "site__api-cdr-land-v1-docs-documents__028df78e01cc5ee3b5982ac3063c7872ef73abae26c04db2df8f2ff1ccd58d7f88__sri",
                    "site__api-cdr-land-v1-docs-documents__021924522fa0068d40d1d20c24d7a0c667709640802db026aa30eb94608fca28cf__inferlink",
                    "site__api-cdr-land-v1-docs-documents__021924522fa0068d40d1d20c24d7a0c667709640802db026aa30eb94608fca28cf__sri",
                    "site__api-cdr-land-v1-docs-documents__02026fd483f35adec27e48d1011a68135f93ab7d2b10d2d9507cb1412a5211275c__inferlink",
                    "site__api-cdr-land-v1-docs-documents__02026fd483f35adec27e48d1011a68135f93ab7d2b10d2d9507cb1412a5211275c__sri",
                    "site__mrdata-usgs-gov-mrds__10280772__umn",
                    "site__mrdata-usgs-gov-mrds__10280772__sri",
                ],
            ],
        )

        assert new_dedup_ids == [
            "dedup_site__api-cdr-land-v1-docs-documents__02021070bee9adf63d0d61ff45e07de2c2c128272c6209e1fa4ad6849c68505ed1__inferlink",
            "dedup_site__api-cdr-land-v1-docs-documents__02026fd483f35adec27e48d1011a68135f93ab7d2b10d2d9507cb1412a5211275c__inferlink",
        ]

        dedup_res = ms_service.find_dedup_mineral_sites(
            commodity="Q578", return_count=True
        )
        assert dedup_res["total"] == 2 and len(dedup_res["items"]) == 2
        dmsi1, dmsi2 = list(dedup_res["items"].values())
        assert [(ss.site_id, ss.score.score) for ss in dmsi1.dms.ranked_sites] == [
            (
                "site__api-cdr-land-v1-docs-documents__02021070bee9adf63d0d61ff45e07de2c2c128272c6209e1fa4ad6849c68505ed1__inferlink",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__02021070bee9adf63d0d61ff45e07de2c2c128272c6209e1fa4ad6849c68505ed1__sri",
                0.8,
            ),
        ]
        assert [(ss.site_id, ss.score.score) for ss in dmsi2.dms.ranked_sites] == [
            (
                "site__api-cdr-land-v1-docs-documents__029ae7b78f5f44212f0088966cf17f38531c93dbb8b43ed8989f4975eaffc8c1d7__inferlink",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__029ae7b78f5f44212f0088966cf17f38531c93dbb8b43ed8989f4975eaffc8c1d7__sri",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__02965ca0428670b436cb460c27c19c41e53fe0d354e5eba41157a2f6c2e6cb91a0__inferlink",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__02965ca0428670b436cb460c27c19c41e53fe0d354e5eba41157a2f6c2e6cb91a0__sri",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__028df78e01cc5ee3b5982ac3063c7872ef73abae26c04db2df8f2ff1ccd58d7f88__inferlink",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__028df78e01cc5ee3b5982ac3063c7872ef73abae26c04db2df8f2ff1ccd58d7f88__sri",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__021924522fa0068d40d1d20c24d7a0c667709640802db026aa30eb94608fca28cf__inferlink",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__021924522fa0068d40d1d20c24d7a0c667709640802db026aa30eb94608fca28cf__sri",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__02026fd483f35adec27e48d1011a68135f93ab7d2b10d2d9507cb1412a5211275c__inferlink",
                0.8,
            ),
            (
                "site__api-cdr-land-v1-docs-documents__02026fd483f35adec27e48d1011a68135f93ab7d2b10d2d9507cb1412a5211275c__sri",
                0.8,
            ),
            ("site__mrdata-usgs-gov-mrds__10280772__umn", 0.1),
            ("site__mrdata-usgs-gov-mrds__10280772__sri", 0.1),
        ]
