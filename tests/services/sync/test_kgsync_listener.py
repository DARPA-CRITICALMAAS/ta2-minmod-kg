from __future__ import annotations

from minmodkg.api.models.public_mineral_site import InputPublicMineralSite
from minmodkg.models.kg.location_info import LocationInfo
from minmodkg.models.kgrel.user import User
from minmodkg.services.mineral_site import MineralSiteService
from sqlalchemy import Engine


class TestKGSyncListener:
    def test_add_site(self, kgrel: Engine, user1: User):
        service = MineralSiteService(kgrel)
        service.create(
            InputPublicMineralSite(
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
                            normalized_uri=NS_MR.uristr(self.site1_commodity),
                        ),
                        reference=Reference(
                            document=Document(uri="https://mrdata.usgs.gov/mrds")
                        ),
                    )
                ],
            ).to_kgrel(user1)
        )
