from __future__ import annotations

from copy import deepcopy

import pytest
from minmodkg.api.models.public_mineral_site import InputPublicMineralSite
from minmodkg.models.kg.base import NS_MR
from minmodkg.models.kg.candidate_entity import CandidateEntity
from minmodkg.models.kg.location_info import LocationInfo
from minmodkg.models.kg.measure import Measure
from minmodkg.models.kg.mineral_inventory import MineralInventory
from minmodkg.models.kg.reference import Document, Reference
from minmodkg.models.kgrel.mineral_site import MineralSiteAndInventory
from minmodkg.models.kgrel.user import User


@pytest.fixture()
def sync_site1(user1: User) -> InputPublicMineralSite:
    return InputPublicMineralSite(
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
                    normalized_uri=NS_MR.uristr("Q578"),
                ),
                reference=Reference(
                    document=Document(uri="https://mrdata.usgs.gov/mrds")
                ),
            )
        ],
    )


@pytest.fixture()
def sync_site1_update_name_and_inventory(
    sync_site1: InputPublicMineralSite,
) -> InputPublicMineralSite:
    new_site = deepcopy(sync_site1)
    new_site.name = "Frog Mine"
    new_site.mineral_inventory = [
        MineralInventory(
            commodity=CandidateEntity(
                source="https://mrdata.usgs.gov/mrds",
                confidence=1.0,
                observed_name="Nickel",
                normalized_uri=NS_MR.uristr("Q578"),
            ),
            grade=Measure(value=0.5),
            reference=Reference(document=Document(uri="https://mrdata.usgs.gov/mrds")),
        )
    ]
    return new_site


@pytest.fixture()
def sync_site2(user2: User) -> InputPublicMineralSite:
    return InputPublicMineralSite(
        source_id="https://mrdata.usgs.gov/mrds",
        record_id="10109359",
        name="Beaver Mine",
        location_info=LocationInfo(
            location="POINT(-118.7805 44.71207)",
        ),
        created_by=user2.get_uri(),
        mineral_inventory=[
            MineralInventory(
                commodity=CandidateEntity(
                    source="https://mrdata.usgs.gov/mrds",
                    confidence=1.0,
                    observed_name="Lithium",
                    normalized_uri=NS_MR.uristr("Q569"),
                ),
                reference=Reference(
                    document=Document(uri="https://mrdata.usgs.gov/mrds")
                ),
            )
        ],
    )
