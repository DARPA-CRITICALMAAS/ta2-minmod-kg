from __future__ import annotations

import pytest
from minmodkg.api.models.public_mineral_site import InputPublicMineralSite
from minmodkg.libraries.rdf.triple_store import TripleStore
from minmodkg.misc.utils import assert_not_none
from minmodkg.models.kg.base import NS_MR
from minmodkg.models.kg.candidate_entity import CandidateEntity
from minmodkg.models.kg.location_info import LocationInfo
from minmodkg.models.kg.measure import Measure
from minmodkg.models.kg.mineral_inventory import MineralInventory
from minmodkg.models.kg.mineral_site import MineralSite as KGMineralSite
from minmodkg.models.kg.reference import Document, Reference
from minmodkg.models.kgrel.user import User
from minmodkg.services.mineral_site import MineralSiteService
from minmodkg.services.sync.kgsync_listener import KGSyncListener
from minmodkg.services.sync.sync import process_pending_events
from sqlalchemy import Engine


class TestKGSyncListenerData:
    @pytest.fixture(autouse=True)
    def site1_(self, user1: User):
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
                        normalized_uri=NS_MR.uristr("Q578"),
                    ),
                    reference=Reference(
                        document=Document(uri="https://mrdata.usgs.gov/mrds")
                    ),
                )
            ],
        ).to_kgrel(user1)

    @pytest.fixture(autouse=True)
    def site2_(self, user2: User):
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
                        normalized_uri=NS_MR.uristr("Q569"),
                    ),
                    reference=Reference(
                        document=Document(uri="https://mrdata.usgs.gov/mrds")
                    ),
                )
            ],
        ).to_kgrel(user2)


class TestKGSyncListener(TestKGSyncListenerData):
    def test_add_site(self, kg: TripleStore, kgrel: Engine, user1: User):
        service = MineralSiteService(kgrel)
        kgsync_listener = KGSyncListener()

        # create a new mineral site
        service.create(self.site1)

        # fetch the mineral site from the triple store and we find nothing
        assert not kg.has(self.site1.ms.site_uri)

        # the listener is triggered and the mineral site is added to the triple store
        process_pending_events([kgsync_listener])

        # fetch the mineral site from the triple store and we find it
        assert kg.has(self.site1.ms.site_uri)
        kgsite = KGMineralSite.from_graph(
            self.site1.ms.site_uri,
            kgsync_listener._get_mineral_site_graph_by_uri(self.site1.ms.site_uri),
        )
        assert self.site1.ms.to_kg() == kgsite

    def test_update_site(self, kg: TripleStore, kgrel: Engine, user1: User):
        service = MineralSiteService(kgrel)
        kgsync_listener = KGSyncListener()

        # this is continue from the previous test -- update existing mineral site
        self.site1 = InputPublicMineralSite(
            source_id="database::https://mrdata.usgs.gov/mrds",
            record_id="10014570",
            name="Frog Mine",
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
                        normalized_uri=NS_MR.uristr("Q578"),
                    ),
                    grade=Measure(value=0.5),
                    reference=Reference(
                        document=Document(uri="https://mrdata.usgs.gov/mrds")
                    ),
                )
            ],
        ).to_kgrel(user1)
        self.site1.set_id(
            assert_not_none(service.get_site_db_id(self.site1.ms.site_id))
        )
        service.update(self.site1)

        # fetch the mineral site from the triple store and we find it
        assert kg.has(self.site1.ms.site_uri)
        kgsite = KGMineralSite.from_graph(
            self.site1.ms.site_uri,
            kgsync_listener._get_mineral_site_graph_by_uri(self.site1.ms.site_uri),
        )
        assert self.site1.ms.to_kg() != kgsite

        # the listener is triggered and the mineral site is updated in the triple store
        process_pending_events([kgsync_listener])

        kgsite = KGMineralSite.from_graph(
            self.site1.ms.site_uri,
            kgsync_listener._get_mineral_site_graph_by_uri(self.site1.ms.site_uri),
        )
        assert self.site1.ms.to_kg() == kgsite

    def test_update_same_as(
        self, kg: TripleStore, kgrel: Engine, user1: User, user2: User
    ):
        service = MineralSiteService(kgrel)
        kgsync_listener = KGSyncListener()

        # first, make sure that we have two mineral sites in the database
        service.create(self.site2)

        service.update_same_as([[self.site1.ms.site_id, self.site2.ms.site_id]])

        # fetch the mineral sites from the triple store
        process_pending_events([kgsync_listener])

        existing_links = kgsync_listener._get_all_same_as_links(
            [self.site1.ms.site_id, self.site2.ms.site_id]
        )
        key_ns = KGMineralSite.__subj__.key_ns
        assert existing_links == [
            (key_ns[self.site1.ms.site_id], key_ns[self.site2.ms.site_id]),
            (key_ns[self.site2.ms.site_id], key_ns[self.site1.ms.site_id]),
        ]
