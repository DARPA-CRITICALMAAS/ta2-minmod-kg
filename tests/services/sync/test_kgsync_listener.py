from __future__ import annotations

from minmodkg.api.models.public_mineral_site import InputPublicMineralSite
from minmodkg.libraries.rdf.triple_store import TripleStore
from minmodkg.misc.utils import assert_not_none
from minmodkg.models.kg.mineral_site import MineralSite as KGMineralSite
from minmodkg.models.kgrel.user import User
from minmodkg.services.mineral_site import MineralSiteService
from minmodkg.services.sync.kgsync_listener import KGSyncListener
from minmodkg.services.sync.sync import process_pending_events
from sqlalchemy import Engine


class TestKGSyncListener:
    def test_add_site(
        self, kg: TripleStore, kgrel: Engine, sync_site1: InputPublicMineralSite
    ):
        service = MineralSiteService(kgrel)
        kgsync_listener = KGSyncListener()

        relsite1 = sync_site1.to_kgrel(sync_site1.created_by[0])
        # create a new mineral site
        service.create(relsite1)

        # fetch the mineral site from the triple store and we find nothing
        assert not kg.has(sync_site1.uri)

        # the listener is triggered and the mineral site is added to the triple store
        process_pending_events(kgsync_listener)

        # fetch the mineral site from the triple store and we find it
        assert kg.has(sync_site1.uri)
        kgsite = KGMineralSite.from_graph(
            sync_site1.uri,
            kgsync_listener._get_mineral_site_graph_by_uri(sync_site1.uri),
        )
        assert relsite1.ms.to_kg() == kgsite

    def test_update_site(
        self,
        kg: TripleStore,
        kgrel: Engine,
        user1: User,
        sync_site1_update_name_and_inventory: InputPublicMineralSite,
    ):
        service = MineralSiteService(kgrel)
        kgsync_listener = KGSyncListener()

        # this is continue from the previous test -- update existing mineral site
        rel_site1 = sync_site1_update_name_and_inventory.to_kgrel(
            sync_site1_update_name_and_inventory.created_by[0]
        )
        rel_site1.set_id(
            assert_not_none(
                service.get_site_db_id(sync_site1_update_name_and_inventory.id)
            )
        )
        service.update(rel_site1)

        # fetch the mineral site from the triple store and we find it
        assert kg.has(rel_site1.ms.site_uri)
        kgsite = KGMineralSite.from_graph(
            rel_site1.ms.site_uri,
            kgsync_listener._get_mineral_site_graph_by_uri(rel_site1.ms.site_uri),
        )
        assert rel_site1.ms.to_kg() != kgsite

        # the listener is triggered and the mineral site is updated in the triple store
        process_pending_events(kgsync_listener)

        kgsite = KGMineralSite.from_graph(
            rel_site1.ms.site_uri,
            kgsync_listener._get_mineral_site_graph_by_uri(rel_site1.ms.site_uri),
        )
        assert rel_site1.ms.to_kg() == kgsite

    def test_update_same_as(
        self,
        kg: TripleStore,
        kgrel: Engine,
        sync_site1_update_name_and_inventory: InputPublicMineralSite,
        sync_site2: InputPublicMineralSite,
    ):
        service = MineralSiteService(kgrel)
        kgsync_listener = KGSyncListener()

        # first, make sure that we have two mineral sites in the database
        service.create(sync_site2.to_kgrel(sync_site2.created_by[0]))

        service.update_same_as(
            sync_site2.created_by[0],
            [[sync_site1_update_name_and_inventory.id, sync_site2.id]],
        )

        # fetch the mineral sites from the triple store
        process_pending_events(kgsync_listener)

        existing_links = kgsync_listener._get_all_same_as_links(
            [sync_site1_update_name_and_inventory.id, sync_site2.id]
        )
        key_ns = KGMineralSite.__subj__.key_ns
        assert existing_links == [
            (key_ns[sync_site1_update_name_and_inventory.id], key_ns[sync_site2.id]),
            (key_ns[sync_site2.id], key_ns[sync_site1_update_name_and_inventory.id]),
        ]
