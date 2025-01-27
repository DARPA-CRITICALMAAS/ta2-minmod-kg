from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest
import serde.csv
import serde.json
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
from minmodkg.services.sync.backup_listener import BackupListener
from minmodkg.services.sync.kgsync_listener import KGSyncListener
from minmodkg.services.sync.sync import process_pending_events
from sqlalchemy import Engine


class MockRepository:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.commit_message = None

    def commit_all(self, message: str):
        self.commit_message = message
        return self

    def push(self):
        return self


@pytest.fixture()
def patch_git_repo(monkeypatch):
    monkeypatch.setattr(
        "minmodkg.services.sync.backup_listener.GitRepository", MockRepository
    )


class TestBackupListener:
    def test_add_site(
        self,
        kg: TripleStore,
        kgrel: Engine,
        sync_site1: InputPublicMineralSite,
        patch_git_repo,
        tmp_dir: Path,
    ):
        service = MineralSiteService(kgrel)
        backup_listener = BackupListener(tmp_dir)

        # create a new mineral site
        rel_site1 = sync_site1.to_kgrel(sync_site1.created_by)
        service.create(rel_site1)

        # the tmp dir should be empty
        assert not any(os.scandir(tmp_dir))

        # the listener is triggered and the data is written to the tmp dir
        process_pending_events(backup_listener)

        sites = {
            (ms := KGMineralSite.from_dict(r)).id: ms
            for file in tmp_dir.glob("data/mineral-sites/**/*.json*")
            for r in serde.json.deser(file)
        }

        assert rel_site1.ms.to_kg() == sites[rel_site1.ms.site_id]

    def test_update_site(
        self,
        kg: TripleStore,
        kgrel: Engine,
        user1: User,
        sync_site1_update_name_and_inventory: InputPublicMineralSite,
        patch_git_repo,
        tmp_dir: Path,
    ):
        service = MineralSiteService(kgrel)
        backup_listener = BackupListener(tmp_dir)

        # this is continue from the previous test -- update existing mineral site
        rel_site1 = sync_site1_update_name_and_inventory.to_kgrel(
            sync_site1_update_name_and_inventory.created_by
        )
        rel_site1.set_id(
            assert_not_none(
                service.get_site_db_id(sync_site1_update_name_and_inventory.id)
            )
        )
        service.update(rel_site1)

        # the mineral site is stored in the tmp dir
        sites = {
            (ms := KGMineralSite.from_dict(r)).id: ms
            for file in tmp_dir.glob("data/mineral-sites/**/*.json*")
            for r in serde.json.deser(file)
        }
        assert rel_site1.ms.to_kg() != sites[rel_site1.ms.site_id]

        # the listener is triggered and the mineral site is updated in the folder
        process_pending_events(backup_listener)

        sites = {
            (ms := KGMineralSite.from_dict(r)).id: ms
            for file in tmp_dir.glob("data/mineral-sites/**/*.json*")
            for r in serde.json.deser(file)
        }
        assert rel_site1.ms.to_kg() == sites[rel_site1.ms.site_id]

    def test_update_same_as(
        self,
        kg: TripleStore,
        kgrel: Engine,
        sync_site1_update_name_and_inventory: InputPublicMineralSite,
        sync_site2: InputPublicMineralSite,
        tmp_dir: Path,
        patch_git_repo,
    ):
        service = MineralSiteService(kgrel)
        backup_listener = BackupListener(tmp_dir)

        # first, make sure that we have two mineral sites in the database
        service.create(sync_site2.to_kgrel(sync_site2.created_by))

        service.update_same_as(
            sync_site2.created_by,
            [[sync_site1_update_name_and_inventory.id, sync_site2.id]],
        )

        # fetch the mineral sites from the triple store
        process_pending_events(backup_listener)

        assert {
            (r[0], r[1], int(r[3]))
            for file in tmp_dir.glob("data/same-as/**/*.csv*")
            for r in serde.csv.deser(file)[1:]
        } == {
            (sync_site1_update_name_and_inventory.id, sync_site2.id, 1),
        }
