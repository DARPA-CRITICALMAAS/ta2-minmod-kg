from __future__ import annotations

import time
from collections import defaultdict
from pathlib import Path
from typing import Literal

import serde.json
from minmodkg.config import MINMOD_KGREL_DB
from minmodkg.models.kgrel.base import Base
from minmodkg.models.kgrel.data_source import DataSource
from minmodkg.models.kgrel.dedup_mineral_site import DedupMineralSite
from minmodkg.models.kgrel.entities.commodity import Commodity
from minmodkg.models.kgrel.entities.commodity_form import CommodityForm
from minmodkg.models.kgrel.entities.country import Country
from minmodkg.models.kgrel.entities.crs import CRS
from minmodkg.models.kgrel.entities.deposit_type import DepositType
from minmodkg.models.kgrel.entities.state_or_province import StateOrProvince
from minmodkg.models.kgrel.entities.unit import Unit
from minmodkg.models.kgrel.mineral_site import MineralSite
from minmodkg.models.kgrel.views.mineral_inventory_view import MineralInventoryView
from minmodkg.services.mineral_site import MineralSiteService
from sqlalchemy import Engine, create_engine, insert
from sqlalchemy.orm import Session
from tqdm import tqdm

from statickg.models.file_and_path import InputFile
from statickg.services.data_loader import (
    DataLoaderService,
    DataLoaderServiceInvokeArgs,
    DBInfo,
)


class PostgresLoaderService(DataLoaderService):

    def get_db_service_id(self, db_store_dir: Path) -> str:
        return f"kgrel-{db_store_dir.name}"

    def _wait_till_service_is_ready(self, dbinfo: DBInfo):
        """Wait until the service is ready"""
        retry_after = 0.2
        max_wait_seconds = 30
        print("Wait for the service to be ready", end="", flush=True)
        for _ in range(int(max_wait_seconds / retry_after)):
            try:
                self.get_engine(dbinfo)
                break
            except Exception:
                print(".", end="", flush=True)
                time.sleep(retry_after)
        else:
            raise Exception("Service is not ready")
        print("Service is ready")

    def replace_files(
        self,
        args: DataLoaderServiceInvokeArgs,
        dbinfo: DBInfo,
        files: list[InputFile],
    ):
        """Replace the content of the files in the database. We expect the the entities in the files are the same, only the content is different."""
        raise Exception("PostgresLoaderService does not need to replace files")

    def load_files(
        self,
        args: DataLoaderServiceInvokeArgs,
        dbinfo: DBInfo,
        files: list[InputFile],
    ):
        """Load files into the database"""
        assert len(files) > 0
        self.start_service(dbinfo)
        engine = self.get_engine(dbinfo)

        tables = defaultdict(list)
        for file in tqdm(files, desc="Loading files"):
            for table, records in serde.json.deser(file.path).items():
                tables[table].extend(records)

        self.restore(engine, tables)

    def get_engine(self, dbinfo: DBInfo):
        endpoint = dbinfo.endpoint
        assert endpoint is not None

        connection_url = (
            MINMOD_KGREL_DB.split("@", 1)[0]
            + "@"
            + endpoint.hostname[len("http://") :]
            + ":"
            + str(endpoint.port)
            + "/minmod"
        )
        engine = create_engine(connection_url)
        Base.metadata.create_all(engine)
        return engine

    def restore(self, engine: Engine, tables: dict[str, list], batch_size: int = 1024):
        with Session(engine) as session:
            for cls in [
                Unit,
                Commodity,
                DepositType,
                Country,
                StateOrProvince,
                CommodityForm,
                DataSource,
                CRS,
            ]:
                table = cls.__name__
                if table in tables:
                    records = tables[table]
                    for i in tqdm(
                        list(range(0, len(records), batch_size)), desc=f"Saving {table}"
                    ):
                        batch0 = records[i : i + batch_size]
                        batch0 = [cls.from_dict(r) for r in batch0]
                        session.bulk_save_objects(batch0)

            table = "DedupMineralSite"
            if table in tables:
                records = tables[table]
                for i in tqdm(
                    list(range(0, len(records), batch_size)), desc=f"Saving {table}"
                ):
                    batch1 = records[i : i + batch_size]
                    batch1 = [DedupMineralSite.from_dict(r) for r in batch1]
                    # can't use the newer API because I haven't figured out how to make SqlAlchemy
                    # automatically handle the custom types (TypeDecorator) yet.
                    session.bulk_save_objects(batch1)

            table = "MineralSite"
            site2id = {}
            if table in tables:
                records = tables[table]
                for i in tqdm(
                    list(range(0, len(records), batch_size)), desc=f"Saving {table}"
                ):
                    batch2 = records[i : i + batch_size]
                    batch2 = [MineralSite.from_dict(r) for r in batch2]
                    session.bulk_save_objects(batch2, return_defaults=True)
                    for r in batch2:
                        site2id[r.site_id] = r.id

            table = "MineralInventoryView"
            if table in tables:
                records = tables[table]
                for i in tqdm(
                    list(range(0, len(records), batch_size)), desc=f"Saving {table}"
                ):
                    batch3 = []
                    for r in records[i : i + batch_size]:
                        sid = site2id[r["site"]]
                        for inv in r["invs"]:
                            inv["site_id"] = sid
                            batch3.append(inv)
                    if len(batch3) > 0:
                        session.execute(insert(MineralInventoryView), batch3)

            session.commit()
