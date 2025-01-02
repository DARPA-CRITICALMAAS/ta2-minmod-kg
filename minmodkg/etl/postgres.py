from __future__ import annotations

import subprocess
import time
from pathlib import Path

import httpx
import serde.json
from minmodkg.models.views.base import Base
from minmodkg.models.views.computed_mineral_site import ComputedMineralSite
from rdflib import Graph
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from statickg.models.file_and_path import BaseType, InputFile
from statickg.services.data_loader import (
    DataLoaderService,
    DataLoaderServiceInvokeArgs,
    DBInfo,
)


class PostgresLoaderService(DataLoaderService):

    def get_db_service_id(self, db_store_dir: Path) -> str:
        return f"dbviewloader-{db_store_dir.name}"

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
        self, args: DataLoaderServiceInvokeArgs, dbinfo: DBInfo, files: list[InputFile]
    ):
        """Replace the content of the files in the database. We expect the the entities in the files are the same, only the content is different."""
        raise Exception("PostgresLoaderService does not need to replace files")

    def load_files(
        self, args: DataLoaderServiceInvokeArgs, dbinfo: DBInfo, files: list[InputFile]
    ):
        """Load files into the database"""
        assert len(files) > 0
        self.start_service(dbinfo)

        records = [
            ComputedMineralSite.from_dict(record)
            for file in files
            for record in serde.json.deser(file.path)
        ]
        engine = self.get_engine(dbinfo)
        with Session(engine) as session:
            session.bulk_save_objects(records, return_defaults=True)
            for r in records:
                for gt in r.grade_tonnages:
                    assert r.id is not None
                    gt.site_id = r.id
            session.bulk_save_objects([gt for r in records for gt in r.grade_tonnages])
            session.commit()

    def get_engine(self, dbinfo: DBInfo):
        endpoint = dbinfo.endpoint
        assert endpoint is not None

        connection_url = (
            "postgresql+psycopg2://postgres:postgres@"
            + endpoint.hostname[len("http://") :]
            + ":"
            + str(endpoint.port)
            + "/minmod"
        )
        engine = create_engine(connection_url)
        Base.metadata.create_all(engine)
        return engine
