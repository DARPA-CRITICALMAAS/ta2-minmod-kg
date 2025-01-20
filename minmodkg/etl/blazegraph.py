from __future__ import annotations

import subprocess
import time
from pathlib import Path

import httpx
from minmodkg.libraries.rdf.blazegraph import BlazeGraph
from minmodkg.models.kg.base import MINMOD_NS
from rdflib import Graph
from tqdm import tqdm

from statickg.models.file_and_path import BaseType, InputFile
from statickg.services.data_loader import (
    DataLoaderService,
    DataLoaderServiceInvokeArgs,
    DBInfo,
)


class BlazeGraphLoaderService(DataLoaderService):

    query_endpoint = "/blazegraph/sparql"
    update_endpoint = "/blazegraph/sparql"

    def _wait_till_service_is_ready(self, dbinfo: DBInfo):
        """Wait until the service is ready"""
        sparql_endpoint = f"{dbinfo.endpoint}{self.query_endpoint}"
        retry_after = 0.2
        max_wait_seconds = 30
        print("Wait for the service to be ready", end="", flush=True)
        for _ in range(int(max_wait_seconds / retry_after)):
            try:
                resp = httpx.head(sparql_endpoint, timeout=0.3)
                if resp.status_code == 200:
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
        self.start_service(dbinfo)

        g = Graph()
        for file in files:
            g.parse(file.path, format=self.detect_format(file.path))

        assert dbinfo.endpoint is not None
        triplestore = BlazeGraph(
            MINMOD_NS,
            f"{dbinfo.endpoint}{self.query_endpoint}",
            f"{dbinfo.endpoint}{self.update_endpoint}",
        )
        triplestore._sparql_update(
            "DELETE { ?s ?p ?o } INSERT { %s } WHERE { OPTIONAL { ?s ?p ?o VALUES ?s { %s } } }"
            % (
                " . ".join(f"{s.n3()} {p.n3()} {o.n3()}" for s, p, o in g),
                " ".join(s.n3() for s in g.subjects(unique=True)),
            )
        )

    def load_files(
        self, args: DataLoaderServiceInvokeArgs, dbinfo: DBInfo, files: list[InputFile]
    ):
        """Load files into the database"""
        assert len(files) > 0
        if dbinfo.endpoint is not None:
            # the service is already running
            upload_endpoint = f"{dbinfo.endpoint}{self.query_endpoint}"
            for file in tqdm(files, desc="Upload files to Fuseki"):
                self.upload_file(upload_endpoint, file.path)
        else:
            # prepare an input file containing all files needed to be loaded
            assert all(file.basetype == BaseType.DATA_DIR for file in files)

            file_lst_file = files[0].get_basedir() / "data_loader_input_files.txt"
            with open(file_lst_file, "w") as f:
                for file in files:
                    f.write(file.relpath + "\n")

            # execute the load command to load the data
            (subprocess.check_output if self.capture_output else subprocess.check_call)(
                self.load_command.format(
                    ID=self.get_db_service_id(dbinfo.dir),
                    DB_DIR=dbinfo.dir,
                    INPUT_FILE_LST="data_loader_input_files.txt",
                ),
                shell=True,
            )

    def upload_file(self, upload_endpoint: str, file: Path):
        resp = httpx.post(
            upload_endpoint,
            content=file.read_text(),
            headers={"Content-Type": f"text/{self.detect_format(file)}; charset=utf-8"},
            verify=False,
        )
        assert resp.status_code == 200, (resp.status_code, resp.text)

    def detect_format(self, file: Path):
        assert file.suffix == ".ttl", f"Only turtle files (.ttl) are supported: {file}"
        return "turtle"
