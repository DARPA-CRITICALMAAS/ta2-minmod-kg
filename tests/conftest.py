from __future__ import annotations

import shutil
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Generator

import jwt
import minmodkg.models.kgrel.base
import pytest
import serde.json
from fastapi.testclient import TestClient
from minmodkg import config
from minmodkg.api.main import app
from minmodkg.etl.kgrel_entity import EntityDeserFn
from minmodkg.libraries.rdf.blazegraph import BlazeGraph
from minmodkg.libraries.rdf.fuseki import FusekiDB
from minmodkg.libraries.rdf.triple_store import TripleStore
from minmodkg.libraries.rdf.virtuoso import VirtuosoDB
from minmodkg.models.kg.base import MINMOD_KG
from minmodkg.models.kgrel.mineral_site import MineralSiteAndInventory
from minmodkg.models.kgrel.user import User
from minmodkg.services.kgrel_entity import EntityService
from minmodkg.services.mineral_site import MineralSiteService
from rdflib import Graph
from sqlalchemy import Engine
from sqlalchemy.orm import Session
from timer import Timer


@pytest.fixture(scope="session")
def resource_dir():
    return Path(__file__).parent / "resources"


@pytest.fixture(scope="class")
def user1() -> User:
    return User(
        username="admin",
        name="Administrator",
        email="admin@example.com",
        password=User.encrypt_password("admin123@!"),
    )


@pytest.fixture(scope="class")
def user2() -> User:
    return User(
        username="tester",
        name="Tester",
        email="tester@example.com",
        password=User.encrypt_password("tester123@!"),
    )


@pytest.fixture(scope="class")
def users(user1: User, user2: User):
    return [user1, user2]


@pytest.fixture(scope="session")
def kg_singleton(resource_dir: Path):
    if isinstance(MINMOD_KG, FusekiDB):
        start_cmd = (
            "-p 13030:3030 minmod-fuseki fuseki-server --config=fuseki/config.ttl"
        )
    elif isinstance(MINMOD_KG, VirtuosoDB):
        start_cmd = "-p 13030:8890 minmod-virtuoso"
    elif isinstance(MINMOD_KG, BlazeGraph):
        start_cmd = "-p 13030:9999 minmod-blazegraph"
    else:
        raise NotImplementedError()

    try:
        subprocess.check_output(
            f"docker run --name=test-kg --rm -d {start_cmd}",
            shell=True,
        )
    except subprocess.CalledProcessError as e:
        subprocess.check_output("docker rm -f test-kg", shell=True)
        subprocess.check_output(
            f"docker run --name=test-kg --rm -d {start_cmd}",
            shell=True,
        )

    print("\nWaiting for TripleStore to start", end="", flush=True)
    for i in range(100):
        try:
            MINMOD_KG.count_all()
            break
        except Exception as e:
            print(".", end="", flush=True)
            time.sleep(0.5)
    print(" DONE!", flush=True)

    yield MINMOD_KG

    subprocess.check_output("docker container rm -f test-kg", shell=True)


@pytest.fixture(scope="session")
def kgrel_singleton(resource_dir: Path):
    start_cmd = "-p 15432:5432 minmod-postgres"
    try:
        subprocess.check_output(
            f"docker run --name=test-kgrel --rm -d {start_cmd}",
            shell=True,
        )
    except subprocess.CalledProcessError as e:
        subprocess.check_output("docker rm -f test-kgrel", shell=True)
        subprocess.check_output(
            f"docker run --name=test-kgrel --rm -d {start_cmd}",
            shell=True,
        )

    print("\nWaiting for KGRel to start", end="", flush=True)
    for i in range(100):
        try:
            minmodkg.models.kgrel.base.create_db_and_tables()
            break
        except Exception as e:
            print(".", end="", flush=True)
            time.sleep(0.5)
    print(" DONE!", flush=True)

    yield minmodkg.models.kgrel.base.engine

    subprocess.check_output("docker container rm -f test-kgrel", shell=True)


@pytest.fixture(scope="class")
def kg(resource_dir: Path, kg_singleton: TripleStore):
    # insert basic KG info
    with Timer().watch_and_report("Finish loading KG data"):
        g = Graph()
        for file in resource_dir.glob("kgdata/**/*.ttl"):
            g.parse(file, format="ttl")
        kg_singleton.batch_insert(g, batch_size=1024, parallel=True)
        print(
            f"Total triples: inserted = {kg_singleton.count_all()} vs original = {len(g)}"
        )

    yield kg_singleton

    kg_singleton.clear()
    assert kg_singleton.count_all() == 0


@pytest.fixture(scope="class")
def kgrel(
    resource_dir: Path, kgrel_singleton: Engine, users: list[User]
) -> Generator[Engine, Any, None]:
    with Session(kgrel_singleton, expire_on_commit=False) as session:
        for tbl in reversed(minmodkg.models.kgrel.base.Base.metadata.sorted_tables):
            session.execute(tbl.delete())

        for file in ["commodity", "commodity_form", "crs", "data_source"]:
            session.bulk_save_objects(
                EntityDeserFn.read_file(
                    resource_dir / "kgdata/entities" / f"{file}.csv"
                )
            )
        session.add_all(users)
        session.commit()

    yield kgrel_singleton


@pytest.fixture(scope="class")
def kgrel_with_data(
    resource_dir: Path, kg, kgrel: Engine
) -> Generator[Engine, Any, None]:
    mineral_site_service = MineralSiteService(kgrel)
    entity_service = EntityService(kgrel)

    for file in (resource_dir / "kgdata/mineral-sites/json").iterdir():
        for raw_site in serde.json.deser(file):
            msi = MineralSiteAndInventory.from_raw_site(
                raw_site,
                commodity_form_conversion=entity_service.get_commodity_form_conversion(),
                crs_names=entity_service.get_crs_name(),
                source_score=entity_service.get_data_source_score(),
            )
            mineral_site_service.create(msi)

    yield kgrel


@pytest.fixture(scope="class")
def client(kg, kgrel):
    with TestClient(app) as client:
        yield client


@pytest.fixture(scope="class")
def auth_client(kgrel, user1):
    access_token = jwt.encode(
        {
            "username": user1.username,
            "exp": (
                datetime.now(timezone.utc)
                + timedelta(minutes=config.JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
            ).timestamp(),
        },
        config.SECRET_KEY,
        algorithm=config.JWT_ALGORITHM,
    )
    with TestClient(app, cookies={"session": access_token}) as client:
        yield client


@pytest.fixture(scope="class")
def auth_client_2(kgrel, user2):
    access_token = jwt.encode(
        {
            "username": user2.username,
            "exp": (
                datetime.now(timezone.utc)
                + timedelta(minutes=config.JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
            ).timestamp(),
        },
        config.SECRET_KEY,
        algorithm=config.JWT_ALGORITHM,
    )
    with TestClient(app, cookies={"session": access_token}) as client:
        yield client


@pytest.fixture(scope="class")
def tmp_dir():
    """Initialize an empty directory."""
    tmpdir = Path("/tmp/test_minmodkg")
    if tmpdir.exists():
        shutil.rmtree(tmpdir)
    tmpdir.mkdir(exist_ok=True)

    yield tmpdir
