from __future__ import annotations

import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import jwt
import pytest
from fastapi.testclient import TestClient
from minmodkg import config
from minmodkg.api.internal.admin import create_user_priv
from minmodkg.api.main import app
from minmodkg.api.models.db import Session, UserCreate, create_db_and_tables, engine
from minmodkg.misc.rdf_store.fuseki import FusekiDB
from minmodkg.misc.rdf_store.triple_store import TripleStore
from minmodkg.misc.rdf_store.virtuoso import VirtuosoDB
from minmodkg.models.base import MINMOD_KG
from rdflib import Graph
from timer import Timer


@pytest.fixture(scope="session")
def resource_dir():
    return Path(__file__).parent / "resources"


@pytest.fixture(scope="session")
def user1() -> UserCreate:
    return UserCreate(
        username="admin",
        name="Administrator",
        email="admin@example.com",
        password="admin123@!",
    )


@pytest.fixture(scope="session")
def user2() -> UserCreate:
    return UserCreate(
        username="tester",
        name="Tester",
        email="tester@example.com",
        password="tester123@!",
    )


@pytest.fixture(scope="session")
def user1_uri(user1: UserCreate):
    return user1.get_uri()


@pytest.fixture(scope="session")
def user2_uri(user2: UserCreate):
    return user2.get_uri()


@pytest.fixture(scope="session")
def db(user1: UserCreate, user2: UserCreate):
    create_db_and_tables()
    with Session(engine) as session:
        create_user_priv(
            user1,
            session,
        )
        create_user_priv(
            user2,
            session,
        )


@pytest.fixture(scope="session")
def kg_singleton(resource_dir: Path):
    if isinstance(MINMOD_KG, FusekiDB):
        start_cmd = (
            "-p 13030:3030 minmod-fuseki fuseki-server --config=fuseki/config.ttl"
        )
    elif isinstance(MINMOD_KG, VirtuosoDB):
        start_cmd = "-p 13030:8890 minmod-virtuoso"
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


@pytest.fixture(scope="class")
def kg(resource_dir: Path, kg_singleton: TripleStore):
    # insert basic KG info
    with Timer().watch_and_report("Finish loading KG data"):
        g = Graph()
        for file in resource_dir.glob("kgdata/**/*.ttl"):
            g.parse(file, format="ttl")
        kg_singleton.batch_insert(g, batch_size=1024, parallel=True)
        print(f"Total triples: {kg_singleton.count_all()}")

    yield kg_singleton

    kg_singleton.clear()
    assert kg_singleton.count_all() == 0


@pytest.fixture(scope="class")
def client(kg):
    with TestClient(app) as client:
        yield client


@pytest.fixture(scope="class")
def auth_client(db, user1):
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
def auth_client_2(db, user2):
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
