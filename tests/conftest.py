from __future__ import annotations

import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import jwt
import minmodkg.config as config
import pytest
from fastapi.testclient import TestClient
from minmodkg.api.internal.admin import create_user_priv
from minmodkg.api.main import app
from minmodkg.api.models.db import Session, UserCreate, create_db_and_tables, engine
from minmodkg.misc.sparql import sparql_insert
from rdflib import Graph


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
    return f"https://minmod.isi.edu/users/{user1.username}"


@pytest.fixture(scope="session")
def user2_uri(user2: UserCreate):
    return f"https://minmod.isi.edu/users/{user2.username}"


@pytest.fixture(scope="session")
def db(user1: UserCreate, user2: UserCreate):
    import os

    os.environ["CFG_FILE"] = "tests/config.yml"
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


@pytest.fixture(scope="class")
def kg(resource_dir: Path, db):
    subprocess.check_output(
        "docker run --name=test-kg --rm -d -p 13030:3030 minmod-fuseki fuseki-server --config=/home/criticalmaas/fuseki/test_config.ttl",
        shell=True,
    )
    print("\nWaiting for Fuseki to start ", end="", flush=True)
    for i in range(100):
        try:
            resp = httpx.get("http://localhost:13030/minmod/sparql")
            assert resp.text.strip() == "Service Description: /minmod/sparql"
            break
        except Exception as e:
            print(".", end="", flush=True)
            time.sleep(0.1)
    print(" DONE!", flush=True)

    # insert basic KG info
    g = Graph()
    g.parse(resource_dir / "kgversion.ttl", format="ttl")
    g.parse(resource_dir / "source_score.ttl", format="ttl")
    sparql_insert(g)

    yield None

    subprocess.check_output("docker container rm -f test-kg", shell=True)


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
