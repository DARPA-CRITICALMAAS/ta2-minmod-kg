from __future__ import annotations

import subprocess
import time
from datetime import datetime, timedelta, timezone

import httpx
import jwt
import minmodkg.config as config
import pytest
from fastapi.testclient import TestClient
from loguru import logger
from minmodkg.api.internal.admin import create_user_priv
from minmodkg.api.main import app
from minmodkg.api.models.db import Session, UserCreate, create_db_and_tables, engine


@pytest.fixture(scope="session")
def db():
    create_db_and_tables()
    with Session(engine) as session:
        create_user_priv(
            UserCreate(
                username="admin",
                name="Administrator",
                email="admin@example.com",
                password="admin123@!",
            ),
            session,
        )


@pytest.fixture(scope="class")
def kg(db):
    subprocess.check_output(
        "docker run --name=test-kg --rm -d -p 13030:3030 minmod-fuseki fuseki-server --config=/home/criticalmaas/fuseki/config.ttl",
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
    yield None
    subprocess.check_output("docker container rm -f test-kg", shell=True)


@pytest.fixture(scope="class")
def client():
    with TestClient(app) as client:
        yield client


@pytest.fixture(scope="class")
def auth_client(db):
    access_token = jwt.encode(
        {
            "sub": "admin",
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
