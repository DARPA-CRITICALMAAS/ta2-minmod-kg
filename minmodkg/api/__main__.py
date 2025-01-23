from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated

import httpx
import orjson
import serde.csv
import serde.json
import typer
from minmodkg.api.internal.admin import create_user_priv
from minmodkg.api.models.public_user import PublicCreateUser
from minmodkg.models.kgrel.base import create_db_and_tables, get_rel_session
from minmodkg.models.kgrel.user import User, get_username
from minmodkg.transformations import make_site_id
from slugify import slugify
from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert as upsert
from sqlmodel import select
from tqdm import tqdm

app = typer.Typer(pretty_exceptions_short=True, pretty_exceptions_enable=False)


@app.command(help="Create new user (admin only)")
def user(
    username: Annotated[str, typer.Option("-u", help="Username")],
    name: Annotated[str, typer.Option("-n", help="Name")],
    email: Annotated[str, typer.Option("-e", help="Email")],
    password: Annotated[
        str, typer.Option(prompt=True, confirmation_prompt=True, hide_input=True)
    ],
):
    create_db_and_tables()

    with get_rel_session() as session:
        create_user_priv(
            PublicCreateUser(
                username=username,
                name=name,
                email=email,
                role="user",
                password=password,
            ),
            session,
        )


@app.command()
def load_user(
    input_file: Path,
):
    create_db_and_tables()

    users = [User.from_dict(u) for u in serde.json.deser(input_file)]
    with get_rel_session() as session:
        stmt = upsert(User).values([u.to_dict(encode_pwd=False) for u in users])
        stmt = stmt.on_conflict_do_update(
            index_elements=[User.username],
            set_={
                User.name: stmt.excluded.name,
                User.email: stmt.excluded.email,
                User.password: stmt.excluded.password,
                User.role: stmt.excluded.role,
            },
        )
        session.execute(stmt)
        session.commit()


@app.command()
def batch_add_user(
    input_file: Path,
    new_users_file: Path,
):
    if input_file.exists():
        raw_users = serde.json.deser(input_file)
    else:
        raw_users = []

    exist_users = {u["username"] for u in raw_users}

    new_users = serde.csv.deser(new_users_file, deser_as_record=True)
    for user in new_users:
        if user["username"] in exist_users:
            raise ValueError(f"User {user['username']} already exists")

        exist_users.add(user["username"])
        raw_users.append(
            User(
                username=user["username"],
                name=user["name"],
                email=user["email"],
                password=User.encrypt_password(user["password"]),
                role="user",
            ).to_dict()
        )

    serde.json.ser(raw_users, input_file.parent / (input_file.name + ".new"), indent=2)
    os.rename(input_file.parent / (input_file.name + ".new"), input_file)


@app.command()
def add_user(
    input_file: Path,
    username: Annotated[str, typer.Option("-u", help="Username")],
    name: Annotated[str, typer.Option("-n", help="Name")],
    email: Annotated[str, typer.Option("-e", help="Email")],
    password: Annotated[
        str, typer.Option(prompt=True, confirmation_prompt=True, hide_input=True)
    ],
):
    user = User(
        username=username,
        name=name,
        email=email,
        password=User.encrypt_password(password),
        role="user",
    )

    if input_file.exists():
        raw_users = serde.json.deser(input_file)
    else:
        raw_users = []
    exist_users = {u["username"] for u in raw_users}
    if user.username in exist_users:
        raise ValueError(f"User {user.username} already exists")
    raw_users.append(user.to_dict())
    serde.json.ser(raw_users, input_file.parent / (input_file.name + ".new"), indent=2)
    os.rename(input_file.parent / (input_file.name + ".new"), input_file)


@app.command()
def clear_user():
    create_db_and_tables()
    with get_rel_session() as session:
        session.execute(delete(User))
        session.commit()


@app.command()
def login(
    username: Annotated[str, typer.Option("-u", help="Username")],
    password: Annotated[str, typer.Option(prompt=True, hide_input=True)],
    endpoint: Annotated[
        str, typer.Option("-e", help="Endpoint")
    ] = "http://localhost:8080",
):
    cfg_file = Path(
        os.path.expanduser("~/.config/minmod/" + slugify(endpoint) + ".json")
    )
    cfg_file.parent.mkdir(parents=True, exist_ok=True)

    resp = httpx.post(
        f"{endpoint}/api/v1/login",
        json={"username": username, "password": password},
    ).raise_for_status()

    cfg_file.write_bytes(orjson.dumps({"token": resp.cookies["session"]}))
    print(resp.text)


@app.command()
def upload_mineral_sites(
    input_file: Path,
    endpoint: Annotated[
        str, typer.Option("-e", help="Endpoint")
    ] = "http://localhost:8080",
):
    cfg_file = Path(
        os.path.expanduser("~/.config/minmod/" + slugify(endpoint) + ".json")
    )
    cfg_file.parent.mkdir(parents=True, exist_ok=True)
    cookies = {"session": orjson.loads(cfg_file.read_bytes())["token"]}

    assert not endpoint.endswith("/"), "Endpoint must not end with /"
    ns = endpoint + "/resource/"

    sites = orjson.loads(input_file.read_bytes())

    with open(str(input_file.parent / (input_file.stem + ".urls")), "w") as f:
        for site in tqdm(sites, desc="upload mineral sites"):
            resp = httpx.post(
                f"{endpoint}/api/v1/mineral-sites",
                json=site,
                cookies=cookies,
            )

            site_exist = (
                resp.status_code == 403
                and '{"detail":"The site already exists."}' == resp.text
            )
            if resp.status_code != 200 and not site_exist:
                raise Exception(f"Status code: {resp.status_code}. Reason: {resp.text}")

            assert isinstance(site["create_by"], str)
            username = get_username(site["created_by"])
            uri = ns + make_site_id(username, site["source_id"], site["record_id"])
            f.write(uri + "\n")


if __name__ == "__main__":
    app()
