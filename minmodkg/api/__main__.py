from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated

import httpx
import orjson
import typer
from minmodkg.api.internal.admin import create_user_priv
from minmodkg.api.models.db import create_db_and_tables, get_session
from minmodkg.api.models.user import UserCreate
from minmodkg.transformations import make_site_uri
from slugify import slugify
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

    with contextmanager(get_session)() as session:
        create_user_priv(
            UserCreate(username=username, name=name, email=email, password=password),
            session,
        )


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

            uri = make_site_uri(site["source_id"], site["record_id"], namespace=ns)
            f.write(uri + "\n")


if __name__ == "__main__":
    app()
