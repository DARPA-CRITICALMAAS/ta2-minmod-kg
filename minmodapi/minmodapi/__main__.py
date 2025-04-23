from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer
from minmodapi import MinModAPI, replace_site
from tqdm import tqdm

app = typer.Typer(pretty_exceptions_short=True, pretty_exceptions_enable=False)


@app.command("whoami", help="Get current user")
def whoami(
    endpoint: Annotated[
        str, typer.Option("-e", help="Endpoint")
    ] = "https://dev.minmod.isi.edu",
):
    api = MinModAPI(endpoint)
    print(api.whoami())


@app.command("login", help="Login to MinMod")
def login(
    username: Annotated[str, typer.Option("-u", help="Username")],
    password: Annotated[str, typer.Option(prompt=True, hide_input=True)],
    endpoint: Annotated[
        str, typer.Option("-e", help="Endpoint")
    ] = "https://dev.minmod.isi.edu",
):
    api = MinModAPI(endpoint)
    api.login(username, password)
    print("Login successful!")


@app.command("upload", help="Upload mineral sites to MinMod")
def upload(
    file: Annotated[Path, typer.Argument(help="File to upload")],
    endpoint: Annotated[
        str, typer.Option("-e", help="Endpoint")
    ] = "https://dev.minmod.isi.edu",
):
    api = MinModAPI(endpoint)
    for site in tqdm(json.loads(file.read_text()), desc="upload mineral sites"):
        api.upsert_mineral_site(site, replace_site, verbose=False)


@app.command(
    "repo-import-ms",
    help="Import mineral sites dataset into MinMod data repository directly",
)
def repo_import_ms(
    file: Annotated[Path, typer.Argument(help="File to import")],
    repo_dir: Annotated[Path, typer.Option("-o", help="The ta2 data repository")],
):
    from minmodkg.services.kgrel_entity import FileEntityService
    from minmodkg.services.sync.backup_listener import PartitionFn
    from minmodkg.validators import validate_mineral_site

    ent_service = FileEntityService(repo_dir / "data/entities")
    validate_mineral_site(file, ent_service, verbose=True)

    # repo_dir / "data"


if __name__ == "__main__":
    app()
