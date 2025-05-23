from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import serde.json
import typer
from minmodapi import MinModAPI, replace_site
from minmodkg.models.kgrel.user import get_username
from slugify import slugify
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

    # validate the data first
    ent_service = FileEntityService(repo_dir.parent / "kgdata/data/entities")
    lst_ms = serde.json.deser(file)
    validate_mineral_site(lst_ms, ent_service, verbose=True)

    # if the data is valid, import it into the repository
    # repo_dir / "data"

    # assert that created_by is the same for all sites
    created_by_values = {site.get("created_by") for site in lst_ms}
    if len(created_by_values) != 1:
        raise ValueError("All sites must have the same 'created_by' value.")

    # assert that source_id is the same for all sites
    source_id_values = {site.get("source_id") for site in lst_ms}
    if len(source_id_values) != 1:
        raise ValueError("All sites must have the same 'source_id' value.")

    username = get_username(created_by_values.pop())
    data_sources = ent_service.get_data_sources()

    source_id = source_id_values.pop()
    if source_id in data_sources:
        source_name = data_sources[source_id].slug_name
    else:
        if source_id.startswith("https://doi.org/"):
            source_name = "doi-" + slugify(source_id[len("https://doi.org/") :])
        else:
            raise ValueError(
                f"Source ID {source_id} not found in data sources. Please add it to the data sources first."
            )

    buckets = {}
    for ms in lst_ms:
        bucket_no = PartitionFn.get_bucket_no(ms["record_id"])
        if bucket_no not in buckets:
            buckets[bucket_no] = []
        buckets[bucket_no].append(ms)

    for bucket_no, sites in buckets.items():
        outfile = (
            repo_dir
            / "data/mineral-sites"
            / PartitionFn.get_filename(username, source_name, bucket_no)
        )
        outfile.parent.mkdir(parents=True, exist_ok=True)
        serde.json.ser(sites, outfile, indent=2)


if __name__ == "__main__":
    app()
