"""Write GeoChem edits back into the papers' JSON-LD, apart from MinMod's sync.

python -m minmodkg.services.sync.geochem <jsonld_dir>
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Annotated

import typer
from loguru import logger
from minmodkg.services.sync.geochem_listener import GeoChemBackupListener
from minmodkg.services.sync.kgsync_listener import KGSyncListener
from minmodkg.services.sync.sync import process_pending_events

app = typer.Typer(pretty_exceptions_short=True, pretty_exceptions_enable=False)


@app.command()
def main(
    jsonld_dir: Annotated[Path, typer.Argument(help="GeoChem JSON-LD directory")],
    interval: Annotated[
        int, typer.Option(help="Seconds between JSON-LD write-backs")
    ] = 60,
    batch_size: int = 500,
    verbose: Annotated[bool, typer.Option("--verbose")] = False,
):
    if not jsonld_dir.is_dir():
        raise typer.BadParameter(f"{jsonld_dir} is not a directory")
    kg_listener = KGSyncListener(lane="geochem")
    backup_listener = GeoChemBackupListener(jsonld_dir)
    last_backup = 0.0
    while True:
        try:
            process_pending_events(kg_listener, batch_size, verbose=verbose)
        except Exception as e:
            logger.exception(e)
            time.sleep(10)
        if time.time() - last_backup >= interval:
            try:
                process_pending_events(backup_listener, batch_size, verbose=verbose)
                last_backup = time.time()
            except Exception as e:
                logger.exception(e)
        time.sleep(1)


if __name__ == "__main__":
    app()
