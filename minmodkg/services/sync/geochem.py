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
from minmodkg.services.sync.sync import process_pending_events

app = typer.Typer(pretty_exceptions_short=True, pretty_exceptions_enable=False)


@app.command()
def main(
    jsonld_dir: Annotated[Path, typer.Argument(help="GeoChem JSON-LD directory")],
    interval: Annotated[int, typer.Option(help="Seconds between passes")] = 60,
    batch_size: int = 500,
    verbose: Annotated[bool, typer.Option("--verbose")] = False,
):
    if not jsonld_dir.is_dir():
        raise typer.BadParameter(f"{jsonld_dir} is not a directory")
    listener = GeoChemBackupListener(jsonld_dir)
    while True:
        try:
            process_pending_events(listener, batch_size, verbose=verbose)
        except Exception as e:
            logger.exception(e)
        time.sleep(interval)


if __name__ == "__main__":
    app()
