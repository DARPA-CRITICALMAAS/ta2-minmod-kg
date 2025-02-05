from __future__ import annotations

import shutil
import time
from pathlib import Path
from typing import Annotated, Optional

import typer
from loguru import logger
from minmodkg.integrations.cdr.cdr import sync_dedup_mineral_sites, sync_deposit_types

app = typer.Typer(pretty_exceptions_short=True, pretty_exceptions_enable=False)


def sync_cdr(current_run_dir: Path, prev_run_dir: Optional[Path]):
    sync_deposit_types()
    sync_dedup_mineral_sites(current_run_dir, prev_run_dir)


@app.command()
def main(
    interval: int = 86400,
    run_dir: Path = Path("/var/tmp/cdr"),
    run_on_start: Annotated[bool, typer.Option("--run-on-start")] = False,
    verbose: Annotated[bool, typer.Option("--verbose")] = False,
):
    """Synchronize data to CDR."""
    assert interval > 60, "Interval must be greater than 1 minute"
    run_dir.mkdir(exist_ok=True, parents=True)

    if run_on_start:
        last_run = int(time.time() / interval) - 1
    else:
        last_run = int(time.time() / interval)

    while True:
        try:
            # record the current hour
            current_run = int(time.time() / interval)
            if current_run > last_run:
                current_run_dir = run_dir / str(current_run)
                current_run_dir.mkdir(exist_ok=True, parents=True)

                prev_run_dir = run_dir / str(last_run)
                if not prev_run_dir.exists():
                    prev_run_dir = None

                # run the syncs
                sync_cdr(current_run_dir, prev_run_dir)

                # this run is done, we can remove the previous run
                last_run = current_run

                if prev_run_dir is not None:
                    shutil.rmtree(prev_run_dir)
        except Exception as e:
            # exception occurred, we will wait for X second before trying again
            logger.exception(e)
            logger.info("Wait for some time before trying again")

        # sleep for a minute before checking again
        time.sleep(60)
        if verbose:
            print(".", end="", flush=True)


if __name__ == "__main__":
    app()
