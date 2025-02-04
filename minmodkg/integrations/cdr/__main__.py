from __future__ import annotations

import time
from typing import Annotated, Optional

import typer
from loguru import logger
from minmodkg.integrations.cdr.cdr import sync_dedup_mineral_sites, sync_deposit_types

app = typer.Typer(pretty_exceptions_short=True, pretty_exceptions_enable=False)


def sync_cdr():
    sync_deposit_types()
    sync_dedup_mineral_sites()


@app.command()
def main(
    interval: int = 86400,
    run_on_start: Annotated[bool, typer.Option("--run-on-start")] = False,
    verbose: Annotated[bool, typer.Option("--verbose")] = False,
):
    """Synchronize data to CDR."""
    assert interval > 60, "Interval must be greater than 1 minute"

    if run_on_start:
        last_run: Optional[int] = None
    else:
        last_run = int(time.time() / interval)

    while True:
        try:
            # record the current hour
            current_run = int(time.time() / interval)
            if last_run is None or current_run > last_run:
                last_run = current_run
                sync_cdr()
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
