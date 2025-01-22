from __future__ import annotations

import os
import subprocess
import time
from typing import Optional

import typer
from loguru import logger

app = typer.Typer(pretty_exceptions_short=True, pretty_exceptions_enable=False)


def run_entity_linking():
    subprocess.check_call(
        [
            "docker",
            "run",
            "darpa-criticalmaas/fusemine",
            "poetry",
            "run",
            "python3",
            "run_fusemine.py",
            "--commodity",
        ],
        env={"PATH": os.environ.get("PATH", "")},
    )


@app.command()
def main(
    interval: int = 86400,
    run_on_start: bool = False,
    verbose: bool = False,
):
    """Run entity linking."""
    assert interval > 60, "Interval must be greater than a minute"

    if run_on_start:
        last_run: Optional[int] = None
    else:
        last_run = int(time.time() / interval)

    while True:
        try:
            if last_run is None:
                # record the current hour
                last_run = int(time.time() / interval)
            else:
                current_run = int(time.time() / interval)
                if current_run > last_run:
                    last_run = current_run
                    run_entity_linking()
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
