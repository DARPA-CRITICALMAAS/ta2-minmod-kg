from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

import typer
from minmodkg.services.sync.backup_listener import BackupListener
from minmodkg.services.sync.kgsync_listener import KGSyncListener
from minmodkg.services.sync.sync import process_pending_events

app = typer.Typer(pretty_exceptions_short=True, pretty_exceptions_enable=False)


@app.command()
def main(repo_dir: Path, batch_size: int = 500):
    """Synchronize data from the KGRel to KG and CDR."""
    kgsync_listener = KGSyncListener()
    backup_listener = BackupListener(repo_dir)

    last_backup_synced: Optional[int] = None
    backup_synced_interval = 60 * 60  # every hour

    while True:
        # we want kg sync to be near real-time
        process_pending_events(kgsync_listener, batch_size)

        if last_backup_synced is None:
            # record the current hour
            last_backup_synced = int(time.time() / backup_synced_interval)
            # then process all events
            process_pending_events(backup_listener, batch_size)
        else:
            current_hour = int(time.time() / backup_synced_interval)
            if current_hour > last_backup_synced:
                last_backup_synced = current_hour
                process_pending_events(backup_listener, batch_size)

        # wait for a second before checking again
        time.sleep(1)


if __name__ == "__main__":
    app()
