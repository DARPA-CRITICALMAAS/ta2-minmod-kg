from __future__ import annotations

import time
from typing import Sequence

import typer
from minmodkg.misc.utils import norm_literal
from minmodkg.models.kg.base import MINMOD_KG
from minmodkg.models.kgrel.base import get_rel_session
from minmodkg.models.kgrel.event import EventLog
from minmodkg.models.kgrel.mineral_site import MineralSiteAndInventory
from minmodkg.typing import InternalID
from sqlalchemy import select

app = typer.Typer(pretty_exceptions_short=True, pretty_exceptions_enable=False)


@app.command()
def main(batch_size: int = 500):
    """Synchronize data from the KGRel to KG and CDR."""
    listeners = [KGSyncListener()]
    while True:
        # Fetch the latest event logs
        with get_rel_session() as session:
            events = (
                session.execute(
                    select(EventLog).order_by(EventLog.id).limit(batch_size)
                )
                .scalars()
                .all()
            )

            for listener in listeners:
                listener.handle(events)

            session.commit()

        time.sleep(1)


if __name__ == "__main__":
    main()
