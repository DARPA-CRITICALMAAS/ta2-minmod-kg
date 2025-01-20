from __future__ import annotations

import time
from typing import Sequence

import typer
from minmodkg.misc.utils import norm_literal
from minmodkg.models.kg.base import MINMOD_KG
from minmodkg.models.kgrel.base import get_rel_session
from minmodkg.models.kgrel.event import EventLog
from minmodkg.models.kgrel.mineral_site import MineralSiteAndInventory
from minmodkg.services.sync.listener import Listener
from minmodkg.typing import InternalID
from sqlalchemy import delete, select


def process_pending_events(listeners: Sequence[Listener], max_no_events: int = 500):
    with get_rel_session() as session:
        events = (
            session.execute(select(EventLog).order_by(EventLog.id).limit(max_no_events))
            .scalars()
            .all()
        )

        # handle the events
        for listener in listeners:
            listener.handle(events)

        # then delete the events
        session.execute(delete(EventLog).where(EventLog.id.in_([e.id for e in events])))

        # commit the transaction
        session.commit()
