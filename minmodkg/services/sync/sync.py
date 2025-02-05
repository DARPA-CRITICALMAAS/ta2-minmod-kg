from __future__ import annotations

from typing import Literal

from minmodkg.models.kgrel.base import get_rel_session
from minmodkg.models.kgrel.event import EventLog
from minmodkg.services.sync.backup_listener import BackupListener
from minmodkg.services.sync.kgsync_listener import KGSyncListener
from minmodkg.services.sync.listener import Listener
from sqlalchemy import delete, select, update


def process_pending_events(
    listener: Listener,
    max_no_events: int = 500,
    verbose: bool = False,
):
    if isinstance(listener, KGSyncListener):
        listener_field = "kg_synced"
    else:
        assert isinstance(listener, BackupListener)
        listener_field = "backup_synced"

    with get_rel_session() as session:
        events = (
            session.execute(
                select(EventLog)
                .where(getattr(EventLog, listener_field) == False)
                .order_by(EventLog.id)
                .limit(max_no_events)
            )
            .scalars()
            .all()
        )

        # make sure that these events are contiguously
        assert all(events[i - 1].id == events[i].id - 1 for i in range(1, len(events)))

        # handle the events
        listener.handle(events)

        # then we are going to mark the events as synced
        session.execute(
            update(EventLog)
            .where(EventLog.id.in_([e.id for e in events]))
            .values(**{listener_field: True})
        )

        # then, we are going to delete the events that have been synced
        session.execute(
            delete(EventLog).where(
                EventLog.id.in_([e.id for e in events]),
                EventLog.kg_synced == True,
                EventLog.backup_synced == True,
            )
        )

        if verbose and len(events) > 0:
            print(f"[{listener.__class__.__name__}] processed events: {len(events)}")

        # commit the transaction
        session.commit()

        return len(events)
