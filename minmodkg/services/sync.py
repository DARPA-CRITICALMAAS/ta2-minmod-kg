from __future__ import annotations

import time

from minmodkg.models.kgrel.base import get_rel_session
from minmodkg.models.kgrel.event import EventLog
from sqlalchemy import select


def main():
    """Synchronize data from the KGRel to KG and CDR."""
    while True:
        # Fetch the latest event logs
        with get_rel_session() as session:
            events = session.execute(
                select(EventLog).order_by(EventLog.id).limit(10)
            ).all()
        time.sleep(1)


if __name__ == "__main__":
    main()
