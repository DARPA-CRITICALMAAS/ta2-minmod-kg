from __future__ import annotations

from minmodkg.models.views.custom_types import Event


class WALService:

    def __init__(self):
        pass

    def handle(self, event: Event):
        """Handle an event."""
        # write the event to the WAL, then notify the listeners to handle it
