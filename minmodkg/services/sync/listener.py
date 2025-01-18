from __future__ import annotations

from typing import Sequence

import typer
from minmodkg.models.kgrel.event import EventLog
from minmodkg.models.kgrel.mineral_site import MineralSiteAndInventory
from minmodkg.typing import InternalID


class Listener:

    def handle(self, events: Sequence[EventLog]):
        for event in events:
            if event.type == "site:add":
                self.handle_site_add(
                    MineralSiteAndInventory.from_dict(event.data["site"])
                )
            elif event.type == "site:update":
                self.handle_site_update(
                    MineralSiteAndInventory.from_dict(event.data["site"])
                )
            elif event.type == "same-as:update":
                self.handle_same_as_update(event.data["groups"])
            else:
                raise ValueError(f"Unknown event type: {event.type}")

    def handle_site_add(self, site: MineralSiteAndInventory):
        raise NotImplementedError()

    def handle_site_update(self, site: MineralSiteAndInventory):
        raise NotImplementedError()

    def handle_same_as_update(self, groups: list[list[InternalID]]):
        raise NotImplementedError()
