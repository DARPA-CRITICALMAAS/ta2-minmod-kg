from __future__ import annotations

import time
from typing import Literal

from minmodkg.models_v2.kgrel.base import Base
from minmodkg.models_v2.kgrel.mineral_site import MineralSiteAndInventory
from minmodkg.typing import InternalID
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column


class EventLog(MappedAsDataclass, Base):
    __tablename__ = "event_log"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    type: Mapped[Literal["site:add", "site:update", "same-as:update"]] = mapped_column()
    data: Mapped[dict] = mapped_column(JSON)
    timestamp: Mapped[float] = mapped_column(default_factory=time.time)

    @classmethod
    def from_site_add(cls, site: MineralSiteAndInventory) -> EventLog:
        return EventLog(
            type="site:add",
            data={
                "site": site.to_dict(),
            },
        )

    @classmethod
    def from_site_update(cls, site: MineralSiteAndInventory) -> EventLog:
        return EventLog(
            type="site:update",
            data={
                "site": site.to_dict(),
            },
        )

    @classmethod
    def from_same_as_update(cls, groups: list[list[InternalID]]) -> EventLog:
        return EventLog(
            type="same-as:update",
            data={
                "groups": groups,
            },
        )
