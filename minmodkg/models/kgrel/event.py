from __future__ import annotations

import time
from typing import Literal

from minmodkg.models.kgrel.base import Base
from minmodkg.models.kgrel.mineral_site import MineralSiteAndInventory
from minmodkg.models.kgrel.paper import GEOCHEM_USER_URI
from minmodkg.models.kgrel.sample import Sample
from minmodkg.typing import InternalID
from sqlalchemy import JSON, BigInteger
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column


def geochem_flags(maybe_geochem: bool) -> dict:
    """Events that may belong to a GeoChem paper start unsynced in the
    GeoChem lanes; the GeoChem sync decides which ones really do."""
    return {"geochem_kg_synced": not maybe_geochem, "geochem_synced": not maybe_geochem}


class EventLog(MappedAsDataclass, Base):
    __tablename__ = "event_log"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    type: Mapped[
        Literal[
            "site:add",
            "site:update",
            "same-as:update",
            "sample:add",
            "sample:update",
        ]
    ] = mapped_column()
    data: Mapped[dict] = mapped_column(JSON)
    kg_synced: Mapped[bool] = mapped_column(default=False, index=True)
    backup_synced: Mapped[bool] = mapped_column(default=False, index=True)
    # the GeoChem sync's KG update and JSON-LD write-back; true from the start
    # for events that can't belong to a GeoChem paper
    geochem_kg_synced: Mapped[bool] = mapped_column(default=True, index=True)
    geochem_synced: Mapped[bool] = mapped_column(default=True, index=True)
    timestamp: Mapped[int] = mapped_column(BigInteger, default_factory=time.time_ns)

    @classmethod
    def from_site_add(
        cls, site: MineralSiteAndInventory, same_site_ids: list[InternalID]
    ) -> EventLog:
        return EventLog(
            type="site:add",
            data={
                "site": site.to_dict(),
                "same_site_ids": same_site_ids,
            },
            **geochem_flags(site.ms.created_by == GEOCHEM_USER_URI),
        )

    @classmethod
    def from_site_update(cls, site: MineralSiteAndInventory) -> EventLog:
        return EventLog(
            type="site:update",
            data={
                "site": site.to_dict(),
            },
            **geochem_flags(site.ms.created_by == GEOCHEM_USER_URI),
        )

    @classmethod
    def from_sample_add(cls, sample: Sample) -> EventLog:
        return EventLog(
            type="sample:add",
            data={
                "sample": sample.to_dict(),
            },
            **geochem_flags(True),
        )

    @classmethod
    def from_sample_update(cls, sample: Sample) -> EventLog:
        return EventLog(
            type="sample:update",
            data={
                "sample": sample.to_dict(),
            },
            **geochem_flags(True),
        )

    @classmethod
    def from_same_as_update(
        cls,
        user_uri: str,
        groups: list[list[InternalID]],
        diff_groups: dict[InternalID, list[InternalID]],
    ) -> EventLog:
        """Update the same-as links.

        Args:
            groups: each item in the list is a group of internal IDs that are the same.
            diff_groups: a mapping from internal ID to a list of internal IDs that are previously marked as the same but now are different.
        """
        return EventLog(
            type="same-as:update",
            data={
                "user_uri": user_uri,
                "groups": groups,
                "diff_groups": diff_groups,
            },
        )
