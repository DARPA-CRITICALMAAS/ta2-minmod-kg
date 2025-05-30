from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from minmodkg.misc.utils import makedict
from minmodkg.models.kgrel.base import Base
from minmodkg.typing import InternalID
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column, relationship


class MineralInventoryView(MappedAsDataclass, Base):
    __tablename__ = "mineral_inventory_view"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    commodity: Mapped[InternalID] = mapped_column(String(30), index=True)
    contained_metal: Mapped[Optional[float]]
    tonnage: Mapped[Optional[float]]
    grade: Mapped[Optional[float]]
    date: Mapped[Optional[str]]

    site_id: Mapped[int] = mapped_column(
        ForeignKey("mineral_site.id", ondelete="CASCADE"), default=None
    )

    def set_id(self, id: Optional[int]):
        if id is not None:
            self.id = id
        return self

    def to_dict(self):
        return makedict.without_none(
            (
                ("id", self.id),
                ("commodity", self.commodity),
                ("contained_metal", self.contained_metal),
                ("tonnage", self.tonnage),
                ("grade", self.grade),
                ("date", self.date),
            )
        )

    @staticmethod
    def from_dict(d):
        view = MineralInventoryView(
            commodity=d["commodity"],
            contained_metal=d.get("contained_metal"),
            tonnage=d.get("tonnage"),
            grade=d.get("grade"),
            date=d.get("date"),
        )
        if "id" in d:
            view.id = d["id"]
        return view

    def to_dedup_view(self, site_id: InternalID, dedup_site_id: InternalID):
        """
        Convert this view to a deduplication view.
        """
        return DedupMineralInventoryView(
            commodity=self.commodity,
            contained_metal=self.contained_metal,
            tonnage=self.tonnage,
            grade=self.grade,
            date=self.date,
            site_id=site_id,
            dedup_site_id=dedup_site_id,
        )


class DedupMineralInventoryView(MappedAsDataclass, Base):
    __tablename__ = "dedup_mineral_inventory_view"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    commodity: Mapped[InternalID] = mapped_column(String(30), index=True)
    contained_metal: Mapped[Optional[float]]
    tonnage: Mapped[Optional[float]]
    grade: Mapped[Optional[float]]
    date: Mapped[Optional[str]]

    site_id: Mapped[InternalID] = mapped_column()
    dedup_site_id: Mapped[InternalID] = mapped_column(
        ForeignKey("dedup_mineral_site.id", ondelete="CASCADE"), default=None
    )

    def set_id(self, id: Optional[int]):
        if id is not None:
            self.id = id
        return self

    def to_dict(self):
        return makedict.without_none(
            (
                ("id", self.id),
                ("commodity", self.commodity),
                ("contained_metal", self.contained_metal),
                ("tonnage", self.tonnage),
                ("grade", self.grade),
                ("date", self.date),
                ("site_id", self.site_id),
                ("dedup_site_id", self.dedup_site_id),
            )
        )

    @staticmethod
    def from_dict(d):
        view = DedupMineralInventoryView(
            commodity=d["commodity"],
            contained_metal=d.get("contained_metal"),
            tonnage=d.get("tonnage"),
            grade=d.get("grade"),
            date=d.get("date"),
            site_id=d.get("site_id"),
            dedup_site_id=d.get("dedup_site_id"),
        )
        if "id" in d:
            view.id = d["id"]
        return view
