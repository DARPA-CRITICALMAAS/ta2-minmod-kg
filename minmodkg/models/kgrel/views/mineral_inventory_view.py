from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from minmodkg.misc.utils import makedict
from minmodkg.models.kgrel.base import Base
from minmodkg.typing import InternalID
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column, relationship

if TYPE_CHECKING:
    from minmodkg.models.kgrel.dedup_mineral_site import DedupMineralSite
    from minmodkg.models.kgrel.mineral_site import MineralSite


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
    dedup_site_id: Mapped[Optional[InternalID]] = mapped_column(
        ForeignKey("dedup_mineral_site.id", ondelete="SET NULL"), default=None
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
                ("dedup_site_id", self.dedup_site_id),
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
            dedup_site_id=d.get("dedup_site_id"),
        )
        if "id" in d:
            view.id = d["id"]
        return view
