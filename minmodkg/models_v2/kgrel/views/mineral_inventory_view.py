from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from minmodkg.misc.utils import makedict
from minmodkg.models_v2.kgrel.base import Base
from minmodkg.typing import InternalID
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column, relationship

if TYPE_CHECKING:
    from minmodkg.models_v2.kgrel.mineral_site import MineralSite


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
    site: Mapped[MineralSite] = relationship(
        default=None, back_populates="inventory_views", lazy="raise_on_sql"
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
