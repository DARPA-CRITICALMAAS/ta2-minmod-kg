from __future__ import annotations

from minmodkg.models.kg.entities.commodity import Commodity as KGCommodity
from minmodkg.models.kg.entities.commodity_form import CommodityForm as KGCommodityForm
from minmodkg.models.kgrel.base import Base
from minmodkg.typing import IRI, InternalID, NotEmptyStr
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column


class CommodityForm(MappedAsDataclass, Base):
    __tablename__ = "commodity_form"

    id: Mapped[InternalID] = mapped_column(primary_key=True)
    name: Mapped[NotEmptyStr] = mapped_column()
    formula: Mapped[str] = mapped_column()
    commodity: Mapped[InternalID] = mapped_column(
        ForeignKey("commodity.id", ondelete="CASCADE")
    )
    conversion: Mapped[float] = mapped_column()

    @property
    def uri(self) -> IRI:
        return KGCommodityForm.__subj__.key_ns.uristr(self.id)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "formula": self.formula,
            "commodity": self.commodity,
            "conversion": self.conversion,
        }

    @classmethod
    def from_dict(cls, data: dict) -> CommodityForm:
        return cls(
            id=data["id"],
            name=data["name"],
            formula=data["formula"],
            commodity=data["commodity"],
            conversion=data["conversion"],
        )

    def to_kg(self) -> KGCommodityForm:
        return KGCommodityForm(
            uri=self.uri,
            name=self.name,
            formula=self.formula,
            commodity=KGCommodity.__subj__.key_ns.uristr(self.commodity),
            conversion=self.conversion,
        )
