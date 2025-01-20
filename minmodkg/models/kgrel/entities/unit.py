from __future__ import annotations

from minmodkg.models.kg.entities.unit import Unit as KGUnit
from minmodkg.models.kgrel.base import Base
from minmodkg.typing import InternalID
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column


class Unit(MappedAsDataclass, Base):
    __tablename__ = "unit"

    id: Mapped[InternalID] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    aliases: Mapped[list[str]] = mapped_column(JSON)

    @property
    def uri(self):
        return KGUnit.__subj__.key_ns.uristr(self.id)

    def to_dict(self) -> dict:
        return {"id": self.id, "name": self.name, "aliases": self.aliases}

    @classmethod
    def from_dict(cls, data: dict) -> Unit:
        return cls(
            id=data["id"],
            name=data["name"],
            aliases=data["aliases"],
        )

    def to_kg(self) -> KGUnit:
        return KGUnit(
            id=self.id,
            name=self.name,
            aliases=self.aliases,
        )
