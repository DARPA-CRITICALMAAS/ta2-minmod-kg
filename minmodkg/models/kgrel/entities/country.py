from __future__ import annotations

from minmodkg.models.kg.entities.country import Country as KGCountry
from minmodkg.models.kgrel.base import Base
from minmodkg.typing import InternalID, NotEmptyStr
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column


class Country(MappedAsDataclass, Base):
    __tablename__ = "country"

    id: Mapped[InternalID] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    aliases: Mapped[list[NotEmptyStr]] = mapped_column(JSON)

    @property
    def uri(self) -> str:
        return KGCountry.__subj__.key_ns.uristr(self.id)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "aliases": self.aliases,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Country:
        return cls(
            id=data["id"],
            name=data["name"],
            aliases=data["aliases"],
        )

    def to_kg(self) -> KGCountry:
        return KGCountry(
            id=self.id,
            name=self.name,
            aliases=self.aliases,
        )
