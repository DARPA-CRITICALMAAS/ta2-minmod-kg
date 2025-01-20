from __future__ import annotations

from typing import Optional

from minmodkg.models.kg.entities.state_or_province import (
    StateOrProvince as KGStateOrProvince,
)
from minmodkg.models.kgrel.base import Base
from minmodkg.typing import InternalID
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column


class StateOrProvince(MappedAsDataclass, Base):
    __tablename__ = "state_or_province"

    id: Mapped[InternalID] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    country: Mapped[Optional[InternalID]] = mapped_column(
        ForeignKey("country.id", ondelete="CASCADE")
    )

    @property
    def uri(self):
        return KGStateOrProvince.__subj__.key_ns.uristr(self.id)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "country": self.country,
        }

    @classmethod
    def from_dict(cls, data: dict) -> StateOrProvince:
        return cls(
            id=data["id"],
            name=data["name"],
            country=data["country"],
        )

    def to_kg(self) -> KGStateOrProvince:
        return KGStateOrProvince(
            id=self.id,
            name=self.name,
            country=self.country,
        )
