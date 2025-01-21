from __future__ import annotations

from typing import Optional

from minmodkg.misc.utils import makedict
from minmodkg.models.kg.data_source import SourceType
from minmodkg.models.kg.entities.crs import CRS as KGCRS
from minmodkg.models.kgrel.base import Base
from minmodkg.typing import IRI, InternalID, NotEmptyStr
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column


class CRS(MappedAsDataclass, Base):
    __tablename__ = "crs"

    id: Mapped[InternalID] = mapped_column(primary_key=True)
    name: Mapped[NotEmptyStr] = mapped_column()

    @property
    def uri(self) -> IRI:
        return KGCRS.__subj__.key_ns.uristr(self.id)

    def to_dict(self) -> dict:
        return makedict.without_none(
            (
                ("id", self.id),
                ("name", self.name),
            )
        )

    @classmethod
    def from_dict(cls, d: dict) -> CRS:
        return cls(
            id=d["id"],
            name=d["name"],
        )

    def to_kg(self) -> KGCRS:
        return KGCRS(
            uri=KGCRS.__subj__.key_ns.uristr(self.id),
            name=self.name,
        )
