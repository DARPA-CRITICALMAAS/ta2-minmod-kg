from __future__ import annotations

from typing import Optional

from minmodkg.models.kg.entities.commodity import Commodity as KGCommodity
from minmodkg.models.kgrel.base import Base
from minmodkg.typing import InternalID, NotEmptyStr
from sqlalchemy import JSON, ForeignKey
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column


class Commodity(MappedAsDataclass, Base):
    __tablename__ = "commodity"

    id: Mapped[InternalID] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    aliases: Mapped[list[NotEmptyStr]] = mapped_column(JSON)
    parent: Mapped[Optional[InternalID]] = mapped_column(
        ForeignKey("commodity.id", ondelete="SET NULL"), index=True
    )
    is_critical: Mapped[bool] = mapped_column(default=False)
    lower_name: Mapped[str] = mapped_column(
        unique=True,
        init=False,
    )

    def __post_init__(self):
        self.lower_name = self.name.lower()

    @property
    def uri(self) -> str:
        return KGCommodity.__subj__.key_ns.uristr(self.id)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "aliases": self.aliases,
            "parent": self.parent,
            "is_critical": self.is_critical,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Commodity:
        return cls(
            id=data["id"],
            name=data["name"],
            aliases=data["aliases"],
            parent=data.get("parent"),
            is_critical=data["is_critical"],
        )

    def to_kg(self) -> KGCommodity:
        return KGCommodity(
            id=self.id,
            name=self.name,
            aliases=self.aliases,
            parent=self.parent,
            is_critical=self.is_critical,
        )
