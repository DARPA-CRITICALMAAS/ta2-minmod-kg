from __future__ import annotations

from minmodkg.models.kg.entities.category import Category as KGCategory
from minmodkg.models.kgrel.base import Base
from minmodkg.typing import InternalID
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column


class Category(MappedAsDataclass, Base):
    __tablename__ = "category"

    id: Mapped[InternalID] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Category:
        return cls(
            id=data["id"],
            name=data["name"],
        )

    def to_kg(self) -> KGCategory:
        return KGCategory(
            id=self.id,
            name=self.name,
        )
