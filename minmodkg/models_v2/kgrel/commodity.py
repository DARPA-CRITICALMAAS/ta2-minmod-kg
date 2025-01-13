from __future__ import annotations

from typing import TYPE_CHECKING

from minmodkg.models_v2.kgrel.base import Base
from minmodkg.typing import InternalID
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column

if TYPE_CHECKING:
    pass


class Commodity(MappedAsDataclass, Base):
    __tablename__ = "commodity"

    id: Mapped[InternalID] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    is_critical: Mapped[bool] = mapped_column(default=False)
    lower_name: Mapped[str] = mapped_column(
        unique=True,
        init=False,
        default=lambda ctx: ctx.get_current_parameters()["name"].lower(),
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "is_critical": self.is_critical,
        }

    @classmethod
    def from_dict(cls, data: dict) -> Commodity:
        return cls(
            id=data["id"],
            name=data["name"],
            is_critical=data["is_critical"],
        )
