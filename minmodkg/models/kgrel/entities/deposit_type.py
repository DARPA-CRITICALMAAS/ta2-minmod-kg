from __future__ import annotations

from minmodkg.models.kg.entities.deposit_type import DepositType as KGDepositType
from minmodkg.models.kgrel.base import Base
from minmodkg.typing import InternalID
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column


class DepositType(MappedAsDataclass, Base):
    __tablename__ = "deposit_type"

    id: Mapped[InternalID] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    environment: Mapped[str] = mapped_column()
    group: Mapped[str] = mapped_column()

    @property
    def uri(self):
        return KGDepositType.__subj__.key_ns.uristr(self.id)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "environment": self.environment,
            "group": self.group,
        }

    @classmethod
    def from_dict(cls, data: dict) -> DepositType:
        return cls(
            id=data["id"],
            name=data["name"],
            environment=data["environment"],
            group=data["group"],
        )

    def to_kg(self) -> KGDepositType:
        return KGDepositType(
            id=self.id,
            name=self.name,
            environment=self.environment,
            group=self.group,
        )
