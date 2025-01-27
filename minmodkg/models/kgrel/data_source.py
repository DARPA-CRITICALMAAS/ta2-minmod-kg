from __future__ import annotations

from typing import Optional

from minmodkg.misc.utils import makedict
from minmodkg.models.kg.data_source import DataSource as KGDataSource
from minmodkg.models.kg.data_source import SourceType
from minmodkg.models.kgrel.base import Base
from minmodkg.typing import IRI
from slugify import slugify
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column


class DataSource(MappedAsDataclass, Base):
    __tablename__ = "data_source"

    id: Mapped[IRI] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    type: Mapped[SourceType] = mapped_column()
    created_by: Mapped[IRI] = mapped_column()
    description: Mapped[str] = mapped_column()
    score: Mapped[Optional[float]] = mapped_column()
    connection: Mapped[Optional[str]] = mapped_column()
    slug_name: Mapped[str] = mapped_column(
        unique=True,
        init=False,
    )

    def __post_init__(self):
        self.slug_name = slugify(self.name).replace("-", "_")

    def to_dict(self) -> dict:
        return makedict.without_none(
            (
                ("id", self.id),
                ("name", self.name),
                ("type", self.type),
                ("created_by", self.created_by),
                ("description", self.description),
                ("score", self.score),
                ("connection", self.connection),
            )
        )

    @classmethod
    def from_dict(cls, d: dict) -> DataSource:
        return cls(
            id=d["id"],
            name=d["name"],
            type=d["type"],
            created_by=d["created_by"],
            description=d["description"],
            score=d.get("score"),
            connection=d.get("connection"),
        )

    def to_kg(self) -> KGDataSource:
        return KGDataSource(
            uri=self.id,
            name=self.name,
            type=self.type,
            created_by=self.created_by,
            description=self.description,
            score=self.score,
            connection=self.connection,
        )
