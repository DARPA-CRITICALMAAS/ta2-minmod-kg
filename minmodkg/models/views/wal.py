from __future__ import annotations

from minmodkg.models.views.base import Base
from minmodkg.models.views.custom_types import Event
from sqlalchemy import Text
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column


class WAL(MappedAsDataclass, Base):
    __tablename__ = "wal"

    id: Mapped[int] = mapped_column(primary_key=True)
    event: Mapped[Event] = mapped_column(Text)
    applied_to_kg: Mapped[bool] = mapped_column()
    applied_to_view: Mapped[bool] = mapped_column()
