from __future__ import annotations

import time
from typing import Literal

from minmodkg.models_v2.kgrel.base import Base
from sqlalchemy import JSON, Text
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column


class EventLog(MappedAsDataclass, Base):
    __tablename__ = "event_log"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    type: Mapped[Literal["site:add", "site:update", "same-as:update"]] = mapped_column()
    data: Mapped[dict] = mapped_column(JSON)
    timestamp: Mapped[float] = mapped_column(default_factory=time.time)
