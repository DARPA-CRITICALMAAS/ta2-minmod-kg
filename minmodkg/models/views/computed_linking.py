from __future__ import annotations


from minmodkg.models.views.base import Base
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column


class MineralSiteSameAs(MappedAsDataclass, Base):
    __tablename__ = "same_as"

    site_id: Mapped[int] = mapped_column(ForeignKey("computed_mineral_site.id"))
