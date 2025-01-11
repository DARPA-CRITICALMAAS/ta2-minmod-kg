from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from minmodkg.models_v2.kgrel.base import Base
from minmodkg.models_v2.kgrel.mineral_site import MineralSite
from minmodkg.models_v2.kgrel.user import is_system_user
from minmodkg.models_v2.kgrel.views.mineral_inventory_view import MineralInventoryView
from minmodkg.typing import InternalID
from sqlalchemy.orm import (
    Mapped,
    MappedAsDataclass,
    composite,
    mapped_column,
    relationship,
)


@dataclass
class StrRef:
    value: str
    refid: InternalID


class DedupMineralSite(MappedAsDataclass, Base):
    __tablename__ = "dedup_mineral_site"

    id: Mapped[InternalID] = mapped_column(primary_key=True)
    name: Mapped[StrRef] = composite(mapped_column("name"), mapped_column("name_refid"))
    type: Mapped[str] = composite(mapped_column("type"), mapped_column("type_refid"))
    rank: Mapped[str] = composite(mapped_column("rank"), mapped_column("rank_refid"))
    # deposit_types: list[DedupMineralSiteDepositType]
    # location

    is_deleted: Mapped[bool] = mapped_column(default=False)
    modified_at: Mapped[float] = mapped_column(
        default=datetime.now(timezone.utc).timestamp()
    )

    sites: Mapped[list[MineralSite]] = relationship(
        init=False, back_populates="dedup_site", lazy="raise_on_sql"
    )
    inventory_views: Mapped[list[MineralInventoryView]] = relationship(
        init=False, back_populates="dedup_site", lazy="raise_on_sql"
    )

    @classmethod
    def get_site_source(
        cls,
        score: Optional[float],
        created_by: list[str],
        modified_at: datetime,
        default_score: float,
    ):
        if score is None or score < 0:
            score = default_score
        if any(not is_system_user(x) for x in created_by):
            # expert get the highest priority
            return (100.0, modified_at)
        return (min(score, 99.0), modified_at)

    @classmethod
    def from_sites(
        cls,
        sites: list[MineralSite],
    ) -> DedupMineralSite:
        return DedupMineralSite(
            id=sites[0].id,
            name=StrRef(sites[0].name, sites[0].id),
            type=StrRef(sites[0].type, sites[0].id),
            rank=StrRef(sites[0].rank, sites[0].id),
            sites=sites,
            modified_at=datetime.now(timezone.utc).timestamp(),
        )


# @dataclass
# class MineralSiteInfo: ...
