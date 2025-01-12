from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Generic, Optional

from minmodkg.misc.utils import assert_not_none
from minmodkg.models.base import MINMOD_NS
from minmodkg.models_v2.kgrel.base import Base
from minmodkg.models_v2.kgrel.custom_types.location import GeoCoordinate
from minmodkg.models_v2.kgrel.mineral_site import MineralSite
from minmodkg.models_v2.kgrel.user import is_system_user
from minmodkg.models_v2.kgrel.views.mineral_inventory_view import MineralInventoryView
from minmodkg.typing import InternalID, T
from sqlalchemy.orm import (
    Mapped,
    MappedAsDataclass,
    composite,
    mapped_column,
    relationship,
)


@dataclass
class RefValue(Generic[T]):
    value: T
    refid: InternalID

    @classmethod
    def from_sites(
        cls, sorted_sites: list[MineralSite], attr: Callable[[MineralSite], T]
    ):
        for site in sorted_sites:
            value = attr(site)
            if value:
                return cls(value, refid=site.site_id)
        return None
    

@dataclass
class DedupMineralSiteDepositType:
    id: InternalID
    source: str
    confidence: float


class DedupMineralSite(MappedAsDataclass, Base):
    __tablename__ = "dedup_mineral_site"

    id: Mapped[InternalID] = mapped_column(primary_key=True)
    name: Mapped[Optional[RefValue[str]]] = mapped_column()
    type: Mapped[Optional[RefValue[str]]] = mapped_column()
    rank: Mapped[Optional[RefValue[str]]] = mapped_column()
    deposit_types: Mapped[list[DedupMineralSiteDepositType]] = mapped_column()

    coordinates: Mapped[Optional[RefValue[GeoCoordinate]]] = mapped_column()
    country: Mapped[RefValue[list[InternalID]]] = mapped_column()
    state_or_province: Mapped[RefValue[list[InternalID]]] = mapped_column()

    is_deleted: Mapped[bool] = mapped_column(default=False)
    modified_at: Mapped[datetime] = mapped_column(default=datetime.now(timezone.utc))

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
        assert 0 <= score <= 1.0
        if any(not is_system_user(x) for x in created_by):
            # expert get the highest priority
            return (1.0, modified_at)
        return (min(score, 0.99), modified_at)

    @classmethod
    def from_dedup_sites(dedup_sites: list[DedupMineralSite]) -> DedupMineralSite:
        site2score = {}
        for dedup_site in dedup_sites:
            for site in dedup_site.sites:
                self.get_site_source()
        rank_dedup_sites = [dedup_site.sites[0]]
        return DedupMineralSite(
            id=dedup_sites[0].id,
            name=
        )

    @classmethod
    def from_sites(
        cls,
        sites: list[MineralSite],
        default_source_score: float = 0.5,
    ) -> DedupMineralSite:
        _rank_ss = sorted(
            (
                (
                    site,
                    cls.get_site_source(
                        site.source_score,
                        site.created_by,
                        site.modified_at,
                        default_source_score,
                    ),
                )
                for site in sites
            ),
            key=lambda x: x[1],
            reverse=True,
        )
        rank_sites = [site for site, _ in _rank_ss]

        coordinates = next(
            (
                RefValue(
                    GeoCoordinate(site.location_view.lat, site.location_view.lon),
                    site.site_id,
                )
                for site in rank_sites
                if site.location_view.lat is not None
                and site.location_view.lon is not None
            ),
            None,
        )

        country = next(
            (
                RefValue(site.location_view.country, site.site_id)
                for site in rank_sites
                if len(site.location_view.country) > 0
            ),
            RefValue([], rank_sites[0].site_id),
        )
        state_or_province = next(
            (
                RefValue(site.location_view.state_or_province, site.site_id)
                for site in rank_sites
                if len(site.location_view.state_or_province) > 0
            ),
            RefValue([], rank_sites[0].site_id),
        )

        dedup_site = DedupMineralSite(
            id=rank_sites[0].site_id,
            name=RefValue.from_sites(rank_sites, lambda site: site.name),
            type=RefValue.from_sites(rank_sites, lambda site: site.type),
            rank=RefValue.from_sites(rank_sites, lambda site: site.rank),
            deposit_types=cls.top_5_deposit_types(rank_sites),
            coordinates=coordinates,
            country=country,
            state_or_province=state_or_province,
            is_deleted=False,
            modified_at=max(site.modified_at for site in sites),
        )
        dedup_site.sites = rank_sites
        return dedup_site

    @staticmethod
    def top_5_deposit_types(
        sites: list[MineralSite],
    ) -> list[DedupMineralSiteDepositType]:
        _tmp_deposit_types: dict[str, DedupMineralSiteDepositType] = {}

        for site in sites:
            for dt in site.deposit_type_candidates:
                if dt.normalized_uri is None:
                    continue
                dt_id = MINMOD_NS.mr.id(dt.normalized_uri)
                if (
                    dt_id not in _tmp_deposit_types
                    or dt.confidence > _tmp_deposit_types[dt_id].confidence
                ):
                    _tmp_deposit_types[dt_id] = DedupMineralSiteDepositType(
                        id=dt_id,
                        source=dt.source,
                        confidence=dt.confidence,
                    )

        return sorted(
            _tmp_deposit_types.values(), key=lambda x: x.confidence, reverse=True
        )[:5]

# @dataclass
# class MineralSiteInfo: ...
