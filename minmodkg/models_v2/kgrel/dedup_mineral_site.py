from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Generic, Optional

from minmodkg.misc.utils import assert_not_none, makedict
from minmodkg.models.base import MINMOD_NS
from minmodkg.models_v2.kgrel.base import Base
from minmodkg.models_v2.kgrel.custom_types import (
    DedupMineralSiteDepositType,
    GeoCoordinate,
    RefValue,
)
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
    def get_site_score(cls, site: MineralSite, default_score: float):
        return cls._get_site_score(
            site.source_score, site.created_by, site.modified_at, default_score
        )

    @classmethod
    def _get_site_score(
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

    @staticmethod
    def from_dedup_sites(
        dedup_sites: list[DedupMineralSite],
        *,
        is_site_ranked: bool,
        default_source_score: float = 0.5,
    ) -> DedupMineralSite:
        assert is_site_ranked, "Only support merging ranked sites"
        rank_dedup_sites = sorted(
            (
                (
                    dedup_site,
                    DedupMineralSite.get_site_score(
                        dedup_site.sites[0], default_source_score
                    ),
                )
                for dedup_site in dedup_sites
            ),
            key=lambda x: x[1],
            reverse=True,
        )

        _tmp_deposit_types: dict[str, DedupMineralSiteDepositType] = {}
        for dedup_site in dedup_sites:
            for dt in dedup_site.deposit_types:
                if (
                    dt.id not in _tmp_deposit_types
                    or dt.confidence > _tmp_deposit_types[dt.id].confidence
                ):
                    _tmp_deposit_types[dt.id] = dt

        merged_dedup_site = DedupMineralSite(
            id=dedup_sites[0].id,
            name=next(
                (site.name for site, _ in rank_dedup_sites if site.name is not None),
                None,
            ),
            rank=next(
                (site.rank for site, _ in rank_dedup_sites if site.rank is not None),
                None,
            ),
            type=next(
                (site.type for site, _ in rank_dedup_sites if site.type is not None),
                None,
            ),
            deposit_types=sorted(
                _tmp_deposit_types.values(), key=lambda x: x.confidence, reverse=True
            )[:5],
            coordinates=next(
                (
                    site.coordinates
                    for site, _ in rank_dedup_sites
                    if site.coordinates is not None
                ),
                None,
            ),
            country=next(
                (
                    site.country
                    for site, _ in rank_dedup_sites
                    if len(site.country.value) > 0
                ),
                dedup_sites[0].country,
            ),
            state_or_province=next(
                (
                    site.state_or_province
                    for site, _ in rank_dedup_sites
                    if len(site.state_or_province.value) > 0
                ),
                dedup_sites[0].state_or_province,
            ),
            is_deleted=False,
            modified_at=max(dedup_site.modified_at for dedup_site in dedup_sites),
        )
        merged_dedup_site.sites = [
            site
            for site, _ in sorted(
                (
                    (site, DedupMineralSite.get_site_score(site, default_source_score))
                    for dedup_site, _ in rank_dedup_sites
                    for site in dedup_site.sites
                ),
                key=lambda x: x[1],
                reverse=True,
            )
        ]
        return merged_dedup_site

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
                    cls.get_site_score(
                        site,
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

    def to_dict(self):
        return makedict.without_none_or_empty_list(
            (
                ("id", self.id),
                ("name", self.name.to_dict() if self.name is not None else None),
                ("type", self.type.to_dict() if self.type is not None else None),
                ("rank", self.rank.to_dict() if self.rank is not None else None),
                (
                    "deposit_types",
                    [dt.to_dict() for dt in self.deposit_types],
                ),
                (
                    "coordinates",
                    (
                        self.coordinates.to_dict()
                        if self.coordinates is not None
                        else None
                    ),
                ),
                ("country", self.country.to_dict()),
                ("state_or_province", self.state_or_province.to_dict()),
                ("is_deleted", self.is_deleted),
                ("modified_at", self.modified_at.isoformat()),
                ("sites", [site.to_dict() for site in self.sites]),
                ("inventory_views", [inv.to_dict() for inv in self.inventory_views]),
            )
        )

    @classmethod
    def from_dict(cls, d):
        dedup_site = DedupMineralSite(
            id=d["id"],
            name=RefValue.from_dict(d["name"]) if d.get("name") is not None else None,
            type=RefValue.from_dict(d["type"]) if d.get("type") is not None else None,
            rank=RefValue.from_dict(d["rank"]) if d.get("rank") is not None else None,
            deposit_types=[
                DedupMineralSiteDepositType.from_dict(dt)
                for dt in d.get("deposit_types", [])
            ],
            coordinates=(
                RefValue.from_dict(d["coordinates"])
                if d.get("coordinates") is not None
                else None
            ),
            country=RefValue.from_dict(d.get("country", [])),
            state_or_province=RefValue.from_dict(d.get("state_or_province", [])),
            is_deleted=d["is_deleted"],
            modified_at=datetime.fromisoformat(d["modified_at"]),
        )
        dedup_site.sites = [MineralSite.from_dict(site) for site in d.get("sites", [])]
        dedup_site.inventory_views = [
            MineralInventoryView.from_dict(inv) for inv in d.get("inventory_views", [])
        ]
        return dedup_site
