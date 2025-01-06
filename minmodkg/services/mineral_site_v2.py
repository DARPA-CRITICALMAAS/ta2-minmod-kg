from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable, Optional, Sequence

from minmodkg.api.models.user import UserBase
from minmodkg.models_v2.kgrel.base import engine
from minmodkg.models_v2.kgrel.event import EventLog
from minmodkg.models_v2.kgrel.mineral_site import MineralSite
from minmodkg.models_v2.kgrel.views.mineral_inventory_view import MineralInventoryView
from minmodkg.typing import InternalID
from sqlalchemy import Engine, exists, select, update
from sqlalchemy.orm import Session, contains_eager


class MineralSiteService:

    def __init__(self, _engine: Optional[Engine] = None):
        self.engine = _engine or engine

    def contain_id(self, site_id: InternalID) -> bool:
        q = exists().where(MineralSite.site_id == site_id).select()
        with Session(self.engine) as session:
            return session.execute(q).scalar_one()

    def find_by_id(self, site_id: InternalID) -> Optional[MineralSite]:
        with Session(self.engine) as session:
            query = select(MineralSite).where(MineralSite.site_id == site_id)
            site = session.execute(query).scalar_one_or_none()
            return site

    def find_by_ids(self, ids: list[InternalID]) -> dict[str, MineralSite]:
        with Session(self.engine) as session:
            query = select(MineralSite).where(MineralSite.site_id.in_(ids))
            sites = {site.site_id: site for site, in session.execute(query)}
            return sites

    def restore(self, sites: list[MineralSite]):
        with Session(self.engine) as session:
            site_invs = [site.inventory_views for site in sites]
            session.bulk_save_objects(sites, return_defaults=True)
            for site, invs in zip(sites, site_invs):
                for inv in invs:
                    inv.site_id = site.id
            session.bulk_save_objects([x for lst in site_invs for x in lst])
            session.commit()

    def create(self, user: UserBase, site: MineralSite, same_as: list[InternalID]):
        """Create a mineral site"""
        self._update_derived_data(site, user)
        with Session(self.engine, expire_on_commit=False) as session:
            if len(same_as) > 0:
                dedup_site_id = min(min(same_as), site.site_id)
                session.execute(
                    update(MineralSite)
                    .where(MineralSite.site_id.in_(same_as))
                    .values(dedup_site_id=dedup_site_id)
                )
                site.dedup_site_id = dedup_site_id

            session.add(site)
            session.add(
                EventLog(
                    type="site:add",
                    data={
                        "site": site.to_dict(),
                        "same_as": same_as,
                    },
                )
            )
            session.commit()

    def update(self, user: UserBase, site: MineralSite):
        self._update_derived_data(site, user)
        with Session(self.engine) as session:
            session.add(site)
            session.add(
                EventLog(
                    type="site:update",
                    data={
                        "site": site.to_dict(),
                    },
                )
            )
            session.commit()

    def update_same_as(self, groups: list[list[InternalID]]):
        with Session(self.engine) as session:
            for group in groups:
                dedup_site_id = min(group)
                session.execute(
                    update(MineralSite)
                    .where(MineralSite.site_id.in_(group))
                    .values(dedup_site_id=dedup_site_id)
                )
            session.add(
                EventLog(
                    type="same-as:update",
                    data={
                        "groups": groups,
                    },
                )
            )
            session.commit()

    def find_dedup_mineral_sites(
        self,
        *,
        commodity: Optional[InternalID],
        dedup_site_ids: Optional[Sequence[InternalID]] = None,
    ) -> dict[InternalID, list[MineralSite]]:
        query = (
            select(MineralSite)
            .join(MineralInventoryView, MineralSite.id == MineralInventoryView.site_id)
            .options(contains_eager(MineralSite.inventory_views))
            .execution_options(populate_existing=True)
        )
        if commodity is not None:
            query = query.filter(MineralInventoryView.commodity == commodity)
        if dedup_site_ids is not None:
            query = query.filter(MineralSite.dedup_site_id.in_(dedup_site_ids))

        with Session(self.engine) as session:
            sites = session.execute(query).unique().scalars().all()
            dms2sites = defaultdict(list)
            for site in sites:
                dms2sites[site.dedup_site_id].append(site)
            return dms2sites

    def _update_derived_data(self, site: MineralSite, user: UserBase):
        site.modified_at = datetime.now(timezone.utc)
        site.created_by = [user.get_uri()]
        return site

    @staticmethod
    def get_dedup_id(site_ids: Iterable[InternalID]):
        return "dedup_" + min(site_ids)
