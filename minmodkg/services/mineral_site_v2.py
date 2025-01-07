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
from sqlalchemy import Engine, delete, exists, select, update
from sqlalchemy.orm import Session, contains_eager
from tqdm import tqdm


class MineralSiteService:

    def __init__(self, _engine: Optional[Engine] = None):
        self.engine = _engine or engine

    def contain_site_id(self, site_id: InternalID) -> bool:
        q = exists().where(MineralSite.site_id == site_id).select()
        with Session(self.engine) as session:
            return session.execute(q).scalar_one()

    def find_by_id(self, site_id: InternalID) -> Optional[MineralSite]:
        query = self._select_mineral_site().where(MineralSite.site_id == site_id)
        with Session(self.engine, expire_on_commit=False) as session:
            site = session.execute(query).unique().scalar_one_or_none()
            return site

    def find_by_ids(self, ids: list[InternalID]) -> dict[str, MineralSite]:
        query = self._select_mineral_site().where(MineralSite.site_id.in_(ids))
        with Session(self.engine, expire_on_commit=False) as session:
            sites = {site.site_id: site for site, in session.execute(query).unique()}
            return sites

    def restore(self, sites: list[MineralSite], batch_size: int = 1024):
        with Session(self.engine) as session:
            for i in tqdm(list(range(0, len(sites), batch_size))):
                batch = sites[i : i + batch_size]
                batch_invs = [site.inventory_views for site in batch]
                session.bulk_save_objects(batch, return_defaults=True)
                for site, invs in zip(batch, batch_invs):
                    for inv in invs:
                        inv.site_id = site.id
                session.bulk_save_objects([x for lst in batch_invs for x in lst])
                session.commit()

    def create(self, user: UserBase, site: MineralSite):
        """Create a mineral site"""
        self._update_derived_data(site, user)
        with Session(self.engine, expire_on_commit=False) as session:
            session.add(site)
            session.add(
                EventLog(
                    type="site:add",
                    data={
                        "site": site.to_dict(),
                    },
                )
            )
            session.commit()

    def update(self, user: UserBase, site: MineralSite):
        self._update_derived_data(site, user)
        with Session(self.engine, expire_on_commit=False) as session:
            # TODO: improve me! -- should this be done outside?
            session.execute(
                delete(MineralSite).where(MineralSite.site_id == site.site_id)
            )
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
        # TODO: fix the query so we do not have to filter in python
        # query = self._select_mineral_site()
        # # TODO: fix the query so we do not have to filter in python
        # # we have a problem that this query ignore sites that do not have the commodity
        # # eventually missing sites that we should have
        # if commodity is not None:
        #     query = query.filter(MineralInventoryView.commodity == commodity)
        if dedup_site_ids is not None:
            query = (
                select(MineralSite)
                .join(
                    MineralInventoryView,
                    MineralInventoryView.site_id == MineralSite.id,
                    isouter=True,
                )
                .options(contains_eager(MineralSite.inventory_views))
                .execution_options(populate_existing=True)
                .where(MineralSite.dedup_site_id.in_(dedup_site_ids))
            )
        else:
            subquery = (
                select(MineralSite.dedup_site_id)
                .distinct()
                .join(
                    MineralInventoryView, MineralInventoryView.site_id == MineralSite.id
                )
                .where(MineralInventoryView.commodity == commodity)
            ).subquery()
            query = (
                select(MineralSite)
                .join(subquery, MineralSite.dedup_site_id == subquery.c.dedup_site_id)
                .join(
                    MineralInventoryView,
                    MineralInventoryView.site_id == MineralSite.id,
                    isouter=True,
                )
                .options(contains_eager(MineralSite.inventory_views))
                .execution_options(populate_existing=True)
            )

        with Session(self.engine, expire_on_commit=False) as session:
            sites = session.execute(query).unique().scalars().all()
            if commodity is not None:
                for site in sites:
                    site.inventory_views = [
                        inv
                        for inv in site.inventory_views
                        if inv.commodity == commodity
                    ]
            dms2sites = defaultdict(list)
            for site in sites:
                dms2sites[site.dedup_site_id].append(site)
            return dms2sites

    def _update_derived_data(self, site: MineralSite, user: UserBase):
        site.modified_at = datetime.now(timezone.utc)
        site.created_by = [user.get_uri()]
        return site

    def _select_mineral_site(self):
        return (
            select(MineralSite)
            .join(
                MineralInventoryView,
                MineralSite.id == MineralInventoryView.site_id,
                isouter=True,
            )
            .options(contains_eager(MineralSite.inventory_views))
            .execution_options(populate_existing=True)
        )
