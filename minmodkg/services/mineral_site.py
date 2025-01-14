from __future__ import annotations

import time
from typing import Optional, Sequence, Tuple, TypedDict

from minmodkg.models_v2.kgrel.base import engine
from minmodkg.models_v2.kgrel.dedup_mineral_site import (
    DedupMineralSite,
    DedupMineralSiteAndInventory,
)
from minmodkg.models_v2.kgrel.event import EventLog
from minmodkg.models_v2.kgrel.mineral_site import MineralSite, MineralSiteAndInventory
from minmodkg.models_v2.kgrel.user import User
from minmodkg.models_v2.kgrel.views.mineral_inventory_view import MineralInventoryView
from minmodkg.typing import InternalID
from sqlalchemy import Engine, Select, delete, func, select, update
from sqlalchemy.orm import Session

RawMineralInventoryView = dict
FindDedupMineralSiteResult = TypedDict(
    "FindDedupMineralSiteResult",
    {
        "items": dict[InternalID, DedupMineralSiteAndInventory],
        "total": int,
    },
)


class MineralSiteService:

    def __init__(self, _engine: Optional[Engine] = None):
        self.engine = _engine or engine

    def get_site_db_id(self, site_id: InternalID) -> Optional[int]:
        q = select(MineralSite.id).where(MineralSite.site_id == site_id)
        with Session(self.engine) as session:
            return session.execute(q).scalar_one_or_none()

    def find_by_id(self, site_id: InternalID) -> Optional[MineralSiteAndInventory]:
        query = self._select_mineral_site().where(MineralSite.site_id == site_id)
        with Session(self.engine, expire_on_commit=False) as session:
            lst = self._read_mineral_sites(session, query)
            if len(lst) == 0:
                return None
            return lst[0]

    def find_dedup_by_id(
        self, dedup_site_id: InternalID
    ) -> Optional[DedupMineralSiteAndInventory]:
        query = (
            select(DedupMineralSite, self.inv_agg)
            .join(
                MineralInventoryView,
                MineralInventoryView.dedup_site_id == DedupMineralSite.id,
                isouter=True,
            )
            .group_by(DedupMineralSite.id)
            .where(DedupMineralSite.id == dedup_site_id)
        )

        with Session(self.engine, expire_on_commit=False) as session:
            row = session.execute(query).one_or_none()
            if row is None:
                return None

            return DedupMineralSiteAndInventory(
                dms=row[0],
                invs=[MineralInventoryView.from_dict(inv) for inv in row[1]],
            )

    def find_by_ids(
        self, ids: list[InternalID]
    ) -> dict[InternalID, MineralSiteAndInventory]:
        query = self._select_mineral_site().where(MineralSite.site_id.in_(ids))
        with Session(self.engine, expire_on_commit=False) as session:
            return {
                msi.ms.site_id: msi for msi in self._read_mineral_sites(session, query)
            }

    def create(self, user: User, site_and_inv: MineralSiteAndInventory):
        """Create a mineral site"""
        self._update_derived_data(site_and_inv.ms, user)
        with Session(self.engine, expire_on_commit=False) as session:
            session.connection(execution_options={"isolation_level": "REPEATABLE READ"})

            # step 1: retrieve related information that can be used to construct a dedup site
            query = self._select_mineral_site().where(
                MineralSite.dedup_site_id == site_and_inv.ms.dedup_site_id
            )
            all_sites = self._read_mineral_sites(session, query)
            all_sites.append(site_and_inv)
            dedup_site = DedupMineralSite.from_sites(
                all_sites, dedup_site_id=site_and_inv.ms.dedup_site_id
            )

            # step 2: write data
            if len(all_sites) == 1:
                # only have a single mineral site
                session.add(dedup_site)
            else:
                session.execute(dedup_site.get_update_query())
            session.add(site_and_inv.ms)
            session.flush()
            session.execute(
                update(MineralInventoryView),
                [
                    {
                        "id": inv.id,
                        "dedup_site_id": inv.dedup_site_id,
                    }
                    for ms in all_sites[:-1]
                    for inv in ms.invs
                ],
            )
            session.refresh(site_and_inv.ms)
            for inv in site_and_inv.invs:
                inv.site_id = site_and_inv.ms.id
            session.add_all(site_and_inv.invs)
            session.add(EventLog.from_site_add(site_and_inv))

            # step 3: commit data
            session.commit()

    def update(self, user: User, site_and_inv: MineralSiteAndInventory):
        self._update_derived_data(site_and_inv.ms, user)
        with Session(self.engine, expire_on_commit=False) as session:
            session.connection(execution_options={"isolation_level": "REPEATABLE READ"})

            # step 0: we clean up the mineral inventory views of the site
            session.execute(
                delete(MineralInventoryView).where(
                    MineralInventoryView.site_id == site_and_inv.ms.id
                )
            )

            # step 1: retrieve related information that can be used to construct a dedup site
            query = self._select_mineral_site().where(
                MineralSite.dedup_site_id == site_and_inv.ms.dedup_site_id
            )

            all_sites = [
                msi
                for msi in self._read_mineral_sites(session, query)
                if msi.ms.id != site_and_inv.ms.id
            ]
            all_sites.append(site_and_inv)
            dms = DedupMineralSite.from_sites(
                all_sites, dedup_site_id=site_and_inv.ms.dedup_site_id
            )

            # step 2: write data
            session.execute(dms.get_update_query())
            session.execute(site_and_inv.ms.get_update_query())
            for ms in all_sites:
                session.add_all(ms.invs)
            session.add(EventLog.from_site_update(site_and_inv))

            # step 3: commit data
            session.commit()

    def update_same_as(self, groups: list[list[InternalID]]) -> list[InternalID]:
        output = []
        with Session(self.engine) as session:
            session.connection(execution_options={"isolation_level": "REPEATABLE READ"})

            affected_site_ids = {msid for grp in groups for msid in grp}
            affected_dedup_ids: set[InternalID] = set(
                session.execute(
                    select(MineralSite.dedup_site_id)
                    .distinct()
                    .where(MineralSite.site_id.in_(affected_site_ids))
                ).scalars()
            )

            for group in groups:
                dedup_site_id = min(group)
                all_sites = self._read_mineral_sites(
                    session,
                    self._select_mineral_site().where(MineralSite.site_id.in_(group)),
                )
                dedup_site = DedupMineralSite.from_sites(all_sites, dedup_site_id)
                if dedup_site.id in affected_dedup_ids:
                    # the site already exists
                    session.execute(dedup_site.get_update_query())
                else:
                    session.add(dedup_site)
                    session.flush()

                session.execute(
                    update(MineralSite)
                    .where(MineralSite.site_id.in_(group))
                    .values(dedup_site_id=dedup_site_id)
                )
                session.execute(
                    update(MineralInventoryView),
                    [
                        {
                            "id": inv.id,
                            "dedup_site_id": inv.dedup_site_id,
                        }
                        for msi in all_sites
                        for inv in msi.invs
                    ],
                )

                output.append(dedup_site_id)

            session.execute(
                delete(DedupMineralSite).where(
                    DedupMineralSite.id.in_(set(affected_dedup_ids).difference(output))
                )
            )
            session.add(EventLog.from_same_as_update(groups))
            session.commit()
        return output

    def find_dedup_mineral_sites(
        self,
        *,
        commodity: Optional[InternalID],
        dedup_site_ids: Optional[Sequence[InternalID]] = None,
        limit: int = 0,
        offset: int = 0,
        return_count: bool = False,
    ) -> FindDedupMineralSiteResult:
        query = (
            select(
                DedupMineralSite,
                self.inv_agg,
            )
            .join(
                MineralInventoryView,
                MineralInventoryView.dedup_site_id == DedupMineralSite.id,
            )
            .group_by(DedupMineralSite.id)
            .order_by(DedupMineralSite.modified_at.desc())
        )

        count_query = None
        if return_count:
            count_query = (
                select(func.count())
                .select_from(DedupMineralSite)
                .join(
                    MineralInventoryView,
                    MineralInventoryView.dedup_site_id == DedupMineralSite.id,
                )
            )

        if commodity is not None:
            query = query.where(MineralInventoryView.commodity == commodity)
            if count_query is not None:
                count_query = count_query.where(
                    MineralInventoryView.commodity == commodity
                )

        if dedup_site_ids is not None:
            query = query.where(DedupMineralSite.id.in_(dedup_site_ids))

        if limit > 0:
            query = query.limit(limit)
        if offset > 0:
            query = query.offset(offset)

        with Session(self.engine, expire_on_commit=False) as session:
            lst_dms_and_invs = []
            for dms, invs in session.execute(query).all():
                dms: DedupMineralSite
                inventory_views = [
                    MineralInventoryView(
                        commodity=inv["commodity"],
                        contained_metal=inv["contained_metal"],
                        tonnage=inv["tonnage"],
                        grade=inv["grade"],
                        date=inv["date"],
                    )
                    for inv in invs
                ]

                lst_dms_and_invs.append(
                    DedupMineralSiteAndInventory(dms=dms, invs=inventory_views)
                )

            total = (
                session.execute(count_query).scalar_one()
                if count_query is not None
                else 0
            )
            return {
                "items": {dms.id: dms_and_invs for dms_and_invs in lst_dms_and_invs},
                "total": total,
            }

    def _update_derived_data(self, site: MineralSite, user: User):
        site.modified_at = time.time_ns()
        site.created_by = [user.get_uri()]
        return site

    def _select_mineral_site(
        self,
    ) -> Select[Tuple[MineralSite, RawMineralInventoryView]]:
        return (
            select(
                MineralSite,
                self.inv_agg,
            )
            .join(
                MineralInventoryView,
                MineralInventoryView.site_id == MineralSite.id,
                isouter=True,
            )
            .group_by(MineralSite.id)
        )

    def _read_mineral_sites(
        self,
        session: Session,
        query: Select[Tuple[MineralSite, RawMineralInventoryView]],
    ) -> list[MineralSiteAndInventory]:
        out = []
        for row in session.execute(query).all():
            msi = MineralSiteAndInventory(
                ms=row[0],
                invs=[MineralInventoryView.from_dict(inv) for inv in row[1]],
            )
            for inv in msi.invs:
                inv.site_id = msi.ms.id
            out.append(msi)
        return out

    @property
    def inv_agg(self):
        return func.jsonb_agg(
            func.jsonb_build_object(
                "id",
                MineralInventoryView.id,
                "commodity",
                MineralInventoryView.commodity,
                "contained_metal",
                MineralInventoryView.contained_metal,
                "tonnage",
                MineralInventoryView.tonnage,
                "grade",
                MineralInventoryView.grade,
                "date",
                MineralInventoryView.date,
                "dedup_site_id",
                MineralInventoryView.dedup_site_id,
            ),
        )
