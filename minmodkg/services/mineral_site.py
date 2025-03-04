from __future__ import annotations

import time
from collections import defaultdict
from functools import cmp_to_key
from typing import NamedTuple, Optional, Sequence, Tuple, TypedDict

from minmodkg.misc.utils import group_by, makedict
from minmodkg.models.kgrel.base import engine
from minmodkg.models.kgrel.dedup_mineral_site import (
    DedupMineralSite,
    DedupMineralSiteAndInventory,
)
from minmodkg.models.kgrel.event import EventLog
from minmodkg.models.kgrel.mineral_site import MineralSite, MineralSiteAndInventory
from minmodkg.models.kgrel.views.mineral_inventory_view import MineralInventoryView
from minmodkg.typing import InternalID
from sqlalchemy import Engine, Row, Select, delete, distinct, func, select, update
from sqlalchemy.orm import Session

RawMineralInventoryView = dict
FindDedupMineralSiteResult = TypedDict(
    "FindDedupMineralSiteResult",
    {
        "items": dict[InternalID, DedupMineralSiteAndInventory],
        "total": int,
    },
)


class SiteSameAsInfo(NamedTuple):
    id: int
    site_id: str
    source_id: str
    record_id: str
    dedup_site_id: str


class ExpiredSnapshotIdError(Exception):
    pass


class UnsupportOperationError(Exception):
    pass


class ArgumentError(Exception):
    pass


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
            return self._norm_dedup_mineral_site(row)

    def find_by_ids(
        self, ids: list[InternalID]
    ) -> dict[InternalID, MineralSiteAndInventory]:
        query = self._select_mineral_site().where(MineralSite.site_id.in_(ids))
        with Session(self.engine, expire_on_commit=False) as session:
            return {
                msi.ms.site_id: msi for msi in self._read_mineral_sites(session, query)
            }

    def create(self, site_and_inv: MineralSiteAndInventory):
        """Create a mineral site."""
        with Session(self.engine, expire_on_commit=False) as session:
            session.connection(execution_options={"isolation_level": "REPEATABLE READ"})

            if not site_and_inv.ms.has_dedup_site():
                # **ALGO**
                # Handle Axiom 1: All sites that have the same source id and record id must be
                # linked automatically
                sites_auto_linked_via_source_and_records = self._read_mineral_sites(
                    session,
                    self._select_mineral_site().where(
                        MineralSite.source_id == site_and_inv.ms.source_id,
                        MineralSite.record_id == site_and_inv.ms.record_id,
                    ),
                )
                # Note that they must belong to the same dedup site because the data in our
                # relational database is consistent.
                assert (
                    len(sites_auto_linked_via_source_and_records) == 0
                    or len(
                        {
                            r.ms.dedup_site_id
                            for r in sites_auto_linked_via_source_and_records
                        }
                    )
                    == 1
                )

                # **ALGO**
                # mark that the new site is linked to the same dedup site
                if len(sites_auto_linked_via_source_and_records) > 0:
                    site_and_inv.ms.dedup_site_id = (
                        sites_auto_linked_via_source_and_records[0].ms.dedup_site_id
                    )
                else:
                    site_and_inv.ms.dedup_site_id = MineralSite.get_dedup_id(
                        [site_and_inv.ms.site_id]
                    )

                existing_sites = sites_auto_linked_via_source_and_records
            else:
                # **ALGO**
                # if the users provide the dedup site id, we need to link with all sites
                # belong to this dedup site id.

                # However: what if the dedup site id provided by the users is different from `sites_auto_linked_via_source_and_records`?
                # this is rarely happen in practice, and if we merge them automatically, we may do it too fast and the users
                # won't understand. Therefore, we should raise an error so they merge the dedup site first.
                # Note: because of Axiom 1, we only need to fetch a single row.
                single_sites_auto_linked_via_source_and_records = session.execute(
                    select(MineralSite.site_id, MineralSite.dedup_site_id)
                    .where(
                        MineralSite.source_id == site_and_inv.ms.source_id,
                        MineralSite.record_id == site_and_inv.ms.record_id,
                    )
                    .limit(1)
                ).one_or_none()
                if (
                    single_sites_auto_linked_via_source_and_records is not None
                    and site_and_inv.ms.dedup_site_id
                    != single_sites_auto_linked_via_source_and_records[1]
                ):
                    raise ArgumentError(
                        f"The new site are linked automatically with {single_sites_auto_linked_via_source_and_records[1]} via {single_sites_auto_linked_via_source_and_records[0]}. However, the dedup site id is different from the new site. Please merging the dedup site first."
                    )

                sites_with_same_dedup_id = self._read_mineral_sites(
                    session,
                    self._select_mineral_site().where(
                        MineralSite.dedup_site_id == site_and_inv.ms.dedup_site_id
                    ),
                )
                if len(sites_with_same_dedup_id) == 0:
                    # the dedup site id is not found in the database, we should complain as they do something wrong.
                    raise ArgumentError(
                        f"The dedup site id {site_and_inv.ms.dedup_site_id} is not found in the database."
                    )

                existing_sites = sites_with_same_dedup_id

            # **ALGO**
            # update the dedup mineral site in the database -- we can do better by using update_site function
            # we do this first so that the dedup_site_id in MineralInventoryView is updated correctly
            dedup_site = DedupMineralSite.from_sites(
                existing_sites + [site_and_inv],
                dedup_site_id=site_and_inv.ms.dedup_site_id,
            )

            if len(existing_sites) == 0:
                session.add(dedup_site)
                session.flush()
            else:
                session.execute(dedup_site.get_update_query())

            # **ALGO**
            # insert the new site and its inventories into the database
            session.add(site_and_inv.ms)
            session.flush()
            session.refresh(site_and_inv.ms)
            for inv in site_and_inv.invs:
                inv.site_id = site_and_inv.ms.id
            session.add_all(site_and_inv.invs)

            # **ALGO**
            # update the inventory for dedup site in the database
            session.execute(
                update(MineralInventoryView),
                [
                    {
                        "id": inv.id,
                        "dedup_site_id": inv.dedup_site_id,
                    }
                    for ms in existing_sites
                    for inv in ms.invs
                ],
            )

            session.add(
                EventLog.from_site_add(
                    site_and_inv, [ms.ms.site_id for ms in existing_sites]
                )
            )

            # step 3: commit data
            session.commit()

    def update(
        self,
        site_and_inv: MineralSiteAndInventory,
        site_snapshot_id: Optional[int] = None,
    ):
        with Session(self.engine, expire_on_commit=False) as session:
            session.connection(execution_options={"isolation_level": "REPEATABLE READ"})

            # step -1: pull up the data and check to make sure that they don't change
            # the dedup site information and if the site snapshot id matches
            prev_dms_id, prev_snapshot_id = session.execute(
                select(MineralSite.dedup_site_id, MineralSite.modified_at).where(
                    MineralSite.id == site_and_inv.ms.id
                )
            ).one()
            if (
                site_and_inv.ms.has_dedup_site()
                and prev_dms_id != site_and_inv.ms.dedup_site_id
            ):
                raise UnsupportOperationError(
                    f"This service does not support updating dedup site id (`{prev_dms_id}` vs `{site_and_inv.ms.dedup_site_id}`), use `update_same_as` instead."
                )
            site_and_inv.ms.dedup_site_id = prev_dms_id
            if site_snapshot_id is not None and prev_snapshot_id != site_snapshot_id:
                raise ExpiredSnapshotIdError(
                    f"The new snapshot of the site is {prev_snapshot_id}"
                )

            self._read_mineral_sites(
                session,
                self._select_mineral_site().where(MineralSite.id == site_and_inv.ms.id),
            )

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
            update_invs = []
            for inv in site_and_inv.invs:
                if inv.id is not None:
                    update_invs.append(inv.get_update_args())
                else:
                    session.add(inv)
            if len(update_invs) > 0:
                session.execute(update(MineralInventoryView), update_invs)
            session.add(EventLog.from_site_update(site_and_inv))

            # step 3: commit data
            session.commit()

    def update_same_as(
        self, user_uri: str, groups: list[list[InternalID]]
    ) -> list[InternalID]:
        output = []
        with Session(self.engine) as session:
            session.connection(execution_options={"isolation_level": "REPEATABLE READ"})

            # **ALGO**
            # retrieve all dedup sites that are affected by this update
            # because of AXIOM 1, sites that have the same source id and record id will belong to the same
            # dedup sites, so we won't miss anything by querying by the dedup ids
            affected_site_ids = {msid for grp in groups for msid in grp}
            assert len(affected_site_ids) == sum(len(grp) for grp in groups)

            affected_dedup_ids: set[InternalID] = set(
                session.execute(
                    select(MineralSite.dedup_site_id)
                    .distinct()
                    .where(MineralSite.site_id.in_(affected_site_ids))
                ).scalars()
            )

            # **ALGO**
            # retrieve all sites info that are affected by this update
            affected_sites = self._read_mineral_sites(
                session,
                self._select_mineral_site().where(
                    MineralSite.site_id.in_(affected_site_ids)
                ),
            )
            id2msi = {msi.ms.site_id: msi for msi in affected_sites}

            # **ALGO**
            # we must ensure that the update is comply with AXIOM 1
            site_id_to_group = {
                msid: grp_idx for grp_idx, grp in enumerate(groups) for msid in grp
            }
            for lst_ms in makedict.group_keys(
                ((_ms.ms.source_id, _ms.ms.record_id), _ms) for _ms in affected_sites
            ).values():
                grp_idx = site_id_to_group[lst_ms[0].ms.site_id]
                for ms in lst_ms[1:]:
                    if site_id_to_group[ms.ms.site_id] != grp_idx:
                        raise ArgumentError(
                            f"Site {ms.ms.site_id} is marked as different from {lst_ms[0].ms.site_id} but they should be the same."
                        )

            # **ALGO**
            # we must ensure that the provided links are complete, no more missing links
            db_affected_site_ids = {msi.ms.site_id for msi in affected_sites}
            if affected_site_ids != db_affected_site_ids:
                raise ArgumentError(
                    f"Affected sites by this update are different from the list of provided sites: {db_affected_site_ids.symmetric_difference(affected_site_ids)}"
                )

            # **ALGO**
            # compute a mapping from internal ID to a list of internal IDs that are previously marked as the same but now are different.
            # needed as this information is required by the event log
            diff_groups: dict[InternalID, list[InternalID]] = defaultdict(list)
            old_dedup_to_msis = makedict.group_keys(
                (msi.ms.dedup_site_id, msi) for msi in affected_sites
            )
            for msis in old_dedup_to_msis.values():
                for msi in msis:
                    diff_groups[msi.ms.site_id] = [
                        other_msi.ms.site_id
                        for other_msi in msis
                        if site_id_to_group[msi.ms.site_id]
                        != site_id_to_group[other_msi.ms.site_id]
                    ]

            # **ALGO**
            # now perform the update on the groups
            for grp_idx, group in enumerate(groups):
                # **ALGO**
                # recalculate the dedup site id, while this may not be optimal, it ensures that when we restore from
                # the backup, the dedup site id is always the same
                dedup_site_id = MineralSite.get_dedup_id(group)

                # **ALGO**
                # update the dedup site
                group_msi = [id2msi[site_id] for site_id in group]
                dedup_site = DedupMineralSite.from_sites(
                    group_msi, dedup_site_id=dedup_site_id
                )
                if dedup_site_id not in affected_dedup_ids:
                    session.add(dedup_site)
                    session.flush()
                else:
                    session.execute(dedup_site.get_update_query())

                # **ALGO**
                # update the mineral sites and their inventories
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
                        for msi in group_msi
                        for inv in msi.invs
                    ],
                )

                output.append(dedup_site_id)

            session.execute(
                delete(DedupMineralSite).where(
                    DedupMineralSite.id.in_(set(affected_dedup_ids).difference(output))
                )
            )
            session.add(EventLog.from_same_as_update(user_uri, groups, diff_groups))
            session.commit()
        return output

    def find_dedup_mineral_sites(
        self,
        *,
        commodity: Optional[InternalID],
        deposit_type: Optional[InternalID] = None,
        country: Optional[InternalID] = None,
        state_or_province: Optional[InternalID] = None,
        has_grade_tonnage: Optional[bool] = None,
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
                isouter=commodity is not None,
            )
            .group_by(DedupMineralSite.id)
        )

        count_query = None
        if return_count:
            count_query = select(func.count(distinct(DedupMineralSite.id))).select_from(
                DedupMineralSite
            )

        if commodity is not None:
            query = query.where(MineralInventoryView.commodity == commodity)
            if count_query is not None:
                count_query = count_query.join(
                    MineralInventoryView,
                    MineralInventoryView.dedup_site_id == DedupMineralSite.id,
                    isouter=True,
                ).where(MineralInventoryView.commodity == commodity)

        if deposit_type is not None:
            query = query.where(DedupMineralSite.top1_deposit_type == deposit_type)
            if count_query is not None:
                count_query = count_query.where(
                    DedupMineralSite.top1_deposit_type == deposit_type
                )

        if country is not None:
            # TODO: this a temporary solution to retrieve the inner element of the composite property
            # it seems that the good way is to redefine the Comparator
            query = query.where(
                DedupMineralSite.country._comparable_elements[0].any(country)
            )
            if count_query is not None:
                count_query = count_query.where(
                    DedupMineralSite.country._comparable_elements[0].any(country)
                )

        if state_or_province is not None:
            query = query.where(
                DedupMineralSite.state_or_province._comparable_elements[0].any(
                    state_or_province
                )
            )
            if count_query is not None:
                count_query = count_query.where(
                    DedupMineralSite.state_or_province._comparable_elements[0].any(
                        state_or_province
                    )
                )

        if has_grade_tonnage is not None:
            if has_grade_tonnage:
                query = query.where(MineralInventoryView.contained_metal.isnot(None))
                if count_query is not None:
                    count_query = count_query.where(
                        MineralInventoryView.contained_metal.isnot(None)
                    )
            else:
                query = query.where(MineralInventoryView.contained_metal.is_(None))
                if count_query is not None:
                    count_query = count_query.where(
                        MineralInventoryView.contained_metal.is_(None)
                    )

        if dedup_site_ids is not None:
            query = query.where(DedupMineralSite.id.in_(dedup_site_ids))

        if limit > 0:
            query = query.limit(limit)
        if offset > 0:
            query = query.offset(offset)

        with Session(self.engine, expire_on_commit=False) as session:
            lst_dms_and_invs: list[DedupMineralSiteAndInventory] = [
                self._norm_dedup_mineral_site(row) for row in session.execute(query)
            ]
            total = (
                session.execute(count_query).scalar_one()
                if count_query is not None
                else 0
            )
            return {
                "items": {
                    dms_and_invs.dms.id: dms_and_invs
                    for dms_and_invs in lst_dms_and_invs
                },
                "total": total,
            }

    def _select_mineral_site(
        self,
    ) -> Select[Tuple[MineralSite, list[RawMineralInventoryView]]]:
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
        query: Select[Tuple[MineralSite, list[RawMineralInventoryView]]],
    ) -> list[MineralSiteAndInventory]:
        out = []
        for row in session.execute(query).all():
            msi = MineralSiteAndInventory(
                ms=row[0],
                invs=[
                    inv
                    for raw_inv in row[1]
                    if (inv := MineralInventoryView.from_dict(raw_inv)).commodity
                    is not None
                ],
            )
            for inv in msi.invs:
                inv.site_id = msi.ms.id
            out.append(msi)
        return out

    def _norm_dedup_mineral_site(
        self, row: Row[tuple[DedupMineralSite, list[RawMineralInventoryView]]]
    ) -> DedupMineralSiteAndInventory:
        dms: DedupMineralSite = row[0]
        invs = []
        for rawinv in row[1]:
            inv = MineralInventoryView.from_dict(rawinv)
            inv.dedup_site_id = dms.id
            if inv.commodity is None:
                continue
            invs.append(inv)
        return DedupMineralSiteAndInventory(dms=dms, invs=invs)

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
