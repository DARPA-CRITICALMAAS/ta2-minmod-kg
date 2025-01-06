from __future__ import annotations

from functools import lru_cache
from typing import Callable, ContextManager
from uuid import uuid4

from minmodkg.api.dependencies import get_snapshot_id
from minmodkg.api.models.user import UserBase
from minmodkg.api.routers.predefined_entities import get_crs, get_material_forms
from minmodkg.misc.rdf_store.triple_store import TripleStore
from minmodkg.misc.utils import norm_literal
from minmodkg.models.base import MINMOD_KG
from minmodkg.models.dedup_mineral_site import DedupMineralSite
from minmodkg.models.mineral_site import MineralSite
from minmodkg.models.views.computed_mineral_site import ComputedMineralSite
from minmodkg.typing import InternalID, Triple
from rdflib import Graph
from sqlalchemy import select, update
from sqlalchemy.orm import Session


class MineralSiteService:
    def __init__(
        self,
        kgview_session: Callable[[], ContextManager[Session]],
        kg: TripleStore,
        user: UserBase,
    ):
        self.kgview_session = kgview_session
        self.kg = kg
        self.user = user
        self.snapshot_id = get_snapshot_id()
        self.material_form = material_form_uri_to_conversion(self.snapshot_id)
        self.crss = crs_uri_to_name(self.snapshot_id)

    def create(self, site: MineralSite, same_as: list[InternalID]):
        """Create a mineral site and mark it as same as other sites"""
        # update automatic values
        site.update_derived_data(self.user)
        if site.dedup_site_uri is None:
            site.dedup_site_uri = DedupMineralSite.qbuilder.class_namespace.uristr(
                DedupMineralSite.get_id([site.id])
            )

        # persist the site into the view databases -- but make sure that we have a stale version
        site_view = ComputedMineralSite.from_mineral_site(
            site, self.material_form, self.crss
        )
        site_view.is_updated = False

        with self.kgview_session() as session:
            session.add(site)
            session.commit()

        # now save the site
        self.kg.insert(site.to_triples())

        # save the site view -- do it as in a single query
        # as we may have a rare case where someone update the site right after we save it
        # if that case happens, the snapshot id will be different and this update will be void
        # keeping the data consistent
        with self.kgview_session() as session:
            session.execute(
                (
                    update(ComputedMineralSite)
                    .where(
                        ComputedMineralSite.id == site_view.id,
                        ComputedMineralSite.snapshot_id == site_view.snapshot_id,
                    )
                    .values(is_updated=True)
                )
            )
            session.commit()

    def update(self, site: MineralSite):
        site.update_derived_data(self.user)
        assert site.snapshot_id is not None

        with self.kgview_session() as session:
            site_view: ComputedMineralSite = session.execute(
                select(ComputedMineralSite).where(
                    ComputedMineralSite.site_id == site.id
                )
            ).one()[0]

            with self.kg.transaction([site.uri]).transaction():
                # we can lock the site here to prevent concurrent updates
                # at this point, it's likely to be success, tell the view db that we are updating
                site_view.snapshot_id = site.snapshot_id
                site_view.is_updated = False
                session.commit()

                # now we execute the kg update
                del_triples, add_triples = get_site_changes(
                    current_site=MineralSite.get_graph_by_uri(site.uri),
                    new_site=site.to_graph(),
                )
                self.kg.delete_insert(del_triples, add_triples)

                # now we update the view db
                new_site_view = ComputedMineralSite.from_mineral_site(
                    site, self.material_form, self.crss
                )
                new_site_view.id = site_view.id
                session.add(new_site_view)
                session.commit()

    def update_same_as(self, groups: list[list[InternalID]]):
        pass
        # step 1: gather all objects that will be updated
        # site_ids = set()
        # for group in groups:

        # self.kg.delete_insert()
        # """DELETE { ?}"""


@lru_cache(maxsize=1)
def crs_uri_to_name(snapshot_id: str):
    return {crs.uri: crs.name for crs in get_crs(snapshot_id)}


@lru_cache(maxsize=1)
def material_form_uri_to_conversion(snapshot_id: str):
    return {mf.uri: mf.conversion for mf in get_material_forms(snapshot_id)}


def get_site_changes(
    current_site: Graph, new_site: Graph
) -> tuple[list[Triple], list[Triple]]:
    ns_manager = MINMOD_KG.ns.rdflib_namespace_manager
    current_triples = {(s, p, norm_literal(o)) for s, p, o in current_site}
    new_triples = {(s, p, norm_literal(o)) for s, p, o in new_site}
    del_triples = [
        (s.n3(ns_manager), p.n3(ns_manager), o.n3(ns_manager))
        for s, p, o in current_triples.difference(new_triples)
    ]
    add_triples = [
        (s.n3(ns_manager), p.n3(ns_manager), o.n3(ns_manager))
        for s, p, o in new_triples.difference(current_triples)
    ]
    return del_triples, add_triples
