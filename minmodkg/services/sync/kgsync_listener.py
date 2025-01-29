from __future__ import annotations

from typing import Iterable

from minmodkg.misc.utils import norm_literal
from minmodkg.models.kg.base import MINMOD_KG, MINMOD_NS
from minmodkg.models.kg.mineral_site import MineralSite
from minmodkg.models.kgrel.event import EventLog
from minmodkg.models.kgrel.mineral_site import MineralSiteAndInventory
from minmodkg.services.sync.listener import Listener
from minmodkg.typing import IRI, InternalID
from rdflib import Graph, URIRef


class KGSyncListener(Listener):
    owl_same_as = MINMOD_NS.owl.sameAs
    rdf_type = MINMOD_NS.rdf.type
    mo_normalized_uri = MINMOD_NS.mo.normalized_uri

    def handle_site_add(
        self,
        event: EventLog,
        site: MineralSiteAndInventory,
        same_site_ids: list[InternalID],
    ):
        key_ns = MineralSite.__subj__.key_ns
        triples = site.ms.to_kg().to_triples()
        for same_site_id in same_site_ids:
            triples.append(
                (key_ns[site.ms.site_id], self.owl_same_as, key_ns[same_site_id])
            )
        MINMOD_KG.insert(triples)

    def handle_site_update(self, event: EventLog, site: MineralSiteAndInventory):
        kgms = site.ms.to_kg()
        ng = kgms.to_graph()
        og = self._get_mineral_site_graph_by_uri(kgms.uri)

        current_triples = {(s, p, norm_literal(o)) for s, p, o in og}
        new_triples = {(s, p, norm_literal(o)) for s, p, o in ng}

        ns_manager = MINMOD_KG.ns.rdflib_namespace_manager

        del_triples = [
            (s.n3(ns_manager), p.n3(ns_manager), o.n3(ns_manager))
            for s, p, o in current_triples.difference(new_triples)
        ]
        add_triples = [
            (s.n3(ns_manager), p.n3(ns_manager), o.n3(ns_manager))
            for s, p, o in new_triples.difference(current_triples)
        ]

        MINMOD_KG.delete_insert(del_triples, add_triples)

    def handle_same_as_update(
        self,
        event: EventLog,
        user_uri: str,
        groups: list[list[InternalID]],
        diff_groups: dict[InternalID, list[InternalID]],
    ):
        key_ns = MineralSite.__subj__.key_ns
        # potential_existing_links = self._get_all_same_as_links(
        #     {id for group in groups for id in group}
        # )
        # delete same as link to/from other sites, and then insert the new same as links
        delete_links = []
        for site, diff_sites in diff_groups.items():
            s = key_ns[site]
            for diff_site in diff_sites:
                o = key_ns[diff_site]
                delete_links.append((s, self.owl_same_as, o))
                delete_links.append((o, self.owl_same_as, s))

        MINMOD_KG.delete_insert(
            delete_links,
            [
                (key_ns[group[0]], self.owl_same_as, key_ns[target])
                for group in groups
                for target in group[1:]
            ],
        )

    def _get_all_same_as_links(
        self, ids: Iterable[InternalID]
    ) -> list[tuple[InternalID, InternalID]]:
        key_ns = MineralSite.__subj__.key_ns

        # retrieve all the sameAs relations for the affected entities
        lst = MINMOD_KG.query(
            """
SELECT ?s ?o
WHERE {
    ?s (%s|^%s) ?o .
    VALUES ?s { %s }
}
"""
            % (self.owl_same_as, self.owl_same_as, " ".join(key_ns[id] for id in ids)),
            keys=["s", "o"],
        )
        return [(key_ns.abs2rel(so["s"]), key_ns.abs2rel(so["o"])) for so in lst]

    def _get_mineral_site_graph_by_uri(self, uri: IRI | URIRef) -> Graph:
        # Fuseki can optimize this case, but I don't know why sometimes it cannot
        return MINMOD_KG.construct(
            f"""
CONSTRUCT {{
    ?s ?p ?o
}}
WHERE {{
    <{uri}> (!({self.owl_same_as}|{self.rdf_type}|{self.mo_normalized_uri}))* ?s .
    ?s ?p ?o .

    # Exclude owl:sameAs because it's not part of the model
    FILTER (?p != {self.owl_same_as})
}}
"""
        )
