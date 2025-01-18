from __future__ import annotations

import time
from typing import Sequence

import typer
from minmodkg.misc.utils import norm_literal
from minmodkg.models.kg.base import MINMOD_KG
from minmodkg.models.kgrel.base import get_rel_session
from minmodkg.models.kgrel.event import EventLog
from minmodkg.models.kgrel.mineral_site import MineralSiteAndInventory
from minmodkg.services.sync.listener import Listener
from minmodkg.typing import IRI, InternalID
from rdflib import Graph, URIRef
from sqlalchemy import select


class KGSyncListener(Listener):
    def handle_site_add(self, site: MineralSiteAndInventory):
        MINMOD_KG.insert(site.ms.to_kg().to_triples())

    def handle_site_update(self, site: MineralSiteAndInventory):
        kgms = site.ms.to_kg()
        ng = kgms.to_graph()
        og = self.get_mineral_site_graph_by_uri(kgms.uri)

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

    def handle_same_as_update(self, groups: list[list[str]]): ...

    def get_mineral_site_graph_by_uri(self, uri: IRI | URIRef) -> Graph:
        # Fuseki can optimize this case, but I don't know why sometimes it cannot
        return MINMOD_KG.construct(
            """
CONSTRUCT {
    ?s ?p ?o
}
WHERE {
    <%s> (!(owl:sameAs|rdf:type|mo:normalized_uri))* ?s .
    ?s ?p ?o .
}
"""
            % (uri,)
        )
