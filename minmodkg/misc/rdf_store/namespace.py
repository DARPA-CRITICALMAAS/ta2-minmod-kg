from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Iterable

from minmodkg.typing import IRI, InternalID, RelIRI
from rdflib import OWL, RDF, RDFS, SKOS, XSD, Graph, URIRef
from rdflib.namespace import NamespaceManager


@dataclass
class SingleNS:
    alias: str
    namespace: str

    def __post_init__(self):
        assert self.namespace.endswith("/") or self.namespace.endswith(
            "#"
        ), f"Namespace {self.namespace} should end with / or #"

    def id(self, uri: IRI | URIRef) -> str:
        assert uri.startswith(self.namespace)
        return uri[len(self.namespace) :]

    def uri(self, name: InternalID) -> URIRef:
        return URIRef(self.namespace + name)

    def uristr(self, name: InternalID) -> IRI:
        return self.namespace + name

    def __getattr__(self, name: InternalID):
        return self.alias + ":" + name

    def __getitem__(self, name: InternalID):
        return self.alias + ":" + name

    def __contains__(self, uri: IRI | URIRef) -> bool:
        return uri.startswith(self.namespace)

    def rel2abs(self, reluri: RelIRI) -> URIRef:
        return URIRef(self.namespace + reluri.split(":")[1])


class Namespace:
    rdf = SingleNS("rdf", str(RDF))

    def __init__(self, ns_cfg: dict):
        self.mr = SingleNS("mr", ns_cfg["mr"])
        self.mo = SingleNS("mo", ns_cfg["mo"])
        self.md = SingleNS("md", ns_cfg["mo-derived"])
        self.dcterms = SingleNS("dcterms", "http://purl.org/dc/terms/")
        self.rdfs = SingleNS("rdfs", str(RDFS))
        self.xsd = SingleNS("xsd", str(XSD))
        self.owl = SingleNS("owl", str(OWL))
        self.gkbi = SingleNS("gkbi", "https://geokb.wikibase.cloud/entity/")
        self.gkbt = SingleNS("gkbt", "https://geokb.wikibase.cloud/prop/direct/")
        self.geo = SingleNS("geo", "http://www.opengis.net/ont/geosparql#")
        self.skos = SingleNS("skos", str(SKOS))

        self.namespaces = {
            x.alias: x for x in self.__dict__.values() if isinstance(x, SingleNS)
        }
        self.namespaces[self.rdf.alias] = self.rdf

    def iter(self) -> Iterable[SingleNS]:
        return self.namespaces.values()

    def get_by_alias(self, alias: str) -> SingleNS:
        return self.namespaces[alias]

    @cached_property
    def rdflib_namespace_manager(self) -> NamespaceManager:
        nsmanager = NamespaceManager(Graph(), bind_namespaces="none")
        for ns in self.iter():
            nsmanager.bind(ns.alias, ns.namespace)
        return nsmanager
