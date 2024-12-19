from __future__ import annotations

from functools import cached_property
from typing import Annotated, ClassVar, Iterable, Optional

from minmodkg.misc.utils import filter_duplication
from minmodkg.models.base import MinModRDFModel, MinModRDFQueryBuilder
from minmodkg.models.derived_mineral_site import DerivedMineralSite, GradeTonnage
from minmodkg.typing import IRI, InternalID, Triple
from pydantic import BaseModel
from rdflib import Graph, URIRef


class DedupMineralSiteDepositType(BaseModel):
    id: InternalID
    source: str
    confidence: float


class DedupMineralSiteLocation(BaseModel):
    lat: Optional[float] = None
    lon: Optional[float] = None
    country: list[InternalID]
    state_or_province: list[InternalID]


class DedupMineralSiteIdAndScore(BaseModel):
    id: str
    score: float


class DedupMineralSitePublic(BaseModel):
    id: InternalID
    name: str
    type: str
    rank: str
    sites: list[DedupMineralSiteIdAndScore]
    deposit_types: list[DedupMineralSiteDepositType]
    location: Optional[DedupMineralSiteLocation] = None
    grade_tonnage: list[GradeTonnage]
    modified_at: str


class DedupMineralSite(MinModRDFModel):
    id: InternalID
    sites: list[InternalID]
    commodities: list[InternalID]
    site_commodities: list[Annotated[str, "Encoded <site_id>@<list of commodities>"]]

    class QueryBuilder(MinModRDFQueryBuilder):
        def __init__(self):
            ns = self.rdfdata.ns
            self.class_namespace = ns.md
            self.class_reluri = ns.mo.DedupMineralSite
            self.fields = [
                self.PropertyRule(ns.md, "site", is_optional=False),
                self.PropertyRule(ns.md, "commodity", is_optional=False),
                self.PropertyRule(ns.md, "site_commodity", is_optional=False),
            ]

    qbuilder: ClassVar[QueryBuilder] = QueryBuilder()

    @cached_property
    def uri(self) -> IRI:
        return self.qbuilder.class_namespace.uristr(self.id)

    @classmethod
    def from_graph(cls, uid: URIRef, g: Graph):
        mr = cls.rdfdata.ns.mr
        md = cls.rdfdata.ns.md
        return DedupMineralSite(
            id=md.id(uid),
            sites=[mr.id(str(o)) for o in g.objects(uid, md.uri("site"))],
            commodities=[mr.id(str(o)) for o in g.objects(uid, md.uri("commodity"))],
            site_commodities=[str(o) for o in g.objects(uid, md.uri("site_commodity"))],
        )

    def to_triples(self, triples: Optional[list[Triple]] = None) -> list[Triple]:
        if triples is None:
            triples = []
        ns = self.rdfdata.ns
        md = ns.md
        mr = ns.mr
        uri = md[self.id]
        triples.append((uri, ns.rdf.type, ns.mo.DedupMineralSite))
        for site in self.sites:
            triples.append((uri, md.site, mr[site]))
        for commodity in self.commodities:
            triples.append((uri, md.commodity, mr[commodity]))
        for site_commodity in self.site_commodities:
            triples.append((uri, md.site_commodity, f'"{site_commodity}"'))
        return triples

    @staticmethod
    def from_derived_sites(
        sites: list[DerivedMineralSite], id: Optional[InternalID] = None
    ):
        return DedupMineralSite(
            id=id or DedupMineralSite.get_id(site.id for site in sites),
            sites=[site.id for site in sites],
            commodities=filter_duplication(
                (gt.commodity for site in sites for gt in site.grade_tonnage)
            ),
            site_commodities=[
                site.id + "@" + ",".join(gt.commodity for gt in site.grade_tonnage)
                for site in sites
            ],
        )

    # def update_derived_site(self, derived_site: DerivedMineralSite):

    @staticmethod
    def get_id(site_ids: Iterable[InternalID]):
        return "dedup_" + min(site_ids)
