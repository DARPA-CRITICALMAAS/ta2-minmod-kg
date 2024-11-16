from __future__ import annotations

from functools import cached_property
from typing import Annotated, ClassVar, Iterable, Optional

from minmodkg.misc.rdf_store import BaseRDFModel, BaseRDFQueryBuilder
from minmodkg.models.derived_mineral_site import DerivedMineralSite, GradeTonnage
from minmodkg.typing import IRI, InternalID, Triple
from pydantic import BaseModel


class DedupMineralSiteDepositType(BaseModel):
    id: InternalID
    source: str
    confidence: float


class DedupMineralSiteLocation(BaseModel):
    lat: Optional[float]
    lon: Optional[float]
    country: list[InternalID]
    state_or_province: list[InternalID]


class DedupMineralSitePublic(BaseModel):
    id: InternalID
    name: str
    type: str
    rank: str
    sites: list[InternalID]
    deposit_types: list[DedupMineralSiteDepositType]
    location: Optional[DedupMineralSiteLocation]
    grade_tonnage: Optional[GradeTonnage]


class DedupMineralSiteQueryBuilder(BaseRDFQueryBuilder):
    def __init__(self):
        ns = self.rdfdata.ns

        main = "dedup_ms"
        select = []
        where = [f"?{main} rdf:type {ns.mo.DedupMineralSite} ."]
        for field in [ns.md.site, ns.md.commodity, ns.md.site_commodity]:
            var = f"{main}_{field.split(":")[-1]}"
            select.append(f"?{main} {field} ?{var} .")
            where.append(f"?{main} {field} ?{var} .")

        self.main_var: str = main
        self.construct_select: str = "\n".join(select)
        self.construct_where: str = "\n".join(where)


class DedupMineralSite(BaseRDFModel):
    id: InternalID
    sites: list[InternalID]
    commodities: list[InternalID]
    site_commodities: list[Annotated[str, "Encoded <site_id>@<list of commodities>"]]

    query_builder: ClassVar[DedupMineralSiteQueryBuilder] = (
        DedupMineralSiteQueryBuilder()
    )

    @cached_property
    def uri(self) -> IRI:
        return self.rdfdata.ns.mr.uristr(self.id)

    def get_by_uri(self, rel_uri: str):
        self.query_builder.create_get_by_uri(self.rdfdata.ns.mr[self.id])

    def to_triples(self, triples: Optional[list[Triple]] = None) -> list[Triple]:
        triples = triples or []
        ns = self.rdfdata.ns
        md = ns.md
        mr = ns.mr
        uri = mr[self.id]
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
            commodities=list(
                set(gt.commodity for site in sites for gt in site.grade_tonnage)
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
