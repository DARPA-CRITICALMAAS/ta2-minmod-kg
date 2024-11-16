from __future__ import annotations

from typing import Annotated, Iterable, Optional

from minmodkg.misc.rdf_store import BaseRDFModel
from minmodkg.models.derived_mineral_site import DerivedMineralSite, GradeTonnage
from minmodkg.typing import InternalID, Triple
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


class DedupMineralSite(BaseRDFModel):
    id: InternalID
    sites: list[InternalID]
    commodities: list[InternalID]
    site_commodities: list[Annotated[str, "Encoded <site_id>@<list of commodities>"]]

    def to_triples(self) -> list[Triple]:
        ns = self.rdfdata.ns
        md = ns.md
        mr = ns.mr
        uri = mr[self.id]
        triples = [(uri, ns.rdf.type, ns.mo.DedupMineralSite)]
        for site in self.sites:
            triples.append((uri, md.site, mr[site]))
            triples.append((mr[site], md.dedup_site, uri))
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

    @staticmethod
    def get_id(site_ids: Iterable[InternalID]):
        return "dedup_" + min(site_ids)
