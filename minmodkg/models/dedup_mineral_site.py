from __future__ import annotations

from functools import cached_property
from typing import Annotated, Iterable, Optional

from minmodkg.config import MNR_NS
from minmodkg.models.derived_mineral_site import DerivedMineralSite, GradeTonnage
from minmodkg.typing import IRI, InternalID, Triple
from pydantic import BaseModel


class DedupMineralSiteDepositType(BaseModel):
    uri: IRI
    source: str
    confidence: float


class DedupMineralSiteLocation(BaseModel):
    lat: Optional[float]
    long: Optional[float]
    country: list[IRI]
    state_or_province: list[IRI]


class DedupMineralSitePublic(BaseModel):
    uri: IRI
    name: str
    type: str
    rank: str
    sites: list[IRI]
    deposit_types: list[DedupMineralSiteDepositType]
    location: Optional[DedupMineralSiteLocation]
    grade_tonnage: Optional[GradeTonnage]


class DedupMineralSite(BaseModel):
    uri: IRI
    sites: list[IRI]
    commodities: list[IRI]
    site_commodities: list[str]

    @cached_property
    def id(self) -> InternalID:
        assert self.uri.startswith(MNR_NS), self.uri
        return self.uri[len(MNR_NS) :]

    @staticmethod
    def from_derived_sites(sites: list[DerivedMineralSite], uri: Optional[IRI] = None):
        return DedupMineralSite(
            uri=uri or (MNR_NS + DedupMineralSite.get_id((site.id for site in sites))),
            sites=[site.uri for site in sites],
            commodities=list(
                set(
                    MNR_NS + gt.commodity for site in sites for gt in site.grade_tonnage
                )
            ),
            site_commodities=[
                site.id + "@" + ",".join(gt.commodity for gt in site.grade_tonnage)
                for site in sites
            ],
        )

    def get_shorten_triples(self) -> list[Triple]:
        """Get triples shorten with the following prefixes:

        `:`: MNO_NS
        rdf: RDF
        rdfs: RDFS
        mnr: MNR_NS
        owl: OWL
        """
        mnr_ns_len = len(MNR_NS)
        dedup_id = f"mnr:{self.id}"
        triples = [
            (dedup_id, "rdf:type", ":DedupMineralSite"),
        ]
        for site in self.sites:
            site_id = f"mnr:{site[mnr_ns_len:]}"
            triples.append((dedup_id, ":site", site_id))
            triples.append((site_id, ":dedup_site", dedup_id))
        for commodity in self.commodities:
            triples.append((dedup_id, ":commodity", f"mnr:{commodity[mnr_ns_len:]}"))
        for site_commodity in self.site_commodities:
            triples.append((dedup_id, ":site_commodity", f'"{site_commodity}"'))
        return triples

    @staticmethod
    def get_id(site_ids: Iterable[InternalID]):
        return "dedup_" + min(site_ids)
