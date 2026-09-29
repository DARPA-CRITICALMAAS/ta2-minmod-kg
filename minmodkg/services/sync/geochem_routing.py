from __future__ import annotations

from typing import Optional

from minmodkg.models.kgrel.base import engine
from minmodkg.models.kgrel.mineral_site import MineralSite, MineralSiteAndInventory
from minmodkg.models.kgrel.paper import GEOCHEM_USER_URI, Paper
from minmodkg.models.kgrel.sample import Sample
from minmodkg.typing import InternalID
from sqlalchemy import select
from sqlalchemy.orm import Session


class GeoChemRouting:
    """Which site and sample edits belong to a GeoChem paper, and so are
    written back to its JSON-LD by the GeoChem sync instead of being backed
    up by MinMod's. Both backup lanes use it, so each event has one owner."""

    def __init__(self):
        with Session(engine) as session:
            self.papers = {
                p.source_id: p for p in session.execute(select(Paper)).scalars()
            }
        self.site_sources: dict[InternalID, Optional[str]] = {}

    def site_paper(self, site: MineralSiteAndInventory) -> Optional[Paper]:
        if site.ms.created_by != GEOCHEM_USER_URI:
            return None
        return self.papers.get(site.ms.source_id)

    def sample_paper(self, sample: Sample) -> Optional[Paper]:
        site_id = sample.mineral_site_id
        if site_id not in self.site_sources:
            with Session(engine) as session:
                row = session.execute(
                    select(MineralSite.source_id, MineralSite.created_by).where(
                        MineralSite.site_id == site_id
                    )
                ).one_or_none()
            self.site_sources[site_id] = (
                row[0] if row is not None and row[1] == GEOCHEM_USER_URI else None
            )
        source_id = self.site_sources[site_id]
        return self.papers.get(source_id) if source_id is not None else None
