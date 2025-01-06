from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from minmodkg.models.base import MINMOD_NS
from minmodkg.models_v2.inputs.mineral_site import MineralSite
from minmodkg.models_v2.kgrel.mineral_site import MineralSite as RelMineralSite
from minmodkg.typing import IRI, InternalID


@dataclass
class UpsertMineralSite(MineralSite):
    dedup_site_uri: Optional[IRI] = None
    same_as: list[InternalID] = field(default_factory=list)

    def to_kgrel(
        self,
        material_form: dict[str, float],
        crs_names: dict[str, str],
    ):
        site = RelMineralSite.from_raw_site(
            self,
            material_form=material_form,
            crs_names=crs_names,
        )
        if self.dedup_site_uri is not None:
            site.dedup_site_id = MINMOD_NS.md.id(self.dedup_site_uri)
        return site
