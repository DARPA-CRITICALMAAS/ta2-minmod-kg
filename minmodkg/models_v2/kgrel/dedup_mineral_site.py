from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Iterable, Optional

import minmodkg.models.candidate_entity
import minmodkg.models.mineral_inventory
import minmodkg.models.reference
from minmodkg.grade_tonnage_model import GradeTonnageModel
from minmodkg.misc.utils import makedict
from minmodkg.models.base import MINMOD_NS
from minmodkg.models_v2.inputs.candidate_entity import CandidateEntity
from minmodkg.models_v2.inputs.measure import Measure
from minmodkg.models_v2.inputs.mineral_inventory import MineralInventory
from minmodkg.models_v2.inputs.mineral_site import MineralSite as InMineralSite
from minmodkg.models_v2.inputs.reference import (
    BoundingBox,
    Document,
    PageInfo,
    Reference,
)
from minmodkg.models_v2.kgrel.base import Base
from minmodkg.models_v2.kgrel.custom_types import Location, LocationView
from minmodkg.models_v2.kgrel.views.mineral_inventory_view import MineralInventoryView
from minmodkg.typing import IRI, URN, InternalID
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, MappedAsDataclass, composite, mapped_column


@dataclass
class StrRef:
    value: str
    refid: InternalID


class DedupMineralSite(MappedAsDataclass, Base):
    __tablename__ = "dedup_mineral_site"

    id: Mapped[InternalID] = mapped_column(primary_key=True)
    name: Mapped[StrRef] = composite(mapped_column("name"), mapped_column("name_refid"))
    type: Mapped[str] = composite(mapped_column("type"), mapped_column("type_refid"))
    rank: Mapped[str] = composite(mapped_column("rank"), mapped_column("rank_refid"))

    # sites: Mapped[list[MineralSiteIdAndScore]] = mapped_column()
    # modified_at: Mapped[float] = mapped_column()
