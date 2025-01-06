from __future__ import annotations

import re
from collections import defaultdict
from typing import Iterable, Optional

import shapely.wkt
from minmodkg.grade_tonnage_model import GradeTonnageModel
from minmodkg.misc.geo import reproject_wkt
from minmodkg.misc.utils import assert_not_none, exclude_none_or_empty_list
from minmodkg.models.dedup_mineral_site import DedupMineralSite
from minmodkg.models.mineral_site import MineralSite
from minmodkg.models.views.base import Base
from minmodkg.models.views.custom_types import ComputedLocation
from minmodkg.typing import InternalID
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column, relationship


class MineralSiteSameAs(MappedAsDataclass, Base):
    __tablename__ = "same_as"

    site_id: Mapped[int] = mapped_column(ForeignKey("computed_mineral_site.id"))
