from __future__ import annotations

from functools import lru_cache
from typing import Callable, ContextManager
from uuid import uuid4

from minmodkg.api.dependencies import get_snapshot_id
from minmodkg.api.routers.predefined_entities import get_crs, get_material_forms
from minmodkg.misc.rdf_store.triple_store import TripleStore
from minmodkg.misc.utils import norm_literal
from minmodkg.models.base import MINMOD_KG
from minmodkg.models.dedup_mineral_site import DedupMineralSite
from minmodkg.models.mineral_site import MineralSite
from minmodkg.models.views.computed_mineral_site import ComputedMineralSite
from minmodkg.typing import InternalID, Triple
from rdflib import Graph
from sqlalchemy import select, update
from sqlalchemy.orm import Session


class MineralSiteSameAsService:
    def __init__(
        self,
        kgview_session: Callable[[], ContextManager[Session]],
        kg: TripleStore,
        user: UserBase,
    ):
        self.kgview_session = kgview_session
        self.kg = kg
        self.user = user

    def update(self, groups: list[list[InternalID]]):
        pass
