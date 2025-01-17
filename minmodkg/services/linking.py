from __future__ import annotations

from typing import Callable, ContextManager

from minmodkg.misc.rdf_store.triple_store import TripleStore
from minmodkg.typing import InternalID
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
