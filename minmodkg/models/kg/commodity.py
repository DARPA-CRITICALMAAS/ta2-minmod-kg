from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Optional

from minmodkg.misc.rdf_store.rdf_model import RDFModel, Subject
from minmodkg.models.kg.base import NS_MO, NS_MR
from minmodkg.typing import InternalID, NotEmptyStr


@dataclass
class Commodity(RDFModel):
    __subj__ = Subject(cls_ns=NS_MO, key_ns=NS_MR, name="Commodity", key="uri")
    id: InternalID
    name: NotEmptyStr
    aliases: list[NotEmptyStr]
    parent: Optional[InternalID]
    is_critical_commodity: bool

    @cached_property
    def uri(self):
        return self.__subj__.key_ns.uri(self.id)
