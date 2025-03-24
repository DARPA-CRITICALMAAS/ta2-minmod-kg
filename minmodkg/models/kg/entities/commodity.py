from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Annotated, Optional

from minmodkg.libraries.rdf.rdf_model import P, RDFModel, Subject
from minmodkg.models.kg.base import NS_MO, NS_MR, NS_RDFS
from minmodkg.typing import CleanedNotEmptyStr, InternalID


@dataclass
class Commodity(RDFModel):
    __subj__ = Subject(type=NS_MO.term("Commodity"), key_ns=NS_MR, key="uri")

    id: Annotated[InternalID, P()]
    name: Annotated[CleanedNotEmptyStr, P(pred=NS_RDFS.term("label"))]
    aliases: Annotated[list[CleanedNotEmptyStr], P()]
    parent: Annotated[Optional[InternalID], P()]
    is_critical: Annotated[bool, P()]

    @cached_property
    def uri(self):
        return self.__subj__.key_ns.uri(self.id)
