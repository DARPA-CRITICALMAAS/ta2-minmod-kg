from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import Annotated

from minmodkg.libraries.rdf.rdf_model import P, RDFModel, Subject
from minmodkg.models.kg.base import NS_MO, NS_MR, NS_RDFS
from minmodkg.typing import IRI, InternalID


@dataclass
class Country(RDFModel):
    __subj__ = Subject(type=NS_MO.term("Country"), key_ns=NS_MR, key="uri")

    id: Annotated[InternalID, P()]
    name: Annotated[str, P(pred=NS_RDFS.term("label"))]
    aliases: Annotated[list[str], P()]

    @cached_property
    def uri(self) -> IRI:
        return Country.__subj__.key_ns.uristr(self.id)
