from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from minmodkg.libraries.rdf.rdf_model import P, RDFModel, Subject
from minmodkg.models.kg.base import NS_MO, NS_MR, NS_RDFS
from minmodkg.typing import IRI


@dataclass
class CRS(RDFModel):
    __subj__ = Subject(
        type=NS_MO.term("CoordinateReferenceSystem"), key_ns=NS_MR, key="uri"
    )

    uri: Annotated[IRI, P()]
    name: Annotated[str, P(pred=NS_RDFS.term("label"))]
