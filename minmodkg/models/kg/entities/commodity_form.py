from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from minmodkg.libraries.rdf.rdf_model import P, RDFModel, Subject
from minmodkg.models.kg.base import NS_MO, NS_MR, NS_RDFS
from minmodkg.typing import IRI


@dataclass
class CommodityForm(RDFModel):
    __subj__ = Subject(type=NS_MO.term("CommodityForm"), key_ns=NS_MR, key="uri")

    uri: IRI
    name: Annotated[str, P(pred=NS_RDFS.term("label"))]
    formula: Annotated[str, P()]
    commodity: Annotated[IRI, P()]
    conversion: Annotated[float, P()]
