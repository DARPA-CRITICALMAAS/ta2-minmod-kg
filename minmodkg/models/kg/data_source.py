from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Literal, Optional

from minmodkg.libraries.rdf.rdf_model import P, RDFModel, Subject
from minmodkg.models.kg.base import NS_MO, NS_MR_NO_REL, NS_RDFS
from minmodkg.typing import IRI, NotEmptyStr

SourceType = Literal["database", "article", "mining-report", "unpublished"]


@dataclass
class DataSource(RDFModel):
    __subj__ = Subject(type=NS_MO.term("DataSource"), key_ns=NS_MR_NO_REL, key="uri")

    uri: IRI
    name: Annotated[NotEmptyStr, P(pred=NS_RDFS.term("label"))]
    type: Annotated[SourceType, P()]
    created_by: Annotated[IRI, P()]
    description: Annotated[NotEmptyStr, P()]
    score: Annotated[Optional[float], P()]
    connection: Annotated[Optional[NotEmptyStr], P()] = None
