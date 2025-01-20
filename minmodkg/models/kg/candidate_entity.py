from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Optional

from minmodkg.libraries.rdf.rdf_model import P, RDFModel, Subject
from minmodkg.misc.utils import makedict
from minmodkg.models.kg.base import NS_MO, NS_MR
from minmodkg.typing import IRI


@dataclass
class CandidateEntity(RDFModel):
    __subj__ = Subject(type=NS_MO.term("CandidateEntity"), key_ns=NS_MR)

    source: Annotated[str, P()]
    confidence: Annotated[float, P()]
    observed_name: Annotated[Optional[str], P()] = None
    normalized_uri: Annotated[Optional[IRI], P(is_ref_object=True)] = None

    def to_dict(self):
        return makedict.without_none(
            (
                ("source", self.source),
                ("confidence", self.confidence),
                ("observed_name", self.observed_name),
                ("normalized_uri", self.normalized_uri),
            )
        )

    @classmethod
    def from_dict(cls, d):
        return cls(
            source=d["source"],
            confidence=d["confidence"],
            observed_name=d.get("observed_name"),
            normalized_uri=d.get("normalized_uri"),
        )

    def to_tuple(self):
        return (self.source, self.confidence, self.observed_name, self.normalized_uri)
