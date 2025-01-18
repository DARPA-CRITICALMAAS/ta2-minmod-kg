from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Optional

from minmodkg.misc.rdf_store.rdf_model import Property, RDFModel, Subject
from minmodkg.misc.utils import makedict
from minmodkg.models.kg.base import NS_MO
from minmodkg.models.kg.candidate_entity import CandidateEntity


@dataclass
class Measure(RDFModel):
    __subj__ = Subject(ns=NS_MO, name="Measure")

    value: Annotated[Optional[float], Property(ns=NS_MO, name="value")] = None
    unit: Annotated[
        Optional[CandidateEntity],
        Property(ns=NS_MO, name="unit", is_object_property=True),
    ] = None

    def to_dict(self):
        return makedict.without_none(
            (
                ("value", self.value),
                ("unit", self.unit.to_dict() if self.unit else None),
            )
        )

    @classmethod
    def from_dict(cls, d):
        return cls(
            value=d.get("value"),
            unit=CandidateEntity.from_dict(d.get("unit")) if d.get("unit") else None,
        )


if __name__ == "__main__":
    print(
        Measure(
            5,
            CandidateEntity(
                source="usc",
                confidence=1,
                observed_name="Hello",
                normalized_uri="https://minmod.isi.edu/entity/10",
            ),
        ).to_triples()
    )
