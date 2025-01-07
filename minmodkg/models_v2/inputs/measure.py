from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from minmodkg.misc.utils import makedict
from minmodkg.models_v2.inputs.candidate_entity import CandidateEntity


@dataclass
class Measure:
    value: Optional[float] = None
    unit: Optional[CandidateEntity] = None

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
