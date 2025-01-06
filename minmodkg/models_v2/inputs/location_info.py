from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from minmodkg.misc.utils import extend_unique, makedict
from minmodkg.models_v2.inputs.candidate_entity import CandidateEntity


@dataclass
class LocationInfo:
    country: list[CandidateEntity] = field(default_factory=list)
    state_or_province: list[CandidateEntity] = field(default_factory=list)
    crs: Optional[CandidateEntity] = None
    location: Optional[str] = None

    def to_dict(self):
        return makedict.without_none_or_empty_list(
            (
                ("country", [ent.to_dict() for ent in self.country]),
                (
                    "state_or_province",
                    [ent.to_dict() for ent in self.state_or_province],
                ),
                ("crs", self.crs.to_dict() if self.crs else None),
                ("location", self.location),
            )
        )

    @classmethod
    def from_dict(cls, d: dict):
        return cls(
            country=[CandidateEntity.from_dict(ent) for ent in d.get("country", [])],
            state_or_province=[
                CandidateEntity.from_dict(ent) for ent in d.get("state_or_province", [])
            ],
            crs=(
                CandidateEntity.from_dict(d["crs"])
                if d.get("crs") is not None
                else None
            ),
            location=d.get("location"),
        )

    def merge_mut(self, other: LocationInfo):
        """Merge another entity into this entity almost as how knowledge graph does. This behaves similar to convert these entities into triples and merge the triples
        except that field that is expected to be single value will not be overwritten and merged based on the semantic of that field
        """
        self.country = extend_unique(
            self.country, other.country, key_fn=CandidateEntity.to_tuple
        )
        self.state_or_province = extend_unique(
            self.state_or_province,
            other.state_or_province,
            key_fn=CandidateEntity.to_tuple,
        )
        if self.location is None:
            # crs is determined by the location, so if we keep the location, we should keep the old crs
            # this case is certainly not happened in our system
            self.location = other.location
            self.crs = other.crs
