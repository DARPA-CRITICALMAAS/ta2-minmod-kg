from __future__ import annotations

from dataclasses import dataclass

from minmodkg.misc.utils import makedict


@dataclass
class CandidateEntity:
    source: str
    confidence: float
    observed_name: str | None = None
    normalized_uri: str | None = None

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
