from __future__ import annotations

from dataclasses import dataclass

from minmodkg.typing import InternalID


@dataclass
class DedupMineralSiteDepositType:
    id: InternalID
    source: str
    confidence: float

    def to_dict(self):
        return {"id": self.id, "source": self.source, "confidence": self.confidence}

    @classmethod
    def from_dict(cls, d):
        return cls(d["id"], d["source"], d["confidence"])
