from __future__ import annotations

from dataclasses import dataclass

from minmodkg.typing import InternalID


@dataclass
class SiteAndScore:
    site: InternalID
    score: float

    def to_dict(self):
        return {"site": self.site, "score": self.score}

    @classmethod
    def from_dict(cls, d):
        return cls(d["site"], d["score"])
