from __future__ import annotations

from dataclasses import dataclass

from minmodkg.typing import IRI


@dataclass
class CRS:
    uri: IRI
    name: str
