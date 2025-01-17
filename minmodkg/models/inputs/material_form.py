from __future__ import annotations

from dataclasses import dataclass

from minmodkg.typing import IRI


@dataclass
class MaterialForm:
    uri: IRI
    name: str
    formula: str
    commodity: IRI
    conversion: float
