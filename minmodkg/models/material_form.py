from __future__ import annotations

from minmodkg.typing import IRI
from pydantic import BaseModel


class MaterialForm(BaseModel):
    uri: IRI
    name: str
    formula: str
    commodity: IRI
    conversion: float
