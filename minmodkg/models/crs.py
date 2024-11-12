from __future__ import annotations

from minmodkg.typing import IRI
from pydantic import BaseModel


class CRS(BaseModel):
    uri: IRI
    name: str
