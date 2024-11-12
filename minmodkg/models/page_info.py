from __future__ import annotations

from typing import Optional

from minmodkg.config import NS_MNO
from minmodkg.misc.sparql import (
    rdflib_optional_literal_to_python,
    rdflib_optional_object_to_python,
)
from pydantic import BaseModel
from rdflib import Graph
from rdflib.term import Node


class BoundingBox(BaseModel):
    x_max: float
    x_min: float
    y_max: float
    y_min: float

    def to_enc_str(self):
        return f"BB:{self.x_max:.3f}_{self.x_min:.3f}_{self.y_max:.3f}_{self.y_min:.3f}"

    @staticmethod
    def from_graph(id: Node, g: Graph):
        return BoundingBox(
            x_max=rdflib_optional_literal_to_python(next(g.objects(id, NS_MNO.x_max))),
            x_min=rdflib_optional_literal_to_python(next(g.objects(id, NS_MNO.x_min))),
            y_max=rdflib_optional_literal_to_python(next(g.objects(id, NS_MNO.y_max))),
            y_min=rdflib_optional_literal_to_python(next(g.objects(id, NS_MNO.y_min))),
        )


class PageInfo(BaseModel):
    bounding_box: Optional[BoundingBox] = None
    page: int

    def to_enc_str(self):
        if self.bounding_box is None:
            return str(self.page)
        return f"PI:{self.page}|{self.bounding_box.to_enc_str()}"

    @staticmethod
    def from_graph(id: Node, g: Graph):
        return PageInfo(
            bounding_box=rdflib_optional_object_to_python(
                BoundingBox, next(g.objects(id, NS_MNO.bounding_box), None), g
            ),
            page=rdflib_optional_literal_to_python(next(g.objects(id, NS_MNO.page))),
        )
