from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from minmodkg.misc.rdf_store.rdf_model import Property, RDFModel, Subject
from minmodkg.models.kg.base import NS_MO
from minmodkg.typing import IRI


@dataclass
class MaterialForm(RDFModel):
    __subj__ = Subject(cls_ns=NS_MO, name="MaterialForm", key="uri")

    uri: IRI
    name: Annotated[str, Property(ns=NS_MO, name="name")]
    formula: Annotated[str, Property(ns=NS_MO, name="formula")]
    commodity: Annotated[IRI, Property(ns=NS_MO, name="commodity")]
    conversion: Annotated[float, Property(ns=NS_MO, name="conversion")]
