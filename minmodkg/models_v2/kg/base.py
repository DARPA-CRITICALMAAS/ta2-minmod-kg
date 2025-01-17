from __future__ import annotations

from minmodkg.config import MINMOD_KG_CLSARGS, MINMOD_KG_CLSPATH, MINMOD_NS_CFG
from minmodkg.misc.rdf_store import (
    BaseRDFModel,
    BaseRDFQueryBuilder,
    Namespace,
    RDFMetadata,
    TripleStore,
)

from statickg.helper import import_attr

MINMOD_NS = Namespace(MINMOD_NS_CFG)
MINMOD_KG: TripleStore = import_attr(MINMOD_KG_CLSPATH)(MINMOD_NS, **MINMOD_KG_CLSARGS)


class MinModRDFQueryBuilder(BaseRDFQueryBuilder):
    rdfdata = RDFMetadata(MINMOD_KG.ns, MINMOD_KG)


class MinModRDFModel(BaseRDFModel):
    rdfdata = RDFMetadata(MINMOD_KG.ns, MINMOD_KG)
