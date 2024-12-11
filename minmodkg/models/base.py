from __future__ import annotations

from minmodkg.config import MINMOD_NS_CFG, SPARQL_QUERY_ENDPOINT, SPARQL_UPDATE_ENDPOINT
from minmodkg.misc.rdf_store import (
    BaseRDFModel,
    BaseRDFQueryBuilder,
    Namespace,
    RDFMetadata,
    RDFStore,
)

MINMOD_NS = Namespace(MINMOD_NS_CFG)
MINMOD_KG = RDFStore(MINMOD_NS, SPARQL_QUERY_ENDPOINT, SPARQL_UPDATE_ENDPOINT)


class MinModRDFModel(BaseRDFModel):
    rdfdata = RDFMetadata(MINMOD_KG.ns, MINMOD_KG)


class MinModRDFQueryBuilder(BaseRDFQueryBuilder):
    rdfdata = RDFMetadata(MINMOD_KG.ns, MINMOD_KG)
