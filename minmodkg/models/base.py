from __future__ import annotations

from minmodkg.config import (
    MINMOD_KG_CLSPATH,
    MINMOD_NS_CFG,
    SPARQL_QUERY_ENDPOINT,
    SPARQL_UPDATE_ENDPOINT,
)
from minmodkg.misc.rdf_store import (
    BaseRDFModel,
    BaseRDFQueryBuilder,
    Namespace,
    RDFMetadata,
    Sparql11RDFStore,
)

from statickg.helper import import_attr

MINMOD_NS = Namespace(MINMOD_NS_CFG)
MINMOD_KG = import_attr(MINMOD_KG_CLSPATH)(
    MINMOD_NS, SPARQL_QUERY_ENDPOINT, SPARQL_UPDATE_ENDPOINT
)


class MinModRDFQueryBuilder(BaseRDFQueryBuilder):
    rdfdata = RDFMetadata(MINMOD_KG.ns, MINMOD_KG)


class MinModRDFModel(BaseRDFModel):
    rdfdata = RDFMetadata(MINMOD_KG.ns, MINMOD_KG)
