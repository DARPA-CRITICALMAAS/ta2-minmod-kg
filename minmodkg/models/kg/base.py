from __future__ import annotations

from minmodkg.config import MINMOD_KG_CLSARGS, MINMOD_KG_CLSPATH, MINMOD_NS_CFG
from minmodkg.libraries.rdf.namespace import Namespace, NoRelSingleNS, SingleNS
from minmodkg.libraries.rdf.rdf_model import RDFModel
from minmodkg.libraries.rdf.triple_store import TripleStore
from rdflib import RDF

from statickg.helper import import_attr

NS_MR = SingleNS("mr", MINMOD_NS_CFG["mr"])
NS_MO = SingleNS("mo", MINMOD_NS_CFG["mo"])
NS_RDF = SingleNS("rdf", str(RDF))

NS_MR_NO_REL = NoRelSingleNS("mr", MINMOD_NS_CFG["mr"])
MINMOD_NS = Namespace(MINMOD_NS_CFG)
MINMOD_KG: TripleStore = import_attr(MINMOD_KG_CLSPATH)(MINMOD_NS, **MINMOD_KG_CLSARGS)

RDFModel.namespace = MINMOD_NS
