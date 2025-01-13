from typing import Annotated, Any, Optional, TypeVar

from minmodkg.misc.rdf_store.namespace import Namespace, SingleNS
from minmodkg.misc.rdf_store.rdf_model import (
    BaseRDFModel,
    BaseRDFQueryBuilder,
    RDFMetadata,
)
from minmodkg.misc.rdf_store.triple_store import Transaction, TripleStore
from rdflib import Graph, Literal, URIRef
from rdflib.term import Node


def norm_literal(value: Annotated[Any, Literal]) -> Any:
    return (
        None
        if value is None
        else (value.value if value.value is not None else str(value.value))
    )


def norm_uriref(value: Annotated[Any, URIRef]) -> Optional[URIRef]:
    return None if value is None else value


M = TypeVar("M", bound=BaseRDFModel)


def norm_object(clz: type[M], id: Optional[Node], g: Graph) -> Optional[M]:
    return None if id is None else clz.from_graph(id, g)
