from __future__ import annotations

from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Generic,
    Optional,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)
from uuid import uuid4

from drepr.writers.turtle_writer import MyLiteral
from minmodkg.misc.rdf_store.namespace import Namespace, SingleNS
from minmodkg.misc.rdf_store.triple_store import TripleStore
from minmodkg.models.kg.base import NS_RDF
from minmodkg.typing import IRI
from rdflib import RDF, XSD, Graph
from rdflib import Literal as RDFLiteral
from rdflib import URIRef


@dataclass
class Subject:
    cls_ns: SingleNS
    key_ns: SingleNS
    name: str
    key: Optional[str] = None

    rel_uri: str = field(init=False)
    uriref: URIRef = field(init=False)

    def __post_init__(self):
        self.rel_uri = self.key_ns[self.name]
        self.uriref = self.key_ns.uri(self.name)


@dataclass
class Property:
    ns: SingleNS
    name: str
    is_object_property: bool = False
    is_list: bool = False
    datatype: Optional[URIRef] = None
    rel_uri: str = field(init=False)
    uriref: URIRef = field(init=False)

    def __post_init__(self):
        self.rel_uri = self.ns[self.name]
        self.uriref = self.ns.uri(self.name)


@dataclass
class P:
    ns: Optional[SingleNS] = None
    name: Optional[str] = None
    is_object_property: Optional[bool] = None
    is_list: Optional[bool] = None
    datatype: Optional[URIRef] = None


@dataclass
class RDFMetadata:
    ns: Namespace
    store: TripleStore


@dataclass
class ResourceSchema:
    subj: Subject
    dataprops: dict[str, Property] = field(init=False, default_factory=dict)
    objectprops: dict[str, Property] = field(init=False, default_factory=dict)

    def get_uri(self, resource: Any) -> str:
        if self.subj.key is not None:
            return getattr(resource, self.subj.key)
        if not hasattr(resource, "__uri__"):
            setattr(
                resource,
                "__uri__",
                self.subj.cls_ns[self.subj.name + "_" + str(uuid4()).replace("-", "_")],
            )
        return getattr(resource, "__uri__")

    def get_uriref(self, resource: Any) -> URIRef:
        if self.subj.key is not None:
            return URIRef(getattr(resource, self.subj.key))
        if not hasattr(resource, "__uri__"):
            setattr(
                resource,
                "__uri__",
                self.subj.cls_ns.uri(
                    self.subj.name + "_" + str(uuid4()).replace("-", "_")
                ),
            )
        return getattr(resource, "__uri__")

    def add_property(self, attrname: str, prop: Property):
        """
        Args:
            attrname: name of the property in the Python class.
        """
        if prop.is_object_property:
            self.objectprops[attrname] = prop
        else:
            self.dataprops[attrname] = prop


class RDFModel:

    registry: ClassVar[dict[type, ResourceSchema]]
    namespace: ClassVar[Namespace]

    if TYPE_CHECKING:
        __subj__: ClassVar[Subject]
        __schema__: ClassVar[ResourceSchema]
        # for getting back the object from the RDF graph
        __rdf_graph_deser__: ClassVar[int]
        # unique identifier of an instance of the model
        # if the model does not have an ID
        __uri__: ClassVar[IRI]

    def __init_subclass__(cls, *kw: Any) -> None:
        if not hasattr(cls, "__subj__"):
            raise KeyError("Subclass of RDFModel must defined __subj__")

        if not hasattr(RDFModel, "registry"):
            RDFModel.registry = {}

        schema = ResourceSchema(subj=cls.__subj__)

        field_types = get_type_hints(cls, include_extras=True)
        for field_name, field_type in field_types.items():
            args = get_args(field_type)
            origin = get_origin(field_type)

            if origin is not Annotated:
                continue

            for arg in args:
                if isinstance(arg, Property):
                    schema.add_property(field_name, arg)
                    break
                if isinstance(arg, P):
                    schema.add_property(
                        field_name,
                        Property(
                            ns=arg.ns or schema.subj.cls_ns,
                            name=arg.name or field_name,
                            is_object_property=arg.is_object_property or False,
                            is_list=arg.is_list or False,
                            datatype=arg.datatype,
                        ),
                    )
                    break

        RDFModel.registry[cls] = schema
        cls.__schema__ = schema
        super().__init_subclass__(*kw)

    def to_triples(self):
        schema = self.__schema__

        subj = schema.get_uri(self)
        triples = [(subj, NS_RDF.type, schema.subj.rel_uri)]

        for name, prop in schema.dataprops.items():
            value = getattr(self, name)
            if value is None:
                continue
            if prop.is_list:
                for x in value:
                    triples.append(
                        (
                            subj,
                            prop.rel_uri,
                            MyLiteral(x, datatype=prop.datatype).n3(
                                RDFModel.namespace.rdflib_namespace_manager
                            ),
                        )
                    )
            else:
                triples.append(
                    (
                        subj,
                        prop.rel_uri,
                        MyLiteral(value, datatype=prop.datatype).n3(
                            RDFModel.namespace.rdflib_namespace_manager
                        ),
                    )
                )
        for name, prop in schema.objectprops.items():
            value = getattr(self, name)
            if value is None:
                continue
            if prop.is_list:
                for x in value:
                    triples.append((subj, prop.rel_uri, x.__schema__.get_uri(x)))
                    triples.extend(x.to_triples())
            else:
                triples.append((subj, prop.rel_uri, value.__schema__.get_uri(value)))
                triples.extend(value.to_triples())
        return triples

    def to_graph(self, g: Optional[Graph] = None):
        if g is None:
            g = Graph()

        schema = self.__schema__
        subj = schema.get_uriref(self)
        g.add((subj, RDF.type, schema.subj.uriref))

        for name, prop in schema.dataprops.items():
            value = getattr(self, name)
            if value is None:
                continue
            if prop.is_list:
                for x in value:
                    g.add(
                        (
                            subj,
                            prop.uriref,
                            RDFLiteral(x, datatype=prop.datatype),
                        )
                    )
            else:
                g.add(
                    (
                        subj,
                        prop.uriref,
                        RDFLiteral(value, datatype=prop.datatype),
                    )
                )

        for name, prop in schema.objectprops.items():
            value = getattr(self, name)
            if value is None:
                continue
            if prop.is_list:
                for x in value:
                    g.add((subj, prop.uriref, x.__schema__.get_uriref(x)))
                    x.to_graph(g)
            else:
                g.add((subj, prop.uriref, value.__schema__.get_uriref(value)))
                value.to_graph(g)
        return g
