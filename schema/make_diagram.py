import re
from collections import defaultdict
from dataclasses import asdict, dataclass, make_dataclass
from datetime import datetime
from graphlib import TopologicalSorter
from itertools import chain
from pathlib import Path
from typing import TypeAlias, Union

import erdantic as erd
from pydantic import BaseModel, ConfigDict, Field, create_model
from rdflib import OWL, RDF, RDFS, XSD, BNode, Graph, Literal, Namespace, URIRef

mno = Namespace("https://minmod.isi.edu/ontology/")
geo = Namespace("http://www.opengis.net/ont/geosparql#")
hasSubPropertyOf = mno.hasSubPropertyOf


class URI(str): ...


def get_all_values(g: Graph, s: URIRef, p: URIRef, subItemOf: URIRef):
    output = list(g.objects(s, p, unique=True))
    for parent in g.objects(s, subItemOf):
        assert isinstance(parent, URIRef)
        output.extend(get_all_values(g, parent, p, subItemOf))
    return output


def load_ontology():
    ontology_file = Path(__file__).parent / "ontology.ttl"
    g = Graph()
    g.parse(ontology_file, format="turtle")

    # run basic inference to inherit domain and range
    all_props = list(g.subjects(RDF.type, OWL.ObjectProperty, unique=True)) + list(
        g.subjects(RDF.type, OWL.DatatypeProperty, unique=True)
    )
    for prop in all_props:
        assert isinstance(prop, URIRef)
        for attr in [RDFS.domain, RDFS.range]:
            existing_vals = set(g.objects(prop, attr, unique=True))
            for val in get_all_values(g, prop, attr, RDFS.subPropertyOf):
                if val not in existing_vals:
                    g.add((prop, attr, val))

    # add hasSubPropertyOf
    for s, p, o in list(g.triples((None, RDFS.subPropertyOf, None))):
        g.add((o, hasSubPropertyOf, s))
    return g


def read_a_list(g: Graph, s):
    out: list[URIRef] = []
    for pred, obj in g.predicate_objects(s):
        if pred == RDF.rest:
            out.extend(read_a_list(g, obj))
        else:
            assert isinstance(obj, URIRef)
            out.append(obj)
    return out


def make_er_diagram():
    g = load_ontology()

    # get class orders
    edges: dict[URIRef, list[URIRef]] = defaultdict(list)
    for subj in g.subjects(RDF.type, OWL.Class):
        if not isinstance(subj, URIRef):
            continue
        for prop in g.subjects(RDFS.domain, subj):
            for obj in g.objects(prop, RDFS.range, unique=True):
                if isinstance(obj, URIRef):
                    if obj in mno:
                        edges[subj].append(obj)
                else:
                    assert (obj, OWL.unionOf, None) in g
                    for sobj in read_a_list(g, next(g.objects(obj, OWL.unionOf))):
                        if sobj in mno:
                            edges[subj].append(sobj)
    ts = TopologicalSorter(edges)

    namespaces = list(g.namespaces())

    # get data types
    datatypes = {}
    for subj in g.subjects(RDF.type, RDFS.Datatype):
        ((prefix, ns),) = [
            (prefix, ns) for prefix, ns in namespaces if str(subj).startswith(ns)
        ]
        typename = prefix + ":" + str(subj)[len(ns) :]
        datatypes[subj] = make_dataclass(typename, [])

    # iterate over classes and create models
    models = {}
    for subj in ts.static_order():
        clsname = subj[len(mno) :]
        fields = []
        for prop in g.subjects(RDFS.domain, subj):
            assert isinstance(prop, URIRef)
            if (prop, hasSubPropertyOf, None) in g:
                continue
            propname = prop[len(mno) :]
            ranges = list(g.objects(prop, RDFS.range, unique=True))
            if len(ranges) > 1:
                raise Exception("Cannot represent intersection types in pydantic")
            fields.append((propname, get_field_type(g, models, datatypes, ranges[0])))

        models[clsname] = create_model(
            clsname,
            **{name: (type, Field()) for name, type in fields},  # type: ignore
            __config__=ConfigDict(arbitrary_types_allowed=True)
        )  # type: ignore

    graph = erd.create(*list(models.values()))
    graph.draw(out=Path(__file__).parent / "er_diagram.png")
    return


def get_field_type(g: Graph, models, datatypes, obj):
    if obj == XSD.string:
        return str
    if obj == XSD.integer:
        return int
    if obj == XSD.decimal:
        return float
    if obj == XSD.anyURI:
        return URI
    if obj == XSD.dateTime:
        return datetime
    if isinstance(obj, URIRef) and obj in mno:
        return models[str(obj)[len(mno) :]]
    if obj in datatypes:
        return datatypes[obj]
    if isinstance(obj, BNode):
        assert (obj, OWL.unionOf, None) in g
        types = []
        for sobj in read_a_list(g, next(g.objects(obj, OWL.unionOf))):
            types.append(get_field_type(g, models, datatypes, sobj))
        return Union[tuple(types)]  # type: ignore
    raise NotImplementedError(obj)


if __name__ == "__main__":
    make_er_diagram()
