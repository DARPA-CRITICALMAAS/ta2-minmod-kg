import re
from collections import defaultdict
from dataclasses import asdict, dataclass, make_dataclass
from datetime import datetime
from graphlib import TopologicalSorter
from itertools import chain
from pathlib import Path
from typing import List, Optional, TypeAlias, Union

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


def get_domain_or_range(g: Graph, prop: URIRef, attr: URIRef):
    output = set()
    for obj in g.objects(prop, attr, unique=True):
        if isinstance(obj, URIRef):
            output.add(obj)
        else:
            assert (obj, OWL.unionOf, None) in g
            for sobj in read_a_list(g, next(g.objects(obj, OWL.unionOf))):
                output.add(sobj)
    return sorted(output)


def make_er_diagram():
    g = load_ontology()

    # get list of properties
    props = {}
    domain2props = {}
    for prop in chain(
        g.subjects(RDF.type, OWL.ObjectProperty),
        g.subjects(RDF.type, OWL.DatatypeProperty),
        g.subjects(RDF.type, RDF.Property),
    ):
        assert isinstance(prop, URIRef)
        domains = get_domain_or_range(g, prop, RDFS.domain)
        ranges = get_domain_or_range(g, prop, RDFS.range)

        props[prop] = {
            "domain": domains,
            "range": ranges,
        }
        for domain in domains:
            domain2props.setdefault(domain, []).append(prop)
    domain2props = {k: sorted(v) for k, v in domain2props.items()}

    # get class orders
    edges: dict[URIRef, list[URIRef]] = {
        subj: [] for subj in g.subjects(RDF.type, OWL.Class) if isinstance(subj, URIRef)
    }
    for subj in edges.keys():
        for prop in domain2props.get(subj, []):
            edges[subj].extend((t for t in props[prop]["range"] if t in edges))
    ts = TopologicalSorter(edges)

    namespaces = list(g.namespaces())

    # get data types
    datatypes = {}
    for subj in g.subjects(RDF.type, RDFS.Datatype):
        ((prefix, ns),) = [
            (prefix, ns) for prefix, ns in namespaces if str(subj).startswith(ns)
        ]
        typename = prefix + ":" + str(subj)[len(ns) :]
        datatypes[subj] = type(typename, (object,), {})

    # iterate over classes and create models
    models = {}
    for subj in ts.static_order():
        # gather all restrictions -- not parse yet
        prop2restriction = defaultdict(list)
        for obj in g.objects(subj, RDFS.subClassOf):
            if (obj, RDF.type, OWL.Restriction) not in g:
                continue

            (prop,) = list(g.objects(obj, OWL.onProperty))
            prop2restriction[prop].append(obj)

        fields = []

        # things with label
        if (subj, RDFS.subClassOf, mno.ThingHasLabel) in g:
            fields.append(("name", str))

        clsname = subj[len(mno) :]
        for prop in domain2props.get(subj, []):
            assert isinstance(prop, URIRef)
            if (prop, hasSubPropertyOf, None) in g:
                continue
            propname = prop[len(mno) :]
            if sum(1 for _ in g.objects(prop, RDFS.range, unique=True)) > 1:
                raise Exception("Cannot represent intersection types in pydantic")
            fieldtype = get_field_type(
                g, models, datatypes, props[prop]["range"], prop2restriction[prop]
            )
            fields.append((propname, fieldtype))

        if len(fields) == 0:
            continue

        models[clsname] = create_model(
            clsname,
            **{name: (type, Field()) for name, type in fields},  # type: ignore
            __config__=ConfigDict(arbitrary_types_allowed=True)
        )  # type: ignore

    graph = erd.create(*list(models.values()))
    graph.draw(out=Path(__file__).parent / "er_diagram.png")
    return


def get_field_type(
    g: Graph,
    models: dict[str, type],
    datatypes: dict[str, type],
    types: list[URIRef],
    restrictions: list[URIRef | BNode],
):
    if len(restrictions) > 0:
        for restriction in restrictions:
            if (restriction, OWL.allValuesFrom, None) in g:
                (val,) = list(g.objects(restriction, OWL.allValuesFrom))
                if isinstance(val, URIRef):
                    vals = [val]
                else:
                    vals = read_a_list(g, val)
                types = [type for type in types if type in vals]
                assert len(types) > 0

    norm_types = []
    for obj in types:
        if obj == XSD.string:
            norm_types.append(str)
        elif obj == XSD.integer:
            norm_types.append(int)
        elif obj == XSD.decimal:
            norm_types.append(float)
        elif obj == XSD.anyURI:
            norm_types.append(URI)
        elif obj == XSD.dateTime:
            norm_types.append(datetime)
        elif isinstance(obj, URIRef) and obj in mno:
            norm_types.append(models[str(obj)[len(mno) :]])
        elif obj in datatypes:
            norm_types.append(datatypes[obj])
        else:
            raise NotImplementedError(obj)

    if len(norm_types) == 1:
        norm_type = norm_types[0]
    else:
        assert len(norm_types) > 1
        norm_type = Union[tuple(norm_types)]  # type: ignore

    if restrictions is not None:
        update = False
        for restriction in restrictions:
            for cons, cons_val in g.predicate_objects(restriction):
                if cons in (RDF.type, OWL.onProperty, OWL.allValuesFrom, OWL.onClass):
                    continue
                if cons in (OWL.cardinality, OWL.qualifiedCardinality):
                    # do nothing
                    assert isinstance(cons_val, Literal)
                    assert cons_val.value == 1
                    update = True
                elif cons == OWL.maxCardinality:
                    assert isinstance(cons_val, Literal)
                    assert cons_val.value == 1
                    norm_type = Optional[norm_type]  # type: ignore
                    update = True
                else:
                    raise NotImplementedError(cons)
        if not update:
            norm_type = list[norm_type]
    else:
        # by default, it's a list of values
        norm_type = list[norm_type]
    return norm_type


if __name__ == "__main__":
    make_er_diagram()
