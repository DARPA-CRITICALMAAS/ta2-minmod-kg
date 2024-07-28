from __future__ import annotations

from minmodkg.schema.make_diagram import (
    get_class_in_order,
    get_data_types,
    get_domain_to_props,
    hasSubPropertyOf,
    load_ontology,
    mno,
    schema_dir,
)
from rdflib import RDF, RDFS, SKOS, XSD, BNode, Graph, Literal, Namespace, URIRef

sh = Namespace("http://www.w3.org/ns/shacl#")


def make_shapes(skip_empty_field: bool = True, skip_parent_class: bool = True):
    """We do not"""
    g = load_ontology()
    sg = Graph()
    sg.bind("sh", sh)
    for prefix, ns in g.namespaces():
        sg.bind(prefix, ns)

    props, domain2props = get_domain_to_props(g)

    for subj in get_class_in_order(g, props, domain2props):
        clsname = subj[len(mno) :]

        prop_targets = []
        for prop in domain2props.get(subj, []):
            assert isinstance(prop, URIRef)
            if (prop, hasSubPropertyOf, None) in g:
                continue

            prop_targets.append(
                (
                    prop,
                    [
                        t
                        for t in props[prop]["range"]
                        if isinstance(t, URIRef) and t in mno
                    ],
                )
            )

        if len(prop_targets) == 0:
            # has no target links, we skip it as drepr validate for literal values
            continue

        subj_shp = mno[clsname + "Shape"]
        sg.add((subj_shp, RDF.type, sh.NodeShape))
        sg.add((subj_shp, sh.targetClass, subj))

        for prop, targets in prop_targets:
            constraint = BNode()
            sg.add((subj, prop, constraint))
            sg.add((constraint, sh.path, prop))
            for target in targets:
                sg.add((constraint, sh["class"], target))
            sg.add((constraint, sh.severity, sh.Violation))

    sg.serialize(schema_dir / "shapes.ttl", format="ttl")


if __name__ == "__main__":
    make_shapes()
