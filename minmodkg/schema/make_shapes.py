from __future__ import annotations

from minmodkg.schema.make_diagram import (
    get_class_in_order,
    get_data_types,
    get_domain_to_props,
    load_ontology,
    mno,
    schema_dir,
)
from rdflib import RDF, RDFS, XSD, BNode, Graph, Literal, Namespace, URIRef

sh = Namespace("http://www.w3.org/ns/shacl#")


def make_shapes(skip_empty_field: bool = True, skip_parent_class: bool = True):
    g = load_ontology()
    sg = Graph()
    sg.bind("sh", sh)
    for prefix, ns in g.namespaces():
        sg.bind(prefix, ns)

    props, domain2props = get_domain_to_props(g)

    for subj in get_class_in_order(g, props, domain2props):
        clsname = subj[len(mno) :]

        subj_shp = mno[clsname + "Shape"]
        sg.add((subj_shp, RDF.type, sh.NodeShape))
        sg.add((subj_shp, sh.targetClass, subj))

        if (subj, RDFS.subClassOf, mno.ThingHasLabel) in g:
            constraint = BNode()
            sg.add((subj_shp, sh.property, constraint))
            sg.add((constraint, sh.path, RDFS.label))
            sg.add((constraint, sh.datatype, XSD.string))
            sg.add((constraint, sh.severity, sh.Violation))
            sg.add((constraint, sh.maxCount, Literal(1)))

    sg.serialize(schema_dir / "shapes.ttl", format="ttl")


if __name__ == "__main__":
    make_shapes()
