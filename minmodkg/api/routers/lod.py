from __future__ import annotations

from typing import Annotated, Literal

import htbuilder as H
import rdflib
import rdflib.term
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse
from minmodkg.api.dependencies import SPARQL_ENDPOINT
from minmodkg.config import MNO_NS, MNR_NS
from minmodkg.misc import sparql_construct
from minmodkg.misc.sparql import has_uri
from rdflib import RDF, RDFS, BNode, Graph
from rdflib import Literal as RDFLiteral
from rdflib import URIRef

router = APIRouter(tags=["lod"])


@router.get("/resource/{resource_id}")
def get_resource(
    resource_id: str, format: Annotated[Literal["html", "json"], Query()] = "html"
):
    uri = URIRef(MNR_NS + resource_id)
    if not has_uri(uri):
        raise HTTPException(status_code=404, detail="Resource not found")

    if format == "html":
        return render_entity_html(uri, SPARQL_ENDPOINT)
    if format == "json":
        return render_entity_json(uri, SPARQL_ENDPOINT)
    raise HTTPException(status_code=400, detail="Invalid format")


@router.get("/ontology/{resource_id}")
def get_ontology(
    resource_id: str, format: Annotated[Literal["html", "json"], Query()] = "html"
):  # -> HTMLResponse | dict[Any, Any]:
    uri = URIRef(MNO_NS + resource_id)
    if not has_uri(uri):
        raise HTTPException(status_code=404, detail="Resource not found")

    if format == "html":
        return render_entity_html(uri, SPARQL_ENDPOINT)
    if format == "json":
        return render_entity_json(uri, SPARQL_ENDPOINT)
    raise HTTPException(status_code=400, detail="Invalid format")


def get_entity_data(subj: URIRef, endpoint: str) -> Graph:
    # https://stackoverflow.com/questions/37186530/how-do-i-construct-get-the-whole-sub-graph-from-a-given-resource-in-rdf-graph/37213209#37213209
    # adapt from this answer to fit our need
    # we do not blindly follow all paths (<>|!<>)* and filter out the URI nodes because it follows
    # the sameAs path, which leads to explosive results
    return sparql_construct(
        """
    CONSTRUCT { 
        ?a ?b ?c . 
        ?s ?p ?o . 
        ?c rdfs:label ?cname .
        ?o rdfs:label ?oname .
        ?p rdfs:label ?pname .
        ?b rdfs:label ?bname .
    }
    WHERE {
        ?a ?b ?c .
        OPTIONAL { ?c rdfs:label ?cname . }
        OPTIONAL { ?b rdfs:label ?bname . }
        OPTIONAL { 
            ?a (!(owl:sameAs|rdf:type))+ ?s . 
            ?s ?p ?o .
            OPTIONAL { ?o rdfs:label ?oname . }
            OPTIONAL { ?p rdfs:label ?pname .}
        }
        VALUES ?a { <%s> } 
    }
"""
        % subj,
        endpoint=endpoint,
    )


def render_entity_json(subj: URIRef, endpoint: str):
    g = get_entity_data(subj, endpoint)

    def label(obj: rdflib.term.Node):
        if obj == RDFS.label:
            return "@label"
        if obj == RDF.type:
            return "@type"
        assert isinstance(obj, (URIRef, RDFLiteral))
        return obj.n3(g.namespace_manager).rsplit(":", 1)[-1]

    def make_tree(obj: rdflib.term.Node, visited: set):
        if isinstance(obj, RDFLiteral):
            return obj
        if isinstance(obj, URIRef):
            out: dict = {"@id": obj}
        else:
            assert isinstance(obj, BNode)
            out: dict = {}

        if obj in visited:
            # skip visited nodes
            if (obj, RDFS.label, None) in g:
                out["@label"] = next(g.objects(obj, RDFS.label))
            return out

        visited.add(obj)
        for p in g.predicates(obj, unique=True):
            plabel = label(p)
            subobjs = list(g.objects(obj, p))
            fmtsubobjs = [make_tree(subobj, visited) for subobj in subobjs]
            if len(fmtsubobjs) == 1:
                out[plabel] = fmtsubobjs[0]
            else:
                out[plabel] = fmtsubobjs

        return out

    out: dict = {"@id": subj}
    visited = {subj}
    for p in g.predicates(subj, unique=True):
        plabel = label(p)
        objs = list(g.objects(subj, p))
        fmtobjs = [make_tree(obj, visited) for obj in objs]
        if len(fmtobjs) == 1:
            out[plabel] = fmtobjs[0]
        else:
            out[plabel] = fmtobjs
    return out


def render_entity_html(subj: URIRef, endpoint: str):
    g = get_entity_data(subj, endpoint)

    def label(g, subj: rdflib.term.Node):
        if (subj, RDFS.label, None) in g:
            return next(g.objects(subj, RDFS.label))
        assert isinstance(subj, (URIRef, RDFLiteral))
        return subj.n3(g.namespace_manager)

    def make_tree(g, subj: rdflib.term.Node, visited: set):
        if isinstance(subj, RDFLiteral):
            return H.p(subj)
        if isinstance(subj, URIRef):
            subj_name = subj.n3(g.namespace_manager)
            if (subj, RDFS.label, None) in g:
                subj_name = next(g.objects(subj, RDFS.label))

            return H.a(href=subj)(subj_name)

        if subj in visited:
            return H.p(style="font-style: italic")("skiped as visited before")

        visited.add(subj)
        assert isinstance(subj, BNode)
        children = []
        for p, o in g.predicate_objects(subj):
            if p != RDFS.label:
                children.append((H.a(href=p)(label(g, p)), make_tree(g, o, visited)))

        return (
            H.table(_class="table")(
                *[
                    H.tr(
                        H.td(p),
                        H.td(o),
                    )
                    for p, o in children
                ]
            ),
        )

    subj_label = label(g, subj)

    children = []
    for p, o in g.predicate_objects(subj):
        if p != RDFS.label:
            children.append((H.a(href=p)(label(g, p)), make_tree(g, o, set())))

    tree = H.div(_class="container-fluid")(
        H.div(_class="row", style="margin-top: 20px; margin-bottom: 20px")(
            H.div(_class="col")(
                H.h4(
                    H.a(href=subj)(subj_label),
                ),
                H.small(_class="text-muted fw-semibold")(subj),
            )
        ),
        H.table(_class="table table-striped")(
            *[
                H.tr(
                    H.td(p),
                    H.td(o),
                )
                for p, o in children
            ]
        ),
    )
    return HTMLResponse(
        content=f"""
<html>
    <head>
        <title>{subj_label}</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH" crossorigin="anonymous">
        <style>a {{ text-decoration: none; }}</style>
    </head>
    <body>
        {tree}
        <script src="https://cdn.jsdelivr.net/npm/@popperjs/core@2.11.8/dist/umd/popper.min.js" integrity="sha384-I7E8VVD/ismYTF4hNIPjVp/Zjvgyol6VFvRkX/vR+Vc4jQkC+hVqc2pM8ODewa9r" crossorigin="anonymous"></script>
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.min.js" integrity="sha384-0pUGZvbkm6XF6gxjEnlmuGrJXVbNuzT9qBBavbLwCsOGabYfZo0T0to5eqruptLy" crossorigin="anonymous"></script>
    </body>
</html>
                        """,
        status_code=200,
    )
