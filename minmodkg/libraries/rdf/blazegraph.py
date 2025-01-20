from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from time import time
from typing import Literal, Optional, Sequence
from uuid import uuid4

import httpx
from minmodkg.libraries.rdf.namespace import Namespace
from minmodkg.libraries.rdf.triple_store import TripleStore
from minmodkg.misc.exceptions import DBError, TransactionError
from minmodkg.misc.utils import group_by_key
from minmodkg.typing import IRI, SPARQLMainQuery, Triples
from rdflib import Graph, URIRef


class BlazeGraph(TripleStore):
    def __init__(self, namespace: Namespace, query_endpoint: str, update_endpoint: str):
        super().__init__(namespace)
        self.query_endpoint = query_endpoint
        self.update_endpoint = update_endpoint

    def _sparql_query(self, query: SPARQLMainQuery):
        if query.lower().lstrip().startswith("construct"):
            format = "text/turtle"
        else:
            format = "application/sparql-results+json"

        response = httpx.post(
            url=self.query_endpoint,
            data={"query": self.prefix_part + query},
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": format,
            },
            timeout=None,
        )
        if response.status_code != 200:
            raise DBError(
                f"Failed to execute SPARQL query. Status code: {response.status_code}. Response: {response.text}",
                response,
            )
        return response

    def _sparql_update(self, query: SPARQLMainQuery):
        response = httpx.post(
            url=self.update_endpoint,
            data={"update": self.prefix_part + query},
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/sparql-results+json",  # Requesting JSON format
            },
            timeout=None,
        )
        if response.status_code != 200:
            raise DBError(
                f"Failed to execute SPARQL query. Status code: {response.status_code}. Response: {response.text}",
                response,
            )
        return response
