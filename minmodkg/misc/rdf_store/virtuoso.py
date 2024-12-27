from __future__ import annotations

from typing import Optional

import httpx
from minmodkg.misc.exceptions import DBError
from minmodkg.misc.rdf_store.namespace import Namespace
from minmodkg.misc.rdf_store.triple_store import TripleStore
from minmodkg.typing import SPARQLMainQuery, Triples
from rdflib import Graph


class VirtuosoDB(TripleStore):
    def __init__(self, namespace: Namespace, query_endpoint: str, update_endpoint: str):
        super().__init__(namespace)
        self.query_endpoint = query_endpoint
        self.update_endpoint = update_endpoint

    def _sparql_query(self, query: SPARQLMainQuery):
        if query.lower().find("construct") != -1:
            return self._sparql(
                query,
                self.query_endpoint,
                "application/sparql-query",
                accept_type="text/turtle",
                # params={
                #     "default-graph-uri": "https://purl.org/drepr/1.0/",
                #     "signal_void": 1,  # important to fix an unreported bug in Virtuoso
                # },
            )
        return self._sparql(query, self.query_endpoint, "application/sparql-query")

    def _sparql_update(self, query: SPARQLMainQuery):
        return self._sparql(query, self.update_endpoint, "application/sparql-update")

    def _sparql(
        self,
        query: SPARQLMainQuery,
        endpoint: str,
        content_type: str,
        *,
        accept_type: str = "application/sparql-results+json",
        params: Optional[dict] = None,
    ):
        """Execute a SPARQL query and ensure the response is successful"""
        response = httpx.post(
            url=endpoint,
            params=params or {"default-graph-uri": "https://purl.org/drepr/1.0/"},
            headers={"Content-Type": content_type, "Accept": accept_type},
            content=self.prefix_part + query,
            timeout=None,
        )
        if response.status_code != 200:
            raise DBError(
                f"Failed to execute SPARQL query. Status code: {response.status_code}. Response: {response.text}",
                response,
            )
        return response
