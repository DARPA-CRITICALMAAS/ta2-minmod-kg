from __future__ import annotations

from time import sleep

import pytest
from minmodkg.api.routers.mineral_site import get_site_as_graph
from minmodkg.config import MNO_NS, MNR_NS, NS_MNO
from minmodkg.models.mineral_site import LocationInfo, MineralSite
from minmodkg.transformations import make_site_uri
from rdflib import RDF, RDFS
from rdflib import Literal as RDFLiteral
from rdflib import Namespace, URIRef


class TestMineralSite:

    @pytest.fixture(autouse=True)
    def site1_(self):
        self.site1 = MineralSite(
            source_id="database::https://mrdata.usgs.gov/mrds",
            record_id="10014570",
            name="Eagle Mine",
            location_info=LocationInfo(
                location="POINT (-87.1 46.9)",
            ),
            created_by=["https://minmod.isi.edu/users/admin"],
        )
        self.site1_id = make_site_uri(
            self.site1.source_id, self.site1.record_id, namespace=""
        )
        self.site1_uri = make_site_uri(self.site1.source_id, self.site1.record_id)

    def test_create_first(self, auth_client, kg):
        # create a mineral site record
        resp = (
            auth_client.post(
                "/api/v1/mineral-sites",
                json=self.site1.model_dump(exclude_none=True),
            )
            .raise_for_status()
            .json()
        )
        assert resp == {
            "status": "success",
            "uri": self.site1_uri,
        }

        resp = (
            auth_client.get(
                f"/resource/{self.site1_id}",
                params={"format": "json"},
            )
            .raise_for_status()
            .json()
        )

        assert resp == {
            "@id": self.site1_uri,
            "@type": {"@id": "https://minmod.isi.edu/ontology/MineralSite"},
            "@label": "Eagle Mine",
            "created_by": "https://minmod.isi.edu/resource/users/admin",
            "location_info": {
                "@id": f"{self.site1_uri}__location_info",
                "@type": {"@id": "https://minmod.isi.edu/ontology/LocationInfo"},
                "location": "POINT (-87.1 46.9)",
            },
            "modified_at": resp["modified_at"],
            "record_id": "10014570",
            "source_id": "database::https://mrdata.usgs.gov/mrds",
        }

    def test_create_exist(self, auth_client, kg):
        resp = auth_client.post(
            "/api/v1/mineral-sites",
            json=self.site1.model_dump(exclude_none=True),
        )
        assert resp.json() == {"detail": "The site already exists."}
        assert resp.status_code == 403

    def test_get_site_data(self, auth_client, kg):
        uri = make_site_uri(self.site1.source_id, self.site1.record_id)
        g = get_site_as_graph(uri)
        triples = set(g)

        resp = (
            auth_client.get(
                f"/resource/{self.site1_id}",
                params={"format": "json"},
            )
            .raise_for_status()
            .json()
        )

        siteref = URIRef(uri)
        locref = URIRef(f"{uri}__location_info")

        gold_triples = {
            (siteref, RDF.type, NS_MNO.MineralSite),
            (siteref, RDFS.label, RDFLiteral(resp["@label"])),
            (siteref, NS_MNO.location_info, locref),
            (locref, RDF.type, NS_MNO.LocationInfo),
            (
                locref,
                NS_MNO.location,
                RDFLiteral(
                    resp["location_info"]["location"],
                    datatype=URIRef("http://www.opengis.net/ont/geosparql#wktLiteral"),
                ),
            ),
        }
        for key in ["created_by", "modified_at", "record_id", "source_id"]:
            gold_triples.add((siteref, NS_MNO[key], RDFLiteral(resp[key])))

        assert triples == gold_triples

    def test_update_site(self, auth_client, kg):
        sleep(1.0)  # to ensure the modified_at is different
        self.site1.name = "Frog Mine"
        resp = (
            auth_client.post(
                f"/api/v1/mineral-sites/{self.site1_id}",
                json=self.site1.model_dump(exclude_none=True),
            )
            .raise_for_status()
            .json()
        )
        assert resp == {
            "status": "success",
            "uri": self.site1_uri,
        }

        resp = (
            auth_client.get(
                f"/resource/{self.site1_id}",
                params={"format": "json"},
            )
            .raise_for_status()
            .json()
        )

        assert resp == {
            "@id": self.site1_uri,
            "@type": {"@id": "https://minmod.isi.edu/ontology/MineralSite"},
            "@label": "Frog Mine",
            "created_by": "https://minmod.isi.edu/resource/users/admin",
            "location_info": {
                "@id": f"{self.site1_uri}__location_info",
                "@type": {"@id": "https://minmod.isi.edu/ontology/LocationInfo"},
                "location": "POINT (-87.1 46.9)",
            },
            "modified_at": resp["modified_at"],
            "record_id": "10014570",
            "source_id": "database::https://mrdata.usgs.gov/mrds",
        }
