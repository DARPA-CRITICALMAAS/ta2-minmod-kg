from __future__ import annotations

import pytest
from minmodkg.api.models.mineral_site import LocationInfo, MineralSiteCreate
from minmodkg.transformations import make_site_uri


class TestMineralSite:

    @pytest.fixture(autouse=True)
    def site1_(self):
        self.site1 = MineralSiteCreate(
            source_id="database::https://mrdata.usgs.gov/mrds",
            record_id="10014570",
            name="Eagle Mine",
            location_info=LocationInfo(
                location="POINT (-87.1 46.9)",
            ),
        )

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
            "uri": make_site_uri(self.site1.source_id, self.site1.record_id),
        }

        resp = (
            auth_client.get(
                "/resource/"
                + make_site_uri(
                    self.site1.source_id, self.site1.record_id, namespace=""
                ),
                params={"format": "json"},
            )
            .raise_for_status()
            .json()
        )

        assert resp == {
            "@id": make_site_uri(self.site1.source_id, self.site1.record_id),
            "@type": {"@id": "https://minmod.isi.edu/ontology/MineralSite"},
            "@label": "Eagle Mine",
            "created_by": "https://minmod.isi.edu/resource/users/admin",
            "location_info": {
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
