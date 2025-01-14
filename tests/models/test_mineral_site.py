from __future__ import annotations

from pathlib import Path

import serde.json
from minmodkg.api.routers.mineral_site import (
    crs_uri_to_name,
    material_form_uri_to_conversion,
    source_uri_to_score,
)
from minmodkg.models_v2.kgrel.mineral_site import MineralSite, MineralSiteAndInventory
from minmodkg.services.mineral_site import MineralSiteService
from sqlalchemy import select
from sqlalchemy.orm import Session


class TestMineralSite:

    def test_create_mineral_site(self, resource_dir: Path, user1, kg, kgrel):
        crss = crs_uri_to_name(None)
        material_form = material_form_uri_to_conversion(None)
        source_score = source_uri_to_score(None)

        mineral_site_service = MineralSiteService(kgrel)
        id2site = {}
        for file in (resource_dir / "kgdata/mineral-sites/json").iterdir():
            for raw_site in serde.json.deser(file):
                msi = MineralSiteAndInventory.from_raw_site(
                    raw_site,
                    material_form=material_form,
                    crs_names=crss,
                    source_score=source_score,
                )
                id2site[msi.ms.site_id] = msi.ms
                mineral_site_service.create(user1, msi)

        with Session(kgrel) as session:
            results = session.execute(select(MineralSite)).all()
            assert {id: site.name for id, site in id2site.items()} == {
                site.site_id: site.name for site, in results
            }
