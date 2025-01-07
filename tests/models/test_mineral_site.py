from __future__ import annotations

from pathlib import Path

import serde.json
from minmodkg.api.routers.mineral_site import (
    crs_uri_to_name,
    material_form_uri_to_conversion,
)
from minmodkg.models_v2.kgrel.mineral_site import MineralSite
from sqlalchemy import select
from sqlalchemy.orm import Session


class TestMineralSite:

    def test_create_mineral_site(self, resource_dir: Path, kg, kgrel):
        crss = crs_uri_to_name(None)
        material_form = material_form_uri_to_conversion(None)

        id2site = {}
        for file in (resource_dir / "kgdata/mineral-sites/json").iterdir():
            for raw_site in serde.json.deser(file):
                site = MineralSite.from_raw_site(raw_site, material_form, crss)
                id2site[site.site_id] = site
                with Session(kgrel, expire_on_commit=False) as session:
                    session.add(site)
                    session.commit()

        with Session(kgrel) as session:
            results = session.execute(select(MineralSite)).all()
            assert {id: site.name for id, site in id2site.items()} == {
                site.site_id: site.name for site, in results
            }
