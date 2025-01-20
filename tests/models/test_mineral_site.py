from __future__ import annotations

from pathlib import Path

import serde.json
from minmodkg.models.kg.mineral_site import MineralSite as KGMineralSite
from minmodkg.models.kgrel.mineral_site import MineralSite
from sqlalchemy import select
from sqlalchemy.orm import Session


class TestMineralSite:

    def test_create_mineral_site(self, resource_dir: Path, user1, kg, kgrel_with_data):
        id2site = {}
        for file in (resource_dir / "kgdata/mineral-sites/json").iterdir():
            for raw_site in serde.json.deser(file):
                ms = KGMineralSite.from_dict(raw_site)
                id2site[ms.id] = ms

        with Session(kgrel_with_data) as session:
            results = session.execute(select(MineralSite)).all()
            assert {id: site.name for id, site in id2site.items()} == {
                site.site_id: site.name for site, in results
            }
