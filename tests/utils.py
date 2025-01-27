from __future__ import annotations

from pathlib import Path
from typing import Callable

import httpx
import serde.json
from loguru import logger
from minmodkg.models.kgrel.dedup_mineral_site import MineralSiteAndInventory
from minmodkg.models.kgrel.user import User
from minmodkg.services.kgrel_entity import EntityService
from minmodkg.services.mineral_site import MineralSiteService
from sqlalchemy import Engine


def check_req(cb: Callable[[], httpx.Response]):
    try:
        resp = cb()
        resp.raise_for_status()
        return resp
    except httpx.HTTPStatusError as err:
        logger.exception(err)
        raise


def load_mineral_sites(kgrel: Engine, user: User, files: list[Path]):
    ms_service = MineralSiteService(kgrel)
    entity_service = EntityService(kgrel)

    for file in files:
        lst_msi = [
            MineralSiteAndInventory.from_raw_site(
                raw_site,
                commodity_form_conversion=entity_service.get_commodity_form_conversion(),
                crs_names=entity_service.get_crs_name(),
                source_score=entity_service.get_data_source_score(),
            )
            for raw_site in serde.json.deser(file)
        ]

        for msi in lst_msi:
            ms_service.create(msi)
        ms_service.update_same_as(user.get_uri(), [[msi.ms.site_id for msi in lst_msi]])
