from __future__ import annotations

from typing import Annotated, Literal, Optional

from fastapi import APIRouter, Body, HTTPException, Query, Response, status
from minmodkg.api.dependencies import (
    CurrentUserDep,
    MineralSiteServiceDep,
    RelSessionDep,
)
from minmodkg.api.models.public_mineral_site import (
    CandidateEntity,
    InputPublicMineralSite,
    MineralInventory,
    OutputPublicMineralSite,
)
from minmodkg.misc.utils import format_nanoseconds
from minmodkg.models.kg.base import NS_MR
from minmodkg.models.kgrel.mineral_site import MineralSite
from minmodkg.services.mineral_site import (
    ExpiredSnapshotIdError,
    UnsupportOperationError,
)
from minmodkg.transformations import make_site_id
from minmodkg.typing import InternalID
from pydantic import BaseModel
from sqlalchemy import select

router = APIRouter(tags=["cdr"])


@router.get("/cdr/mining-report")
def get_mining_reports(session: RelSessionDep):
    source_id = "https://api.cdr.land/v1/docs/documents"

    output = []
    for row in session.execute(
        select(
            MineralSite.site_id,
            MineralSite.name,
            MineralSite.record_id,
            MineralSite.inventories,
        ).where(MineralSite.source_id == source_id)
    ).all():
        invs: list[MineralInventory] = row[3]
        output.append(
            {
                "site_id": row[0],
                "source_id": source_id,
                "record_id": row[2],
                "name": row[1],
                "inventories": [
                    {
                        "commodity": inv.commodity.to_dict(),
                        "reference": inv.reference.to_dict(),
                        "date": inv.date,
                    }
                    for inv in invs
                ],
            }
        )

    return output
