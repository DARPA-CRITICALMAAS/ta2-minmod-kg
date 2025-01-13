from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Optional

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyCookie
from minmodkg.config import JWT_ALGORITHM, SECRET_KEY
from minmodkg.models.base import MINMOD_KG, MINMOD_NS
from minmodkg.models_v2.kgrel.base import get_rel_session
from minmodkg.models_v2.kgrel.commodity import Commodity
from minmodkg.models_v2.kgrel.user import User
from minmodkg.services.mineral_site import MineralSiteService
from minmodkg.typing import InternalID
from sqlalchemy import select
from sqlalchemy.orm import Session

# for login/security
token_from_cookie = APIKeyCookie(name="session")
TokenDep = Annotated[str, Depends(token_from_cookie)]
RelSessionDep = Annotated[Session, Depends(get_rel_session.__wrapped__)]


async def get_current_user(session: RelSessionDep, token: TokenDep):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALGORITHM])
        username: str = payload.get("username")
        if username is None:
            raise credentials_exception
        expired_at = datetime.fromtimestamp(payload.get("exp"), timezone.utc)
        if expired_at < datetime.now(timezone.utc):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Credentials expired",
            )
    except jwt.InvalidTokenError:
        raise credentials_exception

    user = session.get(User, username)
    if user is None:
        raise credentials_exception
    return user


def get_mineral_site_service():
    return MineralSiteService()


CurrentUserDep = Annotated[User, Depends(get_current_user)]
MineralSiteServiceDep = Annotated[MineralSiteService, Depends(get_mineral_site_service)]


def get_snapshot_id():
    query = f"SELECT ?snapshot_id WHERE {{ {MINMOD_NS.mr.alias}:kg {MINMOD_NS.dcterms.alias}:hasVersion ?snapshot_id }}"
    qres = MINMOD_KG.query(query)
    return qres[0]["snapshot_id"]


def norm_commodity(commodity: str) -> InternalID:
    if commodity.startswith("http"):
        raise HTTPException(
            status_code=404,
            detail=f"Expect commodity to be either just an id (QXXX) or name. Get `{commodity}` instead",
        )
    if not is_minmod_id(commodity):
        norm_commodity = get_commodity_by_name(commodity)
        if norm_commodity is None:
            raise HTTPException(
                status_code=404, detail=f"Commodity `{commodity}` not found"
            )
    else:
        norm_commodity = commodity
    return norm_commodity


def is_minmod_id(text: str) -> bool:
    return text.startswith("Q") and text[1:].isdigit()


def get_commodity_by_name(
    name: str,
) -> Optional[InternalID]:
    with get_rel_session() as session:
        commodity_id = session.scalar(
            select(Commodity.id).where(Commodity.lower_name == name.lower())
        )
        return commodity_id
