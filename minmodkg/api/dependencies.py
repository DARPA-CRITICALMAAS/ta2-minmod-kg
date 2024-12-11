from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Annotated, Optional

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyCookie
from minmodkg.api.models.db import SessionDep
from minmodkg.api.models.user import User, is_system_user
from minmodkg.config import JWT_ALGORITHM, SECRET_KEY
from minmodkg.misc import LongestPrefixIndex
from minmodkg.models.base import MINMOD_KG, MINMOD_NS
from minmodkg.typing import IRI, InternalID

# for login/security
token_from_cookie = APIKeyCookie(name="session")
TokenDep = Annotated[str, Depends(token_from_cookie)]


async def get_current_user(session: SessionDep, token: TokenDep):
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


CurrentUserDep = Annotated[User, Depends(get_current_user)]


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
    query = f'SELECT ?uri WHERE {{ ?uri a {MINMOD_NS.mo.alias}:Commodity ; {MINMOD_NS.rdfs.alias}:label ?name . FILTER(LCASE(STR(?name)) = "{name.lower()}") }} LIMIT 1'
    qres = MINMOD_KG.query(query)
    if len(qres) == 0:
        return None
    uri = qres[0]["uri"]
    return MINMOD_NS.mr.id(uri)


def rank_source(
    source_id: IRI,
    created_by: str,
    snapshot_id: str,
) -> float:
    """Get ranking of a source, higher is better"""
    # TODO: fix me!!!
    if not is_system_user(created_by):
        return 1000

    default_score = 0.5
    score = get_source_scores(snapshot_id).get_score(source_id)
    if score is None:
        # print("Unknown source id:", source_id)
        return default_score
    return score


@dataclass
class SourceScore:
    source2score: dict[str, int]
    index: LongestPrefixIndex

    def get_score(self, source_id: str) -> Optional[int]:
        prefix = self.index.get(source_id)
        if prefix is not None:
            return self.source2score[prefix]
        return None


@lru_cache(maxsize=1)
def get_source_scores(snapshot_id: str):
    mo = MINMOD_NS.mo
    query = f"""
    SELECT ?uri ?prefixed_id ?score
    WHERE {{
        ?uri a {mo.SourceConfig} ;
            {mo.prefix} ?prefixed_id ;
            {mo.score} ?score
    }}
    """
    print(query)
    qres = MINMOD_KG.query(query)
    source2score = {record["prefixed_id"]: record["score"] for record in qres}
    index = LongestPrefixIndex.create(list(source2score.keys()))
    return SourceScore(source2score, index)
