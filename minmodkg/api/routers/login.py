from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Annotated

import jwt
from fastapi import APIRouter, Body, HTTPException, Response, status
from minmodkg.api.dependencies import CurrentUserDep, RelSessionDep
from minmodkg.api.models.public_user import PublicUser
from minmodkg.config import JWT_ACCESS_TOKEN_EXPIRE_MINUTES, JWT_ALGORITHM, SECRET_KEY
from minmodkg.models.kgrel.user import User, get_username
from sqlmodel import col, select

router = APIRouter(tags=["login"])


@router.post("/login")
def login(
    session: RelSessionDep,
    response: Response,
    username: Annotated[str, Body()],
    password: Annotated[str, Body()],
):
    # authenticate user
    user = session.get(User, username)
    if user is None or not user.verify_password(password):
        # validate password
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )

    expired_at = datetime.now(timezone.utc) + timedelta(
        minutes=JWT_ACCESS_TOKEN_EXPIRE_MINUTES
    )
    access_token = jwt.encode(
        {"username": user.username, "exp": expired_at.timestamp()},
        SECRET_KEY,
        algorithm=JWT_ALGORITHM,
    )
    response.set_cookie(
        key="session",
        value=access_token,
    )
    return PublicUser.from_kgrel(user)


@router.get("/users/find_by_ids")
def get_users_by_ids(
    user_uris: Annotated[list[str], Body(embed=True)],
    session: RelSessionDep,
) -> list[PublicUser]:
    statement = select(User).where(
        User.username.in_([get_username(uri) for uri in user_uris])
    )
    output = session.execute(statement).scalars().all()
    return [PublicUser.from_kgrel(u) for u in output]


@router.get("/whoami")
def whoami(user: CurrentUserDep):
    return PublicUser.from_kgrel(user)
