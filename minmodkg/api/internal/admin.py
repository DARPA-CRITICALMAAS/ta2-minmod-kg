from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from minmodkg.api.dependencies import CurrentUserDep, RelSessionDep
from minmodkg.api.models.public_user import PublicCreateUser, PublicUser
from minmodkg.models.kgrel.user import User
from sqlmodel import select

router = APIRouter(tags=["admin"])


@router.get("/users")
def get_users(
    session: RelSessionDep,
    user: CurrentUserDep,
    offset: int = 0,
    limit: Annotated[int, Query(le=100)] = 100,
):
    """Get all users"""
    users = session.execute(select(User).offset(offset).limit(limit)).scalars().all()
    return [PublicUser.from_kgrel(u) for u in users]


@router.post("/users")
def create_user(
    user: PublicCreateUser, session: RelSessionDep, current_user: CurrentUserDep
):
    """Create a new user"""
    if current_user.role != "admin":
        raise HTTPException(403, "You do not have permission to create a new user")
    return create_user_priv(user, session)


def create_user_priv(user: PublicCreateUser, session: RelSessionDep):
    exist_user = session.get(User, user.username)
    if exist_user is not None:
        raise HTTPException(400, "Username already exists")

    dbuser = user.to_kgrel()

    session.add(dbuser)
    session.commit()
    session.refresh(dbuser)
    return dbuser
