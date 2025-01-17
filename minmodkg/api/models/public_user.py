from __future__ import annotations

from typing import Annotated, Literal

from minmodkg.models.kgrel.user import User
from minmodkg.typing import IRI
from pydantic import BaseModel


class PublicUser(BaseModel):
    username: str
    uri: Annotated[IRI, "URI of the user (e.g., https://minmod.isi.edu/users/s/usc)"]
    name: str
    email: str
    role: Literal["admin", "user", "system"]

    @classmethod
    def from_kgrel(cls, user: User) -> PublicUser:
        return cls(
            username=user.username,
            uri=user.get_uri(),
            name=user.name,
            email=user.email,
            role=user.role,
        )


class PublicCreateUser(BaseModel):
    username: str
    name: str
    email: str
    password: str
    role: Literal["admin", "user", "system"]

    def to_kgrel(self) -> User:
        return User(
            username=self.username,
            name=self.name,
            email=self.email,
            password=User.encrypt_password(self.password),
            role=self.role,
        )
