from __future__ import annotations

from enum import Enum
from typing import Literal

import bcrypt
from minmodkg.models.kgrel.base import Base
from minmodkg.models.kgrel.user_extra import is_system_user
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column


class Role(str, Enum):
    admin = "admin"
    user = "user"
    system = "system"


class User(MappedAsDataclass, Base):
    __tablename__ = "user"

    username: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    email: Mapped[str] = mapped_column(unique=True)
    password: Mapped[bytes] = mapped_column()
    role: Mapped[Literal["admin", "user", "system"]] = mapped_column(default=Role.user)

    def get_uri(self):
        if self.role == Role.system:
            return f"https://minmod.isi.edu/users/s/{self.username}"
        if self.role == Role.user:
            return f"https://minmod.isi.edu/users/u/{self.username}"
        assert self.role == Role.admin, self.role
        return f"https://minmod.isi.edu/users/a/{self.username}"

    def is_system(self):
        return self.role == Role.system

    def verify_password(self, password: str):
        return bcrypt.checkpw(password.encode(), self.password)

    @staticmethod
    def encrypt_password(password: str):
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt())

    def to_dict(self, encode_pwd: bool = True):
        return {
            "username": self.username,
            "name": self.name,
            "email": self.email,
            "password": self.password.hex() if encode_pwd else self.password,
            "role": self.role,
        }

    @classmethod
    def from_dict(cls, d: dict):
        return cls(
            username=d["username"],
            name=d["name"],
            email=d["email"],
            password=bytes.fromhex(d["password"]),
            role=d["role"],
        )


def is_valid_user_uri(uri: str):
    return (
        uri.startswith("https://minmod.isi.edu/users/s/")
        or uri.startswith("https://minmod.isi.edu/users/u/")
        or uri.startswith("https://minmod.isi.edu/users/a/")
    )


def get_username(uri: str):
    return uri.rsplit("/", 1)[1]
