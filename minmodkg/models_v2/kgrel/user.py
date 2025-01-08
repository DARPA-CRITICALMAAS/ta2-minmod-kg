from __future__ import annotations

from enum import Enum
from typing import Literal

import bcrypt
from minmodkg.models_v2.kgrel.base import Base
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column


class Role(str, Enum):
    admin = "admin"
    user = "user"
    system = "system"


class User(MappedAsDataclass, Base):
    username: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    email: Mapped[str] = mapped_column(unique=True)
    role: Mapped[Literal["admin", "user", "system"]] = mapped_column()
    password: Mapped[bytes] = mapped_column()

    def get_uri(self):
        if self.role == Role.system:
            return f"https://minmod.isi.edu/users/s/{self.username}"
        if self.role == Role.user:
            return f"https://minmod.isi.edu/users/u/{self.username}"
        assert self.role == Role.admin, self.role
        return f"https://minmod.isi.edu/users/a/{self.username}"

    def is_system(self):
        return self.role == Role.system

    def encrypt_password(self):
        self.password = bcrypt.hashpw(self.password, bcrypt.gensalt())

    def verify_password(self, password: str):
        return bcrypt.checkpw(password.encode(), self.password)


def is_system_user(created_by: str):
    return created_by.startswith("https://minmod.isi.edu/users/s/")


def is_valid_user_uri(uri: str):
    return (
        uri.startswith("https://minmod.isi.edu/users/s/")
        or uri.startswith("https://minmod.isi.edu/users/u/")
        or uri.startswith("https://minmod.isi.edu/users/a/")
    )


def get_username(uri: str):
    return uri.rsplit("/", 1)[1]
