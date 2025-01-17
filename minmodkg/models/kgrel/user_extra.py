from __future__ import annotations


def is_system_user(created_by: str):
    return created_by.startswith("https://minmod.isi.edu/users/s/")
