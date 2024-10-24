from __future__ import annotations

from contextlib import contextmanager
from typing import Annotated

import typer
from minmodkg.api.internal.admin import create_user_priv
from minmodkg.api.models.db import create_db_and_tables, get_session
from minmodkg.api.models.user import UserCreate

app = typer.Typer(pretty_exceptions_short=True, pretty_exceptions_enable=False)


@app.command()
def user(
    username: Annotated[str, typer.Option("-u", help="Username")],
    name: Annotated[str, typer.Option("-n", help="Name")],
    email: Annotated[str, typer.Option("-e", help="Email")],
    password: Annotated[
        str, typer.Option(prompt=True, confirmation_prompt=True, hide_input=True)
    ],
):
    create_db_and_tables()

    with contextmanager(get_session)() as session:
        create_user_priv(
            UserCreate(username=username, name=name, email=email, password=password),
            session,
        )


if __name__ == "__main__":
    app()
