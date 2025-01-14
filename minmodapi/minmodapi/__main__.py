from __future__ import annotations

from typing import Annotated

import typer
from minmodapi import MinModAPI

app = typer.Typer(pretty_exceptions_short=True, pretty_exceptions_enable=False)


@app.command("whoami", help="Get current user")
def whoami(
    endpoint: Annotated[
        str, typer.Option("-e", help="Endpoint")
    ] = "https://dev.minmod.isi.edu",
):
    api = MinModAPI(endpoint)
    print(api.whoami())


@app.command("login", help="Login to MinMod")
def login(
    username: Annotated[str, typer.Option("-u", help="Username")],
    password: Annotated[str, typer.Option(prompt=True, hide_input=True)],
    endpoint: Annotated[
        str, typer.Option("-e", help="Endpoint")
    ] = "https://dev.minmod.isi.edu",
):
    api = MinModAPI(endpoint)
    api.login(username, password)
    print("Login successful!")


if __name__ == "__main__":
    app()
