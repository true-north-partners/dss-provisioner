"""CLI application for dss-provisioner."""

from __future__ import annotations

import typer

from dss_provisioner import __version__

app = typer.Typer(
    name="dss-provisioner",
    no_args_is_help=True,
    add_completion=False,
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"dss-provisioner {__version__}")
        raise typer.Exit


@app.callback()
def main(
    version: bool | None = typer.Option(
        None,
        "--version",
        "-V",
        help="Show version and exit.",
        callback=_version_callback,
        is_eager=True,
    ),
) -> None:
    """Terraform-style infrastructure-as-code for Dataiku DSS."""


# Register commands after app is created to avoid circular imports.
from dss_provisioner.cli import commands as _commands  # noqa: E402, F401
