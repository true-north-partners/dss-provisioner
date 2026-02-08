"""CLI application for dss-provisioner."""

from __future__ import annotations

import logging
import os
import sys

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


def _configure_logging(verbose: int) -> None:
    """Set up stdlib logging based on ``-v`` flags or ``DSS_LOG`` env var."""
    env_level = os.environ.get("DSS_LOG", "").upper()
    if env_level:
        level = getattr(logging, env_level, logging.INFO)
    elif verbose >= 2:
        level = logging.DEBUG
    elif verbose >= 1:
        level = logging.INFO
    else:
        return  # no flag → stay unconfigured (silent, current behaviour)
    logging.basicConfig(
        level=level,
        format="%(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
        force=True,
    )


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
    verbose: int = typer.Option(
        0,
        "--verbose",
        "-v",
        count=True,
        help="Increase log verbosity (-v info, -vv debug).",
    ),
) -> None:
    """Terraform-style infrastructure-as-code for Dataiku DSS."""
    _ = version
    _configure_logging(verbose)


# Register commands after app is created to avoid circular imports.
from dss_provisioner.cli import commands as _commands  # noqa: E402, F401
