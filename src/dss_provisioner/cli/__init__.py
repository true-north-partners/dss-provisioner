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


_LOG_FORMAT = "%(levelname)s %(name)s: %(message)s"
_VALID_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


def _configure_logging(verbose: int) -> None:
    """Set up stdlib logging based on ``-v`` flags or ``DSS_LOG`` env var."""
    env_level = os.environ.get("DSS_LOG", "").upper()
    if env_level:
        if env_level not in _VALID_LEVELS:
            print(
                f"WARNING: invalid DSS_LOG level '{env_level}', "
                f"expected one of {', '.join(sorted(_VALID_LEVELS))}; defaulting to INFO",
                file=sys.stderr,
            )
        level = getattr(logging, env_level, logging.INFO)
    elif verbose >= 2:
        level = logging.DEBUG
    elif verbose >= 1:
        level = logging.INFO
    else:
        return  # no flag → stay unconfigured (silent, current behaviour)
    logging.basicConfig(
        level=logging.WARNING,
        format=_LOG_FORMAT,
        stream=sys.stderr,
        force=True,
    )
    logging.getLogger("dss_provisioner").setLevel(level)


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
