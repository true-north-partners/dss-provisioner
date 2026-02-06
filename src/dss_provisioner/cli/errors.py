"""Map exceptions to clean stderr messages and exit codes."""

from __future__ import annotations

import typer


def _err(msg: str, *, fg: str | None) -> None:
    """Print a styled message to stderr."""
    typer.echo(typer.style(msg, fg=fg), err=True)


def handle_error(exc: Exception, *, color: bool = True) -> int:
    """Print a clean error message to stderr and return an exit code.

    All errors map to exit code 1.  No tracebacks are printed.
    """
    from dss_provisioner.config.loader import ConfigError
    from dss_provisioner.engine.errors import (
        ApplyCanceled,
        ApplyError,
        StalePlanError,
        StateProjectMismatchError,
        ValidationError,
    )

    fg = typer.colors.RED if color else None

    if isinstance(exc, ConfigError):
        _err(f"Configuration error: {exc}", fg=fg)
    elif isinstance(exc, ValidationError):
        _err("Validation failed:", fg=fg)
        for e in exc.errors:
            _err(f"  - {e}", fg=fg)
    elif isinstance(exc, StalePlanError):
        _err(f"Plan is stale: {exc}", fg=fg)
    elif isinstance(exc, StateProjectMismatchError):
        _err(f"State mismatch: {exc}", fg=fg)
    elif isinstance(exc, ApplyError):
        _err(f"Apply failed: {exc}", fg=fg)
        s = exc.result.summary()
        parts = [
            f"{n} {verb}"
            for n, verb in (
                (s["create"], "added"),
                (s["update"], "changed"),
                (s["delete"], "destroyed"),
            )
            if n
        ]
        if parts:
            _err(f"  Partial result: {', '.join(parts)}.", fg=fg)
    elif isinstance(exc, ApplyCanceled):
        _err("Apply canceled.", fg=fg)
    else:
        _err(f"Error: {exc}", fg=fg)

    return 1
