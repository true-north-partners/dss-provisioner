"""CLI command implementations."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal

import typer

from dss_provisioner.cli import app
from dss_provisioner.cli.errors import handle_error

if TYPE_CHECKING:
    from dss_provisioner.config.schema import Config
    from dss_provisioner.engine.types import ApplyResult, Plan

ConfigPath = Annotated[
    Path,
    typer.Option("--config", "-c", help="Path to the configuration file."),
]

NoColor = Annotated[
    bool,
    typer.Option("--no-color", help="Disable colored output."),
]

AutoApprove = Annotated[
    bool,
    typer.Option("--auto-approve", help="Skip interactive approval."),
]

NoRefresh = Annotated[
    bool,
    typer.Option("--no-refresh", help="Skip refreshing state from DSS."),
]


def _use_color(no_color: bool) -> bool:
    """Determine whether to use color output."""
    return not (no_color or os.environ.get("NO_COLOR"))


def _apply_with_progress(plan_obj: Plan, cfg: Config, *, color: bool) -> ApplyResult:
    """Apply a plan with a Rich progress bar and per-resource status lines."""
    from rich.console import Console
    from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn

    from dss_provisioner.cli.formatting import _ACTION_STYLES
    from dss_provisioner.config import apply
    from dss_provisioner.engine.types import Action, ResourceChange

    console = Console(no_color=not color)
    actionable = [c for c in plan_obj.changes if c.action != Action.NOOP]

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Applying", total=len(actionable))

        def on_progress(change: ResourceChange, event: Literal["start", "done"]) -> None:
            s = _ACTION_STYLES[change.action.value]
            if event == "start":
                progress.update(task, description=f"{change.address}: {s.progress_verb}...")
            elif event == "done":
                progress.console.print(f"  {change.address}: {s.done_verb}")
                progress.advance(task)

        return apply(plan_obj, cfg, progress=on_progress)


def _confirm_and_apply(
    plan_obj: Plan,
    cfg: Config,
    *,
    color: bool,
    auto_approve: bool,
    confirm_msg: str,
    empty_msg: str,
) -> None:
    """Shared flow: show plan -> confirm -> apply with progress -> print summary.

    Exits with code 0 if no actionable changes.
    """
    from dss_provisioner.cli.formatting import (
        format_apply_summary,
        format_plan,
        format_plan_summary,
        has_actionable_changes,
    )

    if not has_actionable_changes(plan_obj):
        typer.echo(empty_msg)
        raise typer.Exit(0)

    typer.echo(format_plan(plan_obj, color=color))
    typer.echo()
    typer.echo(format_plan_summary(plan_obj.summary(), color=color))
    typer.echo()

    if not auto_approve:
        try:
            typer.confirm(confirm_msg, abort=True)
        except typer.Abort as e:
            typer.echo("Apply canceled.", err=True)
            raise typer.Exit(1) from e

    try:
        result = _apply_with_progress(plan_obj, cfg, color=color)
    except Exception as exc:
        raise typer.Exit(handle_error(exc, color=color)) from exc

    typer.echo()
    typer.echo(format_apply_summary(result.summary(), color=color))


@app.command()
def plan(
    config: ConfigPath = Path("dss-provisioner.yaml"),
    out: Annotated[
        Path | None,
        typer.Option("--out", "-o", help="Save plan to file."),
    ] = None,
    no_color: NoColor = False,
    no_refresh: NoRefresh = False,
) -> None:
    """Show changes required by the current configuration."""
    from dss_provisioner.cli.formatting import (
        format_plan,
        format_plan_summary,
        has_actionable_changes,
    )
    from dss_provisioner.config import load
    from dss_provisioner.config import plan as plan_fn

    color = _use_color(no_color)
    try:
        cfg = load(config)
        plan_obj = plan_fn(cfg, refresh=not no_refresh)
    except Exception as exc:
        raise typer.Exit(handle_error(exc, color=color)) from exc

    typer.echo(format_plan(plan_obj, color=color))
    typer.echo()
    typer.echo(format_plan_summary(plan_obj.summary(), color=color))

    if out is not None:
        plan_obj.save(out)
        typer.echo(f"\nPlan saved to {out}")

    if has_actionable_changes(plan_obj):
        raise typer.Exit(2)


@app.command(name="apply")
def apply_cmd(
    plan_file: Annotated[
        Path | None,
        typer.Argument(help="Saved plan file to apply."),
    ] = None,
    config: ConfigPath = Path("dss-provisioner.yaml"),
    auto_approve: AutoApprove = False,
    no_color: NoColor = False,
    no_refresh: NoRefresh = False,
) -> None:
    """Apply the changes required by the current configuration."""
    from dss_provisioner.config import load
    from dss_provisioner.config import plan as plan_fn
    from dss_provisioner.engine.types import Plan

    color = _use_color(no_color)
    try:
        cfg = load(config)
        plan_obj = (
            Plan.load(plan_file) if plan_file is not None else plan_fn(cfg, refresh=not no_refresh)
        )
    except Exception as exc:
        raise typer.Exit(handle_error(exc, color=color)) from exc

    _confirm_and_apply(
        plan_obj,
        cfg,
        color=color,
        auto_approve=auto_approve,
        confirm_msg="Do you want to apply these changes?",
        empty_msg="No changes. Resources are up-to-date.",
    )


@app.command()
def destroy(
    config: ConfigPath = Path("dss-provisioner.yaml"),
    auto_approve: AutoApprove = False,
    no_color: NoColor = False,
) -> None:
    """Destroy all managed resources."""
    from dss_provisioner.config import load
    from dss_provisioner.config import plan as plan_fn

    color = _use_color(no_color)
    try:
        cfg = load(config)
        plan_obj = plan_fn(cfg, destroy=True)
    except Exception as exc:
        raise typer.Exit(handle_error(exc, color=color)) from exc

    _confirm_and_apply(
        plan_obj,
        cfg,
        color=color,
        auto_approve=auto_approve,
        confirm_msg="Do you really want to destroy all resources?",
        empty_msg="No resources to destroy.",
    )


@app.command(name="refresh")
def refresh_cmd(
    config: ConfigPath = Path("dss-provisioner.yaml"),
    auto_approve: AutoApprove = False,
    no_color: NoColor = False,
) -> None:
    """Refresh state from the live DSS instance."""
    from dss_provisioner.cli.formatting import changes_summary, format_changes, format_plan_summary
    from dss_provisioner.config import load, save_state
    from dss_provisioner.config import refresh as refresh_fn

    color = _use_color(no_color)
    try:
        cfg = load(config)
        changes, state = refresh_fn(cfg)
    except Exception as exc:
        raise typer.Exit(handle_error(exc, color=color)) from exc

    if not changes:
        typer.echo("No changes. State is up-to-date with DSS.")
        raise typer.Exit(0)

    typer.echo(format_changes(changes, color=color))
    typer.echo()
    typer.echo(format_plan_summary(changes_summary(changes), color=color, header="Refresh"))
    typer.echo()

    if not auto_approve:
        try:
            typer.confirm("Do you want to update the state file?", abort=True)
        except typer.Abort as e:
            typer.echo("Refresh canceled.", err=True)
            raise typer.Exit(1) from e

    save_state(cfg, state)
    count = len(state.resources)
    typer.echo(f"State refreshed. {count} resource{'s' if count != 1 else ''} tracked.")


@app.command()
def drift(
    config: ConfigPath = Path("dss-provisioner.yaml"),
    no_color: NoColor = False,
) -> None:
    """Show drift between state and the live DSS instance."""
    from dss_provisioner.cli.formatting import format_changes
    from dss_provisioner.config import drift as drift_fn
    from dss_provisioner.config import load

    color = _use_color(no_color)
    try:
        cfg = load(config)
        changes = drift_fn(cfg)
    except Exception as exc:
        raise typer.Exit(handle_error(exc, color=color)) from exc

    if not changes:
        typer.echo("No drift detected. State is up-to-date with DSS.")
        raise typer.Exit(0)

    typer.echo("Drift detected:\n")
    typer.echo(format_changes(changes, color=color))


@app.command()
def preview(
    config: ConfigPath = Path("dss-provisioner.yaml"),
    branch: Annotated[
        str | None,
        typer.Option(
            "--branch",
            help="Override git branch name for preview key and library checkout.",
        ),
    ] = None,
    destroy: Annotated[
        bool,
        typer.Option("--destroy", help="Delete the preview project and preview state."),
    ] = False,
    list_: Annotated[
        bool,
        typer.Option("--list", help="List active preview projects for the base project."),
    ] = False,
    no_color: NoColor = False,
    no_refresh: NoRefresh = False,
) -> None:
    """Create, list, or destroy branch-based preview environments."""
    from dss_provisioner.cli.formatting import (
        format_apply_summary,
        format_plan,
        format_plan_summary,
    )
    from dss_provisioner.config import load
    from dss_provisioner.config.loader import ConfigError
    from dss_provisioner.preview import destroy_preview, list_previews, run_preview

    color = _use_color(no_color)

    if destroy and list_:
        exc = ConfigError("preview options --destroy and --list cannot be used together")
        raise typer.Exit(handle_error(exc, color=color))

    try:
        cfg = load(config)
        if list_:
            previews = list_previews(cfg)
            if not previews:
                typer.echo("No preview projects found.")
                return

            typer.echo("Preview projects:")
            for preview_project in previews:
                branch_text = preview_project.branch or "unknown"
                typer.echo(f"  - {preview_project.project_key} (branch: {branch_text})")
            return

        if destroy:
            spec, deleted = destroy_preview(cfg, branch=branch)
            if deleted:
                typer.echo(f"Deleted preview project: {spec.preview_project_key}")
            else:
                typer.echo(f"Preview project not found: {spec.preview_project_key}")
            typer.echo(f"Cleaned preview state files for: {spec.preview_state_path}")
            return

        spec, plan_obj, result = run_preview(cfg, branch=branch, refresh=not no_refresh)
    except Exception as exc:
        raise typer.Exit(handle_error(exc, color=color)) from exc

    typer.echo(f"Preview project: {spec.preview_project_key}")
    typer.echo(f"Branch: {spec.branch}")
    typer.echo(f"Preview state: {spec.preview_state_path}")
    if cfg.provider.host:
        typer.echo(
            f"Preview URL: {cfg.provider.host.rstrip('/')}/projects/{spec.preview_project_key}/"
        )

    typer.echo()
    typer.echo(format_plan(plan_obj, color=color))
    typer.echo()
    typer.echo(format_plan_summary(plan_obj.summary(), color=color, header="Preview plan"))
    typer.echo()
    typer.echo(format_apply_summary(result.summary(), color=color))


@app.command()
def validate(
    config: ConfigPath = Path("dss-provisioner.yaml"),
    no_color: NoColor = False,
) -> None:
    """Validate the configuration file."""
    from dss_provisioner.cli.formatting import styler
    from dss_provisioner.config import load
    from dss_provisioner.config import plan as plan_fn

    color = _use_color(no_color)
    try:
        cfg = load(config)
        plan_fn(cfg, refresh=False)
    except Exception as exc:
        raise typer.Exit(handle_error(exc, color=color)) from exc

    typer.echo(styler(color)("Configuration is valid.", fg="green"))
