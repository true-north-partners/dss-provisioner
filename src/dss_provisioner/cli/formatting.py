"""Plan and apply output rendering (Terraform-style)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, NamedTuple

import typer

from dss_provisioner.engine.types import Action

if TYPE_CHECKING:
    from collections.abc import Callable

    from dss_provisioner.engine.types import Plan, ResourceChange


class _ActionStyle(NamedTuple):
    color: str
    symbol: str
    progress_verb: str
    done_verb: str


_ACTION_STYLES: dict[str, _ActionStyle] = {
    "create": _ActionStyle("green", "+", "Creating", "Creation complete"),
    "update": _ActionStyle("yellow", "~", "Updating", "Update complete"),
    "delete": _ActionStyle("red", "-", "Destroying", "Destroy complete"),
    "no-op": _ActionStyle("bright_black", " ", "", ""),
}

_ACTION_DESC: dict[str, str] = {
    "create": "will be created",
    "update": "will be updated in-place",
    "delete": "will be destroyed",
    "no-op": "is up-to-date",
}


def styler(color: bool) -> Callable[..., str]:
    """Return ``typer.style`` when *color* is True, otherwise a passthrough."""
    if color:
        return typer.style
    return lambda text, **_kw: text


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def has_actionable_changes(plan: Plan) -> bool:
    """Return True if the plan contains any non-NOOP changes."""
    return any(c.action != Action.NOOP for c in plan.changes)


def _align_values(items: dict[str, str]) -> list[tuple[str, str]]:
    """Right-pad keys so ``=`` signs align."""
    if not items:
        return []
    max_key = max(len(k) for k in items)
    return [(k.ljust(max_key), v) for k, v in items.items()]


def _format_value(value: Any) -> str:
    """Format a value for display in a plan diff block."""
    if isinstance(value, str):
        return f'"{value}"'
    if value is None:
        return "null"
    return str(value)


# ---------------------------------------------------------------------------
# Plan rendering
# ---------------------------------------------------------------------------


def _change_attrs(change: ResourceChange) -> dict[str, str]:
    """Extract displayable ``key → formatted value`` pairs from a change."""
    if change.action == Action.CREATE and change.planned:
        return {k: _format_value(v) for k, v in change.planned.items()}
    if change.action == Action.UPDATE and change.diff:
        return {
            k: f"{_format_value(d['from'])} -> {_format_value(d['to'])}"
            for k, d in change.diff.items()
        }
    return {}


def format_change(change: ResourceChange, *, color: bool = True) -> str:
    """Render a single ResourceChange as a Terraform-style block."""
    style = styler(color)
    action_val = change.action.value
    sc = {"fg": _ACTION_STYLES[action_val].color}
    symbol = _ACTION_STYLES[action_val].symbol

    name = change.address.split(".", 1)[1] if "." in change.address else change.address
    lines = [
        style(f"  # {change.address} {_ACTION_DESC[action_val]}", bold=True, **sc),
        style(f'  {symbol} resource "{change.resource_type}" "{name}" {{', **sc),
        *[
            style(f"      {symbol} {k} = {v}", **sc)
            for k, v in _align_values(_change_attrs(change))
        ],
        style("    }", **sc),
    ]
    return "\n".join(lines)


def format_changes(changes: list[ResourceChange], *, color: bool = True) -> str:
    """Render a list of changes as Terraform-style diff blocks."""
    blocks = [format_change(c, color=color) for c in changes if c.action != Action.NOOP]
    if not blocks:
        return "No changes. Resources are up-to-date."
    return "\n\n".join(blocks)


def format_plan(plan: Plan, *, color: bool = True) -> str:
    """Render the full plan output with per-change diff blocks."""
    return format_changes(plan.changes, color=color)


# ---------------------------------------------------------------------------
# Summaries
# ---------------------------------------------------------------------------

_PLAN_VERBS = ("to add", "to change", "to destroy")
_APPLY_VERBS = ("added", "changed", "destroyed")
_SUMMARY_COLORS = ("green", "yellow", "red")


def _format_summary(summary: dict[str, int], verbs: tuple[str, ...], *, color: bool) -> str:
    """Build the ``N verb, N verb, N verb`` part of a summary line."""
    style = styler(color)
    counts = (summary.get("create", 0), summary.get("update", 0), summary.get("delete", 0))
    parts = [
        style(f"{n} {verb}", fg=fg) if n and color else f"{n} {verb}"
        for n, verb, fg in zip(counts, verbs, _SUMMARY_COLORS, strict=True)
    ]
    return ", ".join(parts)


def changes_summary(changes: list[ResourceChange]) -> dict[str, int]:
    """Count changes by action type (create/update/delete)."""
    summary: dict[str, int] = {"create": 0, "update": 0, "delete": 0}
    for c in changes:
        if c.action != Action.NOOP:
            summary[c.action.value] += 1
    return summary


def format_plan_summary(summary: dict[str, int], *, color: bool = True) -> str:
    """Render ``Plan: 2 to add, 1 to change, 0 to destroy.``"""
    return f"Plan: {_format_summary(summary, _PLAN_VERBS, color=color)}."


def format_apply_summary(summary: dict[str, int], *, color: bool = True) -> str:
    """Render ``Apply complete! Resources: 2 added, 0 changed, 0 destroyed.``"""
    style = styler(color)
    header = style("Apply complete!", fg="green", bold=True)
    return f"{header} Resources: {_format_summary(summary, _APPLY_VERBS, color=color)}."
