from __future__ import annotations

import re

from dss_provisioner.cli.formatting import (
    format_apply_summary,
    format_change,
    format_plan,
    format_plan_summary,
    has_actionable_changes,
)
from dss_provisioner.engine.types import Action, Plan, PlanMetadata, ResourceChange

_META = PlanMetadata(
    project_key="TEST",
    destroy=False,
    refresh=True,
    state_lineage="lineage-1",
    state_serial=0,
    state_digest="digest",
    config_digest="cdigest",
    engine_version="0.1.0",
)


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


class TestFormatPlanSummary:
    def test_all_zeros(self) -> None:
        result = format_plan_summary({"create": 0, "update": 0, "delete": 0}, color=False)
        assert result == "Plan: 0 to add, 0 to change, 0 to destroy."

    def test_with_counts(self) -> None:
        result = format_plan_summary({"create": 2, "update": 1, "delete": 3}, color=False)
        assert result == "Plan: 2 to add, 1 to change, 3 to destroy."

    def test_color_mode_contains_ansi(self) -> None:
        result = format_plan_summary({"create": 1, "update": 0, "delete": 0}, color=True)
        assert "\x1b[" in result
        assert "1 to add" in _strip_ansi(result)


class TestFormatApplySummary:
    def test_all_zeros(self) -> None:
        result = format_apply_summary({"create": 0, "update": 0, "delete": 0}, color=False)
        assert "Apply complete!" in result
        assert "0 added" in result

    def test_with_counts(self) -> None:
        result = format_apply_summary({"create": 1, "update": 2, "delete": 0}, color=False)
        assert "1 added" in result
        assert "2 changed" in result

    def test_color_mode_contains_ansi(self) -> None:
        result = format_apply_summary({"create": 1, "update": 0, "delete": 0}, color=True)
        assert "\x1b[" in result


class TestFormatChange:
    def test_create(self) -> None:
        change = ResourceChange(
            address="dss_dataset.raw",
            resource_type="dss_dataset",
            action=Action.CREATE,
            planned={"connection": "fs_managed", "path": "/data/raw"},
        )
        result = format_change(change, color=False)
        assert "will be created" in result
        assert "+ resource" in result
        assert "connection" in result
        assert "fs_managed" in result

    def test_update(self) -> None:
        change = ResourceChange(
            address="dss_recipe.transform",
            resource_type="dss_recipe",
            action=Action.UPDATE,
            diff={"code_env": {"from": "py311", "to": "py312"}},
        )
        result = format_change(change, color=False)
        assert "will be updated" in result
        assert "~ resource" in result
        assert "py311" in result
        assert "py312" in result

    def test_delete(self) -> None:
        change = ResourceChange(
            address="dss_dataset.old",
            resource_type="dss_dataset",
            action=Action.DELETE,
            prior={"connection": "fs_managed"},
        )
        result = format_change(change, color=False)
        assert "will be destroyed" in result
        assert "- resource" in result

    def test_noop(self) -> None:
        change = ResourceChange(
            address="dss_dataset.ok",
            resource_type="dss_dataset",
            action=Action.NOOP,
        )
        result = format_change(change, color=False)
        assert "is up-to-date" in result

    def test_no_color_has_no_ansi(self) -> None:
        change = ResourceChange(
            address="dss_dataset.raw",
            resource_type="dss_dataset",
            action=Action.CREATE,
            planned={"connection": "fs_managed"},
        )
        result = format_change(change, color=False)
        assert "\x1b[" not in result


class TestFormatPlan:
    def test_no_changes(self) -> None:
        plan = Plan(
            metadata=_META,
            changes=[
                ResourceChange(
                    address="dss_dataset.ok",
                    resource_type="dss_dataset",
                    action=Action.NOOP,
                )
            ],
        )
        assert "No changes" in format_plan(plan, color=False)

    def test_skips_noop(self) -> None:
        plan = Plan(
            metadata=_META,
            changes=[
                ResourceChange(
                    address="dss_dataset.ok",
                    resource_type="dss_dataset",
                    action=Action.NOOP,
                ),
                ResourceChange(
                    address="dss_dataset.new",
                    resource_type="dss_dataset",
                    action=Action.CREATE,
                    planned={"connection": "fs_managed"},
                ),
            ],
        )
        result = format_plan(plan, color=False)
        assert "dss_dataset.ok" not in result
        assert "dss_dataset.new" in result


class TestHasActionableChanges:
    def test_all_noop(self) -> None:
        plan = Plan(
            metadata=_META,
            changes=[
                ResourceChange(
                    address="dss_dataset.ok",
                    resource_type="dss_dataset",
                    action=Action.NOOP,
                )
            ],
        )
        assert has_actionable_changes(plan) is False

    def test_with_create(self) -> None:
        plan = Plan(
            metadata=_META,
            changes=[
                ResourceChange(
                    address="dss_dataset.new",
                    resource_type="dss_dataset",
                    action=Action.CREATE,
                    planned={"connection": "fs_managed"},
                )
            ],
        )
        assert has_actionable_changes(plan) is True

    def test_empty_plan(self) -> None:
        plan = Plan(metadata=_META, changes=[])
        assert has_actionable_changes(plan) is False
