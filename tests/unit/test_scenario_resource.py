"""Tests for scenario resource models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dss_provisioner.resources.scenario import (
    PythonScenarioResource,
    StepBasedScenarioResource,
)


class TestStepBasedScenarioResource:
    def test_address(self) -> None:
        r = StepBasedScenarioResource(name="daily_build")
        assert r.address == "dss_step_scenario.daily_build"

    def test_defaults(self) -> None:
        r = StepBasedScenarioResource(name="daily_build")
        assert r.active is True
        assert r.triggers == []
        assert r.steps == []
        assert r.description == ""
        assert r.tags == []
        assert r.depends_on == []

    def test_custom_values(self) -> None:
        triggers = [{"type": "temporal", "params": {"frequency": "Daily"}}]
        steps = [{"type": "build_flowitem", "name": "Build", "params": {}}]
        r = StepBasedScenarioResource(
            name="daily_build",
            active=False,
            triggers=triggers,
            steps=steps,
            description="Daily build",
            tags=["prod"],
        )
        assert r.active is False
        assert r.triggers == triggers
        assert r.steps == steps
        assert r.description == "Daily build"
        assert r.tags == ["prod"]

    def test_extra_forbid(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            StepBasedScenarioResource(name="s", unknown_field="x")  # type: ignore[call-arg]

    def test_model_dump_shape(self) -> None:
        r = StepBasedScenarioResource(
            name="daily_build",
            steps=[{"type": "build_flowitem"}],
        )
        dump = r.model_dump(exclude_none=True, exclude={"address"})
        assert dump["name"] == "daily_build"
        assert dump["type"] == "step_based"
        assert dump["steps"] == [{"type": "build_flowitem"}]
        assert "address" not in dump

    def test_plan_priority(self) -> None:
        assert StepBasedScenarioResource.plan_priority == 200

    def test_reference_names_empty(self) -> None:
        r = StepBasedScenarioResource(name="s")
        assert r.reference_names() == []


class TestPythonScenarioResource:
    def test_address(self) -> None:
        r = PythonScenarioResource(name="e2e_test")
        assert r.address == "dss_python_scenario.e2e_test"

    def test_defaults(self) -> None:
        r = PythonScenarioResource(name="e2e_test")
        assert r.active is True
        assert r.triggers == []
        assert r.code == ""
        assert r.description == ""
        assert r.tags == []

    def test_custom_values(self) -> None:
        r = PythonScenarioResource(name="e2e_test", code="print('hello')")
        assert r.code == "print('hello')"
        assert r.type == "custom_python"

    def test_code_or_file_exclusive(self) -> None:
        with pytest.raises(ValidationError, match="Cannot set both"):
            PythonScenarioResource(name="s", code="print('hi')", code_file="scenarios/s.py")

    def test_code_file_excluded_from_dump(self) -> None:
        r = PythonScenarioResource(name="s", code_file="scenarios/s.py")
        dump = r.model_dump()
        assert "code_file" not in dump

    def test_extra_forbid(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            PythonScenarioResource(name="s", unknown_field="x")  # type: ignore[call-arg]

    def test_reference_names_empty(self) -> None:
        r = PythonScenarioResource(name="s")
        assert r.reference_names() == []

    def test_plan_priority(self) -> None:
        assert PythonScenarioResource.plan_priority == 200
