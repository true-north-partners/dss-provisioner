"""Scenario resource models for DSS scenarios."""

from __future__ import annotations

from typing import Any, ClassVar, Literal, Self

from pydantic import Field, model_validator

from dss_provisioner.resources.base import Resource


class ScenarioResource(Resource):
    """Base resource for DSS scenarios."""

    resource_type: ClassVar[str] = "dss_scenario"
    namespace: ClassVar[str] = "scenario"
    plan_priority: ClassVar[int] = 200

    type: str
    active: bool = True
    triggers: list[dict[str, Any]] = Field(default_factory=list)


class StepBasedScenarioResource(ScenarioResource):
    """Step-based scenario with declarative steps."""

    resource_type: ClassVar[str] = "dss_step_scenario"

    type: Literal["step_based"] = "step_based"
    steps: list[dict[str, Any]] = Field(default_factory=list)


class PythonScenarioResource(ScenarioResource):
    """Custom Python scenario."""

    resource_type: ClassVar[str] = "dss_python_scenario"
    yaml_alias: ClassVar[str] = "python"

    type: Literal["custom_python"] = "custom_python"
    code: str = ""
    code_file: str | None = Field(default=None, exclude=True)

    @model_validator(mode="after")
    def _check_code_or_file(self) -> Self:
        if self.code and self.code_file:
            msg = "Cannot set both 'code' and 'code_file'"
            raise ValueError(msg)
        return self
