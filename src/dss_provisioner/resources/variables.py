"""Project variables resource model."""

from __future__ import annotations

from typing import Any, ClassVar, Literal

from pydantic import Field

from dss_provisioner.resources.base import Resource


class VariablesResource(Resource):
    """Project variables resource (singleton per project).

    Manages DSS project variables in two scopes:

    - ``standard``: shared across all instances
    - ``local``: instance-specific overrides
    """

    resource_type: ClassVar[str] = "dss_variables"
    plan_priority: ClassVar[int] = 0

    name: Literal["variables"] = "variables"
    standard: dict[str, Any] = Field(default_factory=dict)
    local: dict[str, Any] = Field(default_factory=dict)
