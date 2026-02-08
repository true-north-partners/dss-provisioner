"""Project default code environment resource model."""

from __future__ import annotations

from typing import ClassVar, Literal

from dss_provisioner.resources.base import Resource


class CodeEnvResource(Resource):
    """Project default code environment resource (singleton per project).

    Selects existing instance-level code environments as the project defaults.
    Code environments themselves are not created or managed — only the project
    setting that points to them.
    """

    resource_type: ClassVar[str] = "dss_code_env"
    namespace: ClassVar[str] = "code_env"
    plan_priority: ClassVar[int] = 5

    name: Literal["code_envs"] = "code_envs"
    default_python: str | None = None
    default_r: str | None = None
