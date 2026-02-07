"""Git library resource model."""

from __future__ import annotations

from typing import ClassVar

from dss_provisioner.resources.base import Resource


class GitLibraryResource(Resource):
    """Git library reference for a DSS project.

    Manages external Git repositories imported into the project library.
    Each entry maps to a Git reference in DSS's ``external-libraries.json``.
    """

    resource_type: ClassVar[str] = "dss_git_library"
    plan_priority: ClassVar[int] = 10

    name: str
    repository: str
    checkout: str = "main"
    path: str = ""
    add_to_python_path: bool = True
