"""Flow zone resource model."""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from dss_provisioner.resources.base import Resource


class ZoneResource(Resource):
    """A DSS flow zone.

    Zones partition the flow graph into logical sections (e.g. raw, curated).
    They must be provisioned before datasets/recipes that reference them.
    """

    resource_type: ClassVar[str] = "dss_zone"
    namespace: ClassVar[str] = "zone"
    color: str = Field(default="#2ab1ac", pattern=r"^#[0-9A-Fa-f]{6}$")
