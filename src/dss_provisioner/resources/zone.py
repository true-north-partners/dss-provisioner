"""Flow zone resource model."""

from __future__ import annotations

from typing import ClassVar

from dss_provisioner.resources.base import Resource


class ZoneResource(Resource):
    """A DSS flow zone.

    Zones partition the flow graph into logical sections (e.g. raw, curated).
    They must be provisioned before datasets/recipes that reference them.
    """

    resource_type: ClassVar[str] = "dss_zone"
    color: str = "#2ab1ac"
