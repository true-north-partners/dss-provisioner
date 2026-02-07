"""Handler for DSS flow zones."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import dataikuapi


class ZoneHandler:
    """Handler for DSS flow zone operations."""

    def __init__(self, client: dataikuapi.DSSClient) -> None:
        self.client = client

    def list_zones(self, project_key: str) -> list[Any]:
        """List all zones in a project."""
        project = self.client.get_project(project_key)
        flow = project.get_flow()
        return flow.list_zones()
