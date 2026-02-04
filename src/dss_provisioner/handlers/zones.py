"""Handler for DSS flow zones."""

from typing import Any

import dataikuapi


class ZoneHandler:
    """Handler for DSS flow zone operations."""

    def __init__(self, client: dataikuapi.DSSClient) -> None:
        self.client = client

    def list(self, project_key: str) -> list[Any]:
        """List all zones in a project."""
        project = self.client.get_project(project_key)
        flow = project.get_flow()
        return flow.list_zones()

    # TODO: Implement create, update, delete when we have resource definitions
