"""Handler for DSS projects."""

from typing import Any

import dataikuapi


class ProjectHandler:
    """Handler for DSS project operations."""

    def __init__(self, client: dataikuapi.DSSClient) -> None:
        self.client = client

    def list(self) -> list[str]:
        """List all project keys."""
        return self.client.list_project_keys()

    def get(self, project_key: str) -> dataikuapi.dss.project.DSSProject:
        """Get a project by key."""
        return self.client.get_project(project_key)

    def create(self, project_key: str, name: str) -> dict[str, Any]:
        """Create a new project."""
        project = self.client.create_project(project_key, name)
        return {"project_key": project.project_key}

    def delete(self, project_key: str) -> None:
        """Delete a project."""
        project = self.client.get_project(project_key)
        project.delete()
