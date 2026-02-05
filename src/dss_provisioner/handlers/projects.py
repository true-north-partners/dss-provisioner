"""Handler for DSS projects."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import dataikuapi
    from dataikuapi.dss.project import DSSProject


class ProjectHandler:
    """Handler for DSS project operations."""

    def __init__(self, client: dataikuapi.DSSClient) -> None:
        self.client = client

    def list_projects(self) -> list[str]:
        """List all project keys."""
        return self.client.list_project_keys()

    def get(self, project_key: str) -> DSSProject:
        """Get a project by key."""
        return self.client.get_project(project_key)

    def create(self, project_key: str, name: str, owner: str) -> dict[str, Any]:
        """Create a new project."""
        project = self.client.create_project(project_key, name, owner)
        return {"project_key": project.project_key}

    def delete(self, project_key: str) -> None:
        """Delete a project."""
        project = self.client.get_project(project_key)
        project.delete()
