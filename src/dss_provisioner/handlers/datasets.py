"""Handler for DSS datasets."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import dataikuapi
    from dataikuapi.dss.dataset import DSSDataset


class DatasetHandler:
    """Handler for DSS dataset operations."""

    def __init__(self, client: dataikuapi.DSSClient) -> None:
        self.client = client

    def list_datasets(self, project_key: str) -> list[dict[str, Any]]:
        """List all datasets in a project."""
        project = self.client.get_project(project_key)
        return project.list_datasets()

    def get(self, project_key: str, dataset_name: str) -> DSSDataset:
        """Get a dataset."""
        project = self.client.get_project(project_key)
        return project.get_dataset(dataset_name)

    # TODO: Implement create, update, delete when we have resource definitions
