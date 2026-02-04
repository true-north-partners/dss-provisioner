"""Handler for DSS datasets."""

from typing import Any

import dataikuapi


class DatasetHandler:
    """Handler for DSS dataset operations."""

    def __init__(self, client: dataikuapi.DSSClient) -> None:
        self.client = client

    def list(self, project_key: str) -> list[dict[str, Any]]:
        """List all datasets in a project."""
        project = self.client.get_project(project_key)
        return project.list_datasets()

    def get(
        self, project_key: str, dataset_name: str
    ) -> dataikuapi.dss.dataset.DSSDataset:
        """Get a dataset."""
        project = self.client.get_project(project_key)
        return project.get_dataset(dataset_name)

    # TODO: Implement create, update, delete when we have resource definitions
