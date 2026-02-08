"""Tests for exposed object handlers."""

from __future__ import annotations

from unittest.mock import MagicMock

from dss_provisioner.core import DSSProvider, ResourceInstance
from dss_provisioner.engine.exposed_object_handler import (
    ExposedDatasetHandler,
    ExposedManagedFolderHandler,
)
from dss_provisioner.engine.handlers import EngineContext
from dss_provisioner.resources.exposed_object import (
    ExposedDatasetResource,
    ExposedManagedFolderResource,
)


def _ctx(project: MagicMock) -> EngineContext:
    client = MagicMock()
    client.get_project.return_value = project
    provider = DSSProvider.from_client(client)
    return EngineContext(provider=provider, project_key="PRJ")


def test_create_exposed_dataset_updates_project_settings() -> None:
    project = MagicMock()
    settings = MagicMock()
    settings.settings = {"exposedObjects": {"objects": []}}
    project.get_settings.return_value = settings
    project.get_dataset.return_value.exists.return_value = True

    handler = ExposedDatasetHandler()
    desired = ExposedDatasetResource(name="orders", target_projects=["B", "A", "A"])
    attrs = handler.create(_ctx(project), desired)

    entry = settings.settings["exposedObjects"]["objects"][0]
    assert entry["type"] == "DATASET"
    assert entry["localName"] == "orders"
    assert entry["rules"] == [{"targetProject": "A"}, {"targetProject": "B"}]
    settings.save.assert_called_once()
    assert attrs["target_projects"] == ["A", "B"]


def test_read_exposed_dataset_returns_targets() -> None:
    project = MagicMock()
    settings = MagicMock()
    settings.settings = {
        "exposedObjects": {
            "objects": [
                {
                    "type": "DATASET",
                    "localName": "orders",
                    "rules": [{"targetProject": "ANALYTICS"}],
                }
            ]
        }
    }
    project.get_settings.return_value = settings

    handler = ExposedDatasetHandler()
    prior = ResourceInstance(
        address="dss_exposed_dataset.orders",
        resource_type="dss_exposed_dataset",
        name="orders",
        attributes={"description": "shared", "tags": ["prod"]},
    )
    attrs = handler.read(_ctx(project), prior)

    assert attrs is not None
    assert attrs["name"] == "orders"
    assert attrs["type"] == "DATASET"
    assert attrs["target_projects"] == ["ANALYTICS"]
    assert attrs["description"] == "shared"
    assert attrs["tags"] == ["prod"]


def test_delete_exposed_dataset_removes_entry() -> None:
    project = MagicMock()
    settings = MagicMock()
    settings.settings = {
        "exposedObjects": {
            "objects": [
                {"type": "DATASET", "localName": "orders", "rules": []},
                {"type": "DATASET", "localName": "customers", "rules": []},
            ]
        }
    }
    project.get_settings.return_value = settings

    handler = ExposedDatasetHandler()
    prior = ResourceInstance(
        address="dss_exposed_dataset.orders",
        resource_type="dss_exposed_dataset",
        name="orders",
    )
    handler.delete(_ctx(project), prior)

    objects = settings.settings["exposedObjects"]["objects"]
    assert objects == [{"type": "DATASET", "localName": "customers", "rules": []}]
    settings.save.assert_called_once()


def test_validate_exposed_dataset_checks_current_project_and_existence() -> None:
    project = MagicMock()
    project.get_dataset.return_value.exists.return_value = False
    project.get_settings.return_value = MagicMock(settings={"exposedObjects": {"objects": []}})

    handler = ExposedDatasetHandler()
    desired = ExposedDatasetResource(name="orders", target_projects=["PRJ"])
    errors = handler.validate(_ctx(project), desired)

    assert len(errors) == 2
    assert "does not exist" in errors[0]
    assert "includes current project" in errors[1]


def test_validate_exposed_managed_folder_checks_existence() -> None:
    project = MagicMock()
    project.list_managed_folders.return_value = [{"name": "reports"}]
    project.get_settings.return_value = MagicMock(settings={"exposedObjects": {"objects": []}})

    handler = ExposedManagedFolderHandler()
    desired = ExposedManagedFolderResource(name="models", target_projects=["ANALYTICS"])
    errors = handler.validate(_ctx(project), desired)

    assert len(errors) == 1
    assert "does not exist" in errors[0]
