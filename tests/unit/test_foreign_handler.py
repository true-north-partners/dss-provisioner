"""Tests for foreign object handlers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dss_provisioner.core import DSSProvider, ResourceInstance
from dss_provisioner.engine.foreign_handler import (
    ForeignDatasetHandler,
    ForeignManagedFolderHandler,
)
from dss_provisioner.engine.handlers import EngineContext
from dss_provisioner.resources.foreign import ForeignDatasetResource, ForeignManagedFolderResource


def _ctx(source_project: MagicMock) -> EngineContext:
    client = MagicMock()
    target_project = MagicMock()

    def _get_project(key: str) -> MagicMock:
        return source_project if key == "SOURCE" else target_project

    client.get_project.side_effect = _get_project
    provider = DSSProvider.from_client(client)
    return EngineContext(provider=provider, project_key="PRJ")


def _source_project_for_dataset(
    *,
    exposed: bool = True,
    exists: bool = True,
    dataset_type: str = "Snowflake",
) -> MagicMock:
    source = MagicMock()
    dataset = MagicMock()
    dataset.exists.return_value = exists
    dataset.get_settings.return_value.get_raw.return_value = {"type": dataset_type}
    source.get_dataset.return_value = dataset
    source.get_settings.return_value.settings = {
        "exposedObjects": {
            "objects": [
                {
                    "type": "DATASET",
                    "localName": "customers",
                    "rules": [{"targetProject": "PRJ"}] if exposed else [],
                }
            ]
        }
    }
    return source


def test_create_foreign_dataset_requires_source_and_exposure() -> None:
    source = _source_project_for_dataset(exposed=True, exists=True)
    handler = ForeignDatasetHandler()
    desired = ForeignDatasetResource(
        name="shared_customers",
        source_project="SOURCE",
        source_name="customers",
    )

    attrs = handler.create(_ctx(source), desired)
    assert attrs["name"] == "shared_customers"
    assert attrs["full_ref"] == "SOURCE.customers"
    assert attrs["type"] == "Snowflake"


def test_create_foreign_dataset_raises_when_not_exposed() -> None:
    source = _source_project_for_dataset(exposed=False, exists=True)
    handler = ForeignDatasetHandler()
    desired = ForeignDatasetResource(
        name="shared_customers",
        source_project="SOURCE",
        source_name="customers",
    )

    with pytest.raises(RuntimeError, match="missing or not exposed"):
        handler.create(_ctx(source), desired)


def test_read_foreign_dataset_returns_none_when_prior_missing_source_attrs() -> None:
    source = _source_project_for_dataset(exposed=True, exists=True)
    handler = ForeignDatasetHandler()
    prior = ResourceInstance(
        address="dss_foreign_dataset.shared_customers",
        resource_type="dss_foreign_dataset",
        name="shared_customers",
        attributes={},
    )
    assert handler.read(_ctx(source), prior) is None


def test_validate_rejects_same_source_project() -> None:
    source = _source_project_for_dataset(exposed=True, exists=True)
    handler = ForeignDatasetHandler()
    desired = ForeignDatasetResource(
        name="self_ref",
        source_project="PRJ",
        source_name="customers",
    )

    errors = handler.validate(_ctx(source), desired)
    assert len(errors) == 1
    assert "must reference another project" in errors[0]


def test_create_foreign_managed_folder_checks_exposure() -> None:
    source = MagicMock()
    source.list_managed_folders.return_value = [{"name": "shared_models"}]
    source.get_settings.return_value.settings = {
        "exposedObjects": {
            "objects": [
                {
                    "type": "MANAGED_FOLDER",
                    "localName": "shared_models",
                    "rules": [{"targetProject": "PRJ"}],
                }
            ]
        }
    }

    handler = ForeignManagedFolderHandler()
    desired = ForeignManagedFolderResource(
        name="models_in",
        source_project="SOURCE",
        source_name="shared_models",
    )
    attrs = handler.create(_ctx(source), desired)

    assert attrs["full_ref"] == "SOURCE.shared_models"
    assert "type" not in attrs
