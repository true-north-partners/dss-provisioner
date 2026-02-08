"""Tests for the ManagedFolderHandler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from dss_provisioner.core import DSSProvider, ResourceInstance
from dss_provisioner.core.state import State
from dss_provisioner.engine import DSSEngine
from dss_provisioner.engine.handlers import EngineContext
from dss_provisioner.engine.managed_folder_handler import ManagedFolderHandler
from dss_provisioner.engine.registry import ResourceTypeRegistry
from dss_provisioner.engine.types import Action
from dss_provisioner.resources.managed_folder import (
    FilesystemManagedFolderResource,
    ManagedFolderResource,
    UploadManagedFolderResource,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def mock_client() -> MagicMock:
    client = MagicMock()
    client.get_variables.return_value = {}
    return client


@pytest.fixture
def ctx(mock_client: MagicMock) -> EngineContext:
    provider = DSSProvider.from_client(mock_client)
    return EngineContext(provider=provider, project_key="PRJ")


@pytest.fixture
def handler() -> ManagedFolderHandler:
    return ManagedFolderHandler()


@pytest.fixture
def mock_project(mock_client: MagicMock) -> MagicMock:
    project = MagicMock()
    project.get_variables.return_value = {"standard": {}, "local": {}}
    project.list_managed_folders.return_value = [
        {"id": "abc12345", "name": "my_folder"},
    ]
    mock_client.get_project.return_value = project
    return project


@pytest.fixture
def mock_folder(mock_project: MagicMock) -> MagicMock:
    folder = MagicMock()
    mock_project.get_managed_folder.return_value = folder
    mock_project.create_managed_folder.return_value = folder

    settings = MagicMock()
    raw: dict[str, Any] = {
        "type": "Filesystem",
        "params": {},
        "description": "",
        "tags": [],
    }
    settings.get_raw.return_value = raw
    folder.get_settings.return_value = settings

    default_zone = MagicMock()
    default_zone.id = "default"
    folder.get_zone.return_value = default_zone

    return folder


def _make_raw(
    folder_type: str = "Filesystem",
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "type": folder_type,
        "params": params or {},
        "description": "",
        "tags": [],
    }


class TestCreate:
    def test_calls_create_managed_folder(
        self,
        ctx: EngineContext,
        handler: ManagedFolderHandler,
        mock_project: MagicMock,
        mock_folder: MagicMock,  # noqa: ARG002
    ) -> None:
        desired = FilesystemManagedFolderResource(
            name="trained_models", connection="filesystem_managed", path="/data/models"
        )
        handler.create(ctx, desired)
        mock_project.create_managed_folder.assert_called_once_with(
            "trained_models",
            folder_type="Filesystem",
            connection_name="filesystem_managed",
        )

    def test_updates_settings_with_params(
        self,
        ctx: EngineContext,
        handler: ManagedFolderHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_folder: MagicMock,
    ) -> None:
        raw = _make_raw("Filesystem", params={"connection": "filesystem_managed"})
        mock_folder.get_settings.return_value.get_raw.return_value = raw

        desired = FilesystemManagedFolderResource(
            name="trained_models", connection="filesystem_managed", path="/data/models"
        )
        handler.create(ctx, desired)

        assert raw["params"]["path"] == "/data/models"
        mock_folder.get_settings.return_value.save.assert_called()

    def test_sets_description_and_tags(
        self,
        ctx: EngineContext,
        handler: ManagedFolderHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_folder: MagicMock,
    ) -> None:
        raw = _make_raw()
        mock_folder.get_settings.return_value.get_raw.return_value = raw

        desired = FilesystemManagedFolderResource(
            name="my_folder",
            connection="filesystem_managed",
            path="/data/folder",
            description="My models",
            tags=["ml", "prod"],
        )
        handler.create(ctx, desired)

        assert raw["description"] == "My models"
        assert raw["tags"] == ["ml", "prod"]

    def test_applies_zone(
        self,
        ctx: EngineContext,
        handler: ManagedFolderHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_folder: MagicMock,
    ) -> None:
        desired = FilesystemManagedFolderResource(
            name="my_folder", connection="filesystem_managed", path="/data/folder", zone="raw"
        )
        handler.create(ctx, desired)
        mock_folder.move_to_zone.assert_called_once_with("raw")

    def test_upload_omits_connection_name(
        self,
        ctx: EngineContext,
        handler: ManagedFolderHandler,
        mock_project: MagicMock,
        mock_folder: MagicMock,  # noqa: ARG002
    ) -> None:
        desired = UploadManagedFolderResource(name="uploads")
        handler.create(ctx, desired)
        mock_project.create_managed_folder.assert_called_once_with(
            "uploads",
            folder_type="UploadedFiles",
        )

    def test_no_zone_when_not_specified(
        self,
        ctx: EngineContext,
        handler: ManagedFolderHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_folder: MagicMock,
    ) -> None:
        desired = FilesystemManagedFolderResource(
            name="my_folder", connection="filesystem_managed", path="/data/folder"
        )
        handler.create(ctx, desired)
        mock_folder.move_to_zone.assert_not_called()


class TestRead:
    def test_returns_attributes(
        self,
        ctx: EngineContext,
        handler: ManagedFolderHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_folder: MagicMock,
    ) -> None:
        raw = _make_raw("Filesystem", params={"connection": "local"})
        raw["description"] = "A folder"
        raw["tags"] = ["tag1"]
        mock_folder.get_settings.return_value.get_raw.return_value = raw

        prior = ResourceInstance(
            address="dss_managed_folder.my_folder",
            resource_type="dss_managed_folder",
            name="my_folder",
        )
        result = handler.read(ctx, prior)

        assert result is not None
        assert result["name"] == "my_folder"
        assert result["type"] == "Filesystem"
        assert result["connection"] == "local"
        assert result["description"] == "A folder"
        assert result["tags"] == ["tag1"]

    def test_returns_none_when_not_found(
        self,
        ctx: EngineContext,
        handler: ManagedFolderHandler,
        mock_project: MagicMock,
    ) -> None:
        mock_project.list_managed_folders.return_value = []

        prior = ResourceInstance(
            address="dss_managed_folder.missing",
            resource_type="dss_managed_folder",
            name="missing",
        )
        result = handler.read(ctx, prior)
        assert result is None

    def test_resolves_project_key_variable_in_path(
        self,
        ctx: EngineContext,
        handler: ManagedFolderHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_folder: MagicMock,
    ) -> None:
        raw = _make_raw(
            "Filesystem",
            params={"connection": "filesystem_managed", "path": "${projectKey}/models"},
        )
        mock_folder.get_settings.return_value.get_raw.return_value = raw

        prior = ResourceInstance(
            address="dss_filesystem_managed_folder.my_folder",
            resource_type="dss_filesystem_managed_folder",
            name="my_folder",
        )
        result = handler.read(ctx, prior)

        assert result is not None
        assert result["path"] == "PRJ/models"


class TestUpdate:
    def test_merges_params_and_saves(
        self,
        ctx: EngineContext,
        handler: ManagedFolderHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_folder: MagicMock,
    ) -> None:
        raw = _make_raw("Filesystem", params={"connection": "old_conn"})
        mock_folder.get_settings.return_value.get_raw.return_value = raw

        desired = FilesystemManagedFolderResource(
            name="my_folder", connection="new_conn", path="/new/path"
        )
        prior = ResourceInstance(
            address="dss_filesystem_managed_folder.my_folder",
            resource_type="dss_filesystem_managed_folder",
            name="my_folder",
        )
        handler.update(ctx, desired, prior)

        mock_folder.get_settings.return_value.save.assert_called()
        assert raw["params"]["connection"] == "new_conn"
        assert raw["params"]["path"] == "/new/path"

    def test_updates_description_and_tags(
        self,
        ctx: EngineContext,
        handler: ManagedFolderHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_folder: MagicMock,
    ) -> None:
        raw = _make_raw()
        mock_folder.get_settings.return_value.get_raw.return_value = raw

        desired = ManagedFolderResource(
            name="my_folder",
            type="Filesystem",
            description="Updated desc",
            tags=["new_tag"],
        )
        prior = ResourceInstance(
            address="dss_managed_folder.my_folder",
            resource_type="dss_managed_folder",
            name="my_folder",
        )
        handler.update(ctx, desired, prior)

        assert raw["description"] == "Updated desc"
        assert raw["tags"] == ["new_tag"]

    def test_update_missing_raises(
        self,
        ctx: EngineContext,
        handler: ManagedFolderHandler,
        mock_project: MagicMock,
    ) -> None:
        mock_project.list_managed_folders.return_value = []

        desired = ManagedFolderResource(name="missing", type="Filesystem")
        prior = ResourceInstance(
            address="dss_managed_folder.missing",
            resource_type="dss_managed_folder",
            name="missing",
        )
        with pytest.raises(RuntimeError, match="not found during update"):
            handler.update(ctx, desired, prior)


class TestDelete:
    def test_deletes_folder(
        self,
        ctx: EngineContext,
        handler: ManagedFolderHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_folder: MagicMock,
    ) -> None:
        prior = ResourceInstance(
            address="dss_managed_folder.my_folder",
            resource_type="dss_managed_folder",
            name="my_folder",
        )
        handler.delete(ctx, prior)
        mock_folder.delete.assert_called_once()

    def test_delete_missing_is_noop(
        self,
        ctx: EngineContext,
        handler: ManagedFolderHandler,
        mock_project: MagicMock,
    ) -> None:
        mock_project.list_managed_folders.return_value = []

        prior = ResourceInstance(
            address="dss_managed_folder.missing",
            resource_type="dss_managed_folder",
            name="missing",
        )
        # Should not raise
        handler.delete(ctx, prior)


class TestValidatePlan:
    def test_valid_zone_reference(
        self,
        ctx: EngineContext,
        handler: ManagedFolderHandler,
    ) -> None:
        from dss_provisioner.engine.handlers import PlanContext
        from dss_provisioner.resources.zone import ZoneResource

        desired = ManagedFolderResource(name="my_folder", type="Filesystem", zone="raw")
        zone = ZoneResource(name="raw")

        state = State(serial=0, project_key="PRJ", resources={})
        plan_ctx = PlanContext(
            all_desired={zone.address: zone, desired.address: desired},
            state=state,
        )
        errors = handler.validate_plan(ctx, desired, plan_ctx)
        assert errors == []

    def test_invalid_zone_reference(
        self,
        ctx: EngineContext,
        handler: ManagedFolderHandler,
    ) -> None:
        from dss_provisioner.engine.handlers import PlanContext

        desired = ManagedFolderResource(name="my_folder", type="Filesystem", zone="nonexistent")
        state = State(serial=0, project_key="PRJ", resources={})
        plan_ctx = PlanContext(
            all_desired={desired.address: desired},
            state=state,
        )
        errors = handler.validate_plan(ctx, desired, plan_ctx)
        assert len(errors) == 1
        assert "nonexistent" in errors[0]


def _setup_engine(
    tmp_path: Path,
    raw: dict[str, Any],
) -> tuple[DSSEngine, MagicMock, MagicMock]:
    """Wire up a DSSEngine with ManagedFolderHandler and mocked dataikuapi objects."""
    mock_client = MagicMock()
    mock_client.get_variables.return_value = {}
    provider = DSSProvider.from_client(mock_client)

    project = MagicMock()
    project.get_variables.return_value = {"standard": {}, "local": {}}
    project.list_managed_folders.return_value = [
        {"id": "abc12345", "name": "my_folder"},
    ]
    mock_client.get_project.return_value = project

    folder = MagicMock()
    project.create_managed_folder.return_value = folder
    project.get_managed_folder.return_value = folder

    settings = MagicMock()
    settings.get_raw.return_value = raw
    folder.get_settings.return_value = settings

    default_zone = MagicMock()
    default_zone.id = "default"
    folder.get_zone.return_value = default_zone

    registry = ResourceTypeRegistry()
    handler = ManagedFolderHandler()
    registry.register(ManagedFolderResource, handler)
    registry.register(FilesystemManagedFolderResource, handler)
    registry.register(UploadManagedFolderResource, handler)

    engine = DSSEngine(
        provider=provider,
        project_key="PRJ",
        state_path=tmp_path / "state.json",
        registry=registry,
    )
    return engine, project, folder


class TestEngineRoundtrip:
    def test_create_noop_update_delete_cycle(self, tmp_path: Path) -> None:
        raw = _make_raw(
            "Filesystem", params={"connection": "filesystem_managed", "path": "/models"}
        )
        engine, _project, folder = _setup_engine(tmp_path, raw)

        # --- CREATE ---
        mf = FilesystemManagedFolderResource(
            name="my_folder", connection="filesystem_managed", path="/models"
        )
        plan = engine.plan([mf])
        assert plan.changes[0].action == Action.CREATE
        engine.apply(plan)

        state = State.load(engine.state_path)
        assert "dss_filesystem_managed_folder.my_folder" in state.resources
        assert state.serial == 1

        # --- NOOP (same resource, no changes) ---
        plan2 = engine.plan([mf])
        assert plan2.changes[0].action == Action.NOOP
        engine.apply(plan2)

        state2 = State.load(engine.state_path)
        assert state2.serial == 1

        # --- UPDATE (change description) ---
        mf_updated = FilesystemManagedFolderResource(
            name="my_folder",
            connection="filesystem_managed",
            path="/models",
            description="updated",
        )
        plan3 = engine.plan([mf_updated])
        assert plan3.changes[0].action == Action.UPDATE

        raw["description"] = "updated"
        engine.apply(plan3)
        state3 = State.load(engine.state_path)
        assert state3.serial == 2
        assert (
            state3.resources["dss_filesystem_managed_folder.my_folder"].attributes["description"]
            == "updated"
        )

        # --- DELETE ---
        plan4 = engine.plan([])
        assert any(c.action == Action.DELETE for c in plan4.changes)
        engine.apply(plan4)

        state4 = State.load(engine.state_path)
        assert state4.resources == {}
        folder.delete.assert_called_once()

    def test_upload_roundtrip(self, tmp_path: Path) -> None:
        raw = _make_raw("UploadedFiles")
        engine, _project, _folder = _setup_engine(tmp_path, raw)

        mf = UploadManagedFolderResource(name="my_folder")
        plan = engine.plan([mf])
        assert plan.changes[0].action == Action.CREATE
        engine.apply(plan)

        plan2 = engine.plan([mf])
        assert plan2.changes[0].action == Action.NOOP

    def test_path_variable_substitution_noop(self, tmp_path: Path) -> None:
        """${projectKey} in DSS path should resolve to project key for comparison."""
        mf = FilesystemManagedFolderResource(
            name="my_folder", connection="filesystem_managed", path="PRJ/models"
        )

        raw = _make_raw(
            "Filesystem",
            params={"connection": "filesystem_managed", "path": "${projectKey}/models"},
        )
        engine, _project, _folder = _setup_engine(tmp_path, raw)

        plan = engine.plan([mf])
        assert plan.changes[0].action == Action.CREATE
        engine.apply(plan)

        plan2 = engine.plan([mf])
        assert plan2.changes[0].action == Action.NOOP
