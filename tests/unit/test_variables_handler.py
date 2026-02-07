"""Tests for the VariablesHandler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from dss_provisioner.core import DSSProvider, ResourceInstance
from dss_provisioner.core.state import State
from dss_provisioner.engine import DSSEngine
from dss_provisioner.engine.handlers import EngineContext
from dss_provisioner.engine.registry import ResourceTypeRegistry
from dss_provisioner.engine.types import Action
from dss_provisioner.engine.variables_handler import VariablesHandler
from dss_provisioner.resources.variables import VariablesResource

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def mock_client() -> MagicMock:
    client = MagicMock()
    client.get_variables.return_value = {}
    return client


@pytest.fixture
def mock_project(mock_client: MagicMock) -> MagicMock:
    project = MagicMock()
    project.get_variables.return_value = {"standard": {}, "local": {}}
    mock_client.get_project.return_value = project
    return project


@pytest.fixture
def ctx(mock_client: MagicMock) -> EngineContext:
    provider = DSSProvider.from_client(mock_client)
    return EngineContext(provider=provider, project_key="PRJ")


@pytest.fixture
def handler() -> VariablesHandler:
    return VariablesHandler()


class TestCreate:
    def test_calls_set_variables(
        self,
        ctx: EngineContext,
        handler: VariablesHandler,
        mock_project: MagicMock,
    ) -> None:
        desired = VariablesResource(standard={"env": "prod"}, local={"debug": "false"})
        # Simulate DSS returning what was set
        mock_project.get_variables.return_value = {
            "standard": {"env": "prod"},
            "local": {"debug": "false"},
        }

        result = handler.create(ctx, desired)

        mock_project.set_variables.assert_called_once_with(
            {"standard": {"env": "prod"}, "local": {"debug": "false"}}
        )
        assert result["standard"] == {"env": "prod"}
        assert result["local"] == {"debug": "false"}
        assert result["name"] == "variables"

    def test_preserves_existing_keys(
        self,
        ctx: EngineContext,
        handler: VariablesHandler,
        mock_project: MagicMock,
    ) -> None:
        """Existing DSS keys not in config are preserved on create."""
        mock_project.get_variables.return_value = {
            "standard": {"existing": "keep_me"},
            "local": {"other": "preserved"},
        }

        desired = VariablesResource(standard={"env": "prod"})
        handler.create(ctx, desired)

        mock_project.set_variables.assert_called_once_with(
            {"standard": {"existing": "keep_me", "env": "prod"}, "local": {"other": "preserved"}}
        )


class TestRead:
    def test_returns_current_state(
        self,
        ctx: EngineContext,
        handler: VariablesHandler,
        mock_project: MagicMock,
    ) -> None:
        mock_project.get_variables.return_value = {
            "standard": {"env": "staging"},
            "local": {"x": "1"},
        }

        prior = ResourceInstance(
            address="dss_variables.variables",
            resource_type="dss_variables",
            name="variables",
        )
        result = handler.read(ctx, prior)

        assert result is not None
        assert result["standard"] == {"env": "staging"}
        assert result["local"] == {"x": "1"}
        assert result["description"] == ""
        assert result["tags"] == []

    def test_empty_variables(
        self,
        ctx: EngineContext,
        handler: VariablesHandler,
        mock_project: MagicMock,
    ) -> None:
        mock_project.get_variables.return_value = {"standard": {}, "local": {}}

        prior = ResourceInstance(
            address="dss_variables.variables",
            resource_type="dss_variables",
            name="variables",
        )
        result = handler.read(ctx, prior)

        assert result is not None
        assert result["standard"] == {}
        assert result["local"] == {}


class TestUpdate:
    def test_calls_set_variables(
        self,
        ctx: EngineContext,
        handler: VariablesHandler,
        mock_project: MagicMock,
    ) -> None:
        desired = VariablesResource(standard={"env": "prod"})
        mock_project.get_variables.return_value = {
            "standard": {"env": "prod"},
            "local": {},
        }

        prior = ResourceInstance(
            address="dss_variables.variables",
            resource_type="dss_variables",
            name="variables",
        )
        result = handler.update(ctx, desired, prior)

        mock_project.set_variables.assert_called_once_with(
            {"standard": {"env": "prod"}, "local": {}}
        )
        assert result["standard"] == {"env": "prod"}

    def test_preserves_existing_keys(
        self,
        ctx: EngineContext,
        handler: VariablesHandler,
        mock_project: MagicMock,
    ) -> None:
        """Existing DSS keys not in config are preserved on update."""
        mock_project.get_variables.return_value = {
            "standard": {"existing": "keep_me", "env": "old"},
            "local": {},
        }

        desired = VariablesResource(standard={"env": "new"})
        prior = ResourceInstance(
            address="dss_variables.variables",
            resource_type="dss_variables",
            name="variables",
        )
        handler.update(ctx, desired, prior)

        mock_project.set_variables.assert_called_once_with(
            {"standard": {"existing": "keep_me", "env": "new"}, "local": {}}
        )


class TestDelete:
    def test_clears_variables(
        self,
        ctx: EngineContext,
        handler: VariablesHandler,
        mock_project: MagicMock,
    ) -> None:
        prior = ResourceInstance(
            address="dss_variables.variables",
            resource_type="dss_variables",
            name="variables",
        )
        handler.delete(ctx, prior)

        mock_project.set_variables.assert_called_once_with({"standard": {}, "local": {}})


class TestEngineRoundtrip:
    def _setup_engine(
        self,
        tmp_path: Path,
        project_vars: dict[str, Any],
    ) -> tuple[DSSEngine, MagicMock]:
        mock_client = MagicMock()
        mock_client.get_variables.return_value = {}
        provider = DSSProvider.from_client(mock_client)

        project = MagicMock()
        project.get_variables.return_value = project_vars
        mock_client.get_project.return_value = project

        registry = ResourceTypeRegistry()
        registry.register(VariablesResource, VariablesHandler())

        engine = DSSEngine(
            provider=provider,
            project_key="PRJ",
            state_path=tmp_path / "state.json",
            registry=registry,
        )
        return engine, project

    def test_create_noop_update_delete_cycle(self, tmp_path: Path) -> None:
        engine, project = self._setup_engine(tmp_path, {"standard": {"env": "prod"}, "local": {}})

        # --- CREATE ---
        v = VariablesResource(standard={"env": "prod"})
        plan = engine.plan([v])
        assert plan.changes[0].action == Action.CREATE
        engine.apply(plan)

        state = State.load(engine.state_path)
        assert "dss_variables.variables" in state.resources
        assert state.serial == 1

        # --- NOOP (same resource, no changes) ---
        plan2 = engine.plan([v])
        assert plan2.changes[0].action == Action.NOOP
        engine.apply(plan2)
        assert State.load(engine.state_path).serial == 1

        # --- UPDATE (desired differs from DSS) ---
        v2 = VariablesResource(standard={"env": "staging"})
        plan3 = engine.plan([v2])
        assert plan3.changes[0].action == Action.UPDATE
        # Simulate DSS returning new value after apply
        project.get_variables.return_value = {
            "standard": {"env": "staging"},
            "local": {},
        }
        engine.apply(plan3)
        assert State.load(engine.state_path).serial == 2

        # --- DELETE ---
        project.get_variables.return_value = {"standard": {}, "local": {}}
        plan4 = engine.plan([])
        assert any(c.action == Action.DELETE for c in plan4.changes)
        engine.apply(plan4)

        state4 = State.load(engine.state_path)
        assert state4.resources == {}

    def test_noop_with_extra_dss_keys(self, tmp_path: Path) -> None:
        """DSS has extra keys not in config → NOOP (partial comparison)."""
        engine, _project = self._setup_engine(
            tmp_path,
            {"standard": {"env": "prod", "extra_key": "extra_val"}, "local": {}},
        )

        # CREATE first
        v = VariablesResource(standard={"env": "prod"})
        plan = engine.plan([v])
        assert plan.changes[0].action == Action.CREATE
        engine.apply(plan)

        # Now plan again — DSS has extra_key but we only declare env
        plan2 = engine.plan([v])
        assert plan2.changes[0].action == Action.NOOP

    def test_variables_applied_before_datasets(self, tmp_path: Path) -> None:
        """Variables (priority 0) should appear before datasets (priority 100)."""
        from dss_provisioner.engine.dataset_handler import DatasetHandler
        from dss_provisioner.resources.dataset import DatasetResource

        mock_client = MagicMock()
        mock_client.get_variables.return_value = {}
        provider = DSSProvider.from_client(mock_client)

        project = MagicMock()
        project.get_variables.return_value = {"standard": {"env": "prod"}, "local": {}}
        mock_client.get_project.return_value = project

        # Mock dataset read
        dataset = MagicMock()
        project.get_dataset.return_value = dataset
        project.create_dataset.return_value = dataset
        settings = MagicMock()
        settings.get_raw.return_value = {"type": "Filesystem", "params": {}, "managed": False}
        dataset.get_settings.return_value = settings
        dataset.get_metadata.return_value = {"description": "", "tags": []}
        dataset.get_schema.return_value = {"columns": []}
        default_zone = MagicMock()
        default_zone.id = "default"
        dataset.get_zone.return_value = default_zone

        registry = ResourceTypeRegistry()
        registry.register(VariablesResource, VariablesHandler())
        registry.register(DatasetResource, DatasetHandler())

        engine = DSSEngine(
            provider=provider,
            project_key="PRJ",
            state_path=tmp_path / "state.json",
            registry=registry,
        )

        v = VariablesResource(standard={"env": "prod"})
        ds = DatasetResource(name="my_ds", type="Filesystem")

        # Plan with dataset listed BEFORE variables
        plan = engine.plan([ds, v])
        addrs = [c.address for c in plan.changes]
        assert addrs.index("dss_variables.variables") < addrs.index("dss_dataset.my_ds")
