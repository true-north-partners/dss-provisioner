"""Tests for the DatasetHandler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from dss_provisioner.core import DSSProvider, ResourceInstance
from dss_provisioner.core.state import State
from dss_provisioner.engine import DSSEngine
from dss_provisioner.engine.dataset_handler import DatasetHandler
from dss_provisioner.engine.handlers import EngineContext
from dss_provisioner.engine.registry import ResourceTypeRegistry
from dss_provisioner.engine.types import Action
from dss_provisioner.resources.dataset import (
    Column,
    DatasetResource,
    OracleDatasetResource,
    SnowflakeDatasetResource,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def mock_client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def ctx(mock_client: MagicMock) -> EngineContext:
    provider = DSSProvider.from_client(mock_client)
    return EngineContext(provider=provider, project_key="PRJ")


@pytest.fixture
def handler() -> DatasetHandler:
    return DatasetHandler()


@pytest.fixture
def mock_project(mock_client: MagicMock) -> MagicMock:
    project = MagicMock()
    mock_client.get_project.return_value = project
    return project


@pytest.fixture
def mock_dataset(mock_project: MagicMock) -> MagicMock:
    dataset = MagicMock()
    mock_project.get_dataset.return_value = dataset
    mock_project.create_dataset.return_value = dataset

    # Default settings/metadata/schema
    settings = MagicMock()
    raw: dict[str, Any] = {"type": "Filesystem", "params": {}, "managed": False}
    settings.get_raw.return_value = raw
    dataset.get_settings.return_value = settings
    dataset.get_metadata.return_value = {"description": "", "tags": []}
    dataset.get_schema.return_value = {"columns": []}

    return dataset


def _make_raw(
    ds_type: str = "Filesystem",
    params: dict[str, Any] | None = None,
    managed: bool = False,
    format_type: str | None = None,
    format_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw: dict[str, Any] = {"type": ds_type, "params": params or {}, "managed": managed}
    if format_type is not None:
        raw["formatType"] = format_type
    if format_params is not None:
        raw["formatParams"] = format_params
    return raw


class TestCreateExternalDataset:
    def test_calls_create_dataset(
        self,
        ctx: EngineContext,
        handler: DatasetHandler,
        mock_project: MagicMock,
        mock_dataset: MagicMock,  # noqa: ARG002
    ) -> None:
        desired = DatasetResource(name="my_ds", dataset_type="Filesystem")
        handler.create(ctx, desired)
        mock_project.create_dataset.assert_called_once_with("my_ds", "Filesystem", params={})

    def test_passes_connection_param(
        self,
        ctx: EngineContext,
        handler: DatasetHandler,
        mock_project: MagicMock,
        mock_dataset: MagicMock,  # noqa: ARG002
    ) -> None:
        desired = DatasetResource(name="my_ds", dataset_type="Filesystem", connection="local")
        handler.create(ctx, desired)
        mock_project.create_dataset.assert_called_once_with(
            "my_ds", "Filesystem", params={"connection": "local"}
        )


class TestCreateManagedDataset:
    def test_calls_new_managed_dataset(
        self,
        ctx: EngineContext,
        handler: DatasetHandler,
        mock_project: MagicMock,
        mock_dataset: MagicMock,
    ) -> None:
        builder = MagicMock()
        builder.create.return_value = mock_dataset
        mock_project.new_managed_dataset.return_value = builder

        desired = DatasetResource(
            name="my_ds", dataset_type="Filesystem", managed=True, connection="conn"
        )
        handler.create(ctx, desired)

        mock_project.new_managed_dataset.assert_called_once_with("my_ds")
        builder.with_store_into.assert_called_once_with("conn")
        builder.create.assert_called_once()


class TestCreateSnowflakeDataset:
    def test_snowflake_params_mapping(
        self,
        ctx: EngineContext,
        handler: DatasetHandler,
        mock_project: MagicMock,
        mock_dataset: MagicMock,
    ) -> None:
        raw = _make_raw(
            "Snowflake",
            params={
                "connection": "sf_conn",
                "schema": "PUBLIC",
                "table": "users",
                "writeMode": "OVERWRITE",
            },
        )
        mock_dataset.get_settings.return_value.get_raw.return_value = raw

        desired = SnowflakeDatasetResource(
            name="my_ds", connection="sf_conn", schema_name="PUBLIC", table="users"
        )
        handler.create(ctx, desired)

        mock_project.create_dataset.assert_called_once_with(
            "my_ds",
            "Snowflake",
            params={
                "connection": "sf_conn",
                "schema": "PUBLIC",
                "table": "users",
                "writeMode": "OVERWRITE",
            },
        )


class TestCreateOracleDataset:
    def test_oracle_params_mapping(
        self,
        ctx: EngineContext,
        handler: DatasetHandler,
        mock_project: MagicMock,
        mock_dataset: MagicMock,
    ) -> None:
        raw = _make_raw(
            "Oracle",
            params={"connection": "ora_conn", "schema": "HR", "table": "employees"},
        )
        mock_dataset.get_settings.return_value.get_raw.return_value = raw

        desired = OracleDatasetResource(
            name="my_ds", connection="ora_conn", schema_name="HR", table="employees"
        )
        handler.create(ctx, desired)

        mock_project.create_dataset.assert_called_once_with(
            "my_ds",
            "Oracle",
            params={
                "connection": "ora_conn",
                "schema": "HR",
                "table": "employees",
            },
        )


class TestCreateSetsSchemaWhenColumnsProvided:
    def test_schema_applied(
        self,
        ctx: EngineContext,
        handler: DatasetHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_dataset: MagicMock,
    ) -> None:
        desired = DatasetResource(
            name="my_ds",
            dataset_type="Filesystem",
            columns=[Column(name="id", type="int", description="Primary key")],
        )
        handler.create(ctx, desired)

        mock_dataset.set_schema.assert_called_once_with(
            {
                "columns": [
                    {"name": "id", "type": "int", "comment": "Primary key", "meaning": None}
                ],
                "userModified": True,
            }
        )

    def test_no_schema_when_no_columns(
        self,
        ctx: EngineContext,
        handler: DatasetHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_dataset: MagicMock,
    ) -> None:
        desired = DatasetResource(name="my_ds", dataset_type="Filesystem")
        handler.create(ctx, desired)
        mock_dataset.set_schema.assert_not_called()


class TestCreateSetsFormatWhenSpecified:
    def test_format_applied(
        self,
        ctx: EngineContext,
        handler: DatasetHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_dataset: MagicMock,
    ) -> None:
        raw: dict[str, Any] = {"type": "Filesystem", "params": {}, "managed": False}
        mock_dataset.get_settings.return_value.get_raw.return_value = raw

        desired = DatasetResource(
            name="my_ds",
            dataset_type="Filesystem",
            format_type="csv",
            format_params={"separator": ",", "style": "unix"},
        )
        handler.create(ctx, desired)

        assert raw["formatType"] == "csv"
        assert raw["formatParams"] == {"separator": ",", "style": "unix"}
        mock_dataset.get_settings.return_value.save.assert_called()


class TestCreateSetsZoneWhenSpecified:
    def test_zone_applied(
        self,
        ctx: EngineContext,
        handler: DatasetHandler,
        mock_project: MagicMock,
        mock_dataset: MagicMock,
    ) -> None:
        flow = MagicMock()
        zone = MagicMock()
        mock_project.get_flow.return_value = flow
        flow.get_zone.return_value = zone

        desired = DatasetResource(name="my_ds", dataset_type="Filesystem", zone="raw")
        handler.create(ctx, desired)

        flow.get_zone.assert_called_once_with("raw")
        zone.add_item.assert_called_once_with(mock_dataset)

    def test_no_zone_when_not_specified(
        self,
        ctx: EngineContext,
        handler: DatasetHandler,
        mock_project: MagicMock,
        mock_dataset: MagicMock,  # noqa: ARG002
    ) -> None:
        desired = DatasetResource(name="my_ds", dataset_type="Filesystem")
        handler.create(ctx, desired)
        mock_project.get_flow.assert_not_called()


class TestRead:
    def test_returns_attributes(
        self,
        ctx: EngineContext,
        handler: DatasetHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_dataset: MagicMock,
    ) -> None:
        mock_dataset.exists.return_value = True
        raw = _make_raw("Filesystem", params={"connection": "local"})
        mock_dataset.get_settings.return_value.get_raw.return_value = raw
        mock_dataset.get_metadata.return_value = {"description": "A dataset", "tags": ["tag1"]}
        mock_dataset.get_schema.return_value = {
            "columns": [{"name": "id", "type": "int", "comment": "pk", "meaning": None}]
        }

        prior = ResourceInstance(
            address="dss_dataset.my_ds",
            resource_type="dss_dataset",
            name="my_ds",
            attributes={"zone": None},
        )
        result = handler.read(ctx, prior)

        assert result is not None
        assert result["name"] == "my_ds"
        assert result["dataset_type"] == "Filesystem"
        assert result["connection"] == "local"
        assert result["description"] == "A dataset"
        assert result["tags"] == ["tag1"]
        assert result["columns"] == [
            {"name": "id", "type": "int", "description": "pk", "meaning": None}
        ]

    def test_returns_none_when_deleted(
        self,
        ctx: EngineContext,
        handler: DatasetHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_dataset: MagicMock,
    ) -> None:
        mock_dataset.exists.return_value = False

        prior = ResourceInstance(
            address="dss_dataset.my_ds",
            resource_type="dss_dataset",
            name="my_ds",
        )
        result = handler.read(ctx, prior)
        assert result is None


class TestUpdate:
    def test_saves_settings(
        self,
        ctx: EngineContext,
        handler: DatasetHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_dataset: MagicMock,
    ) -> None:
        raw = _make_raw("Filesystem", params={"connection": "local"})
        mock_dataset.get_settings.return_value.get_raw.return_value = raw

        desired = DatasetResource(name="my_ds", dataset_type="Filesystem", connection="new_conn")
        prior = ResourceInstance(
            address="dss_dataset.my_ds",
            resource_type="dss_dataset",
            name="my_ds",
        )
        handler.update(ctx, desired, prior)

        mock_dataset.get_settings.return_value.save.assert_called()
        assert raw["params"]["connection"] == "new_conn"


class TestDelete:
    def test_delete_external(
        self,
        ctx: EngineContext,
        handler: DatasetHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_dataset: MagicMock,
    ) -> None:
        prior = ResourceInstance(
            address="dss_dataset.my_ds",
            resource_type="dss_dataset",
            name="my_ds",
            attributes={"managed": False},
        )
        handler.delete(ctx, prior)
        mock_dataset.delete.assert_called_once_with(drop_data=False)

    def test_delete_managed(
        self,
        ctx: EngineContext,
        handler: DatasetHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_dataset: MagicMock,
    ) -> None:
        prior = ResourceInstance(
            address="dss_dataset.my_ds",
            resource_type="dss_dataset",
            name="my_ds",
            attributes={"managed": True},
        )
        handler.delete(ctx, prior)
        mock_dataset.delete.assert_called_once_with(drop_data=True)


def _setup_engine(
    tmp_path: Path,
    raw: dict[str, Any],
    meta: dict[str, Any] | None = None,
    schema: dict[str, Any] | None = None,
) -> tuple[DSSEngine, MagicMock, MagicMock]:
    """Wire up a DSSEngine with DatasetHandler and mocked dataikuapi objects."""
    mock_client = MagicMock()
    provider = DSSProvider.from_client(mock_client)

    project = MagicMock()
    mock_client.get_project.return_value = project

    dataset = MagicMock()
    project.create_dataset.return_value = dataset
    project.get_dataset.return_value = dataset

    settings = MagicMock()
    settings.get_raw.return_value = raw
    dataset.get_settings.return_value = settings
    dataset.get_metadata.return_value = meta or {"description": "", "tags": []}
    dataset.get_schema.return_value = schema or {"columns": []}
    dataset.exists.return_value = True

    registry = ResourceTypeRegistry()
    handler = DatasetHandler()
    registry.register(DatasetResource, handler)
    registry.register(SnowflakeDatasetResource, handler)
    registry.register(OracleDatasetResource, handler)

    engine = DSSEngine(
        provider=provider,
        project_key="PRJ",
        state_path=tmp_path / "state.json",
        registry=registry,
    )
    return engine, project, dataset


class TestEngineIntegrationRoundtrip:
    def test_create_noop_update_delete_cycle(self, tmp_path: Path) -> None:
        raw = _make_raw("Filesystem", params={"connection": "local"})
        engine, _project, dataset = _setup_engine(tmp_path, raw)

        # --- CREATE ---
        ds = DatasetResource(name="my_ds", dataset_type="Filesystem", connection="local")
        plan = engine.plan([ds])
        assert plan.changes[0].action == Action.CREATE
        engine.apply(plan)

        state = State.load(engine.state_path)
        assert "dss_dataset.my_ds" in state.resources
        assert state.resources["dss_dataset.my_ds"].attributes["name"] == "my_ds"
        assert state.serial == 1

        # --- NOOP (same resource, no changes) ---
        plan2 = engine.plan([ds])
        assert plan2.changes[0].action == Action.NOOP
        engine.apply(plan2)

        state2 = State.load(engine.state_path)
        assert state2.serial == 1  # no state write on NOOP

        # --- UPDATE (change description) ---
        # Mock still returns old metadata during refresh; desired has new value.
        ds_updated = DatasetResource(
            name="my_ds", dataset_type="Filesystem", connection="local", description="updated"
        )
        plan3 = engine.plan([ds_updated])
        assert plan3.changes[0].action == Action.UPDATE
        assert plan3.changes[0].diff is not None
        assert plan3.changes[0].diff["description"]["from"] == ""
        assert plan3.changes[0].diff["description"]["to"] == "updated"

        # After apply the handler will read back; mock the updated metadata
        dataset.get_metadata.return_value = {"description": "updated", "tags": []}
        engine.apply(plan3)
        state3 = State.load(engine.state_path)
        assert state3.serial == 2
        assert state3.resources["dss_dataset.my_ds"].attributes["description"] == "updated"

        # --- DELETE ---
        plan4 = engine.plan([])
        assert any(c.action == Action.DELETE for c in plan4.changes)
        engine.apply(plan4)

        state4 = State.load(engine.state_path)
        assert state4.resources == {}
        dataset.delete.assert_called_once_with(drop_data=False)

    def test_snowflake_roundtrip(self, tmp_path: Path) -> None:
        raw = _make_raw(
            "Snowflake",
            params={
                "connection": "sf_conn",
                "schema": "PUBLIC",
                "table": "users",
                "writeMode": "OVERWRITE",
            },
        )
        engine, _project, _dataset = _setup_engine(tmp_path, raw)

        ds = SnowflakeDatasetResource(
            name="sf_ds", connection="sf_conn", schema_name="PUBLIC", table="users"
        )
        plan = engine.plan([ds])
        assert plan.changes[0].action == Action.CREATE
        engine.apply(plan)

        # NOOP — verify attributes roundtrip correctly
        plan2 = engine.plan([ds])
        assert plan2.changes[0].action == Action.NOOP

        # UPDATE — change write_mode
        raw["params"]["writeMode"] = "APPEND"
        ds_updated = SnowflakeDatasetResource(
            name="sf_ds",
            connection="sf_conn",
            schema_name="PUBLIC",
            table="users",
            write_mode="APPEND",
        )
        plan3 = engine.plan([ds_updated])
        assert plan3.changes[0].action == Action.NOOP  # read refreshed the state

    def test_oracle_roundtrip(self, tmp_path: Path) -> None:
        raw = _make_raw(
            "Oracle",
            params={"connection": "ora_conn", "schema": "HR", "table": "employees"},
        )
        engine, *_ = _setup_engine(tmp_path, raw)

        ds = OracleDatasetResource(
            name="ora_ds", connection="ora_conn", schema_name="HR", table="employees"
        )
        plan = engine.plan([ds])
        assert plan.changes[0].action == Action.CREATE
        engine.apply(plan)

        # NOOP — verify attributes roundtrip correctly
        plan2 = engine.plan([ds])
        assert plan2.changes[0].action == Action.NOOP
