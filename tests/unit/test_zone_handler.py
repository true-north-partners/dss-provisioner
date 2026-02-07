"""Tests for the ZoneHandler."""

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
from dss_provisioner.engine.zone_handler import ZoneHandler
from dss_provisioner.resources.dataset import DatasetResource
from dss_provisioner.resources.zone import ZoneResource

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
def handler() -> ZoneHandler:
    return ZoneHandler()


@pytest.fixture
def mock_flow(mock_client: MagicMock) -> MagicMock:
    flow = MagicMock()
    project = MagicMock()
    project.get_flow.return_value = flow
    mock_client.get_project.return_value = project
    return flow


def _make_zone(zone_id: str = "raw", name: str = "raw", color: str = "#2ab1ac") -> MagicMock:
    """Create a mock DSSFlowZone."""
    zone = MagicMock()
    zone.id = zone_id
    zone.name = name

    raw: dict[str, Any] = {"id": zone_id, "name": name, "color": color}
    settings = MagicMock()
    settings.get_raw.return_value = raw
    zone.get_settings.return_value = settings
    return zone


class TestCreate:
    def test_calls_create_zone(
        self,
        ctx: EngineContext,
        handler: ZoneHandler,
        mock_flow: MagicMock,
    ) -> None:
        mock_flow.create_zone.return_value = _make_zone("raw", "raw", "#2ab1ac")

        desired = ZoneResource(name="raw")
        result = handler.create(ctx, desired)

        mock_flow.create_zone.assert_called_once_with("raw", color="#2ab1ac")
        assert result["name"] == "raw"
        assert result["color"] == "#2ab1ac"

    def test_custom_color(
        self,
        ctx: EngineContext,
        handler: ZoneHandler,
        mock_flow: MagicMock,
    ) -> None:
        mock_flow.create_zone.return_value = _make_zone("curated", "curated", "#ff5733")

        desired = ZoneResource(name="curated", color="#ff5733")
        handler.create(ctx, desired)

        mock_flow.create_zone.assert_called_once_with("curated", color="#ff5733")

    def test_raises_on_api_error(
        self,
        ctx: EngineContext,
        handler: ZoneHandler,
        mock_flow: MagicMock,
    ) -> None:
        mock_flow.create_zone.side_effect = Exception("404 Not Found")

        desired = ZoneResource(name="raw")
        with pytest.raises(RuntimeError, match="Failed to create zone"):
            handler.create(ctx, desired)


class TestRead:
    def test_returns_attributes(
        self,
        ctx: EngineContext,
        handler: ZoneHandler,
        mock_flow: MagicMock,
    ) -> None:
        mock_flow.list_zones.return_value = [_make_zone("raw", "raw", "#aabbcc")]

        prior = ResourceInstance(
            address="dss_zone.raw",
            resource_type="dss_zone",
            name="raw",
        )
        result = handler.read(ctx, prior)

        assert result is not None
        assert result["name"] == "raw"
        assert result["color"] == "#aabbcc"

    def test_returns_none_when_not_found(
        self,
        ctx: EngineContext,
        handler: ZoneHandler,
        mock_flow: MagicMock,
    ) -> None:
        mock_flow.list_zones.return_value = [_make_zone("other")]

        prior = ResourceInstance(
            address="dss_zone.raw",
            resource_type="dss_zone",
            name="raw",
        )
        result = handler.read(ctx, prior)
        assert result is None

    def test_returns_none_when_api_unavailable(
        self,
        ctx: EngineContext,
        handler: ZoneHandler,
        mock_flow: MagicMock,
    ) -> None:
        mock_flow.list_zones.side_effect = Exception("404 Not Found")

        prior = ResourceInstance(
            address="dss_zone.raw",
            resource_type="dss_zone",
            name="raw",
        )
        result = handler.read(ctx, prior)
        assert result is None


class TestUpdate:
    def test_updates_color(
        self,
        ctx: EngineContext,
        handler: ZoneHandler,
        mock_flow: MagicMock,
    ) -> None:
        zone = _make_zone("raw", "raw", "#2ab1ac")
        mock_flow.list_zones.return_value = [zone]

        desired = ZoneResource(name="raw", color="#ff0000")
        prior = ResourceInstance(
            address="dss_zone.raw",
            resource_type="dss_zone",
            name="raw",
        )
        handler.update(ctx, desired, prior)

        raw = zone.get_settings().get_raw()
        assert raw["color"] == "#ff0000"
        zone.get_settings().save.assert_called_once()

    def test_raises_when_zone_not_found(
        self,
        ctx: EngineContext,
        handler: ZoneHandler,
        mock_flow: MagicMock,
    ) -> None:
        mock_flow.list_zones.return_value = []

        desired = ZoneResource(name="raw")
        prior = ResourceInstance(
            address="dss_zone.raw",
            resource_type="dss_zone",
            name="raw",
        )
        with pytest.raises(RuntimeError, match="not found for update"):
            handler.update(ctx, desired, prior)


class TestDelete:
    def test_deletes_zone(
        self,
        ctx: EngineContext,
        handler: ZoneHandler,
        mock_flow: MagicMock,
    ) -> None:
        zone = _make_zone("raw")
        mock_flow.list_zones.return_value = [zone]

        prior = ResourceInstance(
            address="dss_zone.raw",
            resource_type="dss_zone",
            name="raw",
        )
        handler.delete(ctx, prior)
        zone.delete.assert_called_once()

    def test_noop_when_zone_not_found(
        self,
        ctx: EngineContext,
        handler: ZoneHandler,
        mock_flow: MagicMock,
    ) -> None:
        mock_flow.list_zones.return_value = []

        prior = ResourceInstance(
            address="dss_zone.raw",
            resource_type="dss_zone",
            name="raw",
        )
        handler.delete(ctx, prior)  # should not raise

    def test_noop_when_api_unavailable(
        self,
        ctx: EngineContext,
        handler: ZoneHandler,
        mock_flow: MagicMock,
    ) -> None:
        mock_flow.list_zones.side_effect = Exception("404 Not Found")

        prior = ResourceInstance(
            address="dss_zone.raw",
            resource_type="dss_zone",
            name="raw",
        )
        handler.delete(ctx, prior)  # should not raise


class TestEngineIntegrationRoundtrip:
    def _setup_engine(self, tmp_path: Path, zones: list[MagicMock]) -> tuple[DSSEngine, MagicMock]:
        """Wire up a DSSEngine with ZoneHandler and mocked flow."""
        mock_client = MagicMock()
        provider = DSSProvider.from_client(mock_client)

        project = MagicMock()
        mock_client.get_project.return_value = project

        flow = MagicMock()
        project.get_flow.return_value = flow
        flow.list_zones.return_value = zones

        registry = ResourceTypeRegistry()
        registry.register(ZoneResource, ZoneHandler())

        engine = DSSEngine(
            provider=provider,
            project_key="PRJ",
            state_path=tmp_path / "state.json",
            registry=registry,
        )
        return engine, flow

    def test_create_noop_delete_cycle(self, tmp_path: Path) -> None:
        zone_mock = _make_zone("raw", "raw", "#2ab1ac")
        engine, flow = self._setup_engine(tmp_path, [zone_mock])
        flow.create_zone.return_value = zone_mock

        # --- CREATE ---
        z = ZoneResource(name="raw")
        plan = engine.plan([z])
        assert plan.changes[0].action == Action.CREATE
        engine.apply(plan)

        state = State.load(engine.state_path)
        assert "dss_zone.raw" in state.resources
        assert state.serial == 1

        # --- NOOP (same resource, no changes) ---
        plan2 = engine.plan([z])
        assert plan2.changes[0].action == Action.NOOP
        engine.apply(plan2)
        assert State.load(engine.state_path).serial == 1

        # --- DELETE ---
        plan3 = engine.plan([])
        assert any(c.action == Action.DELETE for c in plan3.changes)
        engine.apply(plan3)

        state3 = State.load(engine.state_path)
        assert state3.resources == {}
        zone_mock.delete.assert_called_once()

    def test_update_color(self, tmp_path: Path) -> None:
        zone_mock = _make_zone("raw", "raw", "#2ab1ac")
        engine, flow = self._setup_engine(tmp_path, [zone_mock])
        flow.create_zone.return_value = zone_mock

        # CREATE baseline
        z = ZoneResource(name="raw")
        engine.apply(engine.plan([z]))

        # Desired has new color, DSS still returns old
        z_updated = ZoneResource(name="raw", color="#ff0000")
        plan = engine.plan([z_updated])
        assert plan.changes[0].action == Action.UPDATE
        assert plan.changes[0].diff is not None
        assert plan.changes[0].diff["color"]["from"] == "#2ab1ac"
        assert plan.changes[0].diff["color"]["to"] == "#ff0000"

        # Simulate DSS returning new color after apply
        zone_mock.get_settings().get_raw()["color"] = "#ff0000"
        engine.apply(plan)


class TestZoneAutoDependency:
    """Verify that datasets with zone: X auto-depend on dss_zone.X."""

    def test_dataset_zone_creates_dependency(self, tmp_path: Path) -> None:
        mock_client = MagicMock()
        mock_client.get_variables.return_value = {}
        provider = DSSProvider.from_client(mock_client)

        project = MagicMock()
        project.get_variables.return_value = {"standard": {}, "local": {}}
        mock_client.get_project.return_value = project

        # Zone mock
        flow = MagicMock()
        project.get_flow.return_value = flow
        zone_mock = _make_zone("raw", "raw", "#2ab1ac")
        flow.list_zones.return_value = [zone_mock]
        flow.create_zone.return_value = zone_mock

        # Dataset mock
        dataset = MagicMock()
        project.create_dataset.return_value = dataset
        project.get_dataset.return_value = dataset

        settings = MagicMock()
        raw: dict[str, Any] = {"type": "Filesystem", "params": {}, "managed": False}
        settings.get_raw.return_value = raw
        dataset.get_settings.return_value = settings
        dataset.get_metadata.return_value = {"description": "", "tags": []}
        dataset.get_schema.return_value = {"columns": []}
        default_zone = MagicMock()
        default_zone.id = "raw"
        dataset.get_zone.return_value = default_zone

        registry = ResourceTypeRegistry()
        registry.register(ZoneResource, ZoneHandler())
        registry.register(DatasetResource, DatasetHandler())

        engine = DSSEngine(
            provider=provider,
            project_key="PRJ",
            state_path=tmp_path / "state.json",
            registry=registry,
        )

        # Zone and dataset — dataset references the zone
        zone = ZoneResource(name="raw")
        ds = DatasetResource(name="my_ds", type="Filesystem", zone="raw")

        # Plan with dataset listed BEFORE zone — engine should still order zone first
        plan = engine.plan([ds, zone])

        addrs = [c.address for c in plan.changes]
        assert addrs.index("dss_zone.raw") < addrs.index("dss_dataset.my_ds")
