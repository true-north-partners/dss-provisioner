from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock

from dss_provisioner.core import DSSProvider, ResourceInstance
from dss_provisioner.engine import DSSEngine
from dss_provisioner.engine.handlers import EngineContext, ResourceHandler
from dss_provisioner.engine.registry import ResourceTypeRegistry
from dss_provisioner.resources.base import Resource


class DummyResource(Resource):
    resource_type: ClassVar[str] = "dummy"
    value: int


def _attrs(resource: DummyResource) -> dict[str, Any]:
    return {
        "id": resource.name,
        "name": resource.name,
        "description": resource.description,
        "tags": list(resource.tags),
        "value": resource.value,
    }


class InMemoryHandler(ResourceHandler[DummyResource]):
    def __init__(self) -> None:
        self.store: dict[str, dict[str, Any]] = {}

    def read(self, ctx: EngineContext, prior: ResourceInstance) -> dict[str, Any] | None:
        _ = ctx
        return self.store.get(prior.address)

    def create(self, ctx: EngineContext, desired: DummyResource) -> dict[str, Any]:
        _ = ctx
        attrs = _attrs(desired)
        self.store[desired.address] = dict(attrs)
        return attrs

    def update(
        self,
        ctx: EngineContext,
        desired: DummyResource,
        prior: ResourceInstance,
    ) -> dict[str, Any]:
        _ = ctx
        _ = prior
        attrs = _attrs(desired)
        self.store[desired.address] = dict(attrs)
        return attrs

    def delete(self, ctx: EngineContext, prior: ResourceInstance) -> None:
        _ = ctx
        self.store.pop(prior.address, None)


def _engine(tmp_path: Path) -> tuple[DSSEngine, InMemoryHandler]:
    provider = DSSProvider.from_client(MagicMock())
    registry = ResourceTypeRegistry()
    handler = InMemoryHandler()
    registry.register(DummyResource, handler)
    engine = DSSEngine(
        provider=provider,
        project_key="PRJ",
        state_path=tmp_path / "state.json",
        registry=registry,
    )
    return engine, handler


def test_refresh_updates_state_and_writes_backup(tmp_path: Path) -> None:
    engine, handler = _engine(tmp_path)

    r1 = DummyResource(name="r1", value=1)
    engine.apply(engine.plan([r1]))

    # Simulate drift
    handler.store["dummy.r1"]["value"] = 99
    state = engine.refresh()

    assert state.serial == 2
    assert state.resources["dummy.r1"].attributes["value"] == 99

    backup_path = Path(str(engine.state_path) + ".backup")
    assert backup_path.exists()

    # Simulate deletion out-of-band
    handler.store.pop("dummy.r1")
    state2 = engine.refresh()

    assert state2.serial == 3
    assert state2.resources == {}
