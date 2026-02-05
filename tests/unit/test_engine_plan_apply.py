from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest

from dss_provisioner.core import DSSProvider, ResourceInstance
from dss_provisioner.core.state import State
from dss_provisioner.engine import DSSEngine
from dss_provisioner.engine.errors import StalePlanError
from dss_provisioner.engine.handlers import EngineContext, ResourceHandler
from dss_provisioner.engine.registry import ResourceTypeRegistry
from dss_provisioner.engine.types import Action, Plan
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


def test_create_update_delete_and_plan_roundtrip(tmp_path: Path) -> None:
    engine, handler = _engine(tmp_path)

    r1 = DummyResource(name="r1", value=1)

    plan1 = engine.plan([r1])
    assert plan1.changes[0].action == Action.CREATE

    plan_path = tmp_path / "plan.json"
    plan1.save(plan_path)
    loaded = Plan.load(plan_path)

    engine.apply(loaded)

    state = State.load(engine.state_path)
    assert state.serial == 1
    assert "dummy.r1" in state.resources
    assert handler.store["dummy.r1"]["value"] == 1

    plan2 = engine.plan([r1])
    assert plan2.changes[0].action == Action.NOOP
    engine.apply(plan2)

    state2 = State.load(engine.state_path)
    assert state2.serial == 1  # NOOP does not write state

    r1b = DummyResource(name="r1", value=2)
    plan3 = engine.plan([r1b])
    assert plan3.changes[0].action == Action.UPDATE
    assert plan3.changes[0].diff is not None
    assert plan3.changes[0].diff["value"]["from"] == 1
    assert plan3.changes[0].diff["value"]["to"] == 2

    engine.apply(plan3)
    assert handler.store["dummy.r1"]["value"] == 2

    backup_path = Path(str(engine.state_path) + ".backup")
    assert backup_path.exists()

    plan4 = engine.plan([])
    assert any(c.action == Action.DELETE for c in plan4.changes)
    engine.apply(plan4)

    state3 = State.load(engine.state_path)
    assert state3.resources == {}


def test_dependency_ordering(tmp_path: Path) -> None:
    engine, _handler = _engine(tmp_path)

    b = DummyResource(name="b", value=1)
    a = DummyResource(name="a", value=1, depends_on=[b.address])

    plan = engine.plan([a, b])
    actions = [(c.address, c.action) for c in plan.changes]
    assert actions[0] == ("dummy.b", Action.CREATE)
    assert actions[1] == ("dummy.a", Action.CREATE)


def test_stale_plan_detection(tmp_path: Path) -> None:
    engine, _handler = _engine(tmp_path)

    r1 = DummyResource(name="r1", value=1)
    engine.apply(engine.plan([r1]))

    plan_update = engine.plan([DummyResource(name="r1", value=2)])

    # Simulate external state change
    state = State.load(engine.state_path)
    state.serial += 1
    state.save(engine.state_path)

    with pytest.raises(StalePlanError):
        engine.apply(plan_update)
