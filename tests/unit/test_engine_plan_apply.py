from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest
from pydantic import Field

from dss_provisioner.core import DSSProvider, ResourceInstance
from dss_provisioner.core.state import State
from dss_provisioner.engine import DSSEngine
from dss_provisioner.engine.engine import _values_differ
from dss_provisioner.engine.errors import (
    ApplyError,
    StalePlanError,
    UnknownResourceTypeError,
    ValidationError,
)
from dss_provisioner.engine.handlers import EngineContext, ResourceHandler
from dss_provisioner.engine.registry import ResourceTypeRegistry
from dss_provisioner.engine.types import Action, Plan
from dss_provisioner.resources.base import Resource


class DummyResource(Resource):
    resource_type: ClassVar[str] = "dummy"
    value: int
    config: dict[str, Any] = Field(default_factory=dict)


def _attrs(resource: DummyResource) -> dict[str, Any]:
    return {
        "id": resource.name,
        "name": resource.name,
        "description": resource.description,
        "tags": list(resource.tags),
        "value": resource.value,
        "config": dict(resource.config),
    }


class InMemoryHandler(ResourceHandler[DummyResource]):
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.store: dict[str, dict[str, Any]] = {}

    def read(self, ctx: EngineContext, prior: ResourceInstance) -> dict[str, Any] | None:
        _ = ctx
        return self.store.get(prior.address)

    def create(self, ctx: EngineContext, desired: DummyResource) -> dict[str, Any]:
        _ = ctx
        self.calls.append(("create", desired.address))
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
        self.calls.append(("update", desired.address))
        attrs = _attrs(desired)
        self.store[desired.address] = dict(attrs)
        return attrs

    def delete(self, ctx: EngineContext, prior: ResourceInstance) -> None:
        _ = ctx
        self.calls.append(("delete", prior.address))
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


def test_apply_uses_dependency_graph_not_plan_order(tmp_path: Path) -> None:
    engine, handler = _engine(tmp_path)

    b = DummyResource(name="b", value=1)
    a = DummyResource(name="a", value=1, depends_on=[b.address])

    plan = engine.plan([a, b])
    scrambled = Plan(metadata=plan.metadata, changes=list(reversed(plan.changes)))

    engine.apply(scrambled)

    assert handler.calls[0] == ("create", "dummy.b")
    assert handler.calls[1] == ("create", "dummy.a")


def test_apply_runs_create_update_before_deletes(tmp_path: Path) -> None:
    engine, handler = _engine(tmp_path)

    engine.apply(engine.plan([DummyResource(name="r1", value=1)]))
    handler.calls.clear()

    plan = engine.plan([DummyResource(name="r2", value=2)])
    scrambled = Plan(metadata=plan.metadata, changes=list(reversed(plan.changes)))
    engine.apply(scrambled)

    assert handler.calls[0] == ("create", "dummy.r2")
    assert handler.calls[1] == ("delete", "dummy.r1")


def test_destroy_plan_deletes_all_in_reverse_dependency_order(tmp_path: Path) -> None:
    engine, handler = _engine(tmp_path)

    b = DummyResource(name="b", value=1)
    a = DummyResource(name="a", value=1, depends_on=[b.address])

    engine.apply(engine.plan([a, b]))
    handler.calls.clear()

    destroy_plan = engine.plan([a, b], destroy=True)
    actions = [(c.address, c.action) for c in destroy_plan.changes]
    assert actions[0] == ("dummy.a", Action.DELETE)
    assert actions[1] == ("dummy.b", Action.DELETE)

    scrambled = Plan(metadata=destroy_plan.metadata, changes=list(reversed(destroy_plan.changes)))
    engine.apply(scrambled)
    assert handler.calls == [("delete", "dummy.a"), ("delete", "dummy.b")]

    state = State.load(engine.state_path)
    assert state.resources == {}


def test_plan_refresh_false_fails_on_unregistered_delete(tmp_path: Path) -> None:
    engine, _handler = _engine(tmp_path)

    state = State(
        project_key=engine.project_key,
        resources={
            "missing.r1": ResourceInstance(
                address="missing.r1",
                resource_type="missing",
                name="r1",
            )
        },
    )
    state.save(engine.state_path)

    with pytest.raises(UnknownResourceTypeError):
        engine.plan([], refresh=False)


def test_destroy_plan_refresh_false_fails_on_unregistered_resource(tmp_path: Path) -> None:
    engine, _handler = _engine(tmp_path)

    state = State(
        project_key=engine.project_key,
        resources={
            "missing.r1": ResourceInstance(
                address="missing.r1",
                resource_type="missing",
                name="r1",
            )
        },
    )
    state.save(engine.state_path)

    with pytest.raises(UnknownResourceTypeError):
        engine.plan([], destroy=True, refresh=False)


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


class TestValuesDiffer:
    """Unit tests for the partial-dict-aware comparison helper."""

    def test_equal_scalars(self) -> None:
        assert _values_differ(1, 1) is False

    def test_different_scalars(self) -> None:
        assert _values_differ(1, 2) is True

    def test_dict_ignores_extra_prior_keys(self) -> None:
        desired = {"separator": ","}
        prior = {"separator": ",", "charset": "UTF-8", "escapeChar": "\\"}
        assert _values_differ(desired, prior) is False

    def test_dict_detects_changed_declared_key(self) -> None:
        desired = {"separator": ","}
        prior = {"separator": ";", "charset": "UTF-8"}
        assert _values_differ(desired, prior) is True

    def test_dict_detects_missing_key_in_prior(self) -> None:
        desired = {"separator": ","}
        prior = {"charset": "UTF-8"}
        assert _values_differ(desired, prior) is True

    def test_empty_desired_dict_always_matches(self) -> None:
        assert _values_differ({}, {"separator": ",", "charset": "UTF-8"}) is False

    def test_both_empty_dicts(self) -> None:
        assert _values_differ({}, {}) is False

    def test_nested_dict_ignores_extra_prior_keys(self) -> None:
        desired = {"opts": {"a": 1}}
        prior = {"opts": {"a": 1, "b": 2}, "extra": True}
        assert _values_differ(desired, prior) is False

    def test_nested_dict_detects_changed_key(self) -> None:
        desired = {"opts": {"a": 1}}
        prior = {"opts": {"a": 99, "b": 2}}
        assert _values_differ(desired, prior) is True

    def test_dict_vs_none_prior(self) -> None:
        assert _values_differ({"a": 1}, None) is True

    def test_none_vs_dict_prior(self) -> None:
        assert _values_differ(None, {"a": 1}) is True

    def test_lists_use_strict_equality(self) -> None:
        assert _values_differ([1, 2], [1, 2]) is False
        assert _values_differ([1, 2], [1, 2, 3]) is True


class TestDictFieldDiffInPlan:
    """Engine-level test: dict fields with provider-added defaults don't cause spurious updates."""

    def test_noop_when_prior_has_extra_dict_keys(self, tmp_path: Path) -> None:
        engine, handler = _engine(tmp_path)

        r = DummyResource(name="r1", value=1, config={"separator": ","})
        engine.apply(engine.plan([r]))

        # Simulate provider expanding the dict with extra default keys
        handler.store["dummy.r1"]["config"] = {
            "separator": ",",
            "charset": "UTF-8",
            "escapeChar": "\\",
        }
        state = State.load(engine.state_path)
        state.resources["dummy.r1"].attributes["config"] = handler.store["dummy.r1"]["config"]
        state.save(engine.state_path)

        plan = engine.plan([r], refresh=False)
        assert plan.changes[0].action == Action.NOOP

    def test_update_when_declared_dict_key_changed(self, tmp_path: Path) -> None:
        engine, handler = _engine(tmp_path)

        r = DummyResource(name="r1", value=1, config={"separator": ","})
        engine.apply(engine.plan([r]))

        # Provider expanded, and someone changed a declared key
        handler.store["dummy.r1"]["config"] = {"separator": ";", "charset": "UTF-8"}
        state = State.load(engine.state_path)
        state.resources["dummy.r1"].attributes["config"] = handler.store["dummy.r1"]["config"]
        state.save(engine.state_path)

        plan = engine.plan([r], refresh=False)
        assert plan.changes[0].action == Action.UPDATE
        assert plan.changes[0].diff is not None
        assert plan.changes[0].diff["config"]["to"]["separator"] == ","

    def test_nested_dicts_use_partial_comparison(self, tmp_path: Path) -> None:
        """Nested dict values also get partial comparison — recursive."""
        engine, _handler = _engine(tmp_path)

        r = DummyResource(name="r1", value=1, config={"opts": {"a": 1}})
        engine.apply(engine.plan([r]))

        # Provider adds a nested key — NOOP because partial comparison is recursive
        state = State.load(engine.state_path)
        state.resources["dummy.r1"].attributes["config"] = {"opts": {"a": 1, "b": 2}}
        state.save(engine.state_path)

        plan = engine.plan([r], refresh=False)
        assert plan.changes[0].action == Action.NOOP


# ---------------------------------------------------------------------------
# Engine-level validation tests
# ---------------------------------------------------------------------------


class TestEngineValidation:
    """Tests that engine.plan() invokes handler validation and raises ValidationError."""

    def test_plan_raises_validation_error_sql_no_sql_input(self, tmp_path: Path) -> None:
        """plan() fails for SQL recipe with non-SQL input — Level 2 validation."""
        from dss_provisioner.engine.dataset_handler import DatasetHandler
        from dss_provisioner.engine.recipe_handler import SQLQueryRecipeHandler
        from dss_provisioner.resources.dataset import FilesystemDatasetResource
        from dss_provisioner.resources.recipe import SQLQueryRecipeResource

        registry = ResourceTypeRegistry()
        registry.register(SQLQueryRecipeResource, SQLQueryRecipeHandler())
        registry.register(FilesystemDatasetResource, DatasetHandler())

        engine = DSSEngine(
            provider=DSSProvider.from_client(MagicMock()),
            project_key="PRJ",
            state_path=tmp_path / "state3.json",
            registry=registry,
        )

        fs_ds = FilesystemDatasetResource(
            name="fs_ds", connection="filesystem_managed", path="/data"
        )
        recipe = SQLQueryRecipeResource(
            name="my_sql", inputs=["fs_ds"], outputs=["out_ds"], code="SELECT 1"
        )

        with pytest.raises(ValidationError, match="SQL connection"):
            engine.plan([recipe, fs_ds], refresh=False)

    def test_plan_succeeds_sql_with_sql_input(self, tmp_path: Path) -> None:
        """plan() succeeds with co-planned SQL dataset — validation passes."""
        from dss_provisioner.engine.dataset_handler import DatasetHandler
        from dss_provisioner.engine.recipe_handler import SQLQueryRecipeHandler
        from dss_provisioner.resources.dataset import SnowflakeDatasetResource
        from dss_provisioner.resources.recipe import SQLQueryRecipeResource

        registry = ResourceTypeRegistry()
        registry.register(SQLQueryRecipeResource, SQLQueryRecipeHandler())
        registry.register(SnowflakeDatasetResource, DatasetHandler())

        engine = DSSEngine(
            provider=DSSProvider.from_client(MagicMock()),
            project_key="PRJ",
            state_path=tmp_path / "state4.json",
            registry=registry,
        )

        sf_ds = SnowflakeDatasetResource(
            name="sf_ds", connection="snowflake_conn", schema_name="public", table="t1"
        )
        recipe = SQLQueryRecipeResource(
            name="my_sql", inputs=["sf_ds"], outputs=["out_ds"], code="SELECT 1"
        )

        # Should not raise — validation passes
        plan = engine.plan([recipe, sf_ds], refresh=False)
        assert any(c.action == Action.CREATE for c in plan.changes)


# ---------------------------------------------------------------------------
# Partial failure + recovery tests
# ---------------------------------------------------------------------------


class TestProgressCallback:
    """Tests that engine.apply() invokes the progress callback correctly."""

    def test_progress_callback_receives_start_and_done(self, tmp_path: Path) -> None:
        engine, _handler = _engine(tmp_path)

        r1 = DummyResource(name="r1", value=1)
        plan = engine.plan([r1])

        events: list[tuple[str, str]] = []

        def on_progress(change: Any, event: str) -> None:
            events.append((change.address, event))

        engine.apply(plan, progress=on_progress)

        assert ("dummy.r1", "start") in events
        assert ("dummy.r1", "done") in events
        # "start" must come before "done" for the same resource
        start_idx = events.index(("dummy.r1", "start"))
        done_idx = events.index(("dummy.r1", "done"))
        assert start_idx < done_idx

    def test_progress_callback_not_called_for_noop(self, tmp_path: Path) -> None:
        engine, _handler = _engine(tmp_path)

        r1 = DummyResource(name="r1", value=1)
        engine.apply(engine.plan([r1]))

        # Second plan is all NOOP — the engine skips NOOP changes entirely,
        # so the progress callback must not be invoked at all.
        plan2 = engine.plan([r1])
        events: list[tuple[str, str]] = []
        engine.apply(plan2, progress=lambda c, e: events.append((c.address, e)))
        assert events == []

    def test_progress_callback_multiple_resources(self, tmp_path: Path) -> None:
        engine, _handler = _engine(tmp_path)

        r1 = DummyResource(name="a", value=1)
        r2 = DummyResource(name="b", value=2, depends_on=["dummy.a"])
        plan = engine.plan([r1, r2])

        done_events: list[str] = []
        engine.apply(
            plan, progress=lambda c, e: done_events.append(c.address) if e == "done" else None
        )

        assert done_events == ["dummy.a", "dummy.b"]


class FailOnceHandler(InMemoryHandler):
    """Handler that raises on the first create of a specific resource, then works."""

    def __init__(self, fail_address: str) -> None:
        super().__init__()
        self.fail_address = fail_address

    def create(self, ctx: EngineContext, desired: DummyResource) -> dict[str, Any]:
        if desired.address == self.fail_address:
            self.fail_address = ""  # clear — next attempt succeeds
            msg = "Simulated API error"
            raise RuntimeError(msg)
        return super().create(ctx, desired)


class TestPartialFailureRecovery:
    """Tests that partial apply saves state and re-run recovers."""

    def test_apply_error_carries_partial_result(self, tmp_path: Path) -> None:
        """ApplyError wraps what was applied before the failure."""
        provider = DSSProvider.from_client(MagicMock())
        registry = ResourceTypeRegistry()
        handler = FailOnceHandler(fail_address="dummy.b")
        registry.register(DummyResource, handler)
        engine = DSSEngine(
            provider=provider,
            project_key="PRJ",
            state_path=tmp_path / "state.json",
            registry=registry,
        )

        a = DummyResource(name="a", value=1)
        b = DummyResource(name="b", value=2, depends_on=["dummy.a"])

        plan = engine.plan([a, b], refresh=False)
        assert plan.summary()["create"] == 2

        with pytest.raises(ApplyError, match=r"dummy\.b") as exc_info:
            engine.apply(plan)

        # A was applied before B failed
        assert exc_info.value.result.summary() == {
            "create": 1,
            "update": 0,
            "delete": 0,
            "no-op": 0,
        }
        assert exc_info.value.address == "dummy.b"

    def test_rerun_after_partial_failure_recovers(self, tmp_path: Path) -> None:
        """Re-running plan+apply against the same state file picks up where it left off."""
        provider = DSSProvider.from_client(MagicMock())
        registry = ResourceTypeRegistry()
        handler = FailOnceHandler(fail_address="dummy.b")
        registry.register(DummyResource, handler)
        engine = DSSEngine(
            provider=provider,
            project_key="PRJ",
            state_path=tmp_path / "state.json",
            registry=registry,
        )

        a = DummyResource(name="a", value=1)
        b = DummyResource(name="b", value=2, depends_on=["dummy.a"])

        # First attempt — A succeeds, B fails
        plan1 = engine.plan([a, b], refresh=False)
        with pytest.raises(ApplyError):
            engine.apply(plan1)

        # State has A but not B
        state = State.load(engine.state_path)
        assert "dummy.a" in state.resources
        assert "dummy.b" not in state.resources

        # Second attempt — handler is "fixed" (fail_address cleared)
        plan2 = engine.plan([a, b], refresh=False)
        assert plan2.summary() == {"create": 1, "update": 0, "delete": 0, "no-op": 1}

        result = engine.apply(plan2)
        assert result.summary() == {"create": 1, "update": 0, "delete": 0, "no-op": 0}

        # Both resources now in state
        state = State.load(engine.state_path)
        assert "dummy.a" in state.resources
        assert "dummy.b" in state.resources
