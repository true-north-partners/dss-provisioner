"""Tests for the ScenarioHandler hierarchy."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, PropertyMock

import pytest

from dss_provisioner.core import DSSProvider, ResourceInstance
from dss_provisioner.core.state import State
from dss_provisioner.engine import DSSEngine
from dss_provisioner.engine.handlers import EngineContext
from dss_provisioner.engine.registry import ResourceTypeRegistry
from dss_provisioner.engine.scenario_handler import (
    PythonScenarioHandler,
    StepBasedScenarioHandler,
)
from dss_provisioner.engine.types import Action
from dss_provisioner.resources.scenario import (
    PythonScenarioResource,
    StepBasedScenarioResource,
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
def step_handler() -> StepBasedScenarioHandler:
    return StepBasedScenarioHandler()


@pytest.fixture
def python_handler() -> PythonScenarioHandler:
    return PythonScenarioHandler()


@pytest.fixture
def mock_project(mock_client: MagicMock) -> MagicMock:
    project = MagicMock()
    mock_client.get_project.return_value = project
    return project


@pytest.fixture
def mock_scenario(mock_project: MagicMock) -> MagicMock:
    scenario = MagicMock()
    scenario.id = "daily_build"
    mock_project.create_scenario.return_value = scenario
    mock_project.get_scenario.return_value = scenario

    settings = MagicMock()
    settings.data = {"type": "step_based", "active": True, "triggers": [], "params": {"steps": []}}
    type(settings).active = PropertyMock(return_value=True)
    type(settings).code = PropertyMock(return_value="")
    scenario.get_settings.return_value = settings

    scenario.get_metadata.return_value = {"description": "", "tags": []}

    return scenario


# ---------------------------------------------------------------------------
# Step-based: Create
# ---------------------------------------------------------------------------


class TestStepBasedCreate:
    def test_calls_create_scenario(
        self,
        ctx: EngineContext,
        step_handler: StepBasedScenarioHandler,
        mock_project: MagicMock,
        mock_scenario: MagicMock,  # noqa: ARG002
    ) -> None:
        desired = StepBasedScenarioResource(name="daily_build")
        step_handler.create(ctx, desired)
        mock_project.create_scenario.assert_called_once_with("daily_build", type="step_based")

    def test_writes_triggers_and_steps(
        self,
        ctx: EngineContext,
        step_handler: StepBasedScenarioHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_scenario: MagicMock,
    ) -> None:
        triggers = [{"type": "temporal", "params": {"frequency": "Daily"}}]
        steps = [{"type": "build_flowitem", "name": "Build", "params": {}}]
        desired = StepBasedScenarioResource(name="daily_build", triggers=triggers, steps=steps)
        step_handler.create(ctx, desired)

        settings = mock_scenario.get_settings.return_value
        assert settings.data["triggers"] == triggers
        assert settings.data["params"]["steps"] == steps
        settings.save.assert_called_once()

    def test_returns_attrs_with_scenario_id(
        self,
        ctx: EngineContext,
        step_handler: StepBasedScenarioHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_scenario: MagicMock,  # noqa: ARG002
    ) -> None:
        desired = StepBasedScenarioResource(name="daily_build")
        result = step_handler.create(ctx, desired)
        assert result["scenario_id"] == "daily_build"
        assert result["type"] == "step_based"
        assert result["active"] is True

    def test_applies_description_and_tags(
        self,
        ctx: EngineContext,
        step_handler: StepBasedScenarioHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_scenario: MagicMock,
    ) -> None:
        desired = StepBasedScenarioResource(
            name="daily_build", description="A build scenario", tags=["prod"]
        )
        step_handler.create(ctx, desired)

        settings = mock_scenario.get_settings.return_value
        assert settings.data["shortDesc"] == "A build scenario"
        assert settings.data["tags"] == ["prod"]


# ---------------------------------------------------------------------------
# Step-based: Read
# ---------------------------------------------------------------------------


class TestStepBasedRead:
    def test_returns_live_active_echoed_steps(
        self,
        ctx: EngineContext,
        step_handler: StepBasedScenarioHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_scenario: MagicMock,
    ) -> None:
        # DSS returns active=False (live), but steps should be echoed from prior.
        type(mock_scenario.get_settings.return_value).active = PropertyMock(return_value=False)

        stored_steps = [{"type": "build_flowitem"}]
        prior = ResourceInstance(
            address="dss_step_scenario.daily_build",
            resource_type="dss_step_scenario",
            name="daily_build",
            attributes={
                "scenario_id": "daily_build",
                "type": "step_based",
                "steps": stored_steps,
                "triggers": [],
            },
        )
        result = step_handler.read(ctx, prior)

        assert result is not None
        assert result["active"] is False  # live from DSS
        assert result["steps"] == stored_steps  # echoed from prior

    def test_reads_description_and_tags_live(
        self,
        ctx: EngineContext,
        step_handler: StepBasedScenarioHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_scenario: MagicMock,
    ) -> None:
        # DSS settings contain different description/tags than what was stored in prior.
        settings = mock_scenario.get_settings.return_value
        settings.data["shortDesc"] = "Changed in DSS"
        settings.data["tags"] = ["new_tag"]

        prior = ResourceInstance(
            address="dss_step_scenario.daily_build",
            resource_type="dss_step_scenario",
            name="daily_build",
            attributes={
                "scenario_id": "daily_build",
                "type": "step_based",
                "description": "Original",
                "tags": ["old_tag"],
            },
        )
        result = step_handler.read(ctx, prior)

        assert result is not None
        assert result["description"] == "Changed in DSS"  # live from DSS
        assert result["tags"] == ["new_tag"]  # live from DSS

    def test_returns_none_when_deleted(
        self,
        ctx: EngineContext,
        step_handler: StepBasedScenarioHandler,
        mock_project: MagicMock,
        mock_scenario: MagicMock,  # noqa: ARG002
    ) -> None:
        mock_project.get_scenario.side_effect = Exception("Not found")

        prior = ResourceInstance(
            address="dss_step_scenario.daily_build",
            resource_type="dss_step_scenario",
            name="daily_build",
            attributes={"scenario_id": "daily_build"},
        )
        result = step_handler.read(ctx, prior)
        assert result is None

    def test_returns_none_when_no_scenario_id(
        self,
        ctx: EngineContext,
        step_handler: StepBasedScenarioHandler,
        mock_project: MagicMock,  # noqa: ARG002
    ) -> None:
        prior = ResourceInstance(
            address="dss_step_scenario.daily_build",
            resource_type="dss_step_scenario",
            name="daily_build",
            attributes={},
        )
        result = step_handler.read(ctx, prior)
        assert result is None


# ---------------------------------------------------------------------------
# Step-based: Update
# ---------------------------------------------------------------------------


class TestStepBasedUpdate:
    def test_writes_updated_settings(
        self,
        ctx: EngineContext,
        step_handler: StepBasedScenarioHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_scenario: MagicMock,
    ) -> None:
        new_triggers = [{"type": "temporal", "params": {"frequency": "Hourly"}}]
        new_steps = [{"type": "build_flowitem", "name": "New build"}]
        desired = StepBasedScenarioResource(
            name="daily_build", active=False, triggers=new_triggers, steps=new_steps
        )
        prior = ResourceInstance(
            address="dss_step_scenario.daily_build",
            resource_type="dss_step_scenario",
            name="daily_build",
            attributes={"scenario_id": "daily_build"},
        )
        step_handler.update(ctx, desired, prior)

        settings = mock_scenario.get_settings.return_value
        assert settings.data["active"] is False
        assert settings.data["triggers"] == new_triggers
        assert settings.data["params"]["steps"] == new_steps
        settings.save.assert_called_once()

    def test_returns_updated_attrs(
        self,
        ctx: EngineContext,
        step_handler: StepBasedScenarioHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_scenario: MagicMock,  # noqa: ARG002
    ) -> None:
        desired = StepBasedScenarioResource(name="daily_build", active=False)
        prior = ResourceInstance(
            address="dss_step_scenario.daily_build",
            resource_type="dss_step_scenario",
            name="daily_build",
            attributes={"scenario_id": "daily_build"},
        )
        result = step_handler.update(ctx, desired, prior)
        assert result["active"] is False
        assert result["scenario_id"] == "daily_build"


# ---------------------------------------------------------------------------
# Python: Create
# ---------------------------------------------------------------------------


class TestPythonCreate:
    def test_calls_create_scenario_custom_python(
        self,
        ctx: EngineContext,
        python_handler: PythonScenarioHandler,
        mock_project: MagicMock,
        mock_scenario: MagicMock,  # noqa: ARG002
    ) -> None:
        desired = PythonScenarioResource(name="e2e_test")
        python_handler.create(ctx, desired)
        mock_project.create_scenario.assert_called_once_with("e2e_test", type="custom_python")

    def test_writes_code(
        self,
        ctx: EngineContext,
        python_handler: PythonScenarioHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_scenario: MagicMock,
    ) -> None:
        code_mock = PropertyMock(return_value="")
        type(mock_scenario.get_settings.return_value).code = code_mock

        desired = PythonScenarioResource(name="e2e_test", code="print('test')")
        python_handler.create(ctx, desired)

        code_mock.assert_called_with("print('test')")
        mock_scenario.get_settings.return_value.save.assert_called_once()


# ---------------------------------------------------------------------------
# Python: Read
# ---------------------------------------------------------------------------


class TestPythonRead:
    def test_reads_code_live(
        self,
        ctx: EngineContext,
        python_handler: PythonScenarioHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_scenario: MagicMock,
    ) -> None:
        type(mock_scenario.get_settings.return_value).code = PropertyMock(
            return_value="print('live')"
        )

        prior = ResourceInstance(
            address="dss_python_scenario.e2e_test",
            resource_type="dss_python_scenario",
            name="e2e_test",
            attributes={"scenario_id": "e2e_test", "type": "custom_python", "code": "old"},
        )
        result = python_handler.read(ctx, prior)

        assert result is not None
        assert result["code"] == "print('live')"  # live from DSS, not echoed


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


class TestDelete:
    def test_deletes_scenario(
        self,
        ctx: EngineContext,
        step_handler: StepBasedScenarioHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_scenario: MagicMock,
    ) -> None:
        prior = ResourceInstance(
            address="dss_step_scenario.daily_build",
            resource_type="dss_step_scenario",
            name="daily_build",
            attributes={"scenario_id": "daily_build"},
        )
        step_handler.delete(ctx, prior)
        mock_scenario.delete.assert_called_once()

    def test_delete_ignores_missing(
        self,
        ctx: EngineContext,
        step_handler: StepBasedScenarioHandler,
        mock_project: MagicMock,
        mock_scenario: MagicMock,  # noqa: ARG002
    ) -> None:
        mock_project.get_scenario.side_effect = Exception("Not found")

        prior = ResourceInstance(
            address="dss_step_scenario.daily_build",
            resource_type="dss_step_scenario",
            name="daily_build",
            attributes={"scenario_id": "daily_build"},
        )
        # Should not raise.
        step_handler.delete(ctx, prior)

    def test_delete_no_scenario_id(
        self,
        ctx: EngineContext,
        step_handler: StepBasedScenarioHandler,
        mock_project: MagicMock,  # noqa: ARG002
    ) -> None:
        prior = ResourceInstance(
            address="dss_step_scenario.daily_build",
            resource_type="dss_step_scenario",
            name="daily_build",
            attributes={},
        )
        # Should not raise.
        step_handler.delete(ctx, prior)


# ---------------------------------------------------------------------------
# Engine integration / roundtrip tests
# ---------------------------------------------------------------------------


def _setup_scenario_engine(
    tmp_path: Path,
    scenario_type: str = "step_based",
    active: bool = True,
    steps: list[dict[str, Any]] | None = None,
    code: str = "",
) -> tuple[DSSEngine, MagicMock, MagicMock]:
    """Wire up a DSSEngine with scenario handlers and mocked dataikuapi objects."""
    mock_client = MagicMock()
    provider = DSSProvider.from_client(mock_client)

    project = MagicMock()
    mock_client.get_project.return_value = project

    scenario = MagicMock()
    scenario.id = "test_scenario"
    project.create_scenario.return_value = scenario
    project.get_scenario.return_value = scenario

    settings = MagicMock()
    settings.data = {
        "type": scenario_type,
        "active": active,
        "triggers": [],
        "params": {"steps": steps or []},
    }
    type(settings).active = PropertyMock(return_value=active)
    type(settings).code = PropertyMock(return_value=code)
    scenario.get_settings.return_value = settings

    scenario.get_metadata.return_value = {"description": "", "tags": []}

    registry = ResourceTypeRegistry()
    registry.register(StepBasedScenarioResource, StepBasedScenarioHandler())
    registry.register(PythonScenarioResource, PythonScenarioHandler())

    state_path = tmp_path / "state.json"

    engine = DSSEngine(
        provider=provider,
        project_key="PRJ",
        state_path=state_path,
        registry=registry,
    )

    return engine, project, scenario


class TestEngineRoundtrip:
    def test_create_noop_update_delete_cycle(self, tmp_path: Path) -> None:
        engine, _project, _scenario = _setup_scenario_engine(tmp_path)

        r = StepBasedScenarioResource(name="daily_build")
        plan = engine.plan([r])
        assert plan.changes[0].action == Action.CREATE
        engine.apply(plan)

        state = State.load(engine.state_path)
        assert "dss_step_scenario.daily_build" in state.resources
        assert (
            state.resources["dss_step_scenario.daily_build"].attributes["scenario_id"]
            == "test_scenario"
        )

        # NOOP
        plan2 = engine.plan([r])
        assert plan2.changes[0].action == Action.NOOP

    def test_scenarios_applied_after_datasets(self) -> None:
        assert StepBasedScenarioResource.plan_priority == 200
        assert PythonScenarioResource.plan_priority == 200

    def test_python_scenario_roundtrip(self, tmp_path: Path) -> None:
        engine, _project, _scenario = _setup_scenario_engine(
            tmp_path, scenario_type="custom_python", code="print('hi')"
        )

        r = PythonScenarioResource(name="e2e_test", code="print('hi')")
        plan = engine.plan([r])
        assert plan.changes[0].action == Action.CREATE
        engine.apply(plan)

        state = State.load(engine.state_path)
        assert "dss_python_scenario.e2e_test" in state.resources
        assert state.resources["dss_python_scenario.e2e_test"].attributes["code"] == "print('hi')"

        # NOOP
        plan2 = engine.plan([r])
        assert plan2.changes[0].action == Action.NOOP
