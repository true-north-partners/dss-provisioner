"""Tests for the CodeEnvHandler."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from dss_provisioner.core import DSSProvider, ResourceInstance
from dss_provisioner.core.state import State
from dss_provisioner.engine import DSSEngine
from dss_provisioner.engine.code_env_handler import CodeEnvHandler
from dss_provisioner.engine.handlers import EngineContext, PlanContext
from dss_provisioner.engine.registry import ResourceTypeRegistry
from dss_provisioner.engine.types import Action
from dss_provisioner.resources.code_env import CodeEnvResource

if TYPE_CHECKING:
    from pathlib import Path


def _make_raw(
    python_mode: str = "INHERIT",
    python_name: str = "",
    r_mode: str = "INHERIT",
    r_name: str = "",
) -> dict[str, Any]:
    """Build a raw settings dict matching the DSS API structure."""
    raw: dict[str, Any] = {"settings": {"codeEnvs": {}}}
    if python_mode == "EXPLICIT_ENV":
        raw["settings"]["codeEnvs"]["python"] = {
            "mode": "EXPLICIT_ENV",
            "envName": python_name,
        }
    else:
        raw["settings"]["codeEnvs"]["python"] = {"mode": python_mode}
    if r_mode == "EXPLICIT_ENV":
        raw["settings"]["codeEnvs"]["r"] = {
            "mode": "EXPLICIT_ENV",
            "envName": r_name,
        }
    else:
        raw["settings"]["codeEnvs"]["r"] = {"mode": r_mode}
    return raw


@pytest.fixture
def mock_client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_project(mock_client: MagicMock) -> MagicMock:
    project = MagicMock()
    mock_client.get_project.return_value = project
    settings = MagicMock()
    settings.get_raw.return_value = _make_raw()
    project.get_settings.return_value = settings
    return project


@pytest.fixture
def ctx(mock_client: MagicMock) -> EngineContext:
    provider = DSSProvider.from_client(mock_client)
    return EngineContext(provider=provider, project_key="PRJ")


@pytest.fixture
def handler() -> CodeEnvHandler:
    return CodeEnvHandler()


class TestCreate:
    def test_sets_python_env(
        self,
        ctx: EngineContext,
        handler: CodeEnvHandler,
        mock_project: MagicMock,
    ) -> None:
        raw = _make_raw(python_mode="EXPLICIT_ENV", python_name="py39_ml")
        mock_project.get_settings.return_value.get_raw.return_value = raw

        desired = CodeEnvResource(default_python="py39_ml")
        result = handler.create(ctx, desired)

        mock_project.get_settings.return_value.set_python_code_env.assert_called_once_with(
            "py39_ml"
        )
        mock_project.get_settings.return_value.save.assert_called()
        assert result["default_python"] == "py39_ml"

    def test_sets_r_env(
        self,
        ctx: EngineContext,
        handler: CodeEnvHandler,
        mock_project: MagicMock,
    ) -> None:
        raw = _make_raw(r_mode="EXPLICIT_ENV", r_name="r_base")
        mock_project.get_settings.return_value.get_raw.return_value = raw

        desired = CodeEnvResource(default_r="r_base")
        result = handler.create(ctx, desired)

        mock_project.get_settings.return_value.set_r_code_env.assert_called_once_with("r_base")
        mock_project.get_settings.return_value.save.assert_called()
        assert result["default_r"] == "r_base"

    def test_sets_both_envs(
        self,
        ctx: EngineContext,
        handler: CodeEnvHandler,
        mock_project: MagicMock,
    ) -> None:
        raw = _make_raw(
            python_mode="EXPLICIT_ENV",
            python_name="py39",
            r_mode="EXPLICIT_ENV",
            r_name="r_base",
        )
        mock_project.get_settings.return_value.get_raw.return_value = raw

        desired = CodeEnvResource(default_python="py39", default_r="r_base")
        result = handler.create(ctx, desired)

        assert result["default_python"] == "py39"
        assert result["default_r"] == "r_base"

    def test_skips_none_fields(
        self,
        ctx: EngineContext,
        handler: CodeEnvHandler,
        mock_project: MagicMock,
    ) -> None:
        desired = CodeEnvResource()
        handler.create(ctx, desired)

        mock_project.get_settings.return_value.set_python_code_env.assert_not_called()
        mock_project.get_settings.return_value.set_r_code_env.assert_not_called()
        mock_project.get_settings.return_value.save.assert_called()


class TestRead:
    def test_reads_explicit_env(
        self,
        ctx: EngineContext,
        handler: CodeEnvHandler,
        mock_project: MagicMock,
    ) -> None:
        raw = _make_raw(python_mode="EXPLICIT_ENV", python_name="py39_ml")
        mock_project.get_settings.return_value.get_raw.return_value = raw

        prior = ResourceInstance(
            address="dss_code_env.code_envs",
            resource_type="dss_code_env",
            name="code_envs",
        )
        result = handler.read(ctx, prior)

        assert result is not None
        assert result["default_python"] == "py39_ml"
        assert "default_r" not in result

    def test_inherit_mode_omits_field(
        self,
        ctx: EngineContext,
        handler: CodeEnvHandler,
        mock_project: MagicMock,
    ) -> None:
        raw = _make_raw()  # both INHERIT
        mock_project.get_settings.return_value.get_raw.return_value = raw

        prior = ResourceInstance(
            address="dss_code_env.code_envs",
            resource_type="dss_code_env",
            name="code_envs",
        )
        result = handler.read(ctx, prior)

        assert result is not None
        assert "default_python" not in result
        assert "default_r" not in result

    def test_missing_code_envs_section(
        self,
        ctx: EngineContext,
        handler: CodeEnvHandler,
        mock_project: MagicMock,
    ) -> None:
        mock_project.get_settings.return_value.get_raw.return_value = {"settings": {}}

        prior = ResourceInstance(
            address="dss_code_env.code_envs",
            resource_type="dss_code_env",
            name="code_envs",
        )
        result = handler.read(ctx, prior)

        assert result is not None
        assert "default_python" not in result
        assert "default_r" not in result


class TestUpdate:
    def test_delegates_to_create(
        self,
        ctx: EngineContext,
        handler: CodeEnvHandler,
        mock_project: MagicMock,
    ) -> None:
        raw = _make_raw(python_mode="EXPLICIT_ENV", python_name="py39")
        mock_project.get_settings.return_value.get_raw.return_value = raw

        desired = CodeEnvResource(default_python="py39")
        prior = ResourceInstance(
            address="dss_code_env.code_envs",
            resource_type="dss_code_env",
            name="code_envs",
        )
        result = handler.update(ctx, desired, prior)

        mock_project.get_settings.return_value.set_python_code_env.assert_called_once_with("py39")
        assert result["default_python"] == "py39"


class TestDelete:
    def test_resets_to_inherit(
        self,
        ctx: EngineContext,
        handler: CodeEnvHandler,
        mock_project: MagicMock,
    ) -> None:
        raw = _make_raw(python_mode="EXPLICIT_ENV", python_name="py39")
        mock_project.get_settings.return_value.get_raw.return_value = raw

        prior = ResourceInstance(
            address="dss_code_env.code_envs",
            resource_type="dss_code_env",
            name="code_envs",
        )
        handler.delete(ctx, prior)

        result_raw = mock_project.get_settings.return_value.get_raw.return_value
        assert result_raw["settings"]["codeEnvs"]["python"] == {"mode": "INHERIT"}
        assert result_raw["settings"]["codeEnvs"]["r"] == {"mode": "INHERIT"}
        mock_project.get_settings.return_value.save.assert_called()


class TestValidatePlan:
    def test_valid_python_env_accepted(
        self,
        ctx: EngineContext,
        handler: CodeEnvHandler,
    ) -> None:
        ctx.provider.client.list_code_envs.return_value = [
            {"envName": "py39_ml", "envLang": "PYTHON"},
            {"envName": "r_base", "envLang": "R"},
        ]
        desired = CodeEnvResource(default_python="py39_ml")
        plan_ctx = PlanContext({desired.address: desired}, State(project_key="PRJ"))

        errors = handler.validate_plan(ctx, desired, plan_ctx)
        assert errors == []

    def test_unknown_python_env_rejected(
        self,
        ctx: EngineContext,
        handler: CodeEnvHandler,
    ) -> None:
        ctx.provider.client.list_code_envs.return_value = [
            {"envName": "py39_ml", "envLang": "PYTHON"},
        ]
        desired = CodeEnvResource(default_python="nonexistent")
        plan_ctx = PlanContext({desired.address: desired}, State(project_key="PRJ"))

        errors = handler.validate_plan(ctx, desired, plan_ctx)
        assert len(errors) == 1
        assert "nonexistent" in errors[0]

    def test_unknown_r_env_rejected(
        self,
        ctx: EngineContext,
        handler: CodeEnvHandler,
    ) -> None:
        ctx.provider.client.list_code_envs.return_value = [
            {"envName": "r_base", "envLang": "R"},
        ]
        desired = CodeEnvResource(default_r="r_missing")
        plan_ctx = PlanContext({desired.address: desired}, State(project_key="PRJ"))

        errors = handler.validate_plan(ctx, desired, plan_ctx)
        assert len(errors) == 1
        assert "r_missing" in errors[0]

    def test_none_skips_validation(
        self,
        ctx: EngineContext,
        handler: CodeEnvHandler,
    ) -> None:
        desired = CodeEnvResource()
        plan_ctx = PlanContext({desired.address: desired}, State(project_key="PRJ"))

        errors = handler.validate_plan(ctx, desired, plan_ctx)
        assert errors == []
        ctx.provider.client.list_code_envs.assert_not_called()


class TestEngineRoundtrip:
    def _setup_engine(
        self,
        tmp_path: Path,
        raw: dict[str, Any],
    ) -> tuple[DSSEngine, MagicMock]:
        mock_client = MagicMock()
        provider = DSSProvider.from_client(mock_client)

        project = MagicMock()
        settings = MagicMock()
        settings.get_raw.return_value = raw
        project.get_settings.return_value = settings
        mock_client.get_project.return_value = project

        # Provide empty list_code_envs to satisfy validate_plan
        mock_client.list_code_envs.return_value = [
            {"envName": "py39_ml", "envLang": "PYTHON"},
        ]

        registry = ResourceTypeRegistry()
        registry.register(CodeEnvResource, CodeEnvHandler())

        engine = DSSEngine(
            provider=provider,
            project_key="PRJ",
            state_path=tmp_path / "state.json",
            registry=registry,
        )
        return engine, project

    def test_create_noop_update_delete_cycle(self, tmp_path: Path) -> None:
        raw = _make_raw(python_mode="EXPLICIT_ENV", python_name="py39_ml")
        engine, project = self._setup_engine(tmp_path, raw)

        # --- CREATE ---
        r = CodeEnvResource(default_python="py39_ml")
        plan = engine.plan([r])
        assert plan.changes[0].action == Action.CREATE
        engine.apply(plan)

        state = State.load(engine.state_path)
        assert "dss_code_env.code_envs" in state.resources
        assert state.serial == 1

        # --- NOOP ---
        plan2 = engine.plan([r])
        assert plan2.changes[0].action == Action.NOOP
        engine.apply(plan2)
        assert State.load(engine.state_path).serial == 1

        # --- UPDATE (simulate DSS drift, then apply restores) ---
        r2 = CodeEnvResource(default_python="py39_ml")
        raw_updated = _make_raw(python_mode="INHERIT")
        project.get_settings.return_value.get_raw.return_value = raw_updated
        plan3 = engine.plan([r2])
        assert plan3.changes[0].action == Action.UPDATE

        raw_restored = _make_raw(python_mode="EXPLICIT_ENV", python_name="py39_ml")
        project.get_settings.return_value.get_raw.return_value = raw_restored
        engine.apply(plan3)

        state3 = State.load(engine.state_path)
        assert state3.serial > 1

        # --- DELETE ---
        plan4 = engine.plan([])
        assert any(c.action == Action.DELETE for c in plan4.changes)
        engine.apply(plan4)

        state4 = State.load(engine.state_path)
        assert state4.resources == {}
