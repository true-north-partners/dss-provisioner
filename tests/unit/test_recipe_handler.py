"""Tests for the RecipeHandler hierarchy."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from dss_provisioner.core import DSSProvider, ResourceInstance
from dss_provisioner.core.state import State
from dss_provisioner.engine import DSSEngine
from dss_provisioner.engine.dataset_handler import DatasetHandler
from dss_provisioner.engine.handlers import EngineContext, PlanContext
from dss_provisioner.engine.recipe_handler import (
    PythonRecipeHandler,
    RecipeHandler,
    SQLQueryRecipeHandler,
    SyncRecipeHandler,
)
from dss_provisioner.engine.registry import ResourceTypeRegistry
from dss_provisioner.engine.types import Action
from dss_provisioner.resources.dataset import DatasetResource
from dss_provisioner.resources.recipe import (
    PythonRecipeResource,
    RecipeResource,
    SQLQueryRecipeResource,
    SyncRecipeResource,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def mock_client() -> MagicMock:
    client = MagicMock()
    return client


@pytest.fixture
def ctx(mock_client: MagicMock) -> EngineContext:
    provider = DSSProvider.from_client(mock_client)
    return EngineContext(provider=provider, project_key="PRJ")


@pytest.fixture
def handler() -> RecipeHandler:
    return RecipeHandler()


@pytest.fixture
def sync_handler() -> SyncRecipeHandler:
    return SyncRecipeHandler()


@pytest.fixture
def python_handler() -> PythonRecipeHandler:
    return PythonRecipeHandler()


@pytest.fixture
def sql_handler() -> SQLQueryRecipeHandler:
    return SQLQueryRecipeHandler()


@pytest.fixture
def mock_project(mock_client: MagicMock) -> MagicMock:
    project = MagicMock()
    mock_client.get_project.return_value = project
    # Flow zone: make list_zones return empty by default (no zones).
    flow = MagicMock()
    flow.list_zones.return_value = []
    project.get_flow.return_value = flow
    return project


@pytest.fixture
def mock_recipe(mock_project: MagicMock) -> MagicMock:
    recipe = MagicMock()
    mock_project.get_recipe.return_value = recipe

    # Builder pattern for new_recipe
    builder = MagicMock()
    builder.create.return_value = recipe
    mock_project.new_recipe.return_value = builder

    # Default settings
    settings = MagicMock()
    raw_def: dict[str, Any] = {"type": "sync", "params": {}, "inputs": {}, "outputs": {}}
    settings.get_recipe_raw_definition.return_value = raw_def
    settings.get_flat_input_refs.return_value = []
    settings.get_flat_output_refs.return_value = []
    settings.str_payload = ""
    recipe.get_settings.return_value = settings

    recipe.get_metadata.return_value = {"description": "", "tags": []}

    return recipe


# ---------------------------------------------------------------------------
# Create tests
# ---------------------------------------------------------------------------


class TestCreateSyncRecipe:
    def test_calls_new_recipe(
        self,
        ctx: EngineContext,
        sync_handler: SyncRecipeHandler,
        mock_project: MagicMock,
        mock_recipe: MagicMock,  # noqa: ARG002
    ) -> None:
        desired = SyncRecipeResource(name="my_sync", inputs=["ds_a"], outputs=["ds_b"])
        sync_handler.create(ctx, desired)
        mock_project.new_recipe.assert_called_once_with("sync", "my_sync")

    def test_sets_inputs_and_outputs(
        self,
        ctx: EngineContext,
        sync_handler: SyncRecipeHandler,
        mock_project: MagicMock,
        mock_recipe: MagicMock,  # noqa: ARG002
    ) -> None:
        desired = SyncRecipeResource(name="my_sync", inputs=["ds_a"], outputs=["ds_b"])
        sync_handler.create(ctx, desired)
        builder = mock_project.new_recipe.return_value
        builder.with_input.assert_called_once_with("ds_a")
        builder.with_output.assert_called_once_with("ds_b")
        builder.create.assert_called_once()

    def test_no_settings_save_when_no_type_settings(
        self,
        ctx: EngineContext,
        sync_handler: SyncRecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        desired = SyncRecipeResource(name="my_sync", inputs=["ds_a"], outputs=["ds_b"])
        sync_handler.create(ctx, desired)
        mock_recipe.get_settings.return_value.save.assert_not_called()


class TestCreatePythonRecipe:
    def test_sets_code_via_set_payload(
        self,
        ctx: EngineContext,
        python_handler: PythonRecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        desired = PythonRecipeResource(name="my_py", outputs=["out"], code="print('hello')")
        python_handler.create(ctx, desired)
        mock_recipe.get_settings.return_value.set_payload.assert_called_with("print('hello')")
        mock_recipe.get_settings.return_value.save.assert_called()

    def test_sets_env_selection(
        self,
        ctx: EngineContext,
        python_handler: PythonRecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        raw_def: dict[str, Any] = {"type": "python", "params": {}}
        mock_recipe.get_settings.return_value.get_recipe_raw_definition.return_value = raw_def

        desired = PythonRecipeResource(name="my_py", outputs=["out"], code="x=1", code_env="py39")
        python_handler.create(ctx, desired)

        assert raw_def["params"]["envSelection"] == {
            "envMode": "EXPLICIT_ENV",
            "envName": "py39",
        }

    def test_no_code_skips_payload(
        self,
        ctx: EngineContext,
        python_handler: PythonRecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        desired = PythonRecipeResource(name="my_py", outputs=["out"])
        python_handler.create(ctx, desired)
        mock_recipe.get_settings.return_value.set_payload.assert_not_called()


class TestCreateSQLQueryRecipe:
    def test_sets_sql_code_via_set_payload(
        self,
        ctx: EngineContext,
        sql_handler: SQLQueryRecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        desired = SQLQueryRecipeResource(
            name="my_sql", inputs=["in_ds"], outputs=["out"], code="SELECT 1"
        )
        sql_handler.create(ctx, desired)
        mock_recipe.get_settings.return_value.set_payload.assert_called_with("SELECT 1")
        mock_recipe.get_settings.return_value.save.assert_called()


class TestCreateSetsMetadata:
    def test_metadata_applied(
        self,
        ctx: EngineContext,
        sync_handler: SyncRecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        desired = SyncRecipeResource(
            name="my_sync", outputs=["out"], description="A sync recipe", tags=["etl"]
        )
        sync_handler.create(ctx, desired)

        meta = mock_recipe.get_metadata.return_value
        assert meta["description"] == "A sync recipe"
        assert meta["tags"] == ["etl"]
        mock_recipe.set_metadata.assert_called_once_with(meta)


class TestCreateSetsZone:
    def test_zone_applied(
        self,
        ctx: EngineContext,
        sync_handler: SyncRecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        desired = SyncRecipeResource(name="my_sync", outputs=["out"], zone="raw")
        sync_handler.create(ctx, desired)
        mock_recipe.move_to_zone.assert_called_once_with("raw")

    def test_no_zone_when_not_specified(
        self,
        ctx: EngineContext,
        sync_handler: SyncRecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        desired = SyncRecipeResource(name="my_sync", outputs=["out"])
        sync_handler.create(ctx, desired)
        mock_recipe.move_to_zone.assert_not_called()


# ---------------------------------------------------------------------------
# Read tests
# ---------------------------------------------------------------------------


class TestRead:
    def test_returns_attributes(
        self,
        ctx: EngineContext,
        sync_handler: SyncRecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        raw_def: dict[str, Any] = {"type": "sync", "params": {}}
        settings = mock_recipe.get_settings.return_value
        settings.get_recipe_raw_definition.return_value = raw_def
        settings.get_flat_input_refs.return_value = ["ds_a"]
        settings.get_flat_output_refs.return_value = ["ds_b"]
        mock_recipe.get_metadata.return_value = {"description": "desc", "tags": ["t1"]}

        prior = ResourceInstance(
            address="dss_sync_recipe.my_sync",
            resource_type="dss_sync_recipe",
            name="my_sync",
        )
        result = sync_handler.read(ctx, prior)

        assert result is not None
        assert result["name"] == "my_sync"
        assert result["type"] == "sync"
        assert result["inputs"] == ["ds_a"]
        assert result["outputs"] == ["ds_b"]
        assert result["description"] == "desc"
        assert result["tags"] == ["t1"]

    def test_python_includes_code_and_env(
        self,
        ctx: EngineContext,
        python_handler: PythonRecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        raw_def: dict[str, Any] = {
            "type": "python",
            "params": {
                "envSelection": {"envMode": "EXPLICIT_ENV", "envName": "py39"},
            },
        }
        settings = mock_recipe.get_settings.return_value
        settings.get_recipe_raw_definition.return_value = raw_def
        settings.get_flat_input_refs.return_value = []
        settings.get_flat_output_refs.return_value = []
        settings.str_payload = "print('hi')"

        prior = ResourceInstance(
            address="dss_python_recipe.my_py",
            resource_type="dss_python_recipe",
            name="my_py",
        )
        result = python_handler.read(ctx, prior)

        assert result is not None
        assert result["code"] == "print('hi')"
        assert result["code_env"] == "py39"

    def test_python_no_explicit_env_returns_none(
        self,
        ctx: EngineContext,
        python_handler: PythonRecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        raw_def: dict[str, Any] = {
            "type": "python",
            "params": {
                "envSelection": {"envMode": "USE_BUILTIN_MODE"},
            },
        }
        settings = mock_recipe.get_settings.return_value
        settings.get_recipe_raw_definition.return_value = raw_def
        settings.get_flat_input_refs.return_value = []
        settings.get_flat_output_refs.return_value = []
        settings.str_payload = ""

        prior = ResourceInstance(
            address="dss_python_recipe.my_py",
            resource_type="dss_python_recipe",
            name="my_py",
        )
        result = python_handler.read(ctx, prior)

        assert result is not None
        assert result["code_env"] is None

    def test_sql_includes_code(
        self,
        ctx: EngineContext,
        sql_handler: SQLQueryRecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        raw_def: dict[str, Any] = {"type": "sql_query", "params": {}}
        settings = mock_recipe.get_settings.return_value
        settings.get_recipe_raw_definition.return_value = raw_def
        settings.get_flat_input_refs.return_value = []
        settings.get_flat_output_refs.return_value = ["out_ds"]
        settings.str_payload = "SELECT * FROM t"

        prior = ResourceInstance(
            address="dss_sql_query_recipe.my_sql",
            resource_type="dss_sql_query_recipe",
            name="my_sql",
        )
        result = sql_handler.read(ctx, prior)

        assert result is not None
        assert result["code"] == "SELECT * FROM t"

    def test_returns_none_when_deleted(
        self,
        ctx: EngineContext,
        handler: RecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        mock_recipe.get_metadata.side_effect = Exception("Not found")

        prior = ResourceInstance(
            address="dss_sync_recipe.my_sync",
            resource_type="dss_sync_recipe",
            name="my_sync",
        )
        result = handler.read(ctx, prior)
        assert result is None


# ---------------------------------------------------------------------------
# Update tests
# ---------------------------------------------------------------------------


class TestUpdate:
    def test_modifies_inputs_outputs(
        self,
        ctx: EngineContext,
        sync_handler: SyncRecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        raw_def: dict[str, Any] = {"type": "sync", "params": {}, "inputs": {}, "outputs": {}}
        mock_recipe.get_settings.return_value.get_recipe_raw_definition.return_value = raw_def

        desired = SyncRecipeResource(name="my_sync", inputs=["new_in"], outputs=["new_out"])
        prior = ResourceInstance(
            address="dss_sync_recipe.my_sync",
            resource_type="dss_sync_recipe",
            name="my_sync",
        )
        sync_handler.update(ctx, desired, prior)

        assert raw_def["inputs"] == {"main": {"items": [{"ref": "new_in"}]}}
        assert raw_def["outputs"] == {"main": {"items": [{"ref": "new_out", "appendMode": False}]}}

    def test_python_sets_code(
        self,
        ctx: EngineContext,
        python_handler: PythonRecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        raw_def: dict[str, Any] = {"type": "python", "params": {}}
        mock_recipe.get_settings.return_value.get_recipe_raw_definition.return_value = raw_def

        desired = PythonRecipeResource(name="my_py", outputs=["out"], code="new code")
        prior = ResourceInstance(
            address="dss_python_recipe.my_py",
            resource_type="dss_python_recipe",
            name="my_py",
        )
        python_handler.update(ctx, desired, prior)
        mock_recipe.get_settings.return_value.set_payload.assert_called_with("new code")

    def test_sql_sets_code(
        self,
        ctx: EngineContext,
        sql_handler: SQLQueryRecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        raw_def: dict[str, Any] = {"type": "sql_query", "params": {}}
        mock_recipe.get_settings.return_value.get_recipe_raw_definition.return_value = raw_def

        desired = SQLQueryRecipeResource(
            name="my_sql", inputs=["in_ds"], outputs=["out"], code="SELECT 2"
        )
        prior = ResourceInstance(
            address="dss_sql_query_recipe.my_sql",
            resource_type="dss_sql_query_recipe",
            name="my_sql",
        )
        sql_handler.update(ctx, desired, prior)
        mock_recipe.get_settings.return_value.set_payload.assert_called_with("SELECT 2")

    def test_saves_settings(
        self,
        ctx: EngineContext,
        sync_handler: SyncRecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        raw_def: dict[str, Any] = {"type": "sync", "params": {}}
        mock_recipe.get_settings.return_value.get_recipe_raw_definition.return_value = raw_def

        desired = SyncRecipeResource(name="my_sync", outputs=["out"])
        prior = ResourceInstance(
            address="dss_sync_recipe.my_sync",
            resource_type="dss_sync_recipe",
            name="my_sync",
        )
        sync_handler.update(ctx, desired, prior)
        mock_recipe.get_settings.return_value.save.assert_called()


# ---------------------------------------------------------------------------
# Delete tests
# ---------------------------------------------------------------------------


class TestDelete:
    def test_calls_recipe_delete(
        self,
        ctx: EngineContext,
        handler: RecipeHandler,
        mock_project: MagicMock,  # noqa: ARG002
        mock_recipe: MagicMock,
    ) -> None:
        prior = ResourceInstance(
            address="dss_sync_recipe.my_sync",
            resource_type="dss_sync_recipe",
            name="my_sync",
        )
        handler.delete(ctx, prior)
        mock_recipe.delete.assert_called_once()


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------


class TestValidatePlanSQLInput:
    def test_rejects_non_sql_input(
        self,
        ctx: EngineContext,
        sql_handler: SQLQueryRecipeHandler,
    ) -> None:
        from dss_provisioner.resources.dataset import FilesystemDatasetResource

        desired = SQLQueryRecipeResource(
            name="my_sql", inputs=["fs_ds"], outputs=["out_ds"], code="SELECT 1"
        )
        fs_ds = FilesystemDatasetResource(
            name="fs_ds", connection="filesystem_managed", path="/data"
        )
        all_desired = {
            desired.address: desired,
            fs_ds.address: fs_ds,
        }
        plan_ctx = PlanContext(all_desired, State(project_key="PRJ"))

        errors = sql_handler.validate_plan(ctx, desired, plan_ctx)
        assert len(errors) == 1
        assert "SQL connection" in errors[0]

    def test_accepts_co_planned_sql_input(
        self,
        ctx: EngineContext,
        sql_handler: SQLQueryRecipeHandler,
    ) -> None:
        from dss_provisioner.resources.dataset import SnowflakeDatasetResource

        desired = SQLQueryRecipeResource(
            name="my_sql", inputs=["sf_ds"], outputs=["out_ds"], code="SELECT 1"
        )
        sf_ds = SnowflakeDatasetResource(
            name="sf_ds", connection="snowflake_conn", schema_name="public", table="t1"
        )
        all_desired = {
            desired.address: desired,
            sf_ds.address: sf_ds,
        }
        plan_ctx = PlanContext(all_desired, State(project_key="PRJ"))

        errors = sql_handler.validate_plan(ctx, desired, plan_ctx)
        assert errors == []

    def test_accepts_state_sql_input(
        self,
        ctx: EngineContext,
        sql_handler: SQLQueryRecipeHandler,
    ) -> None:
        desired = SQLQueryRecipeResource(
            name="my_sql", inputs=["pg_ds"], outputs=["out_ds"], code="SELECT 1"
        )
        all_desired = {desired.address: desired}
        state = State(
            project_key="PRJ",
            resources={
                "dss_dataset.pg_ds": ResourceInstance(
                    address="dss_dataset.pg_ds",
                    resource_type="dss_dataset",
                    name="pg_ds",
                    attributes={"type": "PostgreSQL"},
                )
            },
        )
        plan_ctx = PlanContext(all_desired, state)

        errors = sql_handler.validate_plan(ctx, desired, plan_ctx)
        assert errors == []


# ---------------------------------------------------------------------------
# Engine integration / roundtrip tests
# ---------------------------------------------------------------------------


def _setup_engine(
    tmp_path: Path,
    raw_def: dict[str, Any],
    meta: dict[str, Any] | None = None,
    str_payload: str = "",
    flat_inputs: list[str] | None = None,
    flat_outputs: list[str] | None = None,
    initial_state_resources: dict[str, ResourceInstance] | None = None,
) -> tuple[DSSEngine, MagicMock, MagicMock]:
    """Wire up a DSSEngine with typed recipe handlers and mocked dataikuapi objects."""
    mock_client = MagicMock()
    # Default code envs for validate_plan.
    mock_client.list_code_envs.return_value = [
        {"envName": "py39", "envLang": "PYTHON"},
    ]
    provider = DSSProvider.from_client(mock_client)

    project = MagicMock()
    mock_client.get_project.return_value = project

    # Flow zones (empty by default).
    flow = MagicMock()
    flow.list_zones.return_value = []
    project.get_flow.return_value = flow

    recipe = MagicMock()
    project.get_recipe.return_value = recipe

    # Builder
    builder = MagicMock()
    builder.create.return_value = recipe
    project.new_recipe.return_value = builder

    # Settings
    settings = MagicMock()
    settings.get_recipe_raw_definition.return_value = raw_def
    settings.get_flat_input_refs.return_value = flat_inputs if flat_inputs is not None else []
    settings.get_flat_output_refs.return_value = flat_outputs if flat_outputs is not None else []
    settings.str_payload = str_payload
    recipe.get_settings.return_value = settings

    recipe.get_metadata.return_value = meta or {"description": "", "tags": []}

    registry = ResourceTypeRegistry()
    registry.register(DatasetResource, DatasetHandler())
    registry.register(RecipeResource, RecipeHandler())
    registry.register(SyncRecipeResource, SyncRecipeHandler())
    registry.register(PythonRecipeResource, PythonRecipeHandler())
    registry.register(SQLQueryRecipeResource, SQLQueryRecipeHandler())

    state_path = tmp_path / "state.json"

    engine = DSSEngine(
        provider=provider,
        project_key="PRJ",
        state_path=state_path,
        registry=registry,
    )

    # Seed initial state if provided (e.g. for cross-resource validation).
    if initial_state_resources:
        state = State(project_key="PRJ", resources=initial_state_resources)
        state.save(state_path)

    return engine, project, recipe


class TestSyncRecipeRoundtrip:
    def test_create_noop_cycle(self, tmp_path: Path) -> None:
        raw_def: dict[str, Any] = {"type": "sync", "params": {}}
        engine, _project, _recipe = _setup_engine(
            tmp_path, raw_def, flat_inputs=["ds_a"], flat_outputs=["ds_b"]
        )

        r = SyncRecipeResource(name="my_sync", inputs=["ds_a"], outputs=["ds_b"])
        plan = engine.plan([r])
        assert plan.changes[0].action == Action.CREATE
        engine.apply(plan)

        state = State.load(engine.state_path)
        assert "dss_sync_recipe.my_sync" in state.resources

        # NOOP
        plan2 = engine.plan([r])
        assert plan2.changes[0].action == Action.NOOP


class TestPythonRecipeRoundtrip:
    def test_create_noop_cycle(self, tmp_path: Path) -> None:
        raw_def: dict[str, Any] = {
            "type": "python",
            "params": {
                "envSelection": {"envMode": "EXPLICIT_ENV", "envName": "py39"},
            },
        }
        engine, _project, _recipe = _setup_engine(
            tmp_path,
            raw_def,
            str_payload="print('hi')",
            flat_inputs=["ds_a"],
            flat_outputs=["ds_b"],
        )

        r = PythonRecipeResource(
            name="my_py",
            inputs=["ds_a"],
            outputs=["ds_b"],
            code="print('hi')",
            code_env="py39",
        )
        plan = engine.plan([r])
        assert plan.changes[0].action == Action.CREATE
        engine.apply(plan)

        state = State.load(engine.state_path)
        assert "dss_python_recipe.my_py" in state.resources
        assert state.resources["dss_python_recipe.my_py"].attributes["code"] == "print('hi')"
        assert state.resources["dss_python_recipe.my_py"].attributes["code_env"] == "py39"

        # NOOP
        plan2 = engine.plan([r])
        assert plan2.changes[0].action == Action.NOOP


class TestSQLQueryRecipeRoundtrip:
    def test_create_noop_cycle(self, tmp_path: Path) -> None:
        raw_def: dict[str, Any] = {"type": "sql_query", "params": {}}
        # Seed a PostgreSQL dataset in state so SQL validate_plan passes.
        pg_ds = ResourceInstance(
            address="dss_dataset.in_ds",
            resource_type="dss_dataset",
            name="in_ds",
            attributes={"type": "PostgreSQL"},
        )
        engine, _project, _recipe = _setup_engine(
            tmp_path,
            raw_def,
            str_payload="SELECT 1",
            flat_inputs=["in_ds"],
            flat_outputs=["out_ds"],
            initial_state_resources={"dss_dataset.in_ds": pg_ds},
        )

        r = SQLQueryRecipeResource(
            name="my_sql", inputs=["in_ds"], outputs=["out_ds"], code="SELECT 1"
        )
        # refresh=False because we seeded state with a resource type not in registry.
        plan = engine.plan([r], refresh=False)
        # Plan includes CREATE for recipe + DELETE for the seeded dataset.
        recipe_changes = [c for c in plan.changes if c.resource_type == "dss_sql_query_recipe"]
        assert recipe_changes[0].action == Action.CREATE
        engine.apply(plan)

        state = State.load(engine.state_path)
        assert "dss_sql_query_recipe.my_sql" in state.resources
        assert state.resources["dss_sql_query_recipe.my_sql"].attributes["code"] == "SELECT 1"

        # Re-seed the state with the dataset for the NOOP plan.
        state.resources["dss_dataset.in_ds"] = pg_ds
        state.save(engine.state_path)

        # NOOP
        plan2 = engine.plan([r], refresh=False)
        recipe_changes2 = [c for c in plan2.changes if c.resource_type == "dss_sql_query_recipe"]
        assert recipe_changes2[0].action == Action.NOOP


class TestRecipeUpdateDetectsCodeChange:
    def test_code_change_triggers_update(self, tmp_path: Path) -> None:
        raw_def: dict[str, Any] = {"type": "python", "params": {}}
        engine, _project, _recipe = _setup_engine(
            tmp_path, raw_def, str_payload="old code", flat_outputs=["out_ds"]
        )

        r = PythonRecipeResource(name="my_py", code="old code", outputs=["out_ds"])

        # CREATE
        plan = engine.plan([r])
        assert plan.changes[0].action == Action.CREATE
        engine.apply(plan)

        # Now change the code
        r_updated = PythonRecipeResource(name="my_py", code="new code", outputs=["out_ds"])
        plan2 = engine.plan([r_updated])
        assert plan2.changes[0].action == Action.UPDATE
        assert plan2.changes[0].diff is not None
        assert plan2.changes[0].diff["code"]["from"] == "old code"
        assert plan2.changes[0].diff["code"]["to"] == "new code"


# ---------------------------------------------------------------------------
# Python recipe code_env validation tests
# ---------------------------------------------------------------------------


class TestValidatePlanCodeEnv:
    def test_valid_code_env_accepted(
        self,
        ctx: EngineContext,
        python_handler: PythonRecipeHandler,
    ) -> None:
        ctx.provider.client.list_code_envs.return_value = [
            {"envName": "py39_ml", "envLang": "PYTHON"},
        ]
        desired = PythonRecipeResource(name="my_py", outputs=["out"], code_env="py39_ml")
        plan_ctx = PlanContext({desired.address: desired}, State(project_key="PRJ"))

        errors = python_handler.validate_plan(ctx, desired, plan_ctx)
        assert errors == []

    def test_unknown_code_env_rejected(
        self,
        ctx: EngineContext,
        python_handler: PythonRecipeHandler,
    ) -> None:
        ctx.provider.client.list_code_envs.return_value = [
            {"envName": "py39_ml", "envLang": "PYTHON"},
        ]
        desired = PythonRecipeResource(name="my_py", outputs=["out"], code_env="nonexistent")
        plan_ctx = PlanContext({desired.address: desired}, State(project_key="PRJ"))

        errors = python_handler.validate_plan(ctx, desired, plan_ctx)
        assert len(errors) == 1
        assert "nonexistent" in errors[0]
        assert "my_py" in errors[0]

    def test_none_code_env_skips_validation(
        self,
        ctx: EngineContext,
        python_handler: PythonRecipeHandler,
    ) -> None:
        desired = PythonRecipeResource(name="my_py", outputs=["out"])
        plan_ctx = PlanContext({desired.address: desired}, State(project_key="PRJ"))

        errors = python_handler.validate_plan(ctx, desired, plan_ctx)
        assert errors == []
        ctx.provider.client.list_code_envs.assert_not_called()

    def test_caches_code_env_list(
        self,
        ctx: EngineContext,
        python_handler: PythonRecipeHandler,
    ) -> None:
        ctx.provider.client.list_code_envs.return_value = [
            {"envName": "py39_ml", "envLang": "PYTHON"},
        ]
        d1 = PythonRecipeResource(name="r1", outputs=["o1"], code_env="py39_ml")
        d2 = PythonRecipeResource(name="r2", outputs=["o2"], code_env="py39_ml")
        plan_ctx = PlanContext({d1.address: d1, d2.address: d2}, State(project_key="PRJ"))

        python_handler.validate_plan(ctx, d1, plan_ctx)
        python_handler.validate_plan(ctx, d2, plan_ctx)

        # Should only call list_code_envs once (cached)
        ctx.provider.client.list_code_envs.assert_called_once()

    def test_r_env_not_matched_as_python(
        self,
        ctx: EngineContext,
        python_handler: PythonRecipeHandler,
    ) -> None:
        ctx.provider.client.list_code_envs.return_value = [
            {"envName": "r_base", "envLang": "R"},
        ]
        desired = PythonRecipeResource(name="my_py", outputs=["out"], code_env="r_base")
        plan_ctx = PlanContext({desired.address: desired}, State(project_key="PRJ"))

        errors = python_handler.validate_plan(ctx, desired, plan_ctx)
        assert len(errors) == 1
