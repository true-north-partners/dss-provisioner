"""Tests for parse-time and plan-time resource validation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar
from unittest.mock import MagicMock

import pytest
from pydantic import Field, ValidationError

from dss_provisioner.core import DSSProvider, ResourceInstance
from dss_provisioner.core.state import State
from dss_provisioner.engine import DSSEngine
from dss_provisioner.engine.dataset_handler import DatasetHandler
from dss_provisioner.engine.errors import ValidationError as EngineValidationError
from dss_provisioner.engine.handlers import EngineContext, ResourceHandler
from dss_provisioner.engine.recipe_handler import (
    RecipeHandler,
    SQLQueryRecipeHandler,
    SyncRecipeHandler,
)
from dss_provisioner.engine.registry import ResourceTypeRegistry
from dss_provisioner.resources.base import Resource
from dss_provisioner.resources.dataset import (
    Column,
    DatasetResource,
    FilesystemDatasetResource,
    OracleDatasetResource,
    SnowflakeDatasetResource,
)
from dss_provisioner.resources.git_library import GitLibraryResource
from dss_provisioner.resources.recipe import (
    RecipeResource,
    SQLQueryRecipeResource,
    SyncRecipeResource,
)
from dss_provisioner.resources.zone import ZoneResource

if TYPE_CHECKING:
    from pathlib import Path


# ---------------------------------------------------------------------------
# Tier 1: Parse-time validation (Pydantic model constraints)
# ---------------------------------------------------------------------------


class TestNameValidation:
    def test_empty_name_rejected(self) -> None:
        with pytest.raises(ValidationError, match="name"):
            ZoneResource(name="")

    def test_name_with_spaces_rejected(self) -> None:
        with pytest.raises(ValidationError, match="name"):
            ZoneResource(name="my zone")

    def test_name_with_special_chars_rejected(self) -> None:
        with pytest.raises(ValidationError, match="name"):
            ZoneResource(name="my-zone")

    @pytest.mark.parametrize("name", ["raw", "Zone_1", "A", "a_b_c", "zone123", "_leading"])
    def test_valid_names_accepted(self, name: str) -> None:
        z = ZoneResource(name=name)
        assert z.name == name


class TestTagValidation:
    def test_empty_tag_rejected(self) -> None:
        with pytest.raises(ValidationError, match="tags"):
            ZoneResource(name="z", tags=["good", ""])

    def test_valid_tags_accepted(self) -> None:
        z = ZoneResource(name="z", tags=["etl", "raw"])
        assert z.tags == ["etl", "raw"]


class TestZoneColorValidation:
    @pytest.mark.parametrize(
        "color",
        ["red", "#GGG", "#12345", "#1234567", "2ab1ac", "##123456"],
    )
    def test_invalid_hex_rejected(self, color: str) -> None:
        with pytest.raises(ValidationError, match="color"):
            ZoneResource(name="z", color=color)

    @pytest.mark.parametrize("color", ["#2ab1ac", "#FFFFFF", "#000000", "#A1B2C3"])
    def test_valid_hex_accepted(self, color: str) -> None:
        z = ZoneResource(name="z", color=color)
        assert z.color == color

    def test_default_color_valid(self) -> None:
        z = ZoneResource(name="z")
        assert z.color == "#2ab1ac"


class TestColumnNameValidation:
    def test_empty_column_name_rejected(self) -> None:
        with pytest.raises(ValidationError, match="name"):
            Column(name="", type="string")

    def test_valid_column_name_accepted(self) -> None:
        c = Column(name="user_id", type="string")
        assert c.name == "user_id"


class TestGitLibraryRepositoryValidation:
    def test_empty_repository_rejected(self) -> None:
        with pytest.raises(ValidationError, match="repository"):
            GitLibraryResource(name="lib", repository="")

    def test_valid_repository_accepted(self) -> None:
        g = GitLibraryResource(name="lib", repository="https://github.com/org/repo.git")
        assert g.repository == "https://github.com/org/repo.git"


class TestDatasetFieldValidation:
    def test_snowflake_empty_schema_rejected(self) -> None:
        with pytest.raises(ValidationError, match="schema_name"):
            SnowflakeDatasetResource(name="ds", connection="conn", schema_name="", table="t")

    def test_snowflake_empty_table_rejected(self) -> None:
        with pytest.raises(ValidationError, match="table"):
            SnowflakeDatasetResource(name="ds", connection="conn", schema_name="public", table="")

    def test_oracle_empty_schema_rejected(self) -> None:
        with pytest.raises(ValidationError, match="schema_name"):
            OracleDatasetResource(name="ds", connection="conn", schema_name="", table="t")

    def test_oracle_empty_table_rejected(self) -> None:
        with pytest.raises(ValidationError, match="table"):
            OracleDatasetResource(name="ds", connection="conn", schema_name="public", table="")

    def test_filesystem_empty_path_rejected(self) -> None:
        with pytest.raises(ValidationError, match="path"):
            FilesystemDatasetResource(name="ds", connection="conn", path="")


class TestRecipeOutputValidation:
    def test_recipe_without_outputs_rejected(self) -> None:
        with pytest.raises(ValidationError, match="outputs"):
            SyncRecipeResource(name="s")  # type: ignore[call-arg]

    def test_recipe_with_empty_outputs_list_rejected(self) -> None:
        with pytest.raises(ValidationError, match="outputs"):
            SyncRecipeResource(name="s", outputs=[])

    def test_recipe_with_empty_output_element_rejected(self) -> None:
        with pytest.raises(ValidationError, match="outputs"):
            SyncRecipeResource(name="s", outputs=[""])

    def test_recipe_with_valid_outputs_accepted(self) -> None:
        r = SyncRecipeResource(name="s", outputs=["out"])
        assert r.outputs == ["out"]

    def test_sql_recipe_without_inputs_rejected(self) -> None:
        with pytest.raises(ValidationError, match="inputs"):
            SQLQueryRecipeResource(name="s", outputs=["out"])  # type: ignore[call-arg]

    def test_sql_recipe_with_empty_inputs_list_rejected(self) -> None:
        with pytest.raises(ValidationError, match="inputs"):
            SQLQueryRecipeResource(name="s", inputs=[], outputs=["out"])

    def test_sql_recipe_with_empty_input_element_rejected(self) -> None:
        with pytest.raises(ValidationError, match="inputs"):
            SQLQueryRecipeResource(name="s", inputs=[""], outputs=["out"])


# ---------------------------------------------------------------------------
# Tier 2: Plan-time validation
# ---------------------------------------------------------------------------


class DummyResource(Resource):
    resource_type: ClassVar[str] = "dummy"
    value: int = 0
    config: dict[str, Any] = Field(default_factory=dict)


class InMemoryHandler(ResourceHandler[DummyResource]):
    def __init__(self) -> None:
        self.store: dict[str, dict[str, Any]] = {}

    def read(self, ctx: EngineContext, prior: ResourceInstance) -> dict[str, Any] | None:
        _ = ctx
        return self.store.get(prior.address)

    def create(self, ctx: EngineContext, desired: DummyResource) -> dict[str, Any]:
        _ = ctx
        attrs = {
            "name": desired.name,
            "value": desired.value,
            "config": desired.config,
            "description": desired.description,
            "tags": list(desired.tags),
        }
        self.store[desired.address] = attrs
        return attrs

    def update(
        self,
        ctx: EngineContext,
        desired: DummyResource,
        prior: ResourceInstance,  # noqa: ARG002
    ) -> dict[str, Any]:
        return self.create(ctx, desired)

    def delete(self, ctx: EngineContext, prior: ResourceInstance) -> None:
        _ = ctx
        self.store.pop(prior.address, None)


def _engine(tmp_path: Path) -> DSSEngine:
    provider = DSSProvider.from_client(MagicMock())
    registry = ResourceTypeRegistry()
    registry.register(DummyResource, InMemoryHandler())
    registry.register(ZoneResource, ResourceHandler())
    registry.register(DatasetResource, DatasetHandler())
    registry.register(FilesystemDatasetResource, DatasetHandler())
    registry.register(SyncRecipeResource, SyncRecipeHandler())
    registry.register(RecipeResource, RecipeHandler())
    registry.register(SQLQueryRecipeResource, SQLQueryRecipeHandler())
    registry.register(SnowflakeDatasetResource, DatasetHandler())
    return DSSEngine(
        provider=provider,
        project_key="PRJ",
        state_path=tmp_path / "state.json",
        registry=registry,
    )


class TestDependsOnValidation:
    def test_depends_on_unknown_address_rejected(self, tmp_path: Path) -> None:
        engine = _engine(tmp_path)
        r = DummyResource(name="a", value=1, depends_on=["dummy.nonexistent"])
        with pytest.raises(EngineValidationError, match=r"unknown address 'dummy\.nonexistent'"):
            engine.plan([r], refresh=False)

    def test_depends_on_known_address_accepted(self, tmp_path: Path) -> None:
        engine = _engine(tmp_path)
        b = DummyResource(name="b", value=2)
        a = DummyResource(name="a", value=1, depends_on=["dummy.b"])
        plan = engine.plan([a, b], refresh=False)
        assert len(plan.changes) == 2

    def test_depends_on_state_address_accepted(self, tmp_path: Path) -> None:
        engine = _engine(tmp_path)
        state = State(
            project_key="PRJ",
            resources={
                "dummy.b": ResourceInstance(
                    address="dummy.b",
                    resource_type="dummy",
                    name="b",
                    attributes={
                        "name": "b",
                        "value": 2,
                        "config": {},
                        "description": "",
                        "tags": [],
                    },
                )
            },
        )
        state.save(engine.state_path)

        a = DummyResource(name="a", value=1, depends_on=["dummy.b"])
        plan = engine.plan([a], refresh=False)
        assert len(plan.changes) >= 1


class TestZoneReferenceValidation:
    def test_dataset_unknown_zone_rejected(self, tmp_path: Path) -> None:
        engine = _engine(tmp_path)
        ds = FilesystemDatasetResource(
            name="ds", connection="conn", path="/data", zone="nonexistent"
        )
        with pytest.raises(EngineValidationError, match="unknown zone 'nonexistent'"):
            engine.plan([ds], refresh=False)

    def test_dataset_known_zone_accepted(self, tmp_path: Path) -> None:
        engine = _engine(tmp_path)
        z = ZoneResource(name="raw")
        ds = FilesystemDatasetResource(name="ds", connection="conn", path="/data", zone="raw")
        plan = engine.plan([ds, z], refresh=False)
        assert len(plan.changes) == 2

    def test_dataset_zone_accepted_with_cross_namespace_name_collision(
        self, tmp_path: Path
    ) -> None:
        engine = _engine(tmp_path)
        z = ZoneResource(name="raw")
        ds = FilesystemDatasetResource(name="raw", connection="conn", path="/data", zone="raw")
        plan = engine.plan([ds, z], refresh=False)
        assert [c.address for c in plan.changes] == ["dss_zone.raw", "dss_filesystem_dataset.raw"]

    def test_recipe_unknown_zone_rejected(self, tmp_path: Path) -> None:
        engine = _engine(tmp_path)
        r = SyncRecipeResource(name="r", outputs=["out"], zone="nonexistent")
        with pytest.raises(EngineValidationError, match="unknown zone 'nonexistent'"):
            engine.plan([r], refresh=False)

    def test_recipe_known_zone_accepted(self, tmp_path: Path) -> None:
        engine = _engine(tmp_path)
        z = ZoneResource(name="raw")
        r = SyncRecipeResource(name="r", outputs=["out"], zone="raw")
        plan = engine.plan([r, z], refresh=False)
        assert len(plan.changes) == 2
