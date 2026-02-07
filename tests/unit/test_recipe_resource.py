"""Tests for recipe resource models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dss_provisioner.resources.recipe import (
    PythonRecipeResource,
    RecipeResource,
    SQLQueryRecipeResource,
    SyncRecipeResource,
)


class TestRecipeResource:
    def test_address(self) -> None:
        r = RecipeResource(name="my_recipe", type="sync")
        assert r.address == "dss_recipe.my_recipe"

    def test_defaults(self) -> None:
        r = RecipeResource(name="my_recipe", type="sync")
        assert r.inputs == []
        assert r.outputs == []
        assert r.zone is None
        assert r.description == ""
        assert r.tags == []
        assert r.depends_on == []

    def test_reference_names_without_zone(self) -> None:
        r = RecipeResource(name="r", type="sync", inputs=["ds_a"], outputs=["ds_b"])
        assert r.reference_names() == ["ds_a", "ds_b"]

    def test_reference_names_with_zone(self) -> None:
        r = RecipeResource(name="r", type="sync", inputs=["ds_a"], zone="raw")
        assert r.reference_names() == ["ds_a", "raw"]

    def test_extra_forbid(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            RecipeResource(name="r", type="sync", unknown_field="x")  # type: ignore[call-arg]

    def test_model_dump_shape(self) -> None:
        r = RecipeResource(
            name="my_recipe",
            type="sync",
            inputs=["ds_a"],
            outputs=["ds_b"],
        )
        dump = r.model_dump(exclude_none=True, exclude={"address"})
        assert dump["name"] == "my_recipe"
        assert dump["type"] == "sync"
        assert dump["inputs"] == ["ds_a"]
        assert dump["outputs"] == ["ds_b"]
        assert "address" not in dump


class TestSyncRecipeResource:
    def test_address(self) -> None:
        r = SyncRecipeResource(name="my_sync")
        assert r.address == "dss_sync_recipe.my_sync"

    def test_defaults(self) -> None:
        r = SyncRecipeResource(name="my_sync")
        assert r.type == "sync"

    def test_type_locked(self) -> None:
        with pytest.raises(ValidationError):
            SyncRecipeResource(name="r", type="python")  # type: ignore[arg-type]

    def test_extra_forbid(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            SyncRecipeResource(name="r", unknown_field="x")  # type: ignore[call-arg]


class TestPythonRecipeResource:
    def test_address(self) -> None:
        r = PythonRecipeResource(name="my_python")
        assert r.address == "dss_python_recipe.my_python"

    def test_defaults(self) -> None:
        r = PythonRecipeResource(name="my_python")
        assert r.type == "python"
        assert r.code == ""
        assert r.code_env is None

    def test_type_locked(self) -> None:
        with pytest.raises(ValidationError):
            PythonRecipeResource(name="r", type="sync")  # type: ignore[arg-type]

    def test_extra_forbid(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            PythonRecipeResource(name="r", unknown_field="x")  # type: ignore[call-arg]

    def test_model_dump_shape(self) -> None:
        r = PythonRecipeResource(
            name="my_python",
            code="print('hello')",
            code_env="py39",
        )
        dump = r.model_dump(exclude_none=True, exclude={"address"})
        assert dump["type"] == "python"
        assert dump["code"] == "print('hello')"
        assert dump["code_env"] == "py39"

    def test_model_dump_excludes_none(self) -> None:
        r = PythonRecipeResource(name="my_python")
        dump = r.model_dump(exclude_none=True, exclude={"address"})
        assert "code_env" not in dump
        assert "zone" not in dump

    def test_code_file_defaults_none(self) -> None:
        r = PythonRecipeResource(name="my_python")
        assert r.code_file is None
        assert r.code_wrapper is False

    def test_code_and_code_file_mutual_exclusion(self) -> None:
        with pytest.raises(ValidationError, match="Cannot set both"):
            PythonRecipeResource(name="r", code="print('hi')", code_file="recipes/r.py")

    def test_code_file_excluded_from_dump(self) -> None:
        r = PythonRecipeResource(name="r", code_file="recipes/r.py")
        dump = r.model_dump()
        assert "code_file" not in dump
        assert "code_wrapper" not in dump


class TestSQLQueryRecipeResource:
    def test_address(self) -> None:
        r = SQLQueryRecipeResource(name="my_sql")
        assert r.address == "dss_sql_query_recipe.my_sql"

    def test_defaults(self) -> None:
        r = SQLQueryRecipeResource(name="my_sql")
        assert r.type == "sql_query"
        assert r.code == ""

    def test_type_locked(self) -> None:
        with pytest.raises(ValidationError):
            SQLQueryRecipeResource(name="r", type="python")  # type: ignore[arg-type]

    def test_extra_forbid(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            SQLQueryRecipeResource(name="r", unknown_field="x")  # type: ignore[call-arg]

    def test_model_dump_shape(self) -> None:
        r = SQLQueryRecipeResource(
            name="my_sql",
            code="SELECT 1",
        )
        dump = r.model_dump(exclude_none=True, exclude={"address"})
        assert dump["type"] == "sql_query"
        assert dump["code"] == "SELECT 1"

    def test_code_file_defaults_none(self) -> None:
        r = SQLQueryRecipeResource(name="my_sql")
        assert r.code_file is None

    def test_code_and_code_file_mutual_exclusion(self) -> None:
        with pytest.raises(ValidationError, match="Cannot set both"):
            SQLQueryRecipeResource(name="r", code="SELECT 1", code_file="recipes/r.sql")

    def test_code_file_excluded_from_dump(self) -> None:
        r = SQLQueryRecipeResource(name="r", code_file="recipes/r.sql")
        dump = r.model_dump()
        assert "code_file" not in dump
