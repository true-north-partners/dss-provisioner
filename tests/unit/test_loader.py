"""Tests for code file loading and wrapper generation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from dss_provisioner.resources.loader import (
    _find_entry_function,
    _wrap_python_code,
    resolve_code_files,
)
from dss_provisioner.resources.recipe import (
    PythonRecipeResource,
    SQLQueryRecipeResource,
    SyncRecipeResource,
)

if TYPE_CHECKING:
    from pathlib import Path


# ---------------------------------------------------------------------------
# _find_entry_function
# ---------------------------------------------------------------------------


class TestFindEntryFunction:
    def test_single_public_function(self) -> None:
        code = "def transform(df):\n    return df"
        assert _find_entry_function(code) == "transform"

    def test_skips_private_functions(self) -> None:
        code = "def _helper():\n    pass\n\ndef process(df):\n    return df"
        assert _find_entry_function(code) == "process"

    def test_first_public_wins(self) -> None:
        code = "def first(df):\n    pass\n\ndef second(df):\n    pass"
        assert _find_entry_function(code) == "first"

    def test_no_public_function_raises(self) -> None:
        code = "def _private():\n    pass"
        with pytest.raises(ValueError, match="No public function"):
            _find_entry_function(code)

    def test_empty_code_raises(self) -> None:
        with pytest.raises(ValueError, match="No public function"):
            _find_entry_function("")

    def test_only_class_no_function(self) -> None:
        code = "class Foo:\n    pass"
        with pytest.raises(ValueError, match="No public function"):
            _find_entry_function(code)


# ---------------------------------------------------------------------------
# _wrap_python_code
# ---------------------------------------------------------------------------


class TestWrapPythonCode:
    def test_basic_wrap(self) -> None:
        code = "def clean(raw):\n    return raw.dropna()"
        result = _wrap_python_code(code, inputs=["raw_data"], outputs=["clean_data"])
        assert "import dataiku" in result
        assert "import pandas as pd" in result
        assert "def clean(raw):" in result
        assert '_inp0 = dataiku.Dataset("raw_data").get_dataframe()' in result
        assert "_result = clean(_inp0)" in result
        assert 'dataiku.Dataset("clean_data").write_with_schema(_result)' in result

    def test_multiple_inputs(self) -> None:
        code = "def merge(a, b):\n    return a.merge(b)"
        result = _wrap_python_code(code, inputs=["ds_a", "ds_b"], outputs=["merged"])
        assert '_inp0 = dataiku.Dataset("ds_a").get_dataframe()' in result
        assert '_inp1 = dataiku.Dataset("ds_b").get_dataframe()' in result
        assert "_result = merge(_inp0, _inp1)" in result

    def test_no_inputs(self) -> None:
        code = "def generate():\n    return 42"
        result = _wrap_python_code(code, inputs=[], outputs=["out"])
        assert "_result = generate()" in result

    def test_trailing_newline(self) -> None:
        code = "def f():\n    pass"
        result = _wrap_python_code(code, inputs=[], outputs=["out"])
        assert result.endswith("\n")

    def test_no_public_function_raises(self) -> None:
        with pytest.raises(ValueError, match="No public function"):
            _wrap_python_code("x = 1", inputs=[], outputs=["out"])


# ---------------------------------------------------------------------------
# resolve_code_files — explicit code_file
# ---------------------------------------------------------------------------


class TestResolveExplicitCodeFile:
    def test_python_raw_mode(self, tmp_path: Path) -> None:
        (tmp_path / "recipes").mkdir()
        (tmp_path / "recipes" / "my_recipe.py").write_text("print('hello')")

        r = PythonRecipeResource(
            name="my_recipe",
            code_file="recipes/my_recipe.py",
            inputs=["in_ds"],
            outputs=["out_ds"],
        )
        resolved = resolve_code_files([r], tmp_path)
        assert len(resolved) == 1
        assert resolved[0].code == "print('hello')"  # type: ignore[union-attr]

    def test_python_wrapped_mode(self, tmp_path: Path) -> None:
        (tmp_path / "recipes").mkdir()
        code = "def clean(raw):\n    return raw.dropna()\n"
        (tmp_path / "recipes" / "clean.py").write_text(code)

        r = PythonRecipeResource(
            name="clean",
            code_file="recipes/clean.py",
            code_wrapper=True,
            inputs=["raw_data"],
            outputs=["clean_data"],
        )
        resolved = resolve_code_files([r], tmp_path)
        assert "import dataiku" in resolved[0].code  # type: ignore[union-attr]
        assert "_result = clean(_inp0)" in resolved[0].code  # type: ignore[union-attr]

    def test_sql_raw_mode(self, tmp_path: Path) -> None:
        (tmp_path / "queries").mkdir()
        (tmp_path / "queries" / "agg.sql").write_text("SELECT count(*) FROM t")

        r = SQLQueryRecipeResource(
            name="agg",
            code_file="queries/agg.sql",
            inputs=["t"],
            outputs=["agg_t"],
        )
        resolved = resolve_code_files([r], tmp_path)
        assert resolved[0].code == "SELECT count(*) FROM t"  # type: ignore[union-attr]

    def test_missing_code_file_raises(self, tmp_path: Path) -> None:
        r = PythonRecipeResource(
            name="missing",
            code_file="recipes/nope.py",
            inputs=["in"],
            outputs=["out"],
        )
        with pytest.raises(FileNotFoundError):
            resolve_code_files([r], tmp_path)


# ---------------------------------------------------------------------------
# resolve_code_files — convention discovery
# ---------------------------------------------------------------------------


class TestResolveConvention:
    def test_python_convention(self, tmp_path: Path) -> None:
        (tmp_path / "recipes").mkdir()
        (tmp_path / "recipes" / "my_recipe.py").write_text("# code")

        r = PythonRecipeResource(name="my_recipe", inputs=["in_ds"], outputs=["out_ds"])
        resolved = resolve_code_files([r], tmp_path)
        assert resolved[0].code == "# code"  # type: ignore[union-attr]

    def test_sql_convention(self, tmp_path: Path) -> None:
        (tmp_path / "recipes").mkdir()
        (tmp_path / "recipes" / "my_sql.sql").write_text("SELECT 1")

        r = SQLQueryRecipeResource(name="my_sql", inputs=["t"], outputs=["out"])
        resolved = resolve_code_files([r], tmp_path)
        assert resolved[0].code == "SELECT 1"  # type: ignore[union-attr]

    def test_convention_file_missing_silently_skips(self, tmp_path: Path) -> None:
        r = PythonRecipeResource(name="no_file", inputs=["in_ds"], outputs=["out_ds"])
        resolved = resolve_code_files([r], tmp_path)
        assert resolved[0].code == ""  # type: ignore[union-attr]

    def test_convention_with_wrapper(self, tmp_path: Path) -> None:
        (tmp_path / "recipes").mkdir()
        code = "def transform(df):\n    return df\n"
        (tmp_path / "recipes" / "my_recipe.py").write_text(code)

        r = PythonRecipeResource(
            name="my_recipe",
            code_wrapper=True,
            inputs=["raw"],
            outputs=["clean"],
        )
        resolved = resolve_code_files([r], tmp_path)
        assert "import dataiku" in resolved[0].code  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# resolve_code_files — pass-through / skip cases
# ---------------------------------------------------------------------------


class TestResolvePassthrough:
    def test_inline_code_preserved(self, tmp_path: Path) -> None:
        r = PythonRecipeResource(name="inline", code="x = 1", inputs=["in"], outputs=["out"])
        resolved = resolve_code_files([r], tmp_path)
        assert resolved[0].code == "x = 1"  # type: ignore[union-attr]

    def test_non_code_resource_passed_through(self, tmp_path: Path) -> None:
        r = SyncRecipeResource(name="my_sync", inputs=["a"], outputs=["b"])
        resolved = resolve_code_files([r], tmp_path)
        assert len(resolved) == 1
        assert resolved[0].name == "my_sync"


# ---------------------------------------------------------------------------
# resolve_code_files — error cases
# ---------------------------------------------------------------------------


class TestResolveErrors:
    def test_wrapper_without_outputs_raises_at_parse_time(self) -> None:
        with pytest.raises(ValidationError, match="outputs"):
            PythonRecipeResource(
                name="no_out",
                code_file="recipes/no_out.py",
                code_wrapper=True,
                inputs=["in_ds"],
                outputs=[],
            )

    def test_wrapper_no_public_function_raises(self, tmp_path: Path) -> None:
        (tmp_path / "recipes").mkdir()
        (tmp_path / "recipes" / "bad.py").write_text("def _private():\n    pass\n")

        r = PythonRecipeResource(
            name="bad",
            code_file="recipes/bad.py",
            code_wrapper=True,
            inputs=["in"],
            outputs=["out"],
        )
        with pytest.raises(ValueError, match="No public function"):
            resolve_code_files([r], tmp_path)
