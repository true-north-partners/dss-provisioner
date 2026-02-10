"""Code file loading and wrapper generation for code-bearing resources."""

from __future__ import annotations

import ast
import textwrap
from typing import TYPE_CHECKING

from dss_provisioner.resources.dataset import OracleDatasetResource, SnowflakeDatasetResource
from dss_provisioner.resources.recipe import PythonRecipeResource, SQLQueryRecipeResource
from dss_provisioner.resources.scenario import PythonScenarioResource

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from dss_provisioner.resources.base import Resource

_CodeResource = PythonRecipeResource | SQLQueryRecipeResource | PythonScenarioResource
_QueryResource = SnowflakeDatasetResource | OracleDatasetResource

_CODE_EXTENSIONS: dict[str, str] = {
    "dss_python_recipe": ".py",
    "dss_sql_query_recipe": ".sql",
    "dss_python_scenario": ".py",
    "dss_snowflake_dataset": ".sql",
    "dss_oracle_dataset": ".sql",
}

_CODE_DIRS: dict[str, str] = {
    "dss_python_recipe": "recipes",
    "dss_sql_query_recipe": "recipes",
    "dss_python_scenario": "scenarios",
    "dss_snowflake_dataset": "queries",
    "dss_oracle_dataset": "queries",
}


def resolve_code_files(resources: Iterable[Resource], base_dir: Path) -> list[Resource]:
    """Resolve ``code_file``/``query_file`` references and convention paths.

    Handles two categories:

    * **Code-bearing** resources (recipes, scenarios) — resolves ``code_file``
      or discovers ``{dir}/{name}{ext}`` by convention.
    * **Query-bearing** datasets (Snowflake, Oracle in query mode) — resolves
      ``query_file`` or discovers ``queries/{name}.sql`` by convention.
    """
    result: list[Resource] = []
    for resource in resources:
        if isinstance(resource, _CodeResource):
            content = _read_code(resource, base_dir)
            if content is not None:
                resource.code = _maybe_wrap(resource, content)
        elif isinstance(resource, _QueryResource):
            content = _read_query(resource, base_dir)
            if content is not None:
                resource.query = content

        result.append(resource)

    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _read_code(resource: _CodeResource, base_dir: Path) -> str | None:
    """Read code from an explicit file, convention path, or return None to skip."""
    if resource.code_file:
        return (base_dir / resource.code_file).read_text()

    if resource.code:
        return None  # already has inline code

    ext = _CODE_EXTENSIONS.get(resource.resource_type)
    if ext is None:
        return None

    code_dir = _CODE_DIRS.get(resource.resource_type, "recipes")
    convention_path = base_dir / code_dir / f"{resource.name}{ext}"
    if convention_path.exists():
        return convention_path.read_text()

    return None


def _read_query(resource: _QueryResource, base_dir: Path) -> str | None:
    """Read SQL from an explicit query_file, convention path, or return None to skip."""
    if resource.mode != "query":
        return None

    if resource.query_file:
        return (base_dir / resource.query_file).read_text()

    if resource.query:
        return None  # already has inline query

    ext = _CODE_EXTENSIONS.get(resource.resource_type)
    if ext is None:
        return None

    code_dir = _CODE_DIRS.get(resource.resource_type, "queries")
    convention_path = base_dir / code_dir / f"{resource.name}{ext}"
    if convention_path.exists():
        return convention_path.read_text()

    return None


def _maybe_wrap(recipe: _CodeResource, content: str) -> str:
    """Apply DSS boilerplate wrapping if ``code_wrapper`` is set."""
    if not isinstance(recipe, PythonRecipeResource) or not recipe.code_wrapper:
        return content

    if not recipe.outputs:
        msg = "code_wrapper requires at least one output dataset"
        raise ValueError(msg)

    return _wrap_python_code(content, inputs=recipe.inputs, outputs=recipe.outputs)


def _find_entry_function(code: str) -> str:
    """Return the name of the first public function in *code*.

    A "public" function is a module-level ``def`` whose name does not start
    with ``_``.
    """
    tree = ast.parse(code)
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef) and not node.name.startswith("_"):
            return node.name
    msg = "No public function found in code file"
    raise ValueError(msg)


_WRAPPER_TEMPLATE = textwrap.dedent("""\
    import dataiku
    import pandas as pd

    {user_code}

    # Auto-generated DSS recipe boilerplate
    {read_inputs}
    _result = {func_name}({func_args})
    dataiku.Dataset("{output_name}").write_with_schema(_result)
""")


def _wrap_python_code(code: str, *, inputs: list[str], outputs: list[str]) -> str:
    """Wrap user code with DSS recipe boilerplate.

    Auto-detects the entry-point function, reads input datasets as
    DataFrames, calls the function, and writes the result to the first
    output dataset.
    """
    func_name = _find_entry_function(code)

    input_vars: list[str] = []
    read_lines: list[str] = []
    for i, inp in enumerate(inputs):
        var = f"_inp{i}"
        input_vars.append(var)
        read_lines.append(f'{var} = dataiku.Dataset("{inp}").get_dataframe()')

    return _WRAPPER_TEMPLATE.format(
        user_code=code.rstrip(),
        read_inputs="\n".join(read_lines),
        func_name=func_name,
        func_args=", ".join(input_vars),
        output_name=outputs[0],
    )
