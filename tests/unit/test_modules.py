"""Tests for the Python module system (config/modules.py)."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from dss_provisioner.config.modules import (
    ModuleExpansionError,
    ModuleSpec,
    _resolve_callable,
    expand_modules,
)
from dss_provisioner.resources.dataset import FilesystemDatasetResource
from dss_provisioner.resources.zone import ZoneResource

if TYPE_CHECKING:
    from pathlib import Path


def _with_spec(call: str, params: dict[str, Any]) -> ModuleSpec:
    """Create a ModuleSpec using the ``with`` alias (keyword-safe helper)."""
    return ModuleSpec.model_validate({"call": call, "with": params})


# ── helpers ─────────────────────────────────────────────────────────


def _make_zone(*, name: str, **_kwargs: Any) -> list[ZoneResource]:
    return [ZoneResource(name=name)]


def _make_dataset(*, name: str, table: str, **_kwargs: Any) -> list[FilesystemDatasetResource]:
    return [
        FilesystemDatasetResource(
            name=name,
            connection="fs_managed",
            path=f"/data/{table}",
        )
    ]


def _bad_return_dict(**_kwargs: Any) -> dict[str, Any]:
    return {"not": "a list"}


def _bad_return_strings(**_kwargs: Any) -> list[str]:
    return ["not", "resources"]


def _raises(**_kwargs: Any) -> list[Any]:
    msg = "something went wrong"
    raise ValueError(msg)


# ── ModuleSpec validation ───────────────────────────────────────────


class TestModuleSpecValidation:
    def test_with_invocation(self) -> None:
        spec = _with_spec("mod:fn", {"key": "val"})
        assert spec.with_ == {"key": "val"}
        assert spec.instances is None

    def test_instances_invocation(self) -> None:
        spec = ModuleSpec(call="mod:fn", instances={"a": {"x": 1}})
        assert spec.instances == {"a": {"x": 1}}
        assert spec.with_ is None

    def test_neither_raises(self) -> None:
        with pytest.raises(ValidationError, match="Exactly one"):
            ModuleSpec(call="mod:fn")

    def test_both_raises(self) -> None:
        with pytest.raises(ValidationError, match="Exactly one"):
            ModuleSpec.model_validate({"call": "mod:fn", "instances": {"a": {}}, "with": {"b": 1}})

    def test_extra_forbid(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            ModuleSpec(call="mod:fn", instances={"a": {}}, unknown="bad")  # type: ignore[call-arg]


# ── _resolve_callable ───────────────────────────────────────────────


class TestResolveCallable:
    def test_entry_point_lookup(self, tmp_path: Path) -> None:
        mock_ep = type("EP", (), {"load": staticmethod(lambda: _make_zone)})()
        with patch(
            "dss_provisioner.config.modules.importlib.metadata.entry_points",
            return_value=[mock_ep],
        ):
            fn = _resolve_callable("my_module", tmp_path)
        assert fn is _make_zone

    def test_entry_point_not_found(self, tmp_path: Path) -> None:
        with (
            patch(
                "dss_provisioner.config.modules.importlib.metadata.entry_points",
                return_value=[],
            ),
            pytest.raises(ModuleExpansionError, match="No entry point found"),
        ):
            _resolve_callable("nonexistent", tmp_path)

    def test_direct_import(self, tmp_path: Path) -> None:
        fn = _resolve_callable("tests.unit.test_modules:_make_zone", tmp_path)
        assert fn is _make_zone

    def test_local_file_fallback(self, tmp_path: Path) -> None:
        mod_dir = tmp_path / "mymod"
        mod_dir.mkdir()
        (mod_dir / "pipeline.py").write_text("def build(name):\n    return []\n")
        fn = _resolve_callable("mymod.pipeline:build", tmp_path)
        assert callable(fn)

    def test_local_file_not_found(self, tmp_path: Path) -> None:
        with pytest.raises(ModuleExpansionError, match="not found"):
            _resolve_callable("nonexistent.mod:fn", tmp_path)

    def test_broken_internal_import_not_swallowed(self, tmp_path: Path) -> None:
        mod_dir = tmp_path / "mymod"
        mod_dir.mkdir()
        (mod_dir / "broken.py").write_text("import nonexistent_dependency_xyz\n")
        with pytest.raises(ModuleExpansionError, match="failed to import"):
            _resolve_callable("mymod.broken:fn", tmp_path)

    def test_missing_function(self, tmp_path: Path) -> None:
        mod_dir = tmp_path / "mymod"
        mod_dir.mkdir()
        (mod_dir / "pipeline.py").write_text("x = 1\n")
        with pytest.raises(ModuleExpansionError, match="has no attribute 'missing'"):
            _resolve_callable("mymod.pipeline:missing", tmp_path)

    def test_not_callable(self, tmp_path: Path) -> None:
        mod_dir = tmp_path / "mymod"
        mod_dir.mkdir()
        (mod_dir / "pipeline.py").write_text("not_a_fn = 42\n")
        with pytest.raises(ModuleExpansionError, match="is not a callable attribute"):
            _resolve_callable("mymod.pipeline:not_a_fn", tmp_path)

    def test_bad_call_syntax(self, tmp_path: Path) -> None:
        with pytest.raises(ModuleExpansionError, match="Invalid call syntax"):
            _resolve_callable(":no_module", tmp_path)


# ── expand_modules — happy path ─────────────────────────────────────


class TestExpandModulesHappyPath:
    def test_single_with(self, tmp_path: Path) -> None:
        spec = _with_spec("tests.unit.test_modules:_make_zone", {"name": "raw"})
        resources = expand_modules([spec], tmp_path)
        assert len(resources) == 1
        assert isinstance(resources[0], ZoneResource)
        assert resources[0].name == "raw"

    def test_instances_passes_name(self, tmp_path: Path) -> None:
        spec = ModuleSpec(
            call="tests.unit.test_modules:_make_zone",
            instances={"staging": {}},
        )
        resources = expand_modules([spec], tmp_path)
        assert len(resources) == 1
        assert resources[0].name == "staging"

    def test_instances_multiple(self, tmp_path: Path) -> None:
        spec = ModuleSpec(
            call="tests.unit.test_modules:_make_dataset",
            instances={
                "customers": {"table": "CUSTOMERS"},
                "orders": {"table": "ORDERS"},
            },
        )
        resources = expand_modules([spec], tmp_path)
        assert len(resources) == 2
        names = {r.name for r in resources}
        assert names == {"customers", "orders"}

    def test_mixed_modules(self, tmp_path: Path) -> None:
        specs = [
            ModuleSpec(
                call="tests.unit.test_modules:_make_zone",
                instances={"raw": {}, "curated": {}},
            ),
            _with_spec("tests.unit.test_modules:_make_dataset", {"name": "ds", "table": "T"}),
        ]
        resources = expand_modules(specs, tmp_path)
        assert len(resources) == 3

    def test_empty_modules_list(self, tmp_path: Path) -> None:
        assert expand_modules([], tmp_path) == []


# ── expand_modules — local imports ──────────────────────────────────


class TestExpandModulesLocalImport:
    def test_local_module_import(self, tmp_path: Path) -> None:
        mod_dir = tmp_path / "modules"
        mod_dir.mkdir()
        (mod_dir / "pipelines.py").write_text(
            "from dss_provisioner.resources.zone import ZoneResource\n"
            "def make_zone(name):\n"
            "    return [ZoneResource(name=name)]\n"
        )
        spec = _with_spec("modules.pipelines:make_zone", {"name": "test_zone"})
        resources = expand_modules([spec], tmp_path)
        assert len(resources) == 1
        assert resources[0].name == "test_zone"

    def test_no_sys_path_mutation(self, tmp_path: Path) -> None:
        original_path = list(sys.path)
        mod_dir = tmp_path / "modules"
        mod_dir.mkdir()
        (mod_dir / "noop.py").write_text(
            "from dss_provisioner.resources.zone import ZoneResource\n"
            "def noop(name):\n"
            "    return [ZoneResource(name=name)]\n"
        )
        spec = _with_spec("modules.noop:noop", {"name": "z"})
        expand_modules([spec], tmp_path)
        assert sys.path == original_path


# ── expand_modules — error cases ────────────────────────────────────


class TestExpandModulesErrors:
    def test_bad_call_syntax_no_colon_no_entry_point(self, tmp_path: Path) -> None:
        with patch(
            "dss_provisioner.config.modules.importlib.metadata.entry_points",
            return_value=[],
        ):
            spec = _with_spec("nonexistent_ep", {"name": "x"})
            with pytest.raises(ModuleExpansionError, match="No entry point found"):
                expand_modules([spec], tmp_path)

    def test_returns_non_list(self, tmp_path: Path) -> None:
        spec = _with_spec("tests.unit.test_modules:_bad_return_dict", {"name": "x"})
        with pytest.raises(ModuleExpansionError, match="must return list\\[Resource\\]"):
            expand_modules([spec], tmp_path)

    def test_returns_non_resources(self, tmp_path: Path) -> None:
        spec = _with_spec("tests.unit.test_modules:_bad_return_strings", {"name": "x"})
        with pytest.raises(ModuleExpansionError, match="must return list\\[Resource\\]"):
            expand_modules([spec], tmp_path)

    def test_function_raises(self, tmp_path: Path) -> None:
        spec = _with_spec("tests.unit.test_modules:_raises", {"name": "x"})
        with pytest.raises(ModuleExpansionError, match="raised ValueError"):
            expand_modules([spec], tmp_path)
