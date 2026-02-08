"""Python module system for composable resource definitions."""

from __future__ import annotations

import importlib
import importlib.metadata
import importlib.util
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import ModuleType

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dss_provisioner.resources.base import Resource


class ModuleExpansionError(Exception):
    """Raised when module resolution or expansion fails."""


class ModuleSpec(BaseModel):
    """Specification for a single module invocation in the YAML config."""

    model_config = ConfigDict(extra="forbid")

    call: str
    instances: dict[str, dict[str, Any]] | None = None
    with_: dict[str, Any] | None = Field(default=None, alias="with")

    @model_validator(mode="after")
    def _exactly_one_invocation(self) -> Self:
        if (self.instances is None) == (self.with_ is None):
            msg = "Exactly one of 'instances' or 'with' must be provided"
            raise ValueError(msg)
        return self

    def invocations(self) -> list[tuple[dict[str, Any], str]]:
        """Return ``(kwargs, label)`` pairs for each call to make."""
        if self.instances is not None:
            return [
                ({"name": name, **params}, f"'{self.call}' instance '{name}'")
                for name, params in self.instances.items()
            ]
        # with_ is guaranteed non-None by the validator
        return [(self.with_, self.call)]  # type: ignore[list-item]


def _load_local_module(module_path: str, config_dir: Path) -> ModuleType:
    """Load a Python module from a file relative to *config_dir*."""
    parts = module_path.split(".")
    candidates = [
        config_dir / Path(*parts).with_suffix(".py"),
        config_dir / Path(*parts) / "__init__.py",
    ]
    file_path = next((p for p in candidates if p.exists()), None)
    if file_path is None:
        raise ModuleExpansionError(f"Module '{module_path}' not found relative to {config_dir}")

    spec = importlib.util.spec_from_file_location(module_path, file_path)
    if spec is None or spec.loader is None:
        raise ModuleExpansionError(f"Failed to create module spec for '{file_path}'")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _get_callable_attr(mod: Any, function_name: str, call: str) -> Callable[..., list[Resource]]:
    """Extract a callable attribute from a module, or raise."""
    obj = getattr(mod, function_name, None)
    if not callable(obj):
        raise ModuleExpansionError(
            f"'{call}' is not a callable attribute"
            if obj is not None
            else f"Module has no attribute '{function_name}' (from '{call}')"
        )
    return obj


def _resolve_callable(call: str, config_dir: Path) -> Callable[..., list[Resource]]:
    """Resolve a *call* string to a Python callable.

    Resolution order:

    1. No ``:``: entry-point lookup (group ``dss_provisioner.modules``).
    2. Has ``:``: split into ``module_path:function_name``.
       a. Try ``importlib.import_module`` (installed packages).
       b. Fall back to ``spec_from_file_location`` (local files relative to *config_dir*).
    """
    if ":" not in call:
        eps = list(importlib.metadata.entry_points(group="dss_provisioner.modules", name=call))
        if not eps:
            raise ModuleExpansionError(
                f"No entry point found for '{call}' in group 'dss_provisioner.modules'"
            )
        return eps[0].load()

    module_path, _, function_name = call.rpartition(":")
    if not module_path or not function_name:
        raise ModuleExpansionError(
            f"Invalid call syntax '{call}': expected 'module.path:function_name'"
        )

    try:
        mod = importlib.import_module(module_path)
    except ModuleNotFoundError:
        mod = _load_local_module(module_path, config_dir)

    return _get_callable_attr(mod, function_name, call)


def _call_fn(
    fn: Callable[..., list[Resource]], kwargs: dict[str, Any], label: str
) -> list[Resource]:
    """Call *fn* with *kwargs*, validate the return type, and wrap errors."""
    try:
        result = fn(**kwargs)
    except ModuleExpansionError:
        raise
    except Exception as exc:
        raise ModuleExpansionError(f"Module {label} raised {type(exc).__name__}: {exc}") from exc

    if not isinstance(result, list) or not all(isinstance(r, Resource) for r in result):
        raise ModuleExpansionError(f"Module {label} must return list[Resource]")
    return result


def expand_modules(modules: list[ModuleSpec], config_dir: Path) -> list[Resource]:
    """Expand all module specs into a flat list of resources."""
    resources: list[Resource] = []
    for spec in modules:
        fn = _resolve_callable(spec.call, config_dir)
        for kwargs, label in spec.invocations():
            resources.extend(_call_fn(fn, kwargs, label))
    return resources
