"""Declarative field markers for resource models.

Three markers attach to Pydantic fields via ``Annotated``:

- ``Ref``      — field references another resource (consumed by ``reference_names()``)
- ``DSSParam`` — field maps to a path in the DSS raw definition dict
- ``Compare``  — field-level comparison strategy used by the engine

Helper functions introspect these markers at runtime to automate
``reference_names()``, ``to_dss_params()``, ``_read_attrs()`` extraction,
and per-field comparison strategies.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, TypeVar

from pydantic_core import PydanticUndefined

if TYPE_CHECKING:
    from pydantic.fields import FieldInfo

M = TypeVar("M")
CompareStrategy: TypeAlias = Literal["partial", "exact", "set"]


@dataclass(frozen=True, slots=True)
class ResourceRef:
    """Resolved reference value extracted from a ``Ref``-annotated field."""

    name: str
    resource_type: str | None = None


# ── Marker dataclasses ──────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class Ref:
    """Field references another resource.

    ``resource_type`` is optional — ``None`` means "any resource".
    """

    resource_type: str | None = None


@dataclass(frozen=True, slots=True)
class DSSParam:
    """Field maps to a path in the DSS raw definition dict.

    ``path`` is dot-separated, e.g. ``"params.schema"`` → ``raw["params"]["schema"]``.
    """

    path: str


@dataclass(frozen=True, slots=True)
class Compare:
    """How the engine should compare the field.

    - ``"partial"``: for dict values, only keys declared in desired are compared
    - ``"exact"``: strict equality comparison
    - ``"set"``: order-insensitive list comparison
    """

    strategy: CompareStrategy


# ── Shared introspection primitives ─────────────────────────────────


def _find_marker(field_info: FieldInfo, marker_type: type[M]) -> M | None:
    """Return the first marker of *marker_type* on a field, or ``None``."""
    return next((m for m in field_info.metadata if isinstance(m, marker_type)), None)


def _iter_marked_fields(
    model_or_cls: Any,
    marker_type: type[M],
) -> list[tuple[str, FieldInfo, M]]:
    """Return ``(field_name, field_info, marker)`` for every field carrying *marker_type*."""
    cls = model_or_cls if isinstance(model_or_cls, type) else type(model_or_cls)
    return [
        (name, fi, marker)
        for name, fi in cls.model_fields.items()
        if (marker := _find_marker(fi, marker_type)) is not None
    ]


def _resolve_path(raw: dict[str, Any], path: str, default: Any = None) -> Any:
    """Resolve a dot-separated path in a nested dict."""
    current: Any = raw
    for segment in path.split("."):
        if not isinstance(current, dict) or segment not in current:
            return default
        current = current[segment]
    return current


def _field_default(fi: FieldInfo) -> Any:
    """Model default for a field, or ``None`` for required fields."""
    if fi.default is not PydanticUndefined:
        return fi.default
    if fi.default_factory is not None:
        return fi.default_factory()  # type: ignore[call-arg]
    return None


def _coerce_to_list(value: Any) -> list[str]:
    """Normalize a scalar, list, or ``None`` to a flat list of strings."""
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


# ── Public helpers ──────────────────────────────────────────────────


def collect_ref_specs(resource: Any) -> list[ResourceRef]:
    """Collect typed references from ``Ref``-annotated fields."""
    refs: list[ResourceRef] = []
    for name, _, marker in _iter_marked_fields(resource, Ref):
        refs.extend(
            ResourceRef(name=ref, resource_type=marker.resource_type)
            for ref in _coerce_to_list(getattr(resource, name))
        )
    return refs


def collect_refs(resource: Any) -> list[str]:
    """Collect reference names from ``Ref``-annotated fields."""
    return [ref.name for ref in collect_ref_specs(resource)]


def collect_compare_strategies(resource_or_cls: Any) -> dict[str, CompareStrategy]:
    """Collect per-field compare strategies from ``Compare`` markers."""
    return {
        name: marker.strategy for name, _, marker in _iter_marked_fields(resource_or_cls, Compare)
    }


def extract_dss_attrs(resource_cls: type, raw: dict[str, Any]) -> dict[str, Any]:
    """Extract model attrs from a DSS raw definition via ``DSSParam`` markers."""
    return {
        name: _resolve_path(raw, marker.path, _field_default(fi))
        for name, fi, marker in _iter_marked_fields(resource_cls, DSSParam)
    }


def build_dss_params(resource: Any) -> dict[str, Any]:
    """Build DSS API params dict from ``DSSParam('params.*')`` fields."""
    return {
        marker.path.removeprefix("params."): getattr(resource, name)
        for name, _, marker in _iter_marked_fields(resource, DSSParam)
        if marker.path.startswith("params.") and getattr(resource, name) is not None
    }
