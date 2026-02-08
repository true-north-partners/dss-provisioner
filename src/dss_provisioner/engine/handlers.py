"""Engine-facing handler interfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from dss_provisioner.core.state import ResourceInstance
from dss_provisioner.resources.base import Resource

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dss_provisioner.core import DSSProvider
    from dss_provisioner.core.state import State

R = TypeVar("R", bound=Resource)


@dataclass(frozen=True)
class EngineContext:
    """Context passed to handlers."""

    provider: DSSProvider
    project_key: str


class PlanContext:
    """Merged view of desired and existing resources for plan-level validation.

    Provides name-based lookups across both planned (desired) and existing
    (state) resources, with desired taking precedence.
    """

    def __init__(self, all_desired: Mapping[str, Resource], state: State) -> None:
        # Preserve all candidates per name (cross-namespace name reuse is valid),
        # while ensuring desired objects take precedence over state.
        self._by_name: dict[str, list[Resource | ResourceInstance]] = {}
        for i in state.resources.values():
            self._by_name.setdefault(i.name, []).append(i)
        for r in all_desired.values():
            entries = [e for e in self._by_name.get(r.name, []) if e.address != r.address]
            self._by_name[r.name] = [r, *entries]
        self._all_addresses: set[str] = set(all_desired.keys()) | set(state.resources.keys())

    @staticmethod
    def _resource_type(item: Resource | ResourceInstance) -> str:
        return item.resource_type

    @classmethod
    def _matches(
        cls,
        item: Resource | ResourceInstance,
        *,
        resource_type: str | None = None,
        resource_type_suffix: str | None = None,
    ) -> bool:
        item_resource_type = cls._resource_type(item)
        if resource_type is not None and item_resource_type != resource_type:
            return False
        return resource_type_suffix is None or item_resource_type.endswith(resource_type_suffix)

    def address_exists(self, address: str) -> bool:
        """Check if an address exists in desired or state."""
        return address in self._all_addresses

    def has_resource(
        self,
        name: str,
        *,
        resource_type: str | None = None,
        resource_type_suffix: str | None = None,
    ) -> bool:
        """Check if a matching resource with this name exists."""
        return any(
            self._matches(
                item,
                resource_type=resource_type,
                resource_type_suffix=resource_type_suffix,
            )
            for item in self._by_name.get(name, [])
        )

    def get_attr(
        self,
        name: str,
        attr: str,
        *,
        resource_type: str | None = None,
        resource_type_suffix: str | None = None,
    ) -> Any:
        """Look up an attribute from the first matching named resource."""
        for item in self._by_name.get(name, []):
            if not self._matches(
                item,
                resource_type=resource_type,
                resource_type_suffix=resource_type_suffix,
            ):
                continue
            if isinstance(item, ResourceInstance):
                return item.attributes.get(attr)
            return getattr(item, attr, None)
        return None


class ResourceHandler(Generic[R]):
    """Base class for resource handlers.

    Handlers are responsible for translating resources into DSS API calls.
    Subclass and override the CRUD methods. Validation methods are optional.
    """

    def validate(self, ctx: EngineContext, desired: R) -> list[str]:
        """Single-resource validation. No cross-resource context needed.

        Return list of error messages (empty = valid).
        """
        _ = ctx, desired
        return []

    def validate_plan(
        self,
        ctx: EngineContext,
        desired: R,
        plan_ctx: PlanContext,
    ) -> list[str]:
        """Cross-resource validation with access to all resources.

        Return list of error messages (empty = valid).
        """
        _ = ctx, desired, plan_ctx
        return []

    def read(self, ctx: EngineContext, prior: ResourceInstance) -> dict[str, Any] | None:
        """Read the resource from DSS. Return None if it no longer exists."""
        raise NotImplementedError

    def create(self, ctx: EngineContext, desired: R) -> dict[str, Any]:
        """Create the resource in DSS. Return stored attributes."""
        raise NotImplementedError

    def update(self, ctx: EngineContext, desired: R, prior: ResourceInstance) -> dict[str, Any]:
        """Update the resource in DSS. Return stored attributes."""
        raise NotImplementedError

    def delete(self, ctx: EngineContext, prior: ResourceInstance) -> None:
        """Delete the resource from DSS."""
        raise NotImplementedError
