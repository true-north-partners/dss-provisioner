"""Engine-facing handler interfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from dss_provisioner.resources.base import Resource

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dss_provisioner.core import DSSProvider, ResourceInstance
    from dss_provisioner.core.state import State

R = TypeVar("R", bound=Resource)


@dataclass(frozen=True)
class EngineContext:
    """Context passed to handlers."""

    provider: DSSProvider
    project_key: str


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
        all_desired: Mapping[str, Resource],
        state: State,
    ) -> list[str]:
        """Cross-resource validation with access to full resource set + state.

        Return list of error messages (empty = valid).
        """
        _ = ctx, desired, all_desired, state
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
