"""Engine-facing handler interfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from dss_provisioner.resources.base import Resource

if TYPE_CHECKING:
    from dss_provisioner.core import DSSProvider, ResourceInstance

R = TypeVar("R", bound=Resource)


@dataclass(frozen=True)
class EngineContext:
    """Context passed to handlers."""

    provider: DSSProvider
    project_key: str


class ResourceHandler(Protocol[R]):
    """CRUD interface for a resource type.

    Handlers are responsible for translating resources into DSS API calls.
    """

    def read(self, ctx: EngineContext, prior: ResourceInstance) -> dict[str, Any] | None:
        """Read the resource from DSS. Return None if it no longer exists."""

    def create(self, ctx: EngineContext, desired: R) -> dict[str, Any]:
        """Create the resource in DSS. Return stored attributes."""

    def update(self, ctx: EngineContext, desired: R, prior: ResourceInstance) -> dict[str, Any]:
        """Update the resource in DSS. Return stored attributes."""

    def delete(self, ctx: EngineContext, prior: ResourceInstance) -> None:
        """Delete the resource from DSS."""
