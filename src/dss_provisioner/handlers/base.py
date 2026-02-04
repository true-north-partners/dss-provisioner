"""Base handler protocol for DSS resources."""

from typing import Any, Protocol, TypeVar

import dataikuapi

from dss_provisioner.resources.base import Resource

R = TypeVar("R", bound=Resource)


class BaseHandler(Protocol[R]):
    """Protocol for resource handlers.

    Each handler knows how to CRUD a specific resource type.
    """

    client: dataikuapi.DSSClient

    def create(self, resource: R) -> dict[str, Any]:
        """Create the resource in DSS."""
        ...

    def read(self, resource: R) -> dict[str, Any] | None:
        """Read the resource from DSS. Returns None if not found."""
        ...

    def update(self, resource: R, prior: dict[str, Any]) -> dict[str, Any]:
        """Update the resource in DSS."""
        ...

    def delete(self, resource: R) -> None:
        """Delete the resource from DSS."""
        ...
