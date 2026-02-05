"""Resource type registry for handler dispatch."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dss_provisioner.engine.errors import UnknownResourceTypeError

if TYPE_CHECKING:
    from dss_provisioner.engine.handlers import ResourceHandler
    from dss_provisioner.resources.base import Resource


@dataclass(frozen=True)
class ResourceTypeRegistration:
    resource_type: str
    model: type[Resource]
    handler: ResourceHandler[Any]


class ResourceTypeRegistry:
    """Registry mapping resource_type -> (model, handler)."""

    def __init__(self) -> None:
        self._registrations: dict[str, ResourceTypeRegistration] = {}

    def register(self, model: type[Resource], handler: ResourceHandler[Any]) -> None:
        resource_type = getattr(model, "resource_type", None)
        if not isinstance(resource_type, str) or not resource_type:
            raise ValueError("Resource model must define a non-empty classvar `resource_type`")

        if resource_type in self._registrations:
            raise ValueError(f"Resource type already registered: {resource_type}")

        self._registrations[resource_type] = ResourceTypeRegistration(
            resource_type=resource_type,
            model=model,
            handler=handler,
        )

    def get(self, resource_type: str) -> ResourceTypeRegistration:
        try:
            return self._registrations[resource_type]
        except KeyError as e:
            raise UnknownResourceTypeError(resource_type) from e
