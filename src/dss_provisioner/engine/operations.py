"""Apply operations.

Terraform runs apply by executing a graph of operations (resource nodes + other
nodes). This module implements a minimal version of that idea: each operation
knows how to apply itself and lists dependencies on other operations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol

from dss_provisioner.core.state import ResourceInstance, State, compute_attributes_hash

if TYPE_CHECKING:
    from dss_provisioner.engine.handlers import EngineContext
    from dss_provisioner.engine.registry import ResourceTypeRegistry
    from dss_provisioner.engine.types import ResourceChange


class Operation(Protocol):
    key: str
    deps: list[str]
    change: ResourceChange | None

    def run(self, *, ctx: EngineContext, state: State, registry: ResourceTypeRegistry) -> bool:
        """Execute this operation.

        Returns:
            True if state should be persisted (serial bump + write).
        """


@dataclass
class BarrierOperation:
    """A no-op node used to enforce ordering between operation phases."""

    key: str
    deps: list[str] = field(default_factory=list)
    change: ResourceChange | None = None

    def run(self, *, ctx: EngineContext, state: State, registry: ResourceTypeRegistry) -> bool:
        _ = ctx
        _ = state
        _ = registry
        return False


def _desired_object(change: ResourceChange, reg: Any, *, action: str) -> Any:
    if change.desired is None:
        raise ValueError(f"Missing desired config for {action}: {change.address}")

    desired_obj = reg.model.model_validate(change.desired)
    if desired_obj.address != change.address:
        raise ValueError(
            f"Desired address mismatch for {action}: {change.address} != {desired_obj.address}"
        )
    return desired_obj


@dataclass
class CreateOperation:
    key: str
    change: ResourceChange | None
    deps: list[str] = field(default_factory=list)

    def run(self, *, ctx: EngineContext, state: State, registry: ResourceTypeRegistry) -> bool:
        assert self.change is not None
        reg = registry.get(self.change.resource_type)
        handler = reg.handler
        desired_obj = _desired_object(self.change, reg, action="create")

        attrs = handler.create(ctx, desired_obj)
        now = datetime.now(UTC)
        inst = ResourceInstance(
            address=self.change.address,
            resource_type=self.change.resource_type,
            name=desired_obj.name,
            attributes=attrs,
            attributes_hash=compute_attributes_hash(attrs),
            dependencies=list(desired_obj.depends_on),
            created_at=now,
            updated_at=now,
        )
        state.resources[self.change.address] = inst
        return True


@dataclass
class UpdateOperation:
    key: str
    change: ResourceChange | None
    deps: list[str] = field(default_factory=list)

    def run(self, *, ctx: EngineContext, state: State, registry: ResourceTypeRegistry) -> bool:
        assert self.change is not None
        reg = registry.get(self.change.resource_type)
        handler = reg.handler
        desired_obj = _desired_object(self.change, reg, action="update")

        prior_inst = state.resources[self.change.address]
        attrs = handler.update(ctx, desired_obj, prior_inst)

        now = datetime.now(UTC)
        prior_inst.attributes = attrs
        prior_inst.attributes_hash = compute_attributes_hash(attrs)
        prior_inst.dependencies = list(desired_obj.depends_on)
        prior_inst.updated_at = now
        return True


@dataclass
class DeleteOperation:
    key: str
    change: ResourceChange | None
    deps: list[str] = field(default_factory=list)

    def run(self, *, ctx: EngineContext, state: State, registry: ResourceTypeRegistry) -> bool:
        assert self.change is not None
        reg = registry.get(self.change.resource_type)
        handler = reg.handler

        prior_inst = state.resources[self.change.address]
        handler.delete(ctx, prior_inst)
        del state.resources[self.change.address]
        return True
