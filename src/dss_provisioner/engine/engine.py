"""Plan/apply engine."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from dss_provisioner import __version__
from dss_provisioner.core.state import (
    ResourceInstance,
    State,
    compute_attributes_hash,
    compute_state_digest,
)
from dss_provisioner.engine.errors import (
    ApplyCanceled,
    DuplicateAddressError,
    StalePlanError,
    StateProjectMismatchError,
    UnknownResourceTypeError,
)
from dss_provisioner.engine.graph import DependencyGraph
from dss_provisioner.engine.handlers import EngineContext
from dss_provisioner.engine.lock import StateLock
from dss_provisioner.engine.types import Action, ApplyResult, Plan, PlanMetadata, ResourceChange

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from dss_provisioner.core import DSSProvider
    from dss_provisioner.engine.registry import ResourceTypeRegistry
    from dss_provisioner.resources.base import Resource


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def _sha256_hex(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _compute_config_digest(resources: Sequence[Resource]) -> str:
    items: list[dict[str, Any]] = []
    for r in resources:
        desired = r.model_dump(exclude_none=True, exclude={"address"})
        planned = dict(desired)
        planned.pop("depends_on", None)
        items.append(
            {
                "address": r.address,
                "resource_type": r.resource_type,
                "planned": planned,
            }
        )
    items.sort(key=lambda x: x["address"])
    return _sha256_hex(_canonical_json(items))


class DSSEngine:
    """Terraform-like plan/apply engine for DSS resources."""

    def __init__(
        self,
        *,
        provider: DSSProvider,
        project_key: str,
        state_path: Path,
        registry: ResourceTypeRegistry,
    ) -> None:
        self._provider = provider
        self._project_key = project_key
        self._state_path = state_path
        self._registry = registry

    @property
    def project_key(self) -> str:
        return self._project_key

    @property
    def state_path(self) -> Path:
        return self._state_path

    def _ctx(self) -> EngineContext:
        return EngineContext(provider=self._provider, project_key=self._project_key)

    def _load_state(self) -> State:
        state = State.load_or_create(self._state_path, project_key=self._project_key)
        if state.project_key != self._project_key:
            raise StateProjectMismatchError(self._project_key, state.project_key)
        return state

    def _load_state_for_apply(self, plan: Plan) -> State:
        if self._state_path.exists():
            return self._load_state()
        # If no state exists, bootstrap from the plan metadata (saved-plan semantics).
        return State(
            project_key=self._project_key,
            lineage=plan.metadata.state_lineage,
            serial=plan.metadata.state_serial,
        )

    def _refresh_state_in_place(self, state: State) -> bool:
        changed = False
        ctx = self._ctx()

        for address, inst in list(state.resources.items()):
            handler = self._registry.get(inst.resource_type).handler
            attrs = handler.read(ctx, inst)
            if attrs is None:
                del state.resources[address]
                changed = True
                continue

            new_hash = compute_attributes_hash(attrs)
            if attrs != inst.attributes or new_hash != inst.attributes_hash:
                inst.attributes = attrs
                inst.attributes_hash = new_hash
                inst.updated_at = datetime.now(UTC)
                changed = True

        return changed

    def refresh(self) -> State:
        with StateLock(self._state_path):
            state = self._load_state()
            changed = self._refresh_state_in_place(state)
            if changed:
                state.serial += 1
                state.save(self._state_path)
            return state

    def plan(
        self, resources: Sequence[Resource], *, destroy: bool = False, refresh: bool = True
    ) -> Plan:
        # Only lock when refresh may write state.
        lock_cm = StateLock(self._state_path) if refresh else _NoopContextManager()
        with lock_cm:
            state = self._load_state()

            if refresh:
                changed = self._refresh_state_in_place(state)
                if changed:
                    state.serial += 1
                    state.save(self._state_path)

            desired_by_addr: dict[str, Resource] = {}
            for r in resources:
                if r.address in desired_by_addr:
                    raise DuplicateAddressError(r.address)
                # Ensure resource type is known up front
                try:
                    self._registry.get(r.resource_type)
                except UnknownResourceTypeError as e:
                    raise e
                desired_by_addr[r.address] = r

            desired_addrs = set(desired_by_addr.keys())
            state_addrs = set(state.resources.keys())

            # Create/update/noop changes in desired topo order
            desired_dep_map: dict[str, list[str]] = {}
            for addr, r in desired_by_addr.items():
                deps = [d for d in r.depends_on if d in desired_addrs]
                desired_dep_map[addr] = deps

            desired_order = DependencyGraph(desired_addrs, desired_dep_map).topological_order()

            changes: list[ResourceChange] = []

            if destroy:
                # Plan deletes for everything in state, ordered by state dependencies.
                delete_order = self._delete_order(state, state_addrs)
                for addr in delete_order:
                    inst = state.resources[addr]
                    changes.append(
                        ResourceChange(
                            address=addr,
                            resource_type=inst.resource_type,
                            action=Action.DELETE,
                            prior=dict(inst.attributes),
                        )
                    )
            else:
                for addr in desired_order:
                    r = desired_by_addr[addr]
                    desired_dump = r.model_dump(exclude_none=True, exclude={"address"})
                    planned_dump = dict(desired_dump)
                    planned_dump.pop("depends_on", None)

                    prior_inst = state.resources.get(addr)
                    if prior_inst is None:
                        changes.append(
                            ResourceChange(
                                address=addr,
                                resource_type=r.resource_type,
                                action=Action.CREATE,
                                desired=desired_dump,
                                planned=planned_dump,
                            )
                        )
                        continue

                    diff: dict[str, Any] = {}
                    for k, v in planned_dump.items():
                        prior_v = prior_inst.attributes.get(k)
                        if prior_v != v:
                            diff[k] = {"from": prior_v, "to": v}

                    if diff:
                        changes.append(
                            ResourceChange(
                                address=addr,
                                resource_type=r.resource_type,
                                action=Action.UPDATE,
                                desired=desired_dump,
                                prior=dict(prior_inst.attributes),
                                planned=planned_dump,
                                diff=diff,
                            )
                        )
                    else:
                        changes.append(
                            ResourceChange(
                                address=addr,
                                resource_type=r.resource_type,
                                action=Action.NOOP,
                                desired=desired_dump,
                                prior=dict(prior_inst.attributes),
                                planned=planned_dump,
                            )
                        )

                # Deletes for resources in state not in desired
                to_delete = state_addrs - desired_addrs
                delete_order = self._delete_order(state, to_delete)
                for addr in delete_order:
                    inst = state.resources[addr]
                    changes.append(
                        ResourceChange(
                            address=addr,
                            resource_type=inst.resource_type,
                            action=Action.DELETE,
                            prior=dict(inst.attributes),
                        )
                    )

            metadata = PlanMetadata(
                project_key=self._project_key,
                created_at=datetime.now(UTC),
                destroy=destroy,
                refresh=refresh,
                state_lineage=state.lineage,
                state_serial=state.serial,
                state_digest=compute_state_digest(state),
                config_digest=_compute_config_digest([] if destroy else resources),
                engine_version=__version__,
            )

            return Plan(metadata=metadata, changes=changes)

    def _delete_order(self, state: State, delete_set: set[str]) -> list[str]:
        dep_map: dict[str, list[str]] = {}
        for addr in delete_set:
            inst = state.resources[addr]
            dep_map[addr] = [d for d in inst.dependencies if d in delete_set]
        return DependencyGraph(delete_set, dep_map).reverse_topological_order()

    def apply(self, plan: Plan) -> ApplyResult:
        with StateLock(self._state_path):
            state = self._load_state_for_apply(plan)
            if state.project_key != self._project_key:
                raise StateProjectMismatchError(self._project_key, state.project_key)

            # Stale plan detection
            if state.lineage != plan.metadata.state_lineage:
                raise StalePlanError("State lineage changed; re-run plan")
            if state.serial != plan.metadata.state_serial:
                raise StalePlanError("State serial changed; re-run plan")
            if compute_state_digest(state) != plan.metadata.state_digest:
                raise StalePlanError("State digest changed; re-run plan")

            ctx = self._ctx()
            applied: list[ResourceChange] = []

            try:
                for change in plan.changes:
                    if change.action == Action.NOOP:
                        continue

                    reg = self._registry.get(change.resource_type)
                    handler = reg.handler

                    now = datetime.now(UTC)
                    if change.action == Action.CREATE:
                        if change.desired is None:
                            raise ValueError(f"Missing desired config for create: {change.address}")
                        desired_obj = reg.model.model_validate(change.desired)
                        attrs = handler.create(ctx, desired_obj)
                        inst = ResourceInstance(
                            address=change.address,
                            resource_type=change.resource_type,
                            name=desired_obj.name,
                            attributes=attrs,
                            attributes_hash=compute_attributes_hash(attrs),
                            dependencies=list(desired_obj.depends_on),
                            created_at=now,
                            updated_at=now,
                        )
                        state.resources[change.address] = inst
                    elif change.action == Action.UPDATE:
                        if change.desired is None:
                            raise ValueError(f"Missing desired config for update: {change.address}")
                        desired_obj = reg.model.model_validate(change.desired)
                        prior_inst = state.resources[change.address]
                        attrs = handler.update(ctx, desired_obj, prior_inst)
                        prior_inst.attributes = attrs
                        prior_inst.attributes_hash = compute_attributes_hash(attrs)
                        prior_inst.dependencies = list(desired_obj.depends_on)
                        prior_inst.updated_at = now
                    elif change.action == Action.DELETE:
                        prior_inst = state.resources[change.address]
                        handler.delete(ctx, prior_inst)
                        del state.resources[change.address]
                    else:
                        raise ValueError(f"Unknown action: {change.action}")

                    state.serial += 1
                    state.save(self._state_path)
                    applied.append(change)
            except KeyboardInterrupt as e:  # pragma: no cover
                raise ApplyCanceled("Apply canceled") from e

            return ApplyResult(applied=applied)


class _NoopContextManager:
    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type, exc, tb) -> None:
        return None
