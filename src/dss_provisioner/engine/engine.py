"""Plan/apply engine."""

from __future__ import annotations

import contextlib
import hashlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from dss_provisioner import __version__
from dss_provisioner.core.state import State, compute_attributes_hash, compute_state_digest
from dss_provisioner.engine.errors import (
    ApplyCanceled,
    DuplicateAddressError,
    StalePlanError,
    StateProjectMismatchError,
)
from dss_provisioner.engine.graph import DependencyGraph
from dss_provisioner.engine.handlers import EngineContext
from dss_provisioner.engine.lock import StateLock
from dss_provisioner.engine.operations import (
    BarrierOperation,
    CreateOperation,
    DeleteOperation,
    UpdateOperation,
)
from dss_provisioner.engine.types import Action, ApplyResult, Plan, PlanMetadata, ResourceChange

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from dss_provisioner.core import DSSProvider
    from dss_provisioner.engine.operations import Operation
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
        lock_cm = StateLock(self._state_path) if refresh else contextlib.nullcontext()
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
                self._registry.get(r.resource_type)
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
                    # Fail early if the plan would require a handler we don't have.
                    self._registry.get(inst.resource_type)
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
                    # Fail early if the plan would require a handler we don't have.
                    self._registry.get(inst.resource_type)
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

    def _operation_order(self, plan: Plan, state: State) -> list[Operation]:
        """Compute a deterministic operation order using an operation graph."""
        ops = self._build_apply_operations(plan, state)
        dep_map = {k: op.deps for k, op in ops.items()}
        order = DependencyGraph(ops.keys(), dep_map).topological_order()
        return [ops[k] for k in order]

    def _build_apply_operations(self, plan: Plan, state: State) -> dict[str, Operation]:
        ops: dict[str, Operation] = {}
        create_update_set: set[str] = set()
        delete_set: set[str] = set()

        for c in plan.changes:
            op: Operation
            match c.action:
                case Action.NOOP:
                    continue
                case Action.CREATE:
                    op = CreateOperation(key=c.address, change=c)
                    create_update_set.add(c.address)
                case Action.UPDATE:
                    op = UpdateOperation(key=c.address, change=c)
                    create_update_set.add(c.address)
                case Action.DELETE:
                    op = DeleteOperation(key=c.address, change=c)
                    delete_set.add(c.address)
                case _:
                    raise ValueError(f"Unknown action: {c.action}")

            if op.key in ops:
                raise ValueError(f"Duplicate operation key in plan: {op.key}")
            ops[op.key] = op

        # create/update: dependencies must run before dependents
        for addr in create_update_set:
            op = ops[addr]
            assert op.change is not None
            if op.change.desired is None:
                raise ValueError(f"Missing desired config for create/update: {addr}")
            deps = op.change.desired.get("depends_on", [])
            if deps is None:
                deps = []
            if not isinstance(deps, list) or not all(isinstance(d, str) for d in deps):
                raise ValueError(f"Invalid depends_on for {addr}: expected list[str]")
            op.deps.extend([d for d in deps if d in create_update_set])

        # deletes: dependents must be deleted before dependencies (invert edges)
        for addr in delete_set:
            inst = state.resources.get(addr)
            if inst is None:
                raise ValueError(f"Missing state for delete operation: {addr}")
            for dep in inst.dependencies:
                if dep in delete_set:
                    ops[dep].deps.append(addr)

        # Ensure create/update runs before deletes (Terraform-like default ordering).
        if create_update_set and delete_set:
            barrier_key = "__engine__.apply_barrier"
            if barrier_key in ops:
                raise ValueError(f"Barrier operation key conflicts with plan: {barrier_key}")

            ops[barrier_key] = BarrierOperation(key=barrier_key, deps=sorted(create_update_set))
            for addr in delete_set:
                ops[addr].deps.append(barrier_key)

        return ops

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
            ordered_ops = self._operation_order(plan, state)

            try:
                for op in ordered_ops:
                    did_change = op.run(ctx=ctx, state=state, registry=self._registry)
                    if not did_change:
                        continue

                    assert op.change is not None

                    state.serial += 1
                    state.save(self._state_path)
                    applied.append(op.change)
            except KeyboardInterrupt as e:  # pragma: no cover
                raise ApplyCanceled("Apply canceled") from e

            return ApplyResult(applied=applied)
