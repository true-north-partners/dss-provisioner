"""Plan/apply engine."""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

from dss_provisioner import __version__
from dss_provisioner.core.state import State, compute_attributes_hash, compute_state_digest
from dss_provisioner.engine.errors import (
    ApplyCanceled,
    ApplyError,
    DuplicateAddressError,
    StalePlanError,
    StateProjectMismatchError,
    ValidationError,
)
from dss_provisioner.engine.graph import DependencyGraph
from dss_provisioner.engine.handlers import EngineContext, PlanContext
from dss_provisioner.engine.lock import StateLock
from dss_provisioner.engine.operations import (
    BarrierOperation,
    CreateOperation,
    DeleteOperation,
    UpdateOperation,
)
from dss_provisioner.engine.types import Action, ApplyResult, Plan, PlanMetadata, ResourceChange
from dss_provisioner.engine.variables import get_variables, resolve_variables
from dss_provisioner.resources.markers import CompareStrategy, collect_compare_strategies

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[ResourceChange, Literal["start", "done"]], None]

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from dss_provisioner.core import DSSProvider
    from dss_provisioner.engine.operations import Operation
    from dss_provisioner.engine.registry import ResourceTypeRegistry
    from dss_provisioner.resources.base import Resource


def _values_differ(
    desired: Any,
    prior: Any,
    *,
    strategy: CompareStrategy | None = None,
) -> bool:
    """Check whether a desired value differs from the prior (stored) value.

    Comparison semantics depend on *strategy*:

    - ``strategy="set"``:
      - If both values are lists, they are compared as sets (order-insensitive).
      - Other types fall back to strict equality.
    - ``strategy="exact"``:
      - Values are compared with strict equality.
      - For dicts, extra or missing keys are treated as differences.
    - ``strategy=None`` or ``"partial"``:
      - For dict values, only keys present in *desired* are compared.
      - Extra keys present only in *prior* (provider-added defaults) are ignored.
      - Non-dict values use strict equality.
    """
    if strategy == "set":
        if isinstance(desired, list) and isinstance(prior, list):
            return set(desired) != set(prior)
        return desired != prior

    if strategy == "exact":
        return desired != prior

    if isinstance(desired, dict) and isinstance(prior, dict):
        return any(_values_differ(v, prior.get(k), strategy="partial") for k, v in desired.items())
    return desired != prior


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
        logger.debug("State loaded: serial=%d, %d resources", state.serial, len(state.resources))
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
        logger.debug("Refreshing state from DSS")
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

        logger.debug("State refreshed, changed=%s", changed)
        return changed

    def refresh(self, *, persist: bool = False) -> tuple[State, State]:
        """Refresh state from DSS. Returns (pre_refresh, post_refresh)."""
        with StateLock(self._state_path):
            state = self._load_state()
            snapshot = state.model_copy(deep=True)
            changed = self._refresh_state_in_place(state)
            if changed and persist:
                state.serial += 1
                state.save(self._state_path)
            return snapshot, state

    @staticmethod
    def _resolve_deps(desired_by_addr: dict[str, Resource]) -> dict[str, list[str]]:
        """Build dependency map: explicit depends_on + implicit from ``Ref`` markers.

        Returns addr → full dep list without mutating the Resource objects.
        """
        name_to_addrs: dict[str, list[str]] = {}
        typed_name_to_addrs: dict[tuple[str, str], list[str]] = {}
        for addr, r in desired_by_addr.items():
            name_to_addrs.setdefault(r.name, []).append(addr)
            typed_name_to_addrs.setdefault((r.resource_type, r.name), []).append(addr)

        dep_map: dict[str, list[str]] = {}
        for addr, r in desired_by_addr.items():
            deps = list(r.depends_on)
            for ref in r.references():
                if ref.resource_type is not None:
                    ref_addrs = typed_name_to_addrs.get((ref.resource_type, ref.name), [])
                else:
                    ref_addrs = name_to_addrs.get(ref.name, [])
                for ref_addr in ref_addrs:
                    if ref_addr != addr and ref_addr not in deps:
                        deps.append(ref_addr)
            dep_map[addr] = deps
        return dep_map

    def _classify_change(
        self,
        addr: str,
        resource: Resource,
        state: State,
        deps: list[str],
        variables: dict[str, str],
    ) -> ResourceChange:
        """Classify a single resource as CREATE, UPDATE, or NOOP."""
        desired_dump = resource.model_dump(exclude_none=True, exclude={"address"})
        desired_dump["depends_on"] = deps
        planned = {k: v for k, v in desired_dump.items() if k != "depends_on"}
        resolved_planned = resolve_variables(planned, variables)

        prior_inst = state.resources.get(addr)
        if prior_inst is None:
            logger.debug("Classified %s as create", addr)
            return ResourceChange(
                address=addr,
                resource_type=resource.resource_type,
                action=Action.CREATE,
                desired=desired_dump,
                planned=resolved_planned,
            )

        prior = dict(prior_inst.attributes)
        compare_strategies = collect_compare_strategies(resource)
        diff = {
            k: {"from": prior.get(k), "to": v}
            for k, v in resolved_planned.items()
            if _values_differ(v, prior.get(k), strategy=compare_strategies.get(k))
        }

        action = Action.UPDATE if diff else Action.NOOP
        logger.debug("Classified %s as %s", addr, action.value)
        return ResourceChange(
            address=addr,
            resource_type=resource.resource_type,
            action=action,
            desired=desired_dump,
            prior=prior,
            planned=resolved_planned,
            diff=diff or None,
        )

    def _plan_deletes(self, state: State, addrs: set[str]) -> list[ResourceChange]:
        """Plan delete changes for the given addresses in reverse dependency order."""
        order = self._delete_order(state, addrs)
        changes: list[ResourceChange] = []
        for addr in order:
            inst = state.resources[addr]
            self._registry.get(inst.resource_type)  # fail early if unknown
            changes.append(
                ResourceChange(
                    address=addr,
                    resource_type=inst.resource_type,
                    action=Action.DELETE,
                    prior=dict(inst.attributes),
                )
            )
        return changes

    def plan(
        self, resources: Sequence[Resource], *, destroy: bool = False, refresh: bool = True
    ) -> Plan:
        logger.info(
            "Planning %d resources (destroy=%s, refresh=%s)", len(resources), destroy, refresh
        )
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
                self._registry.get(r.resource_type)
                desired_by_addr[r.address] = r

            # --- Validation pass ---
            if not destroy:
                ctx = self._ctx()
                errors: list[str] = []
                for r in desired_by_addr.values():
                    reg = self._registry.get(r.resource_type)
                    errors.extend(reg.handler.validate(ctx, r))
                plan_ctx = PlanContext(desired_by_addr, state)
                for r in desired_by_addr.values():
                    reg = self._registry.get(r.resource_type)
                    errors.extend(reg.handler.validate_plan(ctx, r, plan_ctx))
                # Check depends_on references
                for r in desired_by_addr.values():
                    for dep in r.depends_on:
                        if not plan_ctx.address_exists(dep):
                            errors.append(
                                f"Resource '{r.address}' depends on unknown address '{dep}'"
                            )
                if errors:
                    raise ValidationError(errors)

            dep_map = self._resolve_deps(desired_by_addr)
            desired_addrs = set(desired_by_addr)
            state_addrs = set(state.resources)

            if destroy:
                changes = self._plan_deletes(state, state_addrs)
            else:
                variables = get_variables(ctx)
                topo_deps = {a: [d for d in ds if d in desired_addrs] for a, ds in dep_map.items()}
                priorities = {addr: r.plan_priority for addr, r in desired_by_addr.items()}
                order = DependencyGraph(
                    desired_addrs, topo_deps, priorities=priorities
                ).topological_order()
                changes = [
                    self._classify_change(
                        addr, desired_by_addr[addr], state, dep_map[addr], variables
                    )
                    for addr in order
                ]
                changes.extend(self._plan_deletes(state, state_addrs - desired_addrs))

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
        priorities: dict[str, int] = {}
        for addr in delete_set:
            inst = state.resources[addr]
            dep_map[addr] = [d for d in inst.dependencies if d in delete_set]
            priorities[addr] = self._registry.get(inst.resource_type).model.plan_priority
        return DependencyGraph(
            delete_set, dep_map, priorities=priorities
        ).reverse_topological_order()

    def _operation_order(self, plan: Plan, state: State) -> list[Operation]:
        """Compute a deterministic operation order using an operation graph."""
        ops = self._build_apply_operations(plan, state)
        dep_map = {k: op.deps for k, op in ops.items()}
        priorities: dict[str, int] = {}
        for k, op in ops.items():
            if op.change is not None:
                reg = self._registry.get(op.change.resource_type)
                priorities[k] = reg.model.plan_priority
        order = DependencyGraph(ops.keys(), dep_map, priorities=priorities).topological_order()
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

    def apply(self, plan: Plan, *, progress: ProgressCallback | None = None) -> ApplyResult:
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
            logger.info("Applying %d operations", len(ordered_ops))

            try:
                for op in ordered_ops:
                    logger.debug("Applying %s: %s", op.key, type(op).__name__)
                    if progress and op.change is not None:
                        progress(op.change, "start")
                    did_change = op.run(ctx=ctx, state=state, registry=self._registry)
                    if not did_change:
                        continue

                    assert op.change is not None
                    if progress:
                        progress(op.change, "done")

                    state.serial += 1
                    state.save(self._state_path)
                    applied.append(op.change)
            except KeyboardInterrupt as e:  # pragma: no cover
                raise ApplyCanceled("Apply canceled") from e
            except Exception as e:
                raise ApplyError(applied=applied, address=op.key, message=str(e)) from e

            return ApplyResult(applied=applied)
