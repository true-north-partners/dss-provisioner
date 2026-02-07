"""Dependency graph utilities."""

from __future__ import annotations

import heapq
from typing import TYPE_CHECKING

from dss_provisioner.engine.errors import DependencyCycleError

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


class DependencyGraph:
    """A directed graph where nodes depend on other nodes."""

    def __init__(
        self,
        nodes: Iterable[str],
        dependencies: Mapping[str, Iterable[str]],
        priorities: Mapping[str, int] | None = None,
    ) -> None:
        self._nodes = set(nodes)
        self._priorities = priorities or {}
        # node -> filtered deps within graph
        self._deps: dict[str, set[str]] = {}
        for node in self._nodes:
            deps = set(dependencies.get(node, []))
            self._deps[node] = {d for d in deps if d in self._nodes}

    def topological_order(self) -> list[str]:
        """Return deterministic topo order (priority, then lexicographic tie-break)."""
        indegree: dict[str, int] = dict.fromkeys(self._nodes, 0)
        dependents: dict[str, set[str]] = {n: set() for n in self._nodes}

        for node, deps in self._deps.items():
            indegree[node] = len(deps)
            for dep in deps:
                dependents[dep].add(node)

        ready: list[tuple[int, str]] = [
            (self._priorities.get(n, 0), n) for n, deg in indegree.items() if deg == 0
        ]
        heapq.heapify(ready)

        order: list[str] = []
        while ready:
            _, node = heapq.heappop(ready)
            order.append(node)
            for child in sorted(dependents[node]):
                indegree[child] -= 1
                if indegree[child] == 0:
                    heapq.heappush(ready, (self._priorities.get(child, 0), child))

        if len(order) != len(self._nodes):
            remaining = sorted(self._nodes - set(order))
            raise DependencyCycleError(remaining)

        return order

    def reverse_topological_order(self) -> list[str]:
        order = self.topological_order()
        order.reverse()
        return order
