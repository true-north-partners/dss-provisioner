import pytest

from dss_provisioner.engine.errors import DependencyCycleError
from dss_provisioner.engine.graph import DependencyGraph


def test_topological_order_deterministic() -> None:
    graph = DependencyGraph(nodes=["a", "b", "c"], dependencies={"b": ["a"], "c": ["a"]})
    assert graph.topological_order() == ["a", "b", "c"]


def test_topological_order_ignores_external_deps() -> None:
    graph = DependencyGraph(nodes=["a", "b"], dependencies={"b": ["external"]})
    assert graph.topological_order() == ["a", "b"]


def test_cycle_detection() -> None:
    graph = DependencyGraph(nodes=["a", "b"], dependencies={"a": ["b"], "b": ["a"]})
    with pytest.raises(DependencyCycleError):
        graph.topological_order()
