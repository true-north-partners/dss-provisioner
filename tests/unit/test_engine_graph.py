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


def test_priority_ordering() -> None:
    """Nodes with lower priority come first when no deps constrain order."""
    graph = DependencyGraph(
        nodes=["high", "low"],
        dependencies={},
        priorities={"high": 100, "low": 0},
    )
    assert graph.topological_order() == ["low", "high"]


def test_priority_does_not_override_deps() -> None:
    """Dependencies are still respected even with priority mismatch."""
    graph = DependencyGraph(
        nodes=["high", "low"],
        dependencies={"low": ["high"]},
        priorities={"high": 100, "low": 0},
    )
    # "high" must come first due to dependency, despite having higher priority value
    assert graph.topological_order() == ["high", "low"]
