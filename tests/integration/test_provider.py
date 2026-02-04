"""Integration tests for DSSProvider.

These tests can be run:
1. Inside a DSS notebook using `dataiku.api_client()`
2. Externally with an API key (requires paid DSS)
"""

from dss_provisioner.core import DSSProvider


def test_provider_with_injected_client(dss_provider: DSSProvider) -> None:
    """Test that provider works with injected client."""
    projects = dss_provider.projects.list()
    assert isinstance(projects, list)


def test_provider_handlers_available(dss_provider: DSSProvider) -> None:
    """Test that all handlers are accessible."""
    assert dss_provider.projects is not None
    assert dss_provider.datasets is not None
    assert dss_provider.recipes is not None
    assert dss_provider.zones is not None
