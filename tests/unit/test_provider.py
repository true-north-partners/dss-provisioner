"""Unit tests for DSSProvider."""

from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr

from dss_provisioner.core import ApiKeyAuth, DSSProvider


def test_provider_from_client() -> None:
    """Test creating provider with injected client."""
    mock_client = MagicMock()
    mock_client.list_project_keys.return_value = ["PROJECT_A", "PROJECT_B"]

    provider = DSSProvider.from_client(mock_client)

    assert provider.client is mock_client
    assert provider.projects.list_projects() == ["PROJECT_A", "PROJECT_B"]


def test_provider_handlers_use_same_client() -> None:
    """Test that all handlers share the same client."""
    mock_client = MagicMock()
    provider = DSSProvider.from_client(mock_client)

    assert provider.projects.client is mock_client
    assert provider.datasets.client is mock_client
    assert provider.recipes.client is mock_client
    assert provider.zones.client is mock_client


def test_provider_requires_host_and_auth() -> None:
    """Test that provider raises error without host+auth or injected client."""
    provider = DSSProvider.model_construct()

    with pytest.raises(ValueError, match=r"host\+auth"):
        _ = provider.client


def test_provider_with_api_key_auth() -> None:
    """Test provider configuration with API key (doesn't actually connect)."""
    provider = DSSProvider(
        host="https://dss.example.com",
        auth=ApiKeyAuth(api_key=SecretStr("test-key")),
    )

    assert provider.host == "https://dss.example.com"
    assert provider.auth is not None
    assert provider.auth.api_key.get_secret_value() == "test-key"
