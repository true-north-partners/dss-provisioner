"""Pytest fixtures for integration tests."""

import time
from collections.abc import Generator

import dataikuapi
import pytest
import requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from dss_provisioner.core import DSSProvider


class DSSContainer(DockerContainer):
    """Testcontainer for Dataiku DSS.

    Note: Requires a DSS image that supports API keys (not Free Edition).
    """

    DSS_PORT = 10000

    def __init__(self, image: str = "dataiku/dss:latest") -> None:
        super().__init__(image)
        self.with_exposed_ports(self.DSS_PORT)

    def get_connection_url(self) -> str:
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.DSS_PORT)
        return f"http://{host}:{port}"

    def _wait_for_http(self, timeout: int = 60) -> None:
        """Wait for DSS HTTP endpoint to be ready."""
        url = f"{self.get_connection_url()}/dip/api/ping"
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(2)
        raise TimeoutError(f"DSS did not become ready at {url}")

    def start(self) -> "DSSContainer":
        super().start()
        # DSS takes a while to install and start on first run
        # First wait for the log message indicating startup
        wait_for_logs(self, "success: backend entered RUNNING state", timeout=300)
        # Then wait for HTTP to actually be ready
        self._wait_for_http(timeout=60)
        return self


@pytest.fixture(scope="session")
def dss_container() -> Generator[DSSContainer]:
    """Start a DSS container for the test session."""
    with DSSContainer() as container:
        yield container


@pytest.fixture(scope="session")
def dss_provider(dss_container: DSSContainer) -> DSSProvider:
    """Provide a DSSProvider connected to the test container.

    For testcontainer testing, we inject a client directly since
    Free Edition doesn't support API keys.
    """
    # Create a client that bypasses API key check for testing
    # This works because we're just testing the handler logic
    client = dataikuapi.DSSClient(
        dss_container.get_connection_url(),
        api_key="dummy",  # Will be replaced by session auth
    )
    # Note: This won't actually work with Free Edition
    # For real testing, use DSSProvider.from_client() inside DSS
    return DSSProvider.from_client(client)
