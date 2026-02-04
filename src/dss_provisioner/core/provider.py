"""DSS Provider - Connection configuration for a DSS instance."""

from functools import cached_property
from typing import TYPE_CHECKING, Self

import dataikuapi
from pydantic import BaseModel, ConfigDict, SecretStr

if TYPE_CHECKING:
    from dss_provisioner.handlers.datasets import DatasetHandler
    from dss_provisioner.handlers.projects import ProjectHandler
    from dss_provisioner.handlers.recipes import RecipeHandler
    from dss_provisioner.handlers.zones import ZoneHandler


class ApiKeyAuth(BaseModel):
    """API key authentication for DSS."""

    api_key: SecretStr


class DSSProvider(BaseModel):
    """Connection configuration for a DSS instance.

    For external use, provide host and auth. For internal use (inside DSS
    notebooks/recipes), use the `from_client` classmethod to inject a client.

    Examples:
        # External with API key
        provider = DSSProvider(
            host="https://dss.company.com",
            auth=ApiKeyAuth(api_key="my-api-key"),
        )

        # Inside DSS notebook
        import dataiku
        provider = DSSProvider.from_client(dataiku.api_client())
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    host: str | None = None
    auth: ApiKeyAuth | None = None

    # Injected client (for internal use / testing)
    _injected_client: dataikuapi.DSSClient | None = None

    @classmethod
    def from_client(cls, client: dataikuapi.DSSClient) -> Self:
        """Create a provider with an injected client.

        Use this for running inside DSS or for testing with a mock client.

        Args:
            client: A pre-configured DSSClient instance

        Example:
            import dataiku
            provider = DSSProvider.from_client(dataiku.api_client())
        """
        provider = cls.model_construct()
        provider._injected_client = client
        return provider

    @cached_property
    def client(self) -> dataikuapi.DSSClient:
        """Get the DSS client."""
        if self._injected_client is not None:
            return self._injected_client

        if self.host is None or self.auth is None:
            raise ValueError(
                "Either provide host+auth, or use DSSProvider.from_client() "
                "to inject a client"
            )

        return dataikuapi.DSSClient(
            self.host,
            api_key=self.auth.api_key.get_secret_value(),
        )

    # Handlers for each DSS concept
    @cached_property
    def projects(self) -> "ProjectHandler":
        from dss_provisioner.handlers.projects import ProjectHandler

        return ProjectHandler(self.client)

    @cached_property
    def datasets(self) -> "DatasetHandler":
        from dss_provisioner.handlers.datasets import DatasetHandler

        return DatasetHandler(self.client)

    @cached_property
    def recipes(self) -> "RecipeHandler":
        from dss_provisioner.handlers.recipes import RecipeHandler

        return RecipeHandler(self.client)

    @cached_property
    def zones(self) -> "ZoneHandler":
        from dss_provisioner.handlers.zones import ZoneHandler

        return ZoneHandler(self.client)
