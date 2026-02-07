"""Base resource class for DSS resources."""

from typing import ClassVar

from pydantic import BaseModel, ConfigDict, computed_field

from dss_provisioner.resources.markers import collect_refs


class Resource(BaseModel):
    """Base class for all DSS resources.

    Resources are pure data - they define the desired state.
    Handlers know how to CRUD resources.
    """

    model_config = ConfigDict(extra="forbid")

    resource_type: ClassVar[str]

    name: str
    description: str = ""
    tags: list[str] = []

    # Lifecycle
    depends_on: list[str] = []

    def reference_names(self) -> list[str]:
        """Names of other resources this one references (auto-collected from Ref markers)."""
        return collect_refs(self)

    @computed_field
    @property
    def address(self) -> str:
        """Unique address for this resource (e.g., 'dss_dataset.my_dataset')."""
        return f"{self.resource_type}.{self.name}"
