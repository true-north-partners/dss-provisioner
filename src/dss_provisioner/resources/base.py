"""Base resource class for DSS resources."""

from typing import ClassVar

from pydantic import BaseModel, computed_field


class Resource(BaseModel):
    """Base class for all DSS resources.

    Resources are pure data - they define the desired state.
    Handlers know how to CRUD resources.
    """

    resource_type: ClassVar[str]

    name: str
    description: str = ""
    tags: list[str] = []

    # Lifecycle
    depends_on: list[str] = []

    @computed_field
    @property
    def address(self) -> str:
        """Unique address for this resource (e.g., 'dss_dataset.my_dataset')."""
        return f"{self.resource_type}.{self.name}"
