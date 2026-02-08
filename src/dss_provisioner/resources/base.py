"""Base resource class for DSS resources."""

from typing import Annotated, ClassVar

from pydantic import BaseModel, ConfigDict, Field, computed_field

from dss_provisioner.resources.markers import Compare, ResourceRef, collect_ref_specs, collect_refs


class Resource(BaseModel):
    """Base class for all DSS resources.

    Resources are pure data - they define the desired state.
    Handlers know how to CRUD resources.
    """

    model_config = ConfigDict(extra="forbid")

    resource_type: ClassVar[str]
    namespace: ClassVar[str]
    plan_priority: ClassVar[int] = 100

    name: str = Field(pattern=r"^[a-zA-Z0-9_]+$")
    description: str = ""
    tags: Annotated[list[Annotated[str, Field(min_length=1)]], Compare("set")] = Field(
        default_factory=list
    )

    # Lifecycle
    depends_on: list[str] = []

    def reference_names(self) -> list[str]:
        """Names of other resources this one references (auto-collected from Ref markers)."""
        return collect_refs(self)

    def references(self) -> list[ResourceRef]:
        """Typed references declared on this resource."""
        return collect_ref_specs(self)

    @computed_field
    @property
    def address(self) -> str:
        """Unique address for this resource (e.g., 'dss_dataset.my_dataset')."""
        return f"{self.resource_type}.{self.name}"
