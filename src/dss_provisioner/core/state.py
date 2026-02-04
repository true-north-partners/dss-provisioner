"""State management for tracking deployed resources."""

from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class ResourceInstance(BaseModel):
    """A tracked resource instance in the state file.

    Attributes:
        address: Unique resource address (e.g., "dss_recipe.join_orders")
        resource_type: Type of the resource (e.g., "dss_join_recipe")
        name: Resource name (e.g., "join_orders")
        attributes: Current attribute values
        attributes_hash: SHA256 hash for change detection
        dependencies: Addresses of dependencies
        created_at: When the resource was created
        updated_at: When the resource was last updated
    """

    address: str
    resource_type: str
    name: str
    attributes: dict[str, Any] = Field(default_factory=dict)
    attributes_hash: str = ""
    dependencies: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)


class State(BaseModel):
    """Terraform-style state file for tracking deployed resources.

    Attributes:
        version: State file format version
        project_key: DSS project key
        resources: Mapping of resource addresses to instances
        outputs: Output values from the configuration
    """

    version: int = 1
    project_key: str
    resources: dict[str, ResourceInstance] = Field(default_factory=dict)
    outputs: dict[str, Any] = Field(default_factory=dict)

    def save(self, path: Path) -> None:
        """Save state to a JSON file."""
        path.write_text(self.model_dump_json(indent=2))

    @classmethod
    def load(cls, path: Path) -> "State":
        """Load state from a JSON file."""
        return cls.model_validate_json(path.read_text())

    @classmethod
    def load_or_create(cls, path: Path, project_key: str) -> "State":
        """Load existing state or create a new one."""
        if path.exists():
            return cls.load(path)
        return cls(project_key=project_key)
