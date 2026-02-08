"""State management for tracking deployed resources."""

import contextlib
import hashlib
import json
import logging
import os
import tempfile
import uuid
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


def _canonical_json(obj: Any) -> str:
    # Stable encoding for hashes/digests. `default=str` keeps it robust for
    # datetimes/paths/etc while staying deterministic enough for our use.
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def compute_attributes_hash(attrs: Mapping[str, Any]) -> str:
    """Compute a stable hash for a resource's stored attributes."""
    payload = _canonical_json(attrs)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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
    serial: int = 0
    lineage: str = Field(default_factory=lambda: str(uuid.uuid4()))
    resources: dict[str, ResourceInstance] = Field(default_factory=dict)
    outputs: dict[str, Any] = Field(default_factory=dict)

    def save(self, path: Path) -> None:
        """Save state to a JSON file.

        - Writes atomically (temp file + rename)
        - Writes a `.backup` copy of the previous state when overwriting
        """
        path.parent.mkdir(parents=True, exist_ok=True)

        backup_path = Path(str(path) + ".backup")
        # Avoid TOCTOU race between exists() and read_bytes().
        with contextlib.suppress(FileNotFoundError):
            backup_path.write_bytes(path.read_bytes())

        data = self.model_dump(mode="json")
        content = json.dumps(data, indent=2, sort_keys=True) + "\n"

        fd, tmp_path = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        tmp_file = Path(tmp_path)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            tmp_file.replace(path)
        finally:
            with contextlib.suppress(FileNotFoundError):
                tmp_file.unlink()
        logger.debug("State saved: serial=%d path=%s", self.serial, path)

    @classmethod
    def load(cls, path: Path) -> "State":
        """Load state from a JSON file."""
        state = cls.model_validate_json(path.read_text())
        logger.debug("State loaded from %s", path)
        return state

    @classmethod
    def load_or_create(cls, path: Path, project_key: str) -> "State":
        """Load existing state or create a new one."""
        if path.exists():
            return cls.load(path)
        logger.debug("Created new state for project %s", project_key)
        return cls(project_key=project_key)


def compute_state_digest(state: State) -> str:
    """Compute a stable digest of state content (excluding timestamps).

    Used for stale-plan detection. By design this excludes fields that should
    not force a re-plan (e.g., `created_at`/`updated_at` timestamps).
    """
    resources = []
    for address, inst in sorted(state.resources.items(), key=lambda kv: kv[0]):
        resources.append(
            {
                "address": address,
                "resource_type": inst.resource_type,
                "name": inst.name,
                "attributes_hash": inst.attributes_hash,
                "dependencies": sorted(inst.dependencies),
            }
        )

    digestable = {
        "version": state.version,
        "project_key": state.project_key,
        "lineage": state.lineage,
        "serial": state.serial,
        "resources": resources,
    }
    payload = _canonical_json(digestable)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
