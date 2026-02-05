"""Engine types (plan, changes, metadata)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class Action(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    NOOP = "no-op"


class PlanMetadata(BaseModel):
    project_key: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    destroy: bool
    refresh: bool
    state_lineage: str
    state_serial: int
    state_digest: str
    config_digest: str
    engine_version: str


class ResourceChange(BaseModel):
    address: str
    resource_type: str
    action: Action
    desired: dict[str, Any] | None = None
    prior: dict[str, Any] | None = None
    planned: dict[str, Any] | None = None
    diff: dict[str, Any] | None = None


class Plan(BaseModel):
    metadata: PlanMetadata
    changes: list[ResourceChange]

    def summary(self) -> dict[str, int]:
        counts = {a.value: 0 for a in Action}
        for c in self.changes:
            counts[c.action.value] += 1
        return counts

    def save(self, path: Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        data = self.model_dump(mode="json")
        path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    @classmethod
    def load(cls, path: Path) -> Plan:
        path = Path(path)
        return cls.model_validate_json(path.read_text(encoding="utf-8"))


class ApplyResult(BaseModel):
    applied: list[ResourceChange] = Field(default_factory=list)

    def summary(self) -> dict[str, int]:
        counts = {a.value: 0 for a in Action}
        for c in self.applied:
            counts[c.action.value] += 1
        return counts
