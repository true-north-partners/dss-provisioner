"""Resources for declaring foreign (cross-project) object references."""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from dss_provisioner.resources.base import Resource


class ForeignDatasetResource(Resource):
    """A declared foreign dataset reference available to this project."""

    resource_type: ClassVar[str] = "dss_foreign_dataset"
    namespace: ClassVar[str] = "dataset"
    plan_priority: ClassVar[int] = 90

    source_project: str = Field(min_length=1)
    source_name: str = Field(min_length=1)

    @property
    def full_ref(self) -> str:
        return f"{self.source_project}.{self.source_name}"


class ForeignManagedFolderResource(Resource):
    """A declared foreign managed folder reference available to this project."""

    resource_type: ClassVar[str] = "dss_foreign_managed_folder"
    namespace: ClassVar[str] = "managed_folder"
    plan_priority: ClassVar[int] = 90

    source_project: str = Field(min_length=1)
    source_name: str = Field(min_length=1)

    @property
    def full_ref(self) -> str:
        return f"{self.source_project}.{self.source_name}"
