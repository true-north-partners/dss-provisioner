"""Resources for exposing local objects to other DSS projects."""

from __future__ import annotations

from typing import Annotated, ClassVar, Literal, Self

from pydantic import Field, model_validator

from dss_provisioner.resources.base import Resource
from dss_provisioner.resources.markers import Compare


class ExposedObjectResource(Resource):
    """Base resource for DSS exposed objects."""

    resource_type: ClassVar[str] = "dss_exposed_object"
    namespace: ClassVar[str] = "exposed_object"
    plan_priority: ClassVar[int] = 150

    type: str
    target_projects: Annotated[
        list[Annotated[str, Field(min_length=1)]],
        Compare("set"),
    ] = Field(min_length=1)

    @model_validator(mode="after")
    def _dedupe_targets(self) -> Self:
        self.target_projects = list(dict.fromkeys(self.target_projects))
        return self


class ExposedDatasetResource(ExposedObjectResource):
    """Expose a local dataset to one or more target projects."""

    resource_type: ClassVar[str] = "dss_exposed_dataset"
    namespace: ClassVar[str] = "exposed_dataset"
    yaml_alias: ClassVar[str] = "dataset"

    type: Literal["DATASET"] = "DATASET"


class ExposedManagedFolderResource(ExposedObjectResource):
    """Expose a local managed folder to one or more target projects."""

    resource_type: ClassVar[str] = "dss_exposed_managed_folder"
    namespace: ClassVar[str] = "exposed_managed_folder"
    yaml_alias: ClassVar[str] = "managed_folder"

    type: Literal["MANAGED_FOLDER"] = "MANAGED_FOLDER"
