"""Managed folder resource models for DSS managed folders."""

from __future__ import annotations

from typing import Annotated, Any, ClassVar, Literal

from pydantic import Field

from dss_provisioner.resources.base import Resource
from dss_provisioner.resources.markers import DSSParam, Ref, build_dss_params


class ManagedFolderResource(Resource):
    """Base resource for DSS managed folders."""

    resource_type: ClassVar[str] = "dss_managed_folder"

    type: str
    connection: Annotated[str | None, DSSParam("params.connection")] = None
    zone: Annotated[str | None, Ref("dss_zone")] = None

    def to_dss_params(self) -> dict[str, Any]:
        """Build the DSS API params dict from DSSParam-annotated fields."""
        return build_dss_params(self)


class FilesystemManagedFolderResource(ManagedFolderResource):
    """Filesystem-specific managed folder resource."""

    resource_type: ClassVar[str] = "dss_filesystem_managed_folder"
    yaml_alias: ClassVar[str] = "filesystem"

    type: Literal["Filesystem"] = "Filesystem"
    connection: Annotated[str, DSSParam("params.connection")]  # type: ignore[assignment]
    path: Annotated[str, DSSParam("params.path")] = Field(min_length=1)


class UploadManagedFolderResource(ManagedFolderResource):
    """Upload-specific managed folder resource."""

    resource_type: ClassVar[str] = "dss_upload_managed_folder"
    yaml_alias: ClassVar[str] = "upload"

    type: Literal["UploadedFiles"] = "UploadedFiles"
