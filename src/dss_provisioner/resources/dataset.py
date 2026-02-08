"""Dataset resource models for DSS datasets."""

from __future__ import annotations

from typing import Annotated, Any, ClassVar, Literal

from pydantic import BaseModel, Field

from dss_provisioner.resources.base import Resource
from dss_provisioner.resources.markers import Compare, DSSParam, Ref, build_dss_params


class Column(BaseModel):
    """A column in a dataset schema."""

    name: str = Field(min_length=1)
    type: Literal[
        "string",
        "int",
        "bigint",
        "float",
        "double",
        "boolean",
        "date",
        "array",
        "object",
        "map",
    ]
    description: str = ""
    meaning: str | None = None


class DatasetResource(Resource):
    """Base resource for DSS datasets."""

    resource_type: ClassVar[str] = "dss_dataset"
    namespace: ClassVar[str] = "dataset"
    sql_types: ClassVar[set[str]] = {"PostgreSQL", "Snowflake", "Oracle", "MySQL"}

    type: str
    connection: Annotated[str | None, DSSParam("params.connection")] = None
    managed: bool = False
    format_type: Annotated[str | None, DSSParam("formatType")] = None
    format_params: Annotated[dict[str, Any], DSSParam("formatParams"), Compare("partial")] = Field(
        default_factory=dict
    )
    columns: list[Column] = Field(default_factory=list)
    zone: Annotated[str | None, Ref("dss_zone")] = None

    def to_dss_params(self) -> dict[str, Any]:
        """Build the DSS API params dict from DSSParam-annotated fields."""
        return build_dss_params(self)


class SnowflakeDatasetResource(DatasetResource):
    """Snowflake-specific dataset resource."""

    resource_type: ClassVar[str] = "dss_snowflake_dataset"
    yaml_alias: ClassVar[str] = "snowflake"

    type: Literal["Snowflake"] = "Snowflake"
    connection: Annotated[str, DSSParam("params.connection")]  # type: ignore[assignment]
    schema_name: Annotated[str, DSSParam("params.schema")] = Field(min_length=1)
    table: Annotated[str, DSSParam("params.table")] = Field(min_length=1)
    catalog: Annotated[str | None, DSSParam("params.catalog")] = None
    write_mode: Annotated[
        Literal["OVERWRITE", "APPEND", "TRUNCATE"], DSSParam("params.writeMode")
    ] = "OVERWRITE"


class OracleDatasetResource(DatasetResource):
    """Oracle-specific dataset resource."""

    resource_type: ClassVar[str] = "dss_oracle_dataset"
    yaml_alias: ClassVar[str] = "oracle"

    type: Literal["Oracle"] = "Oracle"
    connection: Annotated[str, DSSParam("params.connection")]  # type: ignore[assignment]
    schema_name: Annotated[str, DSSParam("params.schema")] = Field(min_length=1)
    table: Annotated[str, DSSParam("params.table")] = Field(min_length=1)


class FilesystemDatasetResource(DatasetResource):
    """Filesystem-specific dataset resource."""

    resource_type: ClassVar[str] = "dss_filesystem_dataset"
    yaml_alias: ClassVar[str] = "filesystem"

    type: Literal["Filesystem"] = "Filesystem"
    connection: Annotated[str, DSSParam("params.connection")]  # type: ignore[assignment]
    path: Annotated[str, DSSParam("params.path")] = Field(min_length=1)


class UploadDatasetResource(DatasetResource):
    """Upload-specific dataset resource."""

    resource_type: ClassVar[str] = "dss_upload_dataset"
    yaml_alias: ClassVar[str] = "upload"

    type: Literal["UploadedFiles"] = "UploadedFiles"
    managed: bool = True
