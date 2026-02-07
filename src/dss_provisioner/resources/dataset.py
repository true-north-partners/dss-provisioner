"""Dataset resource models for DSS datasets."""

from __future__ import annotations

from typing import Any, ClassVar, Literal

from pydantic import BaseModel, Field

from dss_provisioner.resources.base import Resource


class Column(BaseModel):
    """A column in a dataset schema."""

    name: str
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
    sql_types: ClassVar[set[str]] = {"PostgreSQL", "Snowflake", "Oracle", "MySQL"}

    type: str
    connection: str | None = None
    managed: bool = False
    format_type: str | None = None
    format_params: dict[str, Any] = Field(default_factory=dict)
    columns: list[Column] = Field(default_factory=list)
    zone: str | None = None

    def reference_names(self) -> list[str]:
        return [self.zone] if self.zone else []

    def to_dss_params(self) -> dict[str, Any]:
        """Build the DSS API params dict from resource fields."""
        params: dict[str, Any] = {}
        if self.connection is not None:
            params["connection"] = self.connection
        return params


class SnowflakeDatasetResource(DatasetResource):
    """Snowflake-specific dataset resource."""

    resource_type: ClassVar[str] = "dss_snowflake_dataset"
    yaml_alias: ClassVar[str] = "snowflake"

    type: Literal["Snowflake"] = "Snowflake"
    connection: str  # type: ignore[assignment]
    schema_name: str
    table: str
    catalog: str | None = None
    write_mode: Literal["OVERWRITE", "APPEND", "TRUNCATE"] = "OVERWRITE"

    def to_dss_params(self) -> dict[str, Any]:
        params = super().to_dss_params()
        params["schema"] = self.schema_name
        params["table"] = self.table
        if self.catalog is not None:
            params["catalog"] = self.catalog
        params["writeMode"] = self.write_mode
        return params


class OracleDatasetResource(DatasetResource):
    """Oracle-specific dataset resource."""

    resource_type: ClassVar[str] = "dss_oracle_dataset"
    yaml_alias: ClassVar[str] = "oracle"

    type: Literal["Oracle"] = "Oracle"
    connection: str  # type: ignore[assignment]
    schema_name: str
    table: str

    def to_dss_params(self) -> dict[str, Any]:
        params = super().to_dss_params()
        params["schema"] = self.schema_name
        params["table"] = self.table
        return params


class FilesystemDatasetResource(DatasetResource):
    """Filesystem-specific dataset resource."""

    resource_type: ClassVar[str] = "dss_filesystem_dataset"
    yaml_alias: ClassVar[str] = "filesystem"

    type: Literal["Filesystem"] = "Filesystem"
    connection: str  # type: ignore[assignment]
    path: str

    def to_dss_params(self) -> dict[str, Any]:
        params = super().to_dss_params()
        params["path"] = self.path
        return params


class UploadDatasetResource(DatasetResource):
    """Upload-specific dataset resource."""

    resource_type: ClassVar[str] = "dss_upload_dataset"
    yaml_alias: ClassVar[str] = "upload"

    type: Literal["UploadedFiles"] = "UploadedFiles"
    managed: bool = True
