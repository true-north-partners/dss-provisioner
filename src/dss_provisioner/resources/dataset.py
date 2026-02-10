"""Dataset resource models for DSS datasets."""

from __future__ import annotations

from typing import Annotated, Any, ClassVar, Literal, Self

from pydantic import BaseModel, Field, model_validator

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
    mode: Annotated[Literal["table", "query"], DSSParam("params.mode")] = "table"
    schema_name: Annotated[str, DSSParam("params.schema")] = Field(min_length=1)
    table: Annotated[str | None, DSSParam("params.table")] = None
    query: Annotated[str | None, DSSParam("params.queryString")] = Field(default=None, min_length=1)
    query_file: str | None = Field(default=None, exclude=True)
    catalog: Annotated[str | None, DSSParam("params.catalog")] = None
    write_mode: Annotated[
        Literal["OVERWRITE", "APPEND", "TRUNCATE"], DSSParam("params.writeMode")
    ] = "OVERWRITE"

    @model_validator(mode="after")
    def _check_mode_fields(self) -> Self:
        if self.mode == "table":
            if not self.table:
                raise ValueError("'table' is required when mode is 'table'")
            if self.query is not None or self.query_file is not None:
                raise ValueError("'query'/'query_file' cannot be used when mode is 'table'")
        elif self.mode == "query":
            if self.table is not None:
                raise ValueError("'table' cannot be used when mode is 'query'")
            if self.query is not None and self.query_file is not None:
                raise ValueError("Cannot set both 'query' and 'query_file'")
        return self


class OracleDatasetResource(DatasetResource):
    """Oracle-specific dataset resource."""

    resource_type: ClassVar[str] = "dss_oracle_dataset"
    yaml_alias: ClassVar[str] = "oracle"

    type: Literal["Oracle"] = "Oracle"
    connection: Annotated[str, DSSParam("params.connection")]  # type: ignore[assignment]
    mode: Annotated[Literal["table", "query"], DSSParam("params.mode")] = "table"
    schema_name: Annotated[str, DSSParam("params.schema")] = Field(min_length=1)
    table: Annotated[str | None, DSSParam("params.table")] = None
    query: Annotated[str | None, DSSParam("params.queryString")] = Field(default=None, min_length=1)
    query_file: str | None = Field(default=None, exclude=True)

    @model_validator(mode="after")
    def _check_mode_fields(self) -> Self:
        if self.mode == "table":
            if not self.table:
                raise ValueError("'table' is required when mode is 'table'")
            if self.query is not None or self.query_file is not None:
                raise ValueError("'query'/'query_file' cannot be used when mode is 'table'")
        elif self.mode == "query":
            if self.table is not None:
                raise ValueError("'table' cannot be used when mode is 'query'")
            if self.query is not None and self.query_file is not None:
                raise ValueError("Cannot set both 'query' and 'query_file'")
        return self


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
