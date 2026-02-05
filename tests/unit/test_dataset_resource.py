"""Tests for dataset resource models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dss_provisioner.resources.dataset import (
    Column,
    DatasetResource,
    OracleDatasetResource,
    SnowflakeDatasetResource,
)


class TestColumn:
    def test_valid_column(self) -> None:
        col = Column(name="id", type="int")
        assert col.name == "id"
        assert col.type == "int"
        assert col.description == ""
        assert col.meaning is None

    def test_column_with_all_fields(self) -> None:
        col = Column(
            name="created_at", type="date", description="Creation date", meaning="date:created"
        )
        assert col.description == "Creation date"
        assert col.meaning == "date:created"

    def test_invalid_column_type(self) -> None:
        with pytest.raises(ValidationError):
            Column(name="x", type="invalid_type")  # type: ignore[arg-type]

    def test_all_valid_column_types(self) -> None:
        valid_types = [
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
        for t in valid_types:
            col = Column(name="test", type=t)  # type: ignore[arg-type]
            assert col.type == t


class TestDatasetResource:
    def test_address(self) -> None:
        ds = DatasetResource(name="my_ds", dataset_type="Filesystem")
        assert ds.address == "dss_dataset.my_ds"

    def test_defaults(self) -> None:
        ds = DatasetResource(name="my_ds", dataset_type="Filesystem")
        assert ds.connection is None
        assert ds.managed is False
        assert ds.format_type is None
        assert ds.format_params == {}
        assert ds.columns == []
        assert ds.zone is None
        assert ds.description == ""
        assert ds.tags == []
        assert ds.depends_on == []

    def test_extra_forbid(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            DatasetResource(name="my_ds", dataset_type="Filesystem", unknown_field="x")  # type: ignore[call-arg]

    def test_model_dump_shape(self) -> None:
        ds = DatasetResource(
            name="my_ds",
            dataset_type="Filesystem",
            connection="local",
            managed=True,
            columns=[Column(name="id", type="int")],
        )
        dump = ds.model_dump(exclude_none=True, exclude={"address"})
        assert "name" in dump
        assert "dataset_type" in dump
        assert "connection" in dump
        assert "managed" in dump
        assert "columns" in dump
        assert "address" not in dump

        # Verify column serialization (meaning=None excluded by exclude_none)
        assert dump["columns"] == [{"name": "id", "type": "int", "description": ""}]

    def test_model_dump_excludes_none(self) -> None:
        ds = DatasetResource(name="my_ds", dataset_type="Filesystem")
        dump = ds.model_dump(exclude_none=True, exclude={"address"})
        assert "connection" not in dump
        assert "format_type" not in dump
        assert "zone" not in dump


class TestSnowflakeDatasetResource:
    def test_address(self) -> None:
        ds = SnowflakeDatasetResource(
            name="my_ds", connection="snowflake_conn", schema_name="PUBLIC", table="users"
        )
        assert ds.address == "dss_snowflake_dataset.my_ds"

    def test_defaults(self) -> None:
        ds = SnowflakeDatasetResource(
            name="my_ds", connection="snowflake_conn", schema_name="PUBLIC", table="users"
        )
        assert ds.dataset_type == "Snowflake"
        assert ds.write_mode == "OVERWRITE"
        assert ds.catalog is None

    def test_required_fields(self) -> None:
        with pytest.raises(ValidationError):
            SnowflakeDatasetResource(name="my_ds")  # type: ignore[call-arg]

    def test_dataset_type_locked(self) -> None:
        with pytest.raises(ValidationError):
            SnowflakeDatasetResource(
                name="my_ds",
                connection="conn",
                schema_name="PUBLIC",
                table="t",
                dataset_type="Oracle",  # type: ignore[arg-type]
            )

    def test_extra_forbid(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            SnowflakeDatasetResource(
                name="my_ds",
                connection="conn",
                schema_name="PUBLIC",
                table="t",
                unknown_field="x",  # type: ignore[call-arg]
            )

    def test_model_dump_shape(self) -> None:
        ds = SnowflakeDatasetResource(
            name="my_ds",
            connection="snowflake_conn",
            schema_name="PUBLIC",
            table="users",
            write_mode="APPEND",
        )
        dump = ds.model_dump(exclude_none=True, exclude={"address"})
        assert dump["dataset_type"] == "Snowflake"
        assert dump["connection"] == "snowflake_conn"
        assert dump["schema_name"] == "PUBLIC"
        assert dump["table"] == "users"
        assert dump["write_mode"] == "APPEND"

    def test_write_mode_validation(self) -> None:
        with pytest.raises(ValidationError):
            SnowflakeDatasetResource(
                name="my_ds",
                connection="conn",
                schema_name="PUBLIC",
                table="t",
                write_mode="INVALID",  # type: ignore[arg-type]
            )


class TestOracleDatasetResource:
    def test_address(self) -> None:
        ds = OracleDatasetResource(
            name="my_ds", connection="oracle_conn", schema_name="HR", table="employees"
        )
        assert ds.address == "dss_oracle_dataset.my_ds"

    def test_defaults(self) -> None:
        ds = OracleDatasetResource(
            name="my_ds", connection="oracle_conn", schema_name="HR", table="employees"
        )
        assert ds.dataset_type == "Oracle"

    def test_required_fields(self) -> None:
        with pytest.raises(ValidationError):
            OracleDatasetResource(name="my_ds")  # type: ignore[call-arg]

    def test_dataset_type_locked(self) -> None:
        with pytest.raises(ValidationError):
            OracleDatasetResource(
                name="my_ds",
                connection="conn",
                schema_name="HR",
                table="t",
                dataset_type="Snowflake",  # type: ignore[arg-type]
            )

    def test_extra_forbid(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            OracleDatasetResource(
                name="my_ds",
                connection="conn",
                schema_name="HR",
                table="t",
                unknown_field="x",  # type: ignore[call-arg]
            )

    def test_model_dump_shape(self) -> None:
        ds = OracleDatasetResource(
            name="my_ds",
            connection="oracle_conn",
            schema_name="HR",
            table="employees",
        )
        dump = ds.model_dump(exclude_none=True, exclude={"address"})
        assert dump["dataset_type"] == "Oracle"
        assert dump["connection"] == "oracle_conn"
        assert dump["schema_name"] == "HR"
        assert dump["table"] == "employees"
