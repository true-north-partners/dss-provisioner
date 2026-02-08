"""Tests for declarative field markers and introspection helpers."""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, Field

from dss_provisioner.resources.dataset import (
    DatasetResource,
    FilesystemDatasetResource,
    OracleDatasetResource,
    SnowflakeDatasetResource,
)
from dss_provisioner.resources.markers import (
    Compare,
    DSSParam,
    Ref,
    ResourceRef,
    build_dss_params,
    collect_compare_strategies,
    collect_ref_specs,
    collect_refs,
    extract_dss_attrs,
)
from dss_provisioner.resources.recipe import RecipeResource

# ── Ref tests ───────────────────────────────────────────────────────


class TestCollectRefs:
    def test_collect_ref_specs_preserves_type_metadata(self) -> None:
        class M(BaseModel):
            zone: Annotated[str, Ref("dss_zone")] = ""
            inputs: Annotated[list[str], Ref()] = Field(default_factory=list)

        m = M(zone="raw", inputs=["a", "b"])
        assert collect_ref_specs(m) == [
            ResourceRef(name="raw", resource_type="dss_zone"),
            ResourceRef(name="a", resource_type=None),
            ResourceRef(name="b", resource_type=None),
        ]

    def test_string_field(self) -> None:
        """Single str field with Ref() returns [value]."""

        class M(BaseModel):
            zone: Annotated[str, Ref("dss_zone")] = ""

        m = M(zone="raw")
        assert collect_refs(m) == ["raw"]

    def test_list_field(self) -> None:
        """list[str] field with Ref() returns all items."""

        class M(BaseModel):
            inputs: Annotated[list[str], Ref()] = Field(default_factory=list)

        m = M(inputs=["a", "b"])
        assert collect_refs(m) == ["a", "b"]

    def test_none_skipped(self) -> None:
        """str | None field with Ref(), value is None → []."""

        class M(BaseModel):
            zone: Annotated[str | None, Ref("dss_zone")] = None

        m = M()
        assert collect_refs(m) == []

    def test_no_markers(self) -> None:
        """Model with no Ref fields → []."""

        class M(BaseModel):
            name: str = ""

        m = M()
        assert collect_refs(m) == []

    def test_dataset_reference_names(self) -> None:
        ds = DatasetResource(name="my_ds", type="Filesystem", zone="raw")
        assert ds.reference_names() == ["raw"]

    def test_dataset_reference_names_no_zone(self) -> None:
        ds = DatasetResource(name="my_ds", type="Filesystem")
        assert ds.reference_names() == []

    def test_recipe_reference_names(self) -> None:
        r = RecipeResource(name="my_recipe", type="sync", inputs=["a"], outputs=["b"], zone="raw")
        assert r.reference_names() == ["a", "b", "raw"]

    def test_recipe_reference_names_no_zone(self) -> None:
        r = RecipeResource(name="my_recipe", type="sync", inputs=["a"], outputs=["b"])
        assert r.reference_names() == ["a", "b"]


# ── DSSParam tests ──────────────────────────────────────────────────


class TestExtractDssAttrs:
    def test_nested_path(self) -> None:
        """DSSParam("params.schema") extracts from raw["params"]["schema"]."""

        class M(BaseModel):
            schema_name: Annotated[str, DSSParam("params.schema")] = ""

        raw = {"params": {"schema": "PUBLIC"}}
        assert extract_dss_attrs(M, raw) == {"schema_name": "PUBLIC"}

    def test_toplevel_path(self) -> None:
        """DSSParam("formatType") extracts from raw["formatType"]."""

        class M(BaseModel):
            format_type: Annotated[str | None, DSSParam("formatType")] = None

        raw = {"formatType": "csv"}
        assert extract_dss_attrs(M, raw) == {"format_type": "csv"}

    def test_missing_key_returns_default(self) -> None:
        """Returns field default when key absent from raw."""

        class M(BaseModel):
            catalog: Annotated[str | None, DSSParam("params.catalog")] = None

        raw: dict[str, Any] = {"params": {}}
        assert extract_dss_attrs(M, raw) == {"catalog": None}

    def test_missing_parent_returns_default(self) -> None:
        """Returns field default when parent dict is absent."""

        class M(BaseModel):
            schema_name: Annotated[str, DSSParam("params.schema")] = ""

        raw: dict[str, Any] = {}
        assert extract_dss_attrs(M, raw) == {"schema_name": ""}

    def test_required_field_missing_returns_none(self) -> None:
        """Required fields (no default) return None when absent from raw."""

        class M(BaseModel):
            table: Annotated[str, DSSParam("params.table")]

        raw: dict[str, Any] = {"params": {}}
        assert extract_dss_attrs(M, raw) == {"table": None}


class TestBuildDssParams:
    def test_builds_params(self) -> None:
        """Builds correct params dict from annotated fields."""

        class M(BaseModel):
            connection: Annotated[str, DSSParam("params.connection")] = ""
            schema_name: Annotated[str, DSSParam("params.schema")] = ""

        m = M(connection="conn", schema_name="PUBLIC")
        assert build_dss_params(m) == {"connection": "conn", "schema": "PUBLIC"}

    def test_skips_none(self) -> None:
        """None values omitted from params."""

        class M(BaseModel):
            connection: Annotated[str | None, DSSParam("params.connection")] = None

        m = M()
        assert build_dss_params(m) == {}

    def test_skips_non_params_fields(self) -> None:
        """Fields with DSSParam paths not starting with 'params.' are excluded."""

        class M(BaseModel):
            format_type: Annotated[str | None, DSSParam("formatType")] = "csv"
            connection: Annotated[str, DSSParam("params.connection")] = "conn"

        m = M()
        assert build_dss_params(m) == {"connection": "conn"}


class TestCollectCompareStrategies:
    def test_collects_marked_fields(self) -> None:
        class M(BaseModel):
            tags: Annotated[list[str], Compare("set")] = Field(default_factory=list)
            config: Annotated[dict[str, Any], Compare("partial")] = Field(default_factory=dict)
            name: str = ""

        assert collect_compare_strategies(M) == {"tags": "set", "config": "partial"}

    def test_resource_tags_use_set_strategy(self) -> None:
        assert collect_compare_strategies(DatasetResource)["tags"] == "set"


# ── Behavioural equivalence tests ───────────────────────────────────


class TestSnowflakeToDssParams:
    def test_full_params(self) -> None:
        ds = SnowflakeDatasetResource(
            name="my_ds",
            connection="sf_conn",
            schema_name="PUBLIC",
            table="users",
            catalog="MY_CAT",
            write_mode="APPEND",
        )
        assert ds.to_dss_params() == {
            "connection": "sf_conn",
            "schema": "PUBLIC",
            "table": "users",
            "catalog": "MY_CAT",
            "writeMode": "APPEND",
        }

    def test_no_catalog(self) -> None:
        ds = SnowflakeDatasetResource(
            name="my_ds", connection="sf_conn", schema_name="PUBLIC", table="users"
        )
        params = ds.to_dss_params()
        assert "catalog" not in params
        assert params == {
            "connection": "sf_conn",
            "schema": "PUBLIC",
            "table": "users",
            "writeMode": "OVERWRITE",
        }


class TestOracleToDssParams:
    def test_params(self) -> None:
        ds = OracleDatasetResource(
            name="my_ds", connection="ora_conn", schema_name="HR", table="employees"
        )
        assert ds.to_dss_params() == {
            "connection": "ora_conn",
            "schema": "HR",
            "table": "employees",
        }


class TestFilesystemToDssParams:
    def test_params(self) -> None:
        ds = FilesystemDatasetResource(
            name="my_ds", connection="filesystem_managed", path="/data/input"
        )
        assert ds.to_dss_params() == {
            "connection": "filesystem_managed",
            "path": "/data/input",
        }


class TestBaseDatasetToDssParams:
    def test_with_connection(self) -> None:
        ds = DatasetResource(name="my_ds", type="Filesystem", connection="local")
        assert ds.to_dss_params() == {"connection": "local"}

    def test_without_connection(self) -> None:
        ds = DatasetResource(name="my_ds", type="Filesystem")
        assert ds.to_dss_params() == {}
