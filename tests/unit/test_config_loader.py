"""Tests for YAML configuration loader and type discrimination."""

from __future__ import annotations

import pytest

from dss_provisioner.config.loader import ConfigError, load_config
from dss_provisioner.resources.dataset import (
    FilesystemDatasetResource,
    OracleDatasetResource,
    SnowflakeDatasetResource,
    UploadDatasetResource,
)
from dss_provisioner.resources.recipe import (
    PythonRecipeResource,
    RecipeResource,
    SQLQueryRecipeResource,
    SyncRecipeResource,
)

_MINIMAL_YAML = """\
provider:
  project: TEST
"""

_FULL_YAML = """\
provider:
  host: https://dss.example.com
  project: ANALYTICS

state_path: custom-state.json

datasets:
  - name: snow_ds
    type: snowflake
    connection: sf_conn
    schema_name: RAW
    table: CUSTOMERS

  - name: ora_ds
    type: oracle
    connection: ora_conn
    schema_name: HR
    table: EMPLOYEES

  - name: fs_ds
    type: filesystem
    connection: fs_managed
    path: /data/input

  - name: up_ds
    type: upload

recipes:
  - name: clean
    type: python
    inputs: raw_data
    outputs: clean_data
    code: "print('hello')"

  - name: query
    type: sql_query
    inputs: [a, b]
    outputs: [c]
    code: "SELECT 1"

  - name: copy
    type: sync
    inputs: src
    outputs: dst
"""


class TestLoadConfigFull:
    def test_full_yaml_parses(self, tmp_path) -> None:
        f = tmp_path / "config.yaml"
        f.write_text(_FULL_YAML)
        config = load_config(f)

        assert config.provider.host == "https://dss.example.com"
        assert config.provider.project == "ANALYTICS"
        assert str(config.state_path) == "custom-state.json"
        assert len(config.resources) == 7

    def test_config_dir_set_to_parent(self, tmp_path) -> None:
        f = tmp_path / "sub" / "config.yaml"
        f.parent.mkdir()
        f.write_text(_MINIMAL_YAML)
        config = load_config(f)
        assert config.config_dir == f.parent


class TestDatasetDiscrimination:
    def test_snowflake(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text(_FULL_YAML)
        config = load_config(f)
        ds = config.resources[0]
        assert isinstance(ds, SnowflakeDatasetResource)
        assert ds.type == "Snowflake"
        assert ds.schema_name == "RAW"
        assert ds.table == "CUSTOMERS"

    def test_oracle(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text(_FULL_YAML)
        config = load_config(f)
        ds = config.resources[1]
        assert isinstance(ds, OracleDatasetResource)
        assert ds.type == "Oracle"

    def test_filesystem(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text(_FULL_YAML)
        config = load_config(f)
        ds = config.resources[2]
        assert isinstance(ds, FilesystemDatasetResource)
        assert ds.path == "/data/input"

    def test_upload(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text(_FULL_YAML)
        config = load_config(f)
        ds = config.resources[3]
        assert isinstance(ds, UploadDatasetResource)


class TestRecipeDiscrimination:
    def test_python(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text(_FULL_YAML)
        config = load_config(f)
        r = config.resources[4]
        assert isinstance(r, PythonRecipeResource)
        assert r.code == "print('hello')"

    def test_sql_query(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text(_FULL_YAML)
        config = load_config(f)
        r = config.resources[5]
        assert isinstance(r, SQLQueryRecipeResource)
        assert r.inputs == ["a", "b"]
        assert r.outputs == ["c"]

    def test_sync(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text(_FULL_YAML)
        config = load_config(f)
        r = config.resources[6]
        assert isinstance(r, SyncRecipeResource)


class TestStringToListCoercion:
    def test_string_inputs_coerced(self) -> None:
        r = RecipeResource(name="r", type="sync", inputs="single")  # type: ignore[arg-type]
        assert r.inputs == ["single"]

    def test_string_outputs_coerced(self) -> None:
        r = RecipeResource(name="r", type="sync", outputs="single")  # type: ignore[arg-type]
        assert r.outputs == ["single"]

    def test_list_unchanged(self) -> None:
        r = RecipeResource(name="r", type="sync", inputs=["a", "b"])
        assert r.inputs == ["a", "b"]

    def test_yaml_string_coerced(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text(_FULL_YAML)
        config = load_config(f)
        r = config.resources[4]
        assert isinstance(r, PythonRecipeResource)
        assert r.inputs == ["raw_data"]
        assert r.outputs == ["clean_data"]


class TestConfigErrors:
    def test_missing_type_on_dataset(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text("provider:\n  project: X\ndatasets:\n  - name: d\n")
        with pytest.raises(ConfigError, match="Unable to extract tag"):
            load_config(f)

    def test_missing_type_on_recipe(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text("provider:\n  project: X\nrecipes:\n  - name: r\n")
        with pytest.raises(ConfigError, match="Unable to extract tag"):
            load_config(f)

    def test_unknown_dataset_type(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text("provider:\n  project: X\ndatasets:\n  - name: d\n    type: PostgreSQL\n")
        with pytest.raises(ConfigError, match="does not match any of the expected tags"):
            load_config(f)

    def test_unknown_recipe_type(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text("provider:\n  project: X\nrecipes:\n  - name: r\n    type: unknown_type\n")
        with pytest.raises(ConfigError, match="does not match any of the expected tags"):
            load_config(f)

    def test_missing_provider(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text("datasets: []\n")
        with pytest.raises(ConfigError, match="provider"):
            load_config(f)

    def test_non_mapping_yaml(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text("- item1\n- item2\n")
        with pytest.raises(ConfigError, match="should be a valid dictionary"):
            load_config(f)

    def test_empty_sections(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text("provider:\n  project: X\ndatasets:\nrecipes:\n")
        config = load_config(f)
        assert config.resources == []

    def test_pydantic_validation_propagates(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text("provider:\n  project: X\ndatasets:\n  - name: d\n    type: snowflake\n")
        with pytest.raises(ConfigError, match="Field required"):
            load_config(f)

    def test_missing_file(self, tmp_path) -> None:
        with pytest.raises(ConfigError, match="Failed to read"):
            load_config(tmp_path / "nonexistent.yaml")

    def test_default_state_path(self, tmp_path) -> None:
        f = tmp_path / "c.yaml"
        f.write_text(_MINIMAL_YAML)
        config = load_config(f)
        assert str(config.state_path) == ".dss-state.json"
