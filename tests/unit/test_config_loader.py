"""Tests for YAML configuration loader and type discrimination."""

from __future__ import annotations

from io import StringIO
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path
from pydantic import ValidationError
from ruamel.yaml import YAML

from dss_provisioner.config.loader import ConfigError, _validate_unique_names, load_config
from dss_provisioner.config.schema import Config
from dss_provisioner.resources.code_env import CodeEnvResource
from dss_provisioner.resources.dataset import (
    FilesystemDatasetResource,
    OracleDatasetResource,
    SnowflakeDatasetResource,
    UploadDatasetResource,
)
from dss_provisioner.resources.git_library import GitLibraryResource
from dss_provisioner.resources.managed_folder import (
    FilesystemManagedFolderResource,
    ManagedFolderResource,
    UploadManagedFolderResource,
)
from dss_provisioner.resources.recipe import (
    PythonRecipeResource,
    RecipeResource,
    SQLQueryRecipeResource,
    SyncRecipeResource,
)
from dss_provisioner.resources.scenario import (
    PythonScenarioResource,
    StepBasedScenarioResource,
)
from dss_provisioner.resources.variables import VariablesResource
from dss_provisioner.resources.zone import ZoneResource

_FULL_YAML = """\
provider:
  host: https://dss.example.com
  project: ANALYTICS

state_path: custom-state.json

zones:
  - name: raw
    color: "#4a90d9"
  - name: curated

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


def _parse(yaml_str: str) -> Config:
    """Parse a YAML string into a Config without touching the filesystem."""
    raw = YAML(typ="safe").load(StringIO(yaml_str))
    try:
        return Config.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(str(exc)) from exc


@pytest.fixture
def full_config() -> Config:
    return _parse(_FULL_YAML)


class TestLoadConfigFull:
    def test_full_yaml_parses(self, full_config: Config) -> None:
        assert full_config.provider.host == "https://dss.example.com"
        assert full_config.provider.project == "ANALYTICS"
        assert str(full_config.state_path) == "custom-state.json"
        assert len(full_config.resources) == 9

    def test_config_dir_set_to_parent(self, tmp_path) -> None:
        f = tmp_path / "sub" / "config.yaml"
        f.parent.mkdir()
        f.write_text("provider:\n  project: TEST\n")
        config = load_config(f)
        assert config.config_dir == f.parent


class TestZoneParsing:
    def test_zones_included_in_resources(self, full_config: Config) -> None:
        zone_resources = [r for r in full_config.resources if isinstance(r, ZoneResource)]
        assert len(zone_resources) == 2

    def test_zone_with_color(self, full_config: Config) -> None:
        z = full_config.zones[0]
        assert z.name == "raw"
        assert z.color == "#4a90d9"

    def test_zone_default_color(self, full_config: Config) -> None:
        z = full_config.zones[1]
        assert z.name == "curated"
        assert z.color == "#2ab1ac"

    def test_empty_zones_section(self) -> None:
        config = _parse("provider:\n  project: X\nzones:\n")
        assert config.zones == []

    def test_unknown_zone_field_rejected(self) -> None:
        with pytest.raises(ConfigError, match="extra"):
            _parse("provider:\n  project: X\nzones:\n  - name: raw\n    unknown: bad\n")


class TestDatasetDiscrimination:
    def test_snowflake(self, full_config: Config) -> None:
        ds = full_config.datasets[0]
        assert isinstance(ds, SnowflakeDatasetResource)
        assert ds.type == "Snowflake"
        assert ds.schema_name == "RAW"
        assert ds.table == "CUSTOMERS"

    def test_oracle(self, full_config: Config) -> None:
        ds = full_config.datasets[1]
        assert isinstance(ds, OracleDatasetResource)
        assert ds.type == "Oracle"

    def test_filesystem(self, full_config: Config) -> None:
        ds = full_config.datasets[2]
        assert isinstance(ds, FilesystemDatasetResource)
        assert ds.path == "/data/input"

    def test_upload(self, full_config: Config) -> None:
        ds = full_config.datasets[3]
        assert isinstance(ds, UploadDatasetResource)


class TestRecipeDiscrimination:
    def test_python(self, full_config: Config) -> None:
        r = full_config.recipes[0]
        assert isinstance(r, PythonRecipeResource)
        assert r.code == "print('hello')"

    def test_sql_query(self, full_config: Config) -> None:
        r = full_config.recipes[1]
        assert isinstance(r, SQLQueryRecipeResource)
        assert r.inputs == ["a", "b"]
        assert r.outputs == ["c"]

    def test_sync(self, full_config: Config) -> None:
        r = full_config.recipes[2]
        assert isinstance(r, SyncRecipeResource)


class TestStringToListCoercion:
    def test_string_inputs_coerced(self) -> None:
        r = RecipeResource(name="r", type="sync", inputs="single", outputs=["out"])  # type: ignore[arg-type]
        assert r.inputs == ["single"]

    def test_string_outputs_coerced(self) -> None:
        r = RecipeResource(name="r", type="sync", outputs="single")  # type: ignore[arg-type]
        assert r.outputs == ["single"]

    def test_list_unchanged(self) -> None:
        r = RecipeResource(name="r", type="sync", inputs=["a", "b"], outputs=["out"])
        assert r.inputs == ["a", "b"]

    def test_yaml_string_coerced(self, full_config: Config) -> None:
        r = full_config.recipes[0]
        assert isinstance(r, PythonRecipeResource)
        assert r.inputs == ["raw_data"]
        assert r.outputs == ["clean_data"]


class TestConfigErrors:
    def test_missing_type_on_dataset(self) -> None:
        with pytest.raises(ConfigError, match="Unable to extract tag"):
            _parse("provider:\n  project: X\ndatasets:\n  - name: d\n")

    def test_missing_type_on_recipe(self) -> None:
        with pytest.raises(ConfigError, match="Unable to extract tag"):
            _parse("provider:\n  project: X\nrecipes:\n  - name: r\n")

    def test_unknown_dataset_type(self) -> None:
        with pytest.raises(ConfigError, match="does not match any of the expected tags"):
            _parse("provider:\n  project: X\ndatasets:\n  - name: d\n    type: PostgreSQL\n")

    def test_unknown_recipe_type(self) -> None:
        with pytest.raises(ConfigError, match="does not match any of the expected tags"):
            _parse("provider:\n  project: X\nrecipes:\n  - name: r\n    type: unknown_type\n")

    def test_missing_provider(self) -> None:
        with pytest.raises(ConfigError, match="provider"):
            _parse("datasets: []\n")

    def test_non_mapping_yaml(self) -> None:
        with pytest.raises(ConfigError, match="should be a valid dictionary"):
            _parse("- item1\n- item2\n")

    def test_empty_sections(self) -> None:
        config = _parse(
            "provider:\n  project: X\nzones:\nmanaged_folders:\ndatasets:\nrecipes:\nscenarios:\n"
        )
        assert config.resources == []

    def test_pydantic_validation_propagates(self) -> None:
        with pytest.raises(ConfigError, match="Field required"):
            _parse("provider:\n  project: X\ndatasets:\n  - name: d\n    type: snowflake\n")

    def test_missing_file(self, tmp_path) -> None:
        with pytest.raises(ConfigError, match="Failed to read"):
            load_config(tmp_path / "nonexistent.yaml")

    def test_default_state_path(self) -> None:
        config = _parse("provider:\n  project: TEST\n")
        assert str(config.state_path) == ".dss-state.json"


class TestVariablesParsing:
    def test_variables_parsed_from_yaml(self) -> None:
        config = _parse(
            "provider:\n  project: X\nvariables:\n  standard:\n    env: prod\n  local:\n    debug: 'false'\n"
        )
        assert isinstance(config.variables, VariablesResource)
        assert config.variables.standard == {"env": "prod"}
        assert config.variables.local == {"debug": "false"}

    def test_variables_included_in_resources(self) -> None:
        config = _parse("provider:\n  project: X\nvariables:\n  standard:\n    key: val\n")
        addrs = [r.address for r in config.resources]
        assert "dss_variables.variables" in addrs

    def test_variables_none_when_omitted(self) -> None:
        config = _parse("provider:\n  project: X\n")
        assert config.variables is None
        assert all(not isinstance(r, VariablesResource) for r in config.resources)


class TestLibraryParsing:
    def test_libraries_parsed_from_yaml(self) -> None:
        config = _parse(
            "provider:\n  project: X\nlibraries:\n"
            "  - name: shared_utils\n"
            "    repository: git@github.com:org/lib.git\n"
            "    checkout: main\n"
            "    path: python\n"
        )
        assert len(config.libraries) == 1
        lib = config.libraries[0]
        assert isinstance(lib, GitLibraryResource)
        assert lib.name == "shared_utils"
        assert lib.repository == "git@github.com:org/lib.git"
        assert lib.checkout == "main"
        assert lib.path == "python"

    def test_libraries_included_in_resources(self) -> None:
        config = _parse(
            "provider:\n  project: X\nlibraries:\n"
            "  - name: lib\n    repository: git@github.com:org/lib.git\n"
        )
        addrs = [r.address for r in config.resources]
        assert "dss_git_library.lib" in addrs

    def test_libraries_empty_when_omitted(self) -> None:
        config = _parse("provider:\n  project: X\n")
        assert config.libraries == []
        assert all(not isinstance(r, GitLibraryResource) for r in config.resources)


class TestScenarioParsing:
    def test_step_scenarios_parsed_from_yaml(self) -> None:
        config = _parse(
            "provider:\n  project: X\nscenarios:\n"
            "  - name: daily_build\n"
            "    type: step_based\n"
            "    active: true\n"
            "    triggers:\n"
            "      - type: temporal\n"
            "        params:\n"
            "          frequency: Daily\n"
            "    steps:\n"
            "      - type: build_flowitem\n"
            "        name: Build all\n"
        )
        assert len(config.scenarios) == 1
        s = config.scenarios[0]
        assert isinstance(s, StepBasedScenarioResource)
        assert s.name == "daily_build"
        assert s.active is True
        assert len(s.triggers) == 1
        assert len(s.steps) == 1

    def test_python_scenarios_parsed_from_yaml(self) -> None:
        config = _parse(
            "provider:\n  project: X\nscenarios:\n"
            "  - name: e2e_test\n"
            "    type: python\n"
            "    code: \"print('hello')\"\n"
        )
        assert len(config.scenarios) == 1
        s = config.scenarios[0]
        assert isinstance(s, PythonScenarioResource)
        assert s.type == "custom_python"
        assert s.code == "print('hello')"

    def test_scenarios_included_in_resources(self) -> None:
        config = _parse("provider:\n  project: X\nscenarios:\n  - name: s1\n    type: step_based\n")
        addrs = [r.address for r in config.resources]
        assert "dss_step_scenario.s1" in addrs

    def test_scenarios_empty_when_omitted(self) -> None:
        config = _parse("provider:\n  project: X\n")
        assert config.scenarios == []
        assert all(
            not isinstance(r, StepBasedScenarioResource | PythonScenarioResource)
            for r in config.resources
        )


class TestCodeEnvParsing:
    def test_both_envs_parsed(self) -> None:
        config = _parse(
            "provider:\n  project: X\ncode_envs:\n  default_python: py39_ml\n  default_r: r_base\n"
        )
        assert isinstance(config.code_envs, CodeEnvResource)
        assert config.code_envs.default_python == "py39_ml"
        assert config.code_envs.default_r == "r_base"

    def test_python_only(self) -> None:
        config = _parse("provider:\n  project: X\ncode_envs:\n  default_python: py39\n")
        assert isinstance(config.code_envs, CodeEnvResource)
        assert config.code_envs.default_python == "py39"
        assert config.code_envs.default_r is None

    def test_included_in_resources(self) -> None:
        config = _parse("provider:\n  project: X\ncode_envs:\n  default_python: py39\n")
        addrs = [r.address for r in config.resources]
        assert "dss_code_env.code_envs" in addrs

    def test_none_when_omitted(self) -> None:
        config = _parse("provider:\n  project: X\n")
        assert config.code_envs is None
        assert all(not isinstance(r, CodeEnvResource) for r in config.resources)

    def test_invalid_field_rejected(self) -> None:
        with pytest.raises(ConfigError, match="extra"):
            _parse("provider:\n  project: X\ncode_envs:\n  bad_field: x\n")


class TestManagedFolderParsing:
    def test_filesystem_type(self) -> None:
        config = _parse(
            "provider:\n  project: X\nmanaged_folders:\n"
            "  - name: models\n"
            "    type: filesystem\n"
            "    connection: filesystem_managed\n"
            "    path: /data/models\n"
        )
        assert len(config.managed_folders) == 1
        mf = config.managed_folders[0]
        assert isinstance(mf, FilesystemManagedFolderResource)
        assert mf.type == "Filesystem"
        assert mf.connection == "filesystem_managed"
        assert mf.path == "/data/models"

    def test_upload_type(self) -> None:
        config = _parse(
            "provider:\n  project: X\nmanaged_folders:\n  - name: reports\n    type: upload\n"
        )
        assert len(config.managed_folders) == 1
        mf = config.managed_folders[0]
        assert isinstance(mf, UploadManagedFolderResource)
        assert mf.type == "UploadedFiles"

    def test_managed_folders_included_in_resources(self) -> None:
        config = _parse(
            "provider:\n  project: X\nmanaged_folders:\n  - name: models\n    type: upload\n"
        )
        addrs = [r.address for r in config.resources]
        assert "dss_upload_managed_folder.models" in addrs

    def test_managed_folders_empty_when_omitted(self) -> None:
        config = _parse("provider:\n  project: X\n")
        assert config.managed_folders == []
        assert all(not isinstance(r, ManagedFolderResource) for r in config.resources)

    def test_invalid_field_rejected(self) -> None:
        with pytest.raises(ConfigError, match="extra"):
            _parse(
                "provider:\n  project: X\nmanaged_folders:\n"
                "  - name: models\n    type: filesystem\n"
                "    connection: c\n    path: /p\n    bad_field: x\n"
            )


class TestModuleIntegration:
    def test_load_config_with_modules(self, tmp_path: Path) -> None:
        mod_dir = tmp_path / "modules"
        mod_dir.mkdir()
        (mod_dir / "pipelines.py").write_text(
            "from dss_provisioner.resources.zone import ZoneResource\n"
            "def make_zones(name):\n"
            "    return [ZoneResource(name=name)]\n"
        )
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "provider:\n  project: TEST\n"
            "modules:\n"
            "  - call: modules.pipelines:make_zones\n"
            "    instances:\n"
            "      raw: {}\n"
            "      curated: {}\n"
        )
        config = load_config(config_file)
        names = {r.name for r in config.resources}
        assert "raw" in names
        assert "curated" in names
        assert len(config.resources) == 2

    def test_empty_modules_section(self) -> None:
        config = _parse("provider:\n  project: X\nmodules:\n")
        assert config.modules == []

    def test_load_config_module_error_wrapped(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "provider:\n  project: TEST\n"
            "modules:\n"
            "  - call: nonexistent.mod:fn\n"
            "    with:\n"
            "      name: x\n"
        )
        with pytest.raises(ConfigError, match="not found"):
            load_config(config_file)


class TestDuplicateNameValidation:
    def test_duplicate_same_subtype(self) -> None:
        resources = [
            FilesystemDatasetResource(name="raw", connection="c", path="/a"),
            FilesystemDatasetResource(name="raw", connection="c", path="/b"),
        ]
        errors = _validate_unique_names(resources)
        assert len(errors) == 1
        assert "Duplicate dataset name 'raw'" in errors[0]

    def test_duplicate_cross_subtype(self) -> None:
        resources = [
            SnowflakeDatasetResource(name="raw", connection="sf", schema_name="S", table="T"),
            FilesystemDatasetResource(name="raw", connection="c", path="/a"),
        ]
        errors = _validate_unique_names(resources)
        assert len(errors) == 1
        assert "dataset" in errors[0]
        assert "dss_snowflake_dataset.raw" in errors[0]
        assert "dss_filesystem_dataset.raw" in errors[0]

    def test_same_name_different_namespace(self) -> None:
        resources = [
            FilesystemDatasetResource(name="raw", connection="c", path="/a"),
            ZoneResource(name="raw"),
        ]
        errors = _validate_unique_names(resources)
        assert errors == []

    def test_no_duplicates(self) -> None:
        resources = [
            FilesystemDatasetResource(name="input", connection="c", path="/a"),
            FilesystemDatasetResource(name="output", connection="c", path="/b"),
        ]
        errors = _validate_unique_names(resources)
        assert errors == []


class TestDuplicateNameIntegration:
    def test_load_config_duplicate_names(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "provider:\n  project: TEST\n"
            "datasets:\n"
            "  - name: raw\n    type: filesystem\n    connection: c\n    path: /a\n"
            "  - name: raw\n    type: upload\n"
        )
        with pytest.raises(ConfigError, match="Duplicate dataset name 'raw'"):
            load_config(config_file)

    def test_load_config_module_duplicate_with_toplevel(self, tmp_path: Path) -> None:
        mod_dir = tmp_path / "modules"
        mod_dir.mkdir()
        (mod_dir / "gen.py").write_text(
            "from dss_provisioner.resources.zone import ZoneResource\n"
            "def make(name):\n"
            "    return [ZoneResource(name=name)]\n"
        )
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "provider:\n  project: TEST\n"
            "zones:\n  - name: raw\n"
            "modules:\n"
            "  - call: modules.gen:make\n"
            "    instances:\n"
            "      raw: {}\n"
        )
        with pytest.raises(ConfigError, match="Duplicate zone name 'raw'"):
            load_config(config_file)
