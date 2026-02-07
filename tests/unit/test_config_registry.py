"""Tests for the default resource type registry factory."""

from __future__ import annotations

from dss_provisioner.config.registry import default_registry
from dss_provisioner.engine.dataset_handler import DatasetHandler
from dss_provisioner.engine.managed_folder_handler import ManagedFolderHandler
from dss_provisioner.engine.recipe_handler import (
    PythonRecipeHandler,
    SQLQueryRecipeHandler,
    SyncRecipeHandler,
)
from dss_provisioner.engine.scenario_handler import (
    PythonScenarioHandler,
    StepBasedScenarioHandler,
)


class TestDefaultRegistry:
    def test_all_managed_folder_types_registered(self) -> None:
        registry = default_registry()
        for rt in [
            "dss_managed_folder",
            "dss_filesystem_managed_folder",
            "dss_upload_managed_folder",
        ]:
            reg = registry.get(rt)
            assert isinstance(reg.handler, ManagedFolderHandler)

    def test_all_dataset_types_registered(self) -> None:
        registry = default_registry()
        for rt in [
            "dss_dataset",
            "dss_snowflake_dataset",
            "dss_oracle_dataset",
            "dss_filesystem_dataset",
            "dss_upload_dataset",
        ]:
            reg = registry.get(rt)
            assert isinstance(reg.handler, DatasetHandler)

    def test_all_recipe_types_registered(self) -> None:
        registry = default_registry()
        assert isinstance(registry.get("dss_sync_recipe").handler, SyncRecipeHandler)
        assert isinstance(registry.get("dss_python_recipe").handler, PythonRecipeHandler)
        assert isinstance(registry.get("dss_sql_query_recipe").handler, SQLQueryRecipeHandler)

    def test_all_scenario_types_registered(self) -> None:
        registry = default_registry()
        assert isinstance(registry.get("dss_step_scenario").handler, StepBasedScenarioHandler)
        assert isinstance(registry.get("dss_python_scenario").handler, PythonScenarioHandler)

    def test_total_count(self) -> None:
        registry = default_registry()
        assert len(registry._registrations) == 17

    def test_independent_instances(self) -> None:
        r1 = default_registry()
        r2 = default_registry()
        assert r1 is not r2
        assert r1._registrations is not r2._registrations
