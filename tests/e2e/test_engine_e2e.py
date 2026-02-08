"""E2e tests for multi-resource engine orchestration against a live DSS instance."""

from __future__ import annotations

from uuid import uuid4

import pytest

from dss_provisioner.config import apply, plan
from dss_provisioner.core.state import State
from dss_provisioner.engine.types import Action
from dss_provisioner.resources.dataset import FilesystemDatasetResource
from dss_provisioner.resources.recipe import SyncRecipeResource
from tests.e2e.conftest import assert_changes

pytestmark = pytest.mark.integration


class TestMultiResourceOrchestration:
    def test_full_plan_apply_noop_destroy(self, make_config, cleanup_datasets, dss_project):
        """Full lifecycle: plan(CREATE) → apply → plan(NOOP) → mutate via API → plan(UPDATE) → apply → destroy."""
        suffix = uuid4().hex[:8]
        ds1 = f"e2e_orch1_{suffix}"
        ds2 = f"e2e_orch2_{suffix}"

        cleanup_datasets.extend([ds2, ds1])

        datasets = [
            FilesystemDatasetResource(
                name=ds1, connection="filesystem_managed", path=f"/tmp/{ds1}"
            ),
            FilesystemDatasetResource(
                name=ds2, connection="filesystem_managed", path=f"/tmp/{ds2}"
            ),
        ]

        # CREATE
        cfg = make_config(datasets=datasets)
        p = plan(cfg)
        assert_changes(p, {ds1: Action.CREATE, ds2: Action.CREATE})
        apply(p, cfg)

        # NOOP
        p2 = plan(cfg)
        assert_changes(p2, {ds1: Action.NOOP, ds2: Action.NOOP})

        # Mutate via direct API (simulate external drift)
        meta = dss_project.get_dataset(ds1).get_metadata()
        meta["description"] = "externally modified"
        dss_project.get_dataset(ds1).set_metadata(meta)

        # Plan should detect the drift after refresh and want to UPDATE
        # (description was "" in config, now "externally modified" in DSS)
        # But since our config says description="" the engine sees the live value
        # differs from desired → UPDATE to restore desired state
        p3 = plan(cfg)
        # The engine refreshes state from DSS, then compares desired vs live.
        # Since our desired description is "" and DSS has "externally modified",
        # this should be an UPDATE.
        assert_changes(p3, {ds1: Action.UPDATE})
        apply(p3, cfg)

        # NOOP after correction
        p4 = plan(cfg)
        assert_changes(p4, {ds1: Action.NOOP})

        # DESTROY
        p5 = plan(cfg, destroy=True)
        assert_changes(p5, {ds1: Action.DELETE, ds2: Action.DELETE})
        apply(p5, cfg)

    def test_state_persistence(self, make_config, cleanup_datasets):
        """Verify state file is created after apply with correct structure."""
        name = f"e2e_state_{uuid4().hex[:8]}"
        cleanup_datasets.append(name)

        cfg = make_config(
            datasets=[
                FilesystemDatasetResource(
                    name=name, connection="filesystem_managed", path=f"/tmp/{name}"
                )
            ]
        )

        # State file should not exist yet
        assert not cfg.state_path.exists()

        p = plan(cfg)
        apply(p, cfg)

        # State file should exist now
        assert cfg.state_path.exists()
        state = State.load(cfg.state_path)
        assert state.serial > 0
        assert len(state.resources) == 1

        first_lineage = state.lineage
        first_serial = state.serial

        # Apply again (NOOP) — serial stays the same, lineage stable
        p2 = plan(cfg)
        apply(p2, cfg)

        state2 = State.load(cfg.state_path)
        assert state2.lineage == first_lineage
        assert state2.serial == first_serial

        # UPDATE — serial increments on actual change
        cfg_updated = make_config(
            datasets=[
                FilesystemDatasetResource(
                    name=name,
                    connection="filesystem_managed",
                    path=f"/tmp/{name}",
                    description="serial test",
                )
            ]
        )
        p3 = plan(cfg_updated)
        apply(p3, cfg_updated)

        state3 = State.load(cfg_updated.state_path)
        assert state3.lineage == first_lineage
        assert state3.serial > first_serial

        # Cleanup
        p4 = plan(cfg_updated, destroy=True)
        apply(p4, cfg_updated)

    def test_multi_resource_dependencies(self, make_config, cleanup_recipes, cleanup_datasets):
        """Create datasets + sync recipe, verify dependency ordering in plan."""
        suffix = uuid4().hex[:8]
        src = f"e2e_dep_s_{suffix}"
        dst = f"e2e_dep_d_{suffix}"
        recipe = f"e2e_dep_r_{suffix}"

        cleanup_recipes.append(recipe)
        cleanup_datasets.extend([dst, src])

        cfg = make_config(
            datasets=[
                FilesystemDatasetResource(
                    name=src, connection="filesystem_managed", path=f"/tmp/{src}"
                ),
                FilesystemDatasetResource(
                    name=dst, connection="filesystem_managed", path=f"/tmp/{dst}"
                ),
            ],
            recipes=[SyncRecipeResource(name=recipe, inputs=[src], outputs=[dst])],
        )

        p = plan(cfg)

        # Recipe should come after its input/output datasets in the plan
        addresses = [c.address for c in p.changes if c.action == Action.CREATE]
        recipe_addr = f"dss_sync_recipe.{recipe}"
        src_addr = f"dss_filesystem_dataset.{src}"
        dst_addr = f"dss_filesystem_dataset.{dst}"

        assert recipe_addr in addresses
        assert src_addr in addresses
        assert dst_addr in addresses
        assert addresses.index(recipe_addr) > addresses.index(src_addr)
        assert addresses.index(recipe_addr) > addresses.index(dst_addr)

        apply(p, cfg)

        # NOOP
        p2 = plan(cfg)
        assert_changes(p2, {src: Action.NOOP, dst: Action.NOOP, recipe: Action.NOOP})

        # DESTROY — recipe should be deleted before datasets
        p3 = plan(cfg, destroy=True)
        del_addresses = [c.address for c in p3.changes if c.action == Action.DELETE]
        assert del_addresses.index(recipe_addr) < del_addresses.index(src_addr)
        assert del_addresses.index(recipe_addr) < del_addresses.index(dst_addr)
        apply(p3, cfg)
