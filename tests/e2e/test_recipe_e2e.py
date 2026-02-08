"""E2e tests for recipe resources against a live DSS instance."""

from __future__ import annotations

from uuid import uuid4

import pytest

from dss_provisioner.config import apply, plan
from dss_provisioner.engine.types import Action
from dss_provisioner.resources.dataset import FilesystemDatasetResource
from dss_provisioner.resources.recipe import PythonRecipeResource, SyncRecipeResource
from tests.e2e.conftest import assert_changes

pytestmark = pytest.mark.integration


class TestSyncRecipe:
    def test_lifecycle(self, make_config, cleanup_recipes, cleanup_datasets):
        suffix = uuid4().hex[:8]
        src = f"e2e_src_{suffix}"
        dst = f"e2e_dst_{suffix}"
        recipe = f"e2e_sync_{suffix}"

        cleanup_recipes.append(recipe)
        cleanup_datasets.extend([dst, src])

        ds_src = FilesystemDatasetResource(
            name=src, connection="filesystem_managed", path=f"/tmp/{src}"
        )
        ds_dst = FilesystemDatasetResource(
            name=dst, connection="filesystem_managed", path=f"/tmp/{dst}"
        )
        rcp = SyncRecipeResource(name=recipe, inputs=[src], outputs=[dst])

        # CREATE all
        cfg = make_config(datasets=[ds_src, ds_dst], recipes=[rcp])
        p = plan(cfg)
        assert_changes(p, {src: Action.CREATE, dst: Action.CREATE, recipe: Action.CREATE})
        apply(p, cfg)

        # NOOP
        p2 = plan(cfg)
        assert_changes(p2, {src: Action.NOOP, dst: Action.NOOP, recipe: Action.NOOP})

        # DESTROY recipe first, then datasets
        p3 = plan(cfg, destroy=True)
        assert_changes(p3, {recipe: Action.DELETE, src: Action.DELETE, dst: Action.DELETE})
        apply(p3, cfg)


class TestPythonRecipe:
    def test_lifecycle(self, make_config, cleanup_recipes, cleanup_datasets):
        suffix = uuid4().hex[:8]
        src = f"e2e_pysrc_{suffix}"
        dst = f"e2e_pydst_{suffix}"
        recipe = f"e2e_py_{suffix}"

        cleanup_recipes.append(recipe)
        cleanup_datasets.extend([dst, src])

        ds_src = FilesystemDatasetResource(
            name=src, connection="filesystem_managed", path=f"/tmp/{src}"
        )
        ds_dst = FilesystemDatasetResource(
            name=dst, connection="filesystem_managed", path=f"/tmp/{dst}"
        )
        rcp = PythonRecipeResource(
            name=recipe,
            inputs=[src],
            outputs=[dst],
            code="import dataiku\n# e2e test recipe\n",
        )

        cfg = make_config(datasets=[ds_src, ds_dst], recipes=[rcp])
        p = plan(cfg)
        assert_changes(p, {src: Action.CREATE, dst: Action.CREATE, recipe: Action.CREATE})
        apply(p, cfg)

        # NOOP
        p2 = plan(cfg)
        assert_changes(p2, {src: Action.NOOP, dst: Action.NOOP, recipe: Action.NOOP})

        # UPDATE code
        rcp_updated = PythonRecipeResource(
            name=recipe,
            inputs=[src],
            outputs=[dst],
            code="import dataiku\n# updated e2e recipe\nprint('hello')\n",
        )
        cfg_updated = make_config(datasets=[ds_src, ds_dst], recipes=[rcp_updated])
        p3 = plan(cfg_updated)
        assert_changes(p3, {recipe: Action.UPDATE})
        apply(p3, cfg_updated)

        # NOOP after update
        p4 = plan(cfg_updated)
        assert_changes(p4, {recipe: Action.NOOP})

        # DESTROY
        p5 = plan(cfg_updated, destroy=True)
        apply(p5, cfg_updated)
