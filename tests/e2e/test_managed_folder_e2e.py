"""E2e tests for managed folder resources against a live DSS instance."""

from __future__ import annotations

from uuid import uuid4

import pytest

from dss_provisioner.config import apply, plan
from dss_provisioner.engine.types import Action
from dss_provisioner.resources.managed_folder import FilesystemManagedFolderResource
from tests.e2e.conftest import assert_changes

pytestmark = pytest.mark.integration


class TestFilesystemManagedFolder:
    def test_lifecycle(self, make_config, cleanup_managed_folders):
        name = f"e2e_mf_{uuid4().hex[:8]}"
        cleanup_managed_folders.append(name)

        cfg = make_config(
            managed_folders=[
                FilesystemManagedFolderResource(
                    name=name,
                    connection="filesystem_folders",
                    path=f"/tmp/{name}",
                )
            ]
        )
        p = plan(cfg)
        assert_changes(p, {name: Action.CREATE})
        apply(p, cfg)

        # NOOP
        p2 = plan(cfg)
        assert_changes(p2, {name: Action.NOOP})

        # UPDATE description
        cfg_updated = make_config(
            managed_folders=[
                FilesystemManagedFolderResource(
                    name=name,
                    connection="filesystem_folders",
                    path=f"/tmp/{name}",
                    description="updated folder",
                )
            ]
        )
        p3 = plan(cfg_updated)
        assert_changes(p3, {name: Action.UPDATE})
        apply(p3, cfg_updated)

        # NOOP after update
        p4 = plan(cfg_updated)
        assert_changes(p4, {name: Action.NOOP})

        # DESTROY
        p5 = plan(cfg_updated, destroy=True)
        assert_changes(p5, {name: Action.DELETE})
        apply(p5, cfg_updated)
