"""E2e tests for dataset resources against a live DSS instance."""

from __future__ import annotations

from uuid import uuid4

import pytest

from dss_provisioner.config import apply, plan
from dss_provisioner.engine.types import Action
from dss_provisioner.resources.dataset import FilesystemDatasetResource, UploadDatasetResource
from tests.e2e.conftest import assert_changes

pytestmark = pytest.mark.integration


class TestFilesystemDataset:
    def test_lifecycle(self, make_config, cleanup_datasets):
        name = f"e2e_fs_{uuid4().hex[:8]}"
        cleanup_datasets.append(name)

        # CREATE
        cfg = make_config(
            datasets=[
                FilesystemDatasetResource(
                    name=name,
                    connection="filesystem_managed",
                    path=f"/tmp/{name}",
                )
            ]
        )
        p = plan(cfg)
        assert_changes(p, {name: Action.CREATE})
        apply(p, cfg)

        # NOOP — idempotent
        p2 = plan(cfg)
        assert_changes(p2, {name: Action.NOOP})

        # UPDATE — change description
        cfg_updated = make_config(
            datasets=[
                FilesystemDatasetResource(
                    name=name,
                    connection="filesystem_managed",
                    path=f"/tmp/{name}",
                    description="updated via e2e test",
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


class TestUploadDataset:
    def test_lifecycle(self, make_config, cleanup_datasets):
        name = f"e2e_up_{uuid4().hex[:8]}"
        cleanup_datasets.append(name)

        cfg = make_config(datasets=[UploadDatasetResource(name=name, managed=False)])
        p = plan(cfg)
        assert_changes(p, {name: Action.CREATE})
        apply(p, cfg)

        # NOOP
        p2 = plan(cfg)
        assert_changes(p2, {name: Action.NOOP})

        # UPDATE tags
        cfg_updated = make_config(
            datasets=[UploadDatasetResource(name=name, managed=False, description="tagged")]
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


class TestFormatParams:
    def test_no_spurious_drift(self, make_config, cleanup_datasets):
        """Regression test for issue #10: format_params should not cause spurious drift."""
        name = f"e2e_fmt_{uuid4().hex[:8]}"
        cleanup_datasets.append(name)

        cfg = make_config(
            datasets=[
                FilesystemDatasetResource(
                    name=name,
                    connection="filesystem_managed",
                    path=f"/tmp/{name}",
                    format_type="csv",
                    format_params={"separator": ",", "style": "unix"},
                )
            ]
        )
        p = plan(cfg)
        assert_changes(p, {name: Action.CREATE})
        apply(p, cfg)

        # Re-plan must be NOOP — no spurious drift from format_params
        p2 = plan(cfg)
        assert_changes(p2, {name: Action.NOOP})

        # DESTROY
        p3 = plan(cfg, destroy=True)
        assert_changes(p3, {name: Action.DELETE})
        apply(p3, cfg)


class TestVariableSubstitution:
    def test_path_with_project_key(self, make_config, cleanup_datasets):
        """Regression test for issue #11: ${projectKey} in path should resolve correctly."""
        name = f"e2e_var_{uuid4().hex[:8]}"
        cleanup_datasets.append(name)

        cfg = make_config(
            datasets=[
                FilesystemDatasetResource(
                    name=name,
                    connection="filesystem_managed",
                    path="/data/${projectKey}/" + name,
                )
            ]
        )
        p = plan(cfg)
        assert_changes(p, {name: Action.CREATE})
        apply(p, cfg)

        # Re-plan must be NOOP — variable substitution should be stable
        p2 = plan(cfg)
        assert_changes(p2, {name: Action.NOOP})

        # DESTROY
        p3 = plan(cfg, destroy=True)
        assert_changes(p3, {name: Action.DELETE})
        apply(p3, cfg)
