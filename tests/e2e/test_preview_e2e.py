"""E2e tests for preview command phase 1 behavior against a live DSS instance."""

from __future__ import annotations

import contextlib
from uuid import uuid4

import pytest
from dataikuapi.utils import DataikuException

from dss_provisioner.preview import destroy_preview, list_previews, run_preview
from dss_provisioner.resources.dataset import FilesystemDatasetResource

pytestmark = pytest.mark.integration

_PROJECT_CREATE_SIGNING_ERROR = "UnsupportedSigningFormatException"


def _run_preview_or_skip(*args, **kwargs):
    try:
        return run_preview(*args, **kwargs)
    except DataikuException as exc:
        if _PROJECT_CREATE_SIGNING_ERROR in str(exc):
            pytest.skip(
                "DSS project creation is blocked by git signing configuration "
                "(UnsupportedSigningFormatException)."
            )
        raise


class TestPreviewPhase1:
    def test_create_list_destroy(self, make_config, dss_client):
        suffix = uuid4().hex[:8]
        branch = f"e2e-preview-{suffix}"
        dataset_name = f"e2e_prev_{suffix}"

        cfg = make_config(
            datasets=[
                FilesystemDatasetResource(
                    name=dataset_name,
                    connection="filesystem_managed",
                    path=f"/tmp/{dataset_name}",
                )
            ],
            state_name=f"preview_{suffix}",
        )

        spec = None
        try:
            spec, _plan_obj, _result = _run_preview_or_skip(cfg, branch=branch)

            # Preview project exists and receives resources.
            preview_project = dss_client.get_project(spec.preview_project_key)
            assert preview_project.get_dataset(dataset_name).exists()

            # List includes the preview with branch metadata.
            previews = list_previews(cfg)
            listed = next((p for p in previews if p.project_key == spec.preview_project_key), None)
            assert listed is not None
            assert listed.branch == branch

            # Preview state file is isolated and persisted.
            assert spec.preview_state_path.exists()
        finally:
            with contextlib.suppress(Exception):
                # Idempotent cleanup for project + preview state artifacts.
                destroy_preview(cfg, branch=branch)

        # Project and preview state are gone after destroy.
        assert spec is not None
        assert spec.preview_project_key not in set(dss_client.list_project_keys())
        assert not spec.preview_state_path.exists()

    def test_multiple_branches_can_coexist(self, make_config, dss_client):
        suffix = uuid4().hex[:8]
        base_branch = f"e2e-preview-n2n-{suffix}"
        branch_a = f"{base_branch}-a"
        branch_b = f"{base_branch}-b"
        dataset_name = f"e2e_prev_n2n_{suffix}"

        cfg = make_config(
            datasets=[
                FilesystemDatasetResource(
                    name=dataset_name,
                    connection="filesystem_managed",
                    path=f"/tmp/{dataset_name}",
                )
            ],
            state_name=f"preview_n2n_{suffix}",
        )

        spec_a = None
        spec_b = None
        try:
            spec_a, _plan_obj_a, _result_a = _run_preview_or_skip(cfg, branch=branch_a)
            spec_b, _plan_obj_b, _result_b = _run_preview_or_skip(cfg, branch=branch_b)

            project_keys = set(dss_client.list_project_keys())
            assert spec_a.preview_project_key in project_keys
            assert spec_b.preview_project_key in project_keys
            assert spec_a.preview_project_key != spec_b.preview_project_key

            previews = list_previews(cfg)
            by_key = {preview.project_key: preview for preview in previews}
            assert by_key[spec_a.preview_project_key].branch == branch_a
            assert by_key[spec_b.preview_project_key].branch == branch_b
        finally:
            with contextlib.suppress(Exception):
                destroy_preview(cfg, branch=branch_a)
            with contextlib.suppress(Exception):
                destroy_preview(cfg, branch=branch_b)

        assert spec_a is not None
        assert spec_b is not None
        remaining = set(dss_client.list_project_keys())
        assert spec_a.preview_project_key not in remaining
        assert spec_b.preview_project_key not in remaining
