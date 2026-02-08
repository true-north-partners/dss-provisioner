from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from dss_provisioner.config.schema import Config, ProviderConfig
from dss_provisioner.engine.types import Action, ApplyResult, Plan, PlanMetadata, ResourceChange
from dss_provisioner.preview import (
    PreviewProject,
    build_preview_config,
    compute_preview_spec,
    destroy_preview,
    list_previews,
    run_preview,
)
from dss_provisioner.resources.git_library import GitLibraryResource


def _config(
    *,
    state_path: Path,
    config_dir: Path,
    libraries: list[GitLibraryResource] | None = None,
) -> Config:
    cfg = Config(
        provider=ProviderConfig(
            host="https://dss.example.com",
            api_key="secret",
            project="ANALYTICS",
        ),
        state_path=state_path,
        libraries=libraries or [],
    )
    cfg.config_dir = config_dir
    return cfg


def _noop_plan() -> Plan:
    return Plan(
        metadata=PlanMetadata(
            project_key="ANALYTICS",
            destroy=False,
            refresh=True,
            state_lineage="lineage",
            state_serial=0,
            state_digest="digest",
            config_digest="config",
            engine_version="0.1.0",
        ),
        changes=[
            ResourceChange(
                address="dss_dataset.raw",
                resource_type="dss_dataset",
                action=Action.NOOP,
            )
        ],
    )


def test_compute_preview_spec_uses_branch_override(tmp_path: Path) -> None:
    cfg = _config(state_path=Path(".dss-state.json"), config_dir=tmp_path)

    spec = compute_preview_spec(cfg, branch="feature/new-scoring")

    assert spec.branch == "feature/new-scoring"
    assert spec.branch_slug == "feature_new_scoring"
    assert spec.preview_project_key == "ANALYTICS__FEATURE_NEW_SCORING"
    assert spec.preview_state_path == Path(".dss-state.preview.feature_new_scoring.json")


def test_build_preview_config_rewrites_self_libraries(tmp_path: Path) -> None:
    cfg = _config(
        state_path=Path(".dss-state.json"),
        config_dir=tmp_path,
        libraries=[
            GitLibraryResource(name="shared_utils", repository="self", checkout="main"),
            GitLibraryResource(
                name="external", repository="git@github.com:org/ext.git", checkout="v1"
            ),
        ],
    )
    spec = compute_preview_spec(cfg, branch="feature/new-scoring")

    with patch("dss_provisioner.preview._git_output") as mock_git_output:
        mock_git_output.return_value = "git@github.com:org/dss-provisioner.git"
        preview_cfg = build_preview_config(cfg, spec)

    assert preview_cfg.provider.project == "ANALYTICS__FEATURE_NEW_SCORING"
    assert preview_cfg.state_path == Path(".dss-state.preview.feature_new_scoring.json")

    assert preview_cfg.libraries[0].repository == "git@github.com:org/dss-provisioner.git"
    assert preview_cfg.libraries[0].checkout == "feature/new-scoring"
    assert preview_cfg.libraries[1].repository == "git@github.com:org/ext.git"
    assert preview_cfg.libraries[1].checkout == "v1"

    # original config must remain unchanged
    assert cfg.provider.project == "ANALYTICS"
    assert cfg.libraries[0].repository == "self"
    assert cfg.libraries[0].checkout == "main"


def test_run_preview_creates_project_and_applies(tmp_path: Path) -> None:
    cfg = _config(
        state_path=Path(".dss-state.json"),
        config_dir=tmp_path,
        libraries=[GitLibraryResource(name="shared_utils", repository="self", checkout="main")],
    )
    mock_client = MagicMock()
    mock_client.list_project_keys.return_value = []
    mock_client.get_auth_info.return_value = {"authIdentifier": "jonas-meyer"}
    project = MagicMock()
    project.get_metadata.return_value = {"tags": []}
    mock_client.get_project.return_value = project

    with (
        patch("dss_provisioner.preview._git_output") as mock_git_output,
        patch("dss_provisioner.preview._provider_from_config") as mock_provider,
        patch("dss_provisioner.preview.plan_fn") as mock_plan,
        patch("dss_provisioner.preview.apply_fn") as mock_apply,
    ):
        mock_provider.return_value = MagicMock(client=mock_client, projects=MagicMock())
        mock_provider.return_value.projects.list_projects.return_value = []
        mock_plan.return_value = _noop_plan()
        mock_apply.return_value = ApplyResult(applied=[])
        mock_git_output.side_effect = [
            "git@github.com:org/dss-provisioner.git",  # remote.origin.url
        ]

        spec, _plan_obj, _result = run_preview(cfg, branch="feature/new-scoring", refresh=False)

    assert spec.preview_project_key == "ANALYTICS__FEATURE_NEW_SCORING"
    mock_provider.return_value.projects.create.assert_called_once()
    mock_plan.assert_called_once()
    _, kwargs = mock_plan.call_args
    assert kwargs["refresh"] is False

    planned_cfg = mock_plan.call_args.args[0]
    assert planned_cfg.provider.project == "ANALYTICS__FEATURE_NEW_SCORING"
    assert planned_cfg.libraries[0].repository == "git@github.com:org/dss-provisioner.git"
    assert planned_cfg.libraries[0].checkout == "feature/new-scoring"
    project.set_metadata.assert_called_once()


def test_list_previews_for_base_project(tmp_path: Path) -> None:
    cfg = _config(state_path=Path(".dss-state.json"), config_dir=tmp_path)
    mock_client = MagicMock()
    mock_client.list_project_keys.return_value = [
        "ANALYTICS__FEATURE_ONE",
        "ANALYTICS__FEATURE_TWO",
        "OTHER__FEATURE",
    ]

    def _project_with_tags(key: str) -> MagicMock:
        p = MagicMock()
        if key == "ANALYTICS__FEATURE_ONE":
            p.get_metadata.return_value = {
                "tags": ["dss-provisioner-preview", "dss-provisioner-branch:feature/one"]
            }
        else:
            p.get_metadata.return_value = {"tags": []}
        return p

    mock_client.get_project.side_effect = _project_with_tags
    provider = MagicMock(client=mock_client, projects=MagicMock())
    provider.projects.list_projects.return_value = mock_client.list_project_keys.return_value

    with patch("dss_provisioner.preview._provider_from_config", return_value=provider):
        previews = list_previews(cfg)

    assert previews == [
        PreviewProject(project_key="ANALYTICS__FEATURE_ONE", branch="feature/one"),
        PreviewProject(project_key="ANALYTICS__FEATURE_TWO", branch=None),
    ]


def test_destroy_preview_deletes_project_and_state_files(tmp_path: Path) -> None:
    base_state = tmp_path / ".dss-state.json"
    cfg = _config(state_path=base_state, config_dir=tmp_path)
    spec = compute_preview_spec(cfg, branch="feature/new-scoring")

    state_files = [
        spec.preview_state_path,
        Path(str(spec.preview_state_path) + ".backup"),
        Path(str(spec.preview_state_path) + ".lock"),
    ]
    for path in state_files:
        path.write_text("x", encoding="utf-8")
        assert path.exists()

    mock_client = MagicMock()
    provider = MagicMock(client=mock_client, projects=MagicMock())
    provider.projects.list_projects.return_value = [spec.preview_project_key]

    with patch("dss_provisioner.preview._provider_from_config", return_value=provider):
        returned_spec, deleted = destroy_preview(cfg, branch="feature/new-scoring")

    assert returned_spec == spec
    assert deleted is True
    provider.projects.delete.assert_called_once_with(spec.preview_project_key)
    for path in state_files:
        assert not path.exists()
