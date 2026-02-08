from __future__ import annotations

import logging
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from dss_provisioner.cli import app
from dss_provisioner.config.loader import ConfigError
from dss_provisioner.engine.errors import ApplyError, ValidationError
from dss_provisioner.engine.types import Action, ApplyResult, Plan, PlanMetadata, ResourceChange
from dss_provisioner.preview import PreviewProject, PreviewSpec

runner = CliRunner()

_META = PlanMetadata(
    project_key="TEST",
    destroy=False,
    refresh=True,
    state_lineage="lineage-1",
    state_serial=0,
    state_digest="digest",
    config_digest="cdigest",
    engine_version="0.1.0",
)

_NOOP_PLAN = Plan(
    metadata=_META,
    changes=[
        ResourceChange(
            address="dss_dataset.ok",
            resource_type="dss_dataset",
            action=Action.NOOP,
        )
    ],
)

_CREATE_PLAN = Plan(
    metadata=_META,
    changes=[
        ResourceChange(
            address="dss_dataset.new",
            resource_type="dss_dataset",
            action=Action.CREATE,
            planned={"connection": "fs_managed", "path": "/data/raw"},
        )
    ],
)


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def _mock_config() -> MagicMock:
    cfg = MagicMock()
    cfg.provider.project = "TEST"
    cfg.state_path = Path(".dss-state.json")
    return cfg


class TestVersion:
    def test_version_flag(self) -> None:
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "dss-provisioner" in result.stdout

    def test_short_version_flag(self) -> None:
        result = runner.invoke(app, ["-V"])
        assert result.exit_code == 0
        assert "dss-provisioner" in result.stdout


class TestPlanCommand:
    @patch("dss_provisioner.config.plan")
    @patch("dss_provisioner.config.load")
    def test_no_changes_exits_0(self, mock_load: MagicMock, mock_plan: MagicMock) -> None:
        mock_load.return_value = _mock_config()
        mock_plan.return_value = _NOOP_PLAN

        result = runner.invoke(app, ["plan", "--no-color", "--config", "test.yaml"])
        assert result.exit_code == 0
        assert "No changes" in result.stdout

    @patch("dss_provisioner.config.plan")
    @patch("dss_provisioner.config.load")
    def test_changes_exits_2(self, mock_load: MagicMock, mock_plan: MagicMock) -> None:
        mock_load.return_value = _mock_config()
        mock_plan.return_value = _CREATE_PLAN

        result = runner.invoke(app, ["plan", "--no-color"])
        assert result.exit_code == 2
        assert "dss_dataset.new" in result.stdout

    @patch("dss_provisioner.config.plan")
    @patch("dss_provisioner.config.load")
    def test_out_saves_plan(
        self, mock_load: MagicMock, mock_plan: MagicMock, tmp_path: Path
    ) -> None:
        mock_load.return_value = _mock_config()
        mock_plan.return_value = _CREATE_PLAN
        out_file = tmp_path / "plan.json"

        result = runner.invoke(app, ["plan", "--no-color", "--out", str(out_file)])
        assert result.exit_code == 2
        assert out_file.exists()
        assert "Plan saved" in result.stdout

    @patch("dss_provisioner.config.load")
    def test_config_error_exits_1(self, mock_load: MagicMock) -> None:
        mock_load.side_effect = ConfigError("bad config")

        result = runner.invoke(app, ["plan", "--no-color"])
        assert result.exit_code == 1
        assert "Configuration error" in result.output

    @patch("dss_provisioner.config.plan")
    @patch("dss_provisioner.config.load")
    def test_no_refresh_flag(self, mock_load: MagicMock, mock_plan: MagicMock) -> None:
        mock_load.return_value = _mock_config()
        mock_plan.return_value = _NOOP_PLAN

        runner.invoke(app, ["plan", "--no-color", "--no-refresh"])
        mock_plan.assert_called_once()
        _, kwargs = mock_plan.call_args
        assert kwargs["refresh"] is False


class TestApplyCommand:
    @patch("dss_provisioner.config.plan")
    @patch("dss_provisioner.config.load")
    def test_no_changes_message(self, mock_load: MagicMock, mock_plan: MagicMock) -> None:
        mock_load.return_value = _mock_config()
        mock_plan.return_value = _NOOP_PLAN

        result = runner.invoke(app, ["apply", "--no-color"])
        assert result.exit_code == 0
        assert "No changes" in result.stdout

    @patch("dss_provisioner.config.apply")
    @patch("dss_provisioner.config.plan")
    @patch("dss_provisioner.config.load")
    def test_auto_approve_skips_prompt(
        self, mock_load: MagicMock, mock_plan: MagicMock, mock_apply: MagicMock
    ) -> None:
        mock_load.return_value = _mock_config()
        mock_plan.return_value = _CREATE_PLAN
        mock_apply.return_value = ApplyResult(applied=_CREATE_PLAN.changes)

        result = runner.invoke(app, ["apply", "--no-color", "--auto-approve"])
        assert result.exit_code == 0
        assert "Apply complete!" in result.stdout

    @patch("dss_provisioner.config.plan")
    @patch("dss_provisioner.config.load")
    def test_user_decline_aborts(self, mock_load: MagicMock, mock_plan: MagicMock) -> None:
        mock_load.return_value = _mock_config()
        mock_plan.return_value = _CREATE_PLAN

        result = runner.invoke(app, ["apply", "--no-color"], input="n\n")
        assert result.exit_code == 1

    @patch("dss_provisioner.config.apply")
    @patch("dss_provisioner.config.plan")
    @patch("dss_provisioner.config.load")
    def test_apply_error_shows_partial(
        self, mock_load: MagicMock, mock_plan: MagicMock, mock_apply: MagicMock
    ) -> None:
        mock_load.return_value = _mock_config()
        mock_plan.return_value = _CREATE_PLAN
        mock_apply.side_effect = ApplyError(
            applied=[], address="dss_dataset.new", message="API error"
        )

        result = runner.invoke(app, ["apply", "--no-color", "--auto-approve"])
        assert result.exit_code == 1
        assert "Apply failed" in result.output


class TestDestroyCommand:
    @patch("dss_provisioner.config.plan")
    @patch("dss_provisioner.config.load")
    def test_no_resources_exits_0(self, mock_load: MagicMock, mock_plan: MagicMock) -> None:
        mock_load.return_value = _mock_config()
        mock_plan.return_value = _NOOP_PLAN

        result = runner.invoke(app, ["destroy", "--no-color"])
        assert result.exit_code == 0
        assert "No resources to destroy" in result.stdout

    @patch("dss_provisioner.config.apply")
    @patch("dss_provisioner.config.plan")
    @patch("dss_provisioner.config.load")
    def test_auto_approve_works(
        self, mock_load: MagicMock, mock_plan: MagicMock, mock_apply: MagicMock
    ) -> None:
        delete_plan = Plan(
            metadata=_META,
            changes=[
                ResourceChange(
                    address="dss_dataset.old",
                    resource_type="dss_dataset",
                    action=Action.DELETE,
                    prior={"connection": "fs_managed"},
                )
            ],
        )
        mock_load.return_value = _mock_config()
        mock_plan.return_value = delete_plan
        mock_apply.return_value = ApplyResult(applied=delete_plan.changes)

        result = runner.invoke(app, ["destroy", "--no-color", "--auto-approve"])
        assert result.exit_code == 0
        assert "Apply complete!" in result.stdout


class TestRefreshCommand:
    _UPDATE_CHANGE = ResourceChange(
        address="dss_dataset.raw",
        resource_type="dss_dataset",
        action=Action.UPDATE,
        prior={"path": "/old"},
        planned={"path": "/new"},
        diff={"path": {"from": "/old", "to": "/new"}},
    )

    @staticmethod
    def _mock_state(n: int) -> MagicMock:
        state = MagicMock()
        state.resources = {f"dss_dataset.r{i}": None for i in range(n)}
        return state

    @patch("dss_provisioner.config.save_state")
    @patch("dss_provisioner.config.refresh")
    @patch("dss_provisioner.config.load")
    def test_no_changes_exits_0(
        self, mock_load: MagicMock, mock_refresh: MagicMock, mock_save: MagicMock
    ) -> None:
        mock_load.return_value = _mock_config()
        mock_refresh.return_value = ([], self._mock_state(2))

        result = runner.invoke(app, ["refresh", "--no-color"])
        assert result.exit_code == 0
        assert "up-to-date" in result.stdout
        mock_save.assert_not_called()

    @patch("dss_provisioner.config.save_state")
    @patch("dss_provisioner.config.refresh")
    @patch("dss_provisioner.config.load")
    def test_auto_approve_skips_prompt(
        self, mock_load: MagicMock, mock_refresh: MagicMock, mock_save: MagicMock
    ) -> None:
        mock_load.return_value = _mock_config()
        mock_refresh.return_value = ([self._UPDATE_CHANGE], self._mock_state(1))

        result = runner.invoke(app, ["refresh", "--no-color", "--auto-approve"])
        assert result.exit_code == 0
        assert "State refreshed" in result.stdout
        mock_save.assert_called_once()

    @patch("dss_provisioner.config.save_state")
    @patch("dss_provisioner.config.refresh")
    @patch("dss_provisioner.config.load")
    def test_user_decline_aborts(
        self, mock_load: MagicMock, mock_refresh: MagicMock, mock_save: MagicMock
    ) -> None:
        mock_load.return_value = _mock_config()
        mock_refresh.return_value = ([self._UPDATE_CHANGE], self._mock_state(1))

        result = runner.invoke(app, ["refresh", "--no-color"], input="n\n")
        assert result.exit_code == 1
        mock_save.assert_not_called()

    @patch("dss_provisioner.config.save_state")
    @patch("dss_provisioner.config.refresh")
    @patch("dss_provisioner.config.load")
    def test_shows_drift_before_confirm(
        self, mock_load: MagicMock, mock_refresh: MagicMock, _mock_save: MagicMock
    ) -> None:
        mock_load.return_value = _mock_config()
        mock_refresh.return_value = ([self._UPDATE_CHANGE], self._mock_state(1))

        result = runner.invoke(app, ["refresh", "--no-color", "--auto-approve"])
        assert "dss_dataset.raw" in result.stdout
        assert "Refresh: " in result.stdout
        assert "1 to change" in result.stdout


class TestDriftCommand:
    @patch("dss_provisioner.config.drift")
    @patch("dss_provisioner.config.load")
    def test_no_drift_exits_0(self, mock_load: MagicMock, mock_drift: MagicMock) -> None:
        mock_load.return_value = _mock_config()
        mock_drift.return_value = []

        result = runner.invoke(app, ["drift", "--no-color"])
        assert result.exit_code == 0
        assert "No drift detected" in result.stdout

    @patch("dss_provisioner.config.drift")
    @patch("dss_provisioner.config.load")
    def test_drift_shows_changes(self, mock_load: MagicMock, mock_drift: MagicMock) -> None:
        mock_load.return_value = _mock_config()
        mock_drift.return_value = [
            ResourceChange(
                address="dss_dataset.raw",
                resource_type="dss_dataset",
                action=Action.UPDATE,
                diff={"path": {"from": "/old", "to": "/new"}},
            )
        ]

        result = runner.invoke(app, ["drift", "--no-color"])
        assert result.exit_code == 0
        assert "Drift detected" in result.stdout
        assert "dss_dataset.raw" in result.stdout


class TestValidateCommand:
    @patch("dss_provisioner.config.plan")
    @patch("dss_provisioner.config.load")
    def test_valid_config(self, mock_load: MagicMock, mock_plan: MagicMock) -> None:
        mock_load.return_value = _mock_config()
        mock_plan.return_value = _NOOP_PLAN

        result = runner.invoke(app, ["validate", "--no-color"])
        assert result.exit_code == 0
        assert "Configuration is valid" in result.stdout

    @patch("dss_provisioner.config.plan")
    @patch("dss_provisioner.config.load")
    def test_validation_error(self, mock_load: MagicMock, mock_plan: MagicMock) -> None:
        mock_load.return_value = _mock_config()
        mock_plan.side_effect = ValidationError(["field X is required", "field Y is invalid"])

        result = runner.invoke(app, ["validate", "--no-color"])
        assert result.exit_code == 1
        assert "Validation failed" in result.output


class TestPreviewCommand:
    @staticmethod
    def _preview_spec() -> PreviewSpec:
        return PreviewSpec(
            base_project_key="TEST",
            branch="feature/new-scoring",
            branch_slug="feature_new_scoring",
            preview_project_key="TEST__FEATURE_NEW_SCORING",
            preview_state_path=Path(".dss-state.preview.feature_new_scoring.json"),
        )

    @patch("dss_provisioner.preview.run_preview")
    @patch("dss_provisioner.config.load")
    def test_preview_apply(
        self,
        mock_load: MagicMock,
        mock_run_preview: MagicMock,
    ) -> None:
        mock_load.return_value = _mock_config()
        mock_run_preview.return_value = (
            self._preview_spec(),
            _CREATE_PLAN,
            ApplyResult(applied=_CREATE_PLAN.changes),
        )

        result = runner.invoke(app, ["preview", "--no-color", "--branch", "feature/new-scoring"])
        assert result.exit_code == 0
        assert "Preview project: TEST__FEATURE_NEW_SCORING" in result.stdout
        mock_run_preview.assert_called_once()
        _, kwargs = mock_run_preview.call_args
        assert kwargs["branch"] == "feature/new-scoring"
        assert kwargs["refresh"] is True
        assert kwargs["force"] is False

    @patch("dss_provisioner.preview.run_preview")
    @patch("dss_provisioner.config.load")
    def test_preview_no_refresh_flag(
        self,
        mock_load: MagicMock,
        mock_run_preview: MagicMock,
    ) -> None:
        mock_load.return_value = _mock_config()
        mock_run_preview.return_value = (
            self._preview_spec(),
            _NOOP_PLAN,
            ApplyResult(applied=[]),
        )

        result = runner.invoke(app, ["preview", "--no-color", "--no-refresh"])
        assert result.exit_code == 0
        _, kwargs = mock_run_preview.call_args
        assert kwargs["refresh"] is False
        assert kwargs["force"] is False

    @patch("dss_provisioner.preview.run_preview")
    @patch("dss_provisioner.config.load")
    def test_preview_force_flag(
        self,
        mock_load: MagicMock,
        mock_run_preview: MagicMock,
    ) -> None:
        mock_load.return_value = _mock_config()
        mock_run_preview.return_value = (
            self._preview_spec(),
            _NOOP_PLAN,
            ApplyResult(applied=[]),
        )

        result = runner.invoke(app, ["preview", "--no-color", "--force"])
        assert result.exit_code == 0
        _, kwargs = mock_run_preview.call_args
        assert kwargs["force"] is True

    @patch("dss_provisioner.preview.destroy_preview")
    @patch("dss_provisioner.config.load")
    def test_preview_destroy(
        self,
        mock_load: MagicMock,
        mock_destroy_preview: MagicMock,
    ) -> None:
        mock_load.return_value = _mock_config()
        mock_destroy_preview.return_value = (self._preview_spec(), True)

        result = runner.invoke(app, ["preview", "--no-color", "--destroy"])
        assert result.exit_code == 0
        assert "Deleted preview project" in result.stdout
        _, kwargs = mock_destroy_preview.call_args
        assert kwargs["force"] is False

    @patch("dss_provisioner.preview.destroy_preview")
    @patch("dss_provisioner.config.load")
    def test_preview_destroy_force_flag(
        self,
        mock_load: MagicMock,
        mock_destroy_preview: MagicMock,
    ) -> None:
        mock_load.return_value = _mock_config()
        mock_destroy_preview.return_value = (self._preview_spec(), True)

        result = runner.invoke(app, ["preview", "--no-color", "--destroy", "--force"])
        assert result.exit_code == 0
        _, kwargs = mock_destroy_preview.call_args
        assert kwargs["force"] is True

    @patch("dss_provisioner.preview.list_previews")
    @patch("dss_provisioner.config.load")
    def test_preview_list(
        self,
        mock_load: MagicMock,
        mock_list_previews: MagicMock,
    ) -> None:
        mock_load.return_value = _mock_config()
        mock_list_previews.return_value = [
            PreviewProject(project_key="TEST__FEATURE_A", branch="feature/a"),
            PreviewProject(project_key="TEST__FEATURE_B", branch=None),
        ]

        result = runner.invoke(app, ["preview", "--no-color", "--list"])
        assert result.exit_code == 0
        assert "Preview projects:" in result.stdout
        assert "TEST__FEATURE_A (branch: feature/a)" in result.stdout
        assert "TEST__FEATURE_B (branch: unknown)" in result.stdout

    def test_preview_destroy_and_list_are_mutually_exclusive(self) -> None:
        result = runner.invoke(app, ["preview", "--no-color", "--destroy", "--list"])
        assert result.exit_code == 1
        assert "Configuration error" in result.output


class TestNoColor:
    @patch("dss_provisioner.config.plan")
    @patch("dss_provisioner.config.load")
    def test_no_color_strips_ansi(self, mock_load: MagicMock, mock_plan: MagicMock) -> None:
        mock_load.return_value = _mock_config()
        mock_plan.return_value = _CREATE_PLAN

        result = runner.invoke(app, ["plan", "--no-color"])
        assert "\x1b[" not in result.stdout


@pytest.fixture(autouse=False)
def _reset_pkg_logger():
    """Reset the dss_provisioner logger level after each logging test."""
    yield
    logging.getLogger("dss_provisioner").setLevel(logging.NOTSET)


@pytest.mark.usefixtures("_reset_pkg_logger")
class TestConfigureLogging:
    """Unit-test ``_configure_logging`` by mocking ``logging.basicConfig``.

    Pytest's logging plugin installs a handler on the root logger, making
    ``basicConfig`` a no-op without ``force=True``.  Rather than fighting
    the handler lifecycle, we mock ``basicConfig`` and assert the call args.
    The function sets the root logger to WARNING (keeping third-party libs
    quiet) and scopes the desired level to the ``dss_provisioner`` logger.
    """

    @patch("logging.basicConfig")
    def test_verbose_flag_configures_info(self, mock_bc: MagicMock) -> None:
        from dss_provisioner.cli import _LOG_FORMAT, _configure_logging

        _configure_logging(1)
        mock_bc.assert_called_once_with(
            level=logging.WARNING,
            format=_LOG_FORMAT,
            stream=sys.stderr,
            force=True,
        )
        assert logging.getLogger("dss_provisioner").level == logging.INFO

    @patch("logging.basicConfig")
    def test_double_verbose_configures_debug(self, mock_bc: MagicMock) -> None:
        from dss_provisioner.cli import _LOG_FORMAT, _configure_logging

        _configure_logging(2)
        mock_bc.assert_called_once_with(
            level=logging.WARNING,
            format=_LOG_FORMAT,
            stream=sys.stderr,
            force=True,
        )
        assert logging.getLogger("dss_provisioner").level == logging.DEBUG

    @patch("logging.basicConfig")
    def test_no_verbose_stays_unconfigured(self, mock_bc: MagicMock) -> None:
        from dss_provisioner.cli import _configure_logging

        _configure_logging(0)
        mock_bc.assert_not_called()

    @patch("logging.basicConfig")
    def test_dss_log_env_var(self, mock_bc: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        from dss_provisioner.cli import _configure_logging

        monkeypatch.setenv("DSS_LOG", "DEBUG")
        _configure_logging(0)
        mock_bc.assert_called_once()
        assert logging.getLogger("dss_provisioner").level == logging.DEBUG

    @patch("logging.basicConfig")
    def test_dss_log_overrides_verbose(
        self, mock_bc: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from dss_provisioner.cli import _configure_logging

        monkeypatch.setenv("DSS_LOG", "WARNING")
        _configure_logging(2)
        mock_bc.assert_called_once()
        assert logging.getLogger("dss_provisioner").level == logging.WARNING

    @patch("logging.basicConfig")
    def test_invalid_dss_log_warns_and_defaults_to_info(
        self,
        mock_bc: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        from dss_provisioner.cli import _configure_logging

        monkeypatch.setenv("DSS_LOG", "BOGUS")
        _configure_logging(0)
        mock_bc.assert_called_once()
        assert logging.getLogger("dss_provisioner").level == logging.INFO
        assert "invalid DSS_LOG level" in capsys.readouterr().err
