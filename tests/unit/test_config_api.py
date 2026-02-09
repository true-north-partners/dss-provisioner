"""Tests for config convenience API and engine wiring."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dss_provisioner.config import _engine_from_config, load, plan
from dss_provisioner.config.loader import ConfigError
from dss_provisioner.config.schema import Config, ProviderConfig

_YAML = """\
provider:
  host: https://dss.example.com
  api_key: test-key
  project: TEST

datasets:
  - name: ds
    type: upload
"""


class TestEngineFromConfig:
    def test_builds_engine_with_correct_project(self) -> None:
        config = Config(
            provider=ProviderConfig(project="MY_PRJ", host="https://h", api_key="k"),
        )
        engine = _engine_from_config(config)
        assert engine.project_key == "MY_PRJ"

    def test_builds_engine_with_state_path(self) -> None:
        config = Config(
            provider=ProviderConfig(project="P", host="https://h", api_key="k"),
            state_path=Path("custom.json"),
        )
        engine = _engine_from_config(config)
        assert engine.state_path == Path("custom.json")

    def test_missing_host_raises(self) -> None:
        config = Config(
            provider=ProviderConfig(project="P", api_key="k"),
        )
        with pytest.raises(ConfigError, match="host"):
            _engine_from_config(config)

    def test_missing_api_key_raises(self) -> None:
        config = Config(
            provider=ProviderConfig(project="P", host="https://h"),
        )
        with pytest.raises(ConfigError, match="api_key"):
            _engine_from_config(config)


class TestProviderConfigEnvResolution:
    def test_api_key_from_env(self, monkeypatch) -> None:
        monkeypatch.setenv("DSS_API_KEY", "env-secret")
        p = ProviderConfig(project="X")
        assert p.api_key == "env-secret"

    def test_host_from_yaml_over_env(self, monkeypatch) -> None:
        monkeypatch.setenv("DSS_HOST", "https://from-env")
        p = ProviderConfig(project="X", host="https://from-yaml")
        assert p.host == "https://from-yaml"

    def test_host_from_env_fallback(self, monkeypatch) -> None:
        monkeypatch.setenv("DSS_HOST", "https://from-env")
        p = ProviderConfig(project="X")
        assert p.host == "https://from-env"

    def test_api_key_from_dotenv_file(self, tmp_path, monkeypatch) -> None:
        env_file = tmp_path / ".env"
        env_file.write_text("DSS_API_KEY=from-dotenv\n")
        monkeypatch.chdir(tmp_path)
        p = ProviderConfig(project="X")
        assert p.api_key == "from-dotenv"

    def test_host_from_dotenv_file(self, tmp_path, monkeypatch) -> None:
        env_file = tmp_path / ".env"
        env_file.write_text("DSS_HOST=https://from-dotenv\n")
        monkeypatch.chdir(tmp_path)
        p = ProviderConfig(project="X")
        assert p.host == "https://from-dotenv"

    def test_env_var_overrides_dotenv_file(self, tmp_path, monkeypatch) -> None:
        env_file = tmp_path / ".env"
        env_file.write_text("DSS_API_KEY=from-dotenv\n")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("DSS_API_KEY", "from-env")
        p = ProviderConfig(project="X")
        assert p.api_key == "from-env"


class TestLoadFunction:
    def test_load_returns_config(self, tmp_path) -> None:
        f = tmp_path / "config.yaml"
        f.write_text(_YAML)
        config = load(f)
        assert config.provider.project == "TEST"
        assert len(config.resources) == 1


class TestPlanIntegration:
    @patch("dss_provisioner.config.resolve_code_files")
    @patch("dss_provisioner.engine.engine.DSSEngine.plan")
    def test_plan_calls_resolve_code_files(
        self, mock_engine_plan: MagicMock, mock_resolve: MagicMock, tmp_path
    ) -> None:
        f = tmp_path / "config.yaml"
        f.write_text(_YAML)
        config = load(f)

        mock_resolve.return_value = list(config.resources)
        mock_engine_plan.return_value = MagicMock()

        plan(config)

        mock_resolve.assert_called_once_with(config.resources, config.config_dir)

    @patch("dss_provisioner.config.resolve_code_files")
    @patch("dss_provisioner.engine.engine.DSSEngine.plan")
    def test_plan_passes_destroy_and_refresh(
        self, mock_engine_plan: MagicMock, mock_resolve: MagicMock, tmp_path
    ) -> None:
        f = tmp_path / "c.yaml"
        f.write_text(_YAML)
        config = load(f)

        mock_resolve.return_value = list(config.resources)
        mock_engine_plan.return_value = MagicMock()

        plan(config, destroy=True, refresh=False)

        _, kwargs = mock_engine_plan.call_args
        assert kwargs["destroy"] is True
        assert kwargs["refresh"] is False
