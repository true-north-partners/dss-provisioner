"""Tests for config convenience API and engine wiring."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dss_provisioner.config import _engine_from_config, load, plan
from dss_provisioner.config.loader import ConfigError, _resolve_provider
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

    def test_no_check_certificate_passed_to_provider(self) -> None:
        config = Config(
            provider=ProviderConfig(project="P", host="https://h", api_key="k", verify_ssl=False),
        )
        engine = _engine_from_config(config)
        assert engine._provider.no_check_certificate is True

    def test_verify_ssl_default_no_check_false(self) -> None:
        config = Config(
            provider=ProviderConfig(project="P", host="https://h", api_key="k"),
        )
        engine = _engine_from_config(config)
        assert engine._provider.no_check_certificate is False


class TestResolveProvider:
    """Unit tests for _resolve_provider priority chain (no YAML parsing)."""

    def test_yaml_value_wins(self) -> None:
        result = _resolve_provider({"host": "https://from-yaml", "project": "P"}, Path())
        assert result["host"] == "https://from-yaml"

    def test_env_var_fallback(self, monkeypatch) -> None:
        monkeypatch.setenv("DSS_HOST", "https://from-env")
        result = _resolve_provider({"project": "P"}, Path())
        assert result["host"] == "https://from-env"

    def test_yaml_value_over_env_var(self, monkeypatch) -> None:
        monkeypatch.setenv("DSS_HOST", "https://from-env")
        result = _resolve_provider({"host": "https://from-yaml", "project": "P"}, Path())
        assert result["host"] == "https://from-yaml"

    def test_yaml_null_falls_through_to_env_var(self, monkeypatch) -> None:
        monkeypatch.setenv("DSS_API_KEY", "from-env")
        result = _resolve_provider({"host": "https://h", "api_key": None, "project": "P"}, Path())
        assert result["api_key"] == "from-env"

    def test_missing_field_omitted(self) -> None:
        result = _resolve_provider({"project": "P"}, Path())
        assert "host" not in result
        assert "api_key" not in result

    def test_dotenv_file(self, tmp_path) -> None:
        (tmp_path / ".env").write_text("DSS_API_KEY=from-dotenv\n")
        result = _resolve_provider({"host": "https://h", "project": "P"}, tmp_path)
        assert result["api_key"] == "from-dotenv"

    def test_env_var_overrides_dotenv(self, tmp_path, monkeypatch) -> None:
        (tmp_path / ".env").write_text("DSS_API_KEY=from-dotenv\n")
        monkeypatch.setenv("DSS_API_KEY", "from-env")
        result = _resolve_provider({"project": "P"}, tmp_path)
        assert result["api_key"] == "from-env"

    def test_yaml_null_falls_through_to_dotenv(self, tmp_path) -> None:
        (tmp_path / ".env").write_text("DSS_API_KEY=from-dotenv\n")
        result = _resolve_provider({"host": "https://h", "api_key": None, "project": "P"}, tmp_path)
        assert result["api_key"] == "from-dotenv"

    def test_dotenv_with_bom(self, tmp_path) -> None:
        (tmp_path / ".env").write_bytes(b"\xef\xbb\xbfDSS_API_KEY=from-bom\n")
        result = _resolve_provider({"host": "https://h", "project": "P"}, tmp_path)
        assert result["api_key"] == "from-bom"

    def test_verify_ssl_defaults_absent(self) -> None:
        result = _resolve_provider({"project": "P"}, Path())
        assert "verify_ssl" not in result

    def test_verify_ssl_from_yaml(self) -> None:
        result = _resolve_provider({"project": "P", "verify_ssl": False}, Path())
        assert result["verify_ssl"] is False

    def test_verify_ssl_from_env_var(self, monkeypatch) -> None:
        monkeypatch.setenv("DSS_VERIFY_SSL", "false")
        result = _resolve_provider({"project": "P"}, Path())
        assert result["verify_ssl"] is False

    def test_verify_ssl_from_dotenv(self, tmp_path) -> None:
        (tmp_path / ".env").write_text("DSS_VERIFY_SSL=false\n")
        result = _resolve_provider({"project": "P"}, tmp_path)
        assert result["verify_ssl"] is False

    def test_verify_ssl_invalid_string_raises(self, monkeypatch) -> None:
        monkeypatch.setenv("DSS_VERIFY_SSL", "nope")
        with pytest.raises(ConfigError, match=r"Invalid boolean.*DSS_VERIFY_SSL"):
            _resolve_provider({"project": "P"}, Path())


class TestLoadFunction:
    def test_load_returns_config(self, make_config) -> None:
        config = make_config(_YAML)
        assert config.provider.project == "TEST"
        assert len(config.resources) == 1

    def test_dotenv_next_to_config(self, make_config) -> None:
        config = make_config(
            "provider:\n  host: https://h\n  project: P\n",
            dotenv="DSS_API_KEY=from-dotenv\n",
        )
        assert config.provider.api_key == "from-dotenv"

    def test_dotenv_from_config_dir_not_cwd(self, tmp_path, monkeypatch) -> None:
        subdir = tmp_path / "infra"
        subdir.mkdir()
        (subdir / ".env").write_text("DSS_API_KEY=from-subdir\n")
        (subdir / "config.yaml").write_text("provider:\n  host: https://h\n  project: P\n")
        monkeypatch.chdir(tmp_path)  # cwd has no .env
        config = load(subdir / "config.yaml")
        assert config.provider.api_key == "from-subdir"

    def test_env_var_through_load(self, make_config, monkeypatch) -> None:
        monkeypatch.setenv("DSS_API_KEY", "env-secret")
        config = make_config("provider:\n  host: https://h\n  project: P\n")
        assert config.provider.api_key == "env-secret"

    def test_verify_ssl_false_in_yaml(self, make_config) -> None:
        config = make_config(
            "provider:\n  host: https://h\n  api_key: k\n  project: P\n  verify_ssl: false\n"
        )
        assert config.provider.verify_ssl is False

    def test_verify_ssl_from_env(self, make_config, monkeypatch) -> None:
        monkeypatch.setenv("DSS_VERIFY_SSL", "false")
        config = make_config("provider:\n  host: https://h\n  api_key: k\n  project: P\n")
        assert config.provider.verify_ssl is False

    def test_verify_ssl_default_true(self, make_config) -> None:
        config = make_config("provider:\n  host: https://h\n  api_key: k\n  project: P\n")
        assert config.provider.verify_ssl is True


class TestPlanIntegration:
    @patch("dss_provisioner.config.resolve_code_files")
    @patch("dss_provisioner.engine.engine.DSSEngine.plan")
    def test_plan_calls_resolve_code_files(
        self, mock_engine_plan: MagicMock, mock_resolve: MagicMock, make_config
    ) -> None:
        config = make_config(_YAML)

        mock_resolve.return_value = list(config.resources)
        mock_engine_plan.return_value = MagicMock()

        plan(config)

        mock_resolve.assert_called_once_with(config.resources, config.config_dir)

    @patch("dss_provisioner.config.resolve_code_files")
    @patch("dss_provisioner.engine.engine.DSSEngine.plan")
    def test_plan_passes_destroy_and_refresh(
        self, mock_engine_plan: MagicMock, mock_resolve: MagicMock, make_config
    ) -> None:
        config = make_config(_YAML)

        mock_resolve.return_value = list(config.resources)
        mock_engine_plan.return_value = MagicMock()

        plan(config, destroy=True, refresh=False)

        _, kwargs = mock_engine_plan.call_args
        assert kwargs["destroy"] is True
        assert kwargs["refresh"] is False
