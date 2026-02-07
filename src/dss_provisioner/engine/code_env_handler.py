"""Code environment handler implementing CRUD via dataikuapi project settings."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dss_provisioner.engine.handlers import ResourceHandler

if TYPE_CHECKING:
    from dss_provisioner.core.state import ResourceInstance
    from dss_provisioner.engine.handlers import EngineContext, PlanContext
    from dss_provisioner.resources.code_env import CodeEnvResource


def _read_env(raw_settings: dict[str, Any], lang: str) -> str | None:
    """Extract the env name for a language if mode is EXPLICIT_ENV, else None."""
    env_cfg = raw_settings.get("settings", {}).get("codeEnvs", {}).get(lang, {})
    if env_cfg.get("mode") == "EXPLICIT_ENV":
        return env_cfg.get("envName") or None
    return None


class CodeEnvHandler(ResourceHandler["CodeEnvResource"]):
    """CRUD handler for project default code environments."""

    def _read_attrs(self, ctx: EngineContext) -> dict[str, Any]:
        project = ctx.provider.client.get_project(ctx.project_key)
        raw = project.get_settings().get_raw()
        attrs: dict[str, Any] = {
            "name": "code_envs",
            "description": "",
            "tags": [],
        }
        python_env = _read_env(raw, "python")
        if python_env is not None:
            attrs["default_python"] = python_env
        r_env = _read_env(raw, "r")
        if r_env is not None:
            attrs["default_r"] = r_env
        return attrs

    def create(self, ctx: EngineContext, desired: CodeEnvResource) -> dict[str, Any]:
        project = ctx.provider.client.get_project(ctx.project_key)
        settings = project.get_settings()
        if desired.default_python is not None:
            settings.set_python_code_env(desired.default_python)
        if desired.default_r is not None:
            settings.set_r_code_env(desired.default_r)
        settings.save()
        return self._read_attrs(ctx)

    def read(self, ctx: EngineContext, prior: ResourceInstance) -> dict[str, Any]:
        _ = prior
        return self._read_attrs(ctx)

    def update(
        self, ctx: EngineContext, desired: CodeEnvResource, prior: ResourceInstance
    ) -> dict[str, Any]:
        _ = prior
        return self.create(ctx, desired)

    def delete(self, ctx: EngineContext, prior: ResourceInstance) -> None:
        _ = prior
        project = ctx.provider.client.get_project(ctx.project_key)
        settings = project.get_settings()
        raw = settings.get_raw()
        code_envs = raw.setdefault("settings", {}).setdefault("codeEnvs", {})
        code_envs["python"] = {"mode": "INHERIT"}
        code_envs["r"] = {"mode": "INHERIT"}
        settings.save()

    def validate_plan(
        self,
        ctx: EngineContext,
        desired: CodeEnvResource,
        plan_ctx: PlanContext,
    ) -> list[str]:
        _ = plan_ctx
        errors: list[str] = []
        if desired.default_python is None and desired.default_r is None:
            return errors

        all_envs = ctx.provider.client.list_code_envs()
        if desired.default_python is not None:
            python_names = {e["envName"] for e in all_envs if e.get("envLang") == "PYTHON"}
            if desired.default_python not in python_names:
                errors.append(
                    f"Code env default_python references unknown "
                    f"Python environment '{desired.default_python}'"
                )
        if desired.default_r is not None:
            r_names = {e["envName"] for e in all_envs if e.get("envLang") == "R"}
            if desired.default_r not in r_names:
                errors.append(
                    f"Code env default_r references unknown R environment '{desired.default_r}'"
                )
        return errors
