"""DSS variable substitution utilities.

DSS uses ``${…}`` syntax for variable interpolation (e.g. ``${projectKey}``).
This module provides helpers used by both the engine (plan-time comparison)
and resource handlers (reading state from DSS).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dss_provisioner.engine.handlers import EngineContext


def resolve_variables(value: Any, variables: dict[str, str]) -> Any:
    """Replace DSS ``${…}`` variables in string values, recursively.

    *variables* maps variable names to their values, e.g.
    ``{"projectKey": "MY_PRJ"}``.
    """
    if isinstance(value, str):
        for var, replacement in variables.items():
            value = value.replace(f"${{{var}}}", replacement)
        return value
    if isinstance(value, dict):
        return {k: resolve_variables(v, variables) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_variables(v, variables) for v in value]
    return value


def get_variables(ctx: EngineContext) -> dict[str, str]:
    """Build the DSS variable substitution map from built-ins + project/instance vars.

    Falls back to ``{"projectKey": ctx.project_key}`` if the variable APIs are
    unavailable (e.g. permission issues).
    """
    variables: dict[str, str] = {"projectKey": ctx.project_key}
    try:
        for k, v in ctx.provider.client.get_global_variables().items():
            if isinstance(v, str):
                variables[k] = v
        project = ctx.provider.client.get_project(ctx.project_key)
        project_vars = project.get_variables()
        for scope in ("standard", "local"):
            for k, v in project_vars.get(scope, {}).items():
                if isinstance(v, str):
                    variables[k] = v
    except Exception:
        pass

    return variables
