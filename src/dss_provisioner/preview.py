"""Branch-based preview environment orchestration."""

from __future__ import annotations

import contextlib
import hashlib
import logging
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import SecretStr

from dss_provisioner.config import apply as apply_fn
from dss_provisioner.config import plan as plan_fn
from dss_provisioner.config.loader import ConfigError
from dss_provisioner.core.provider import ApiKeyAuth, DSSProvider

if TYPE_CHECKING:
    from dss_provisioner.config.schema import Config
    from dss_provisioner.engine.types import ApplyResult, Plan

logger = logging.getLogger(__name__)

_PREVIEW_TAG = "dss-provisioner-preview"
_PREVIEW_BASE_PREFIX = "dss-provisioner-base:"
_PREVIEW_BRANCH_PREFIX = "dss-provisioner-branch:"
_PROJECT_KEY_MAX_LEN = 32


@dataclass(frozen=True)
class PreviewSpec:
    """Computed preview metadata for a base project + branch."""

    base_project_key: str
    branch: str
    branch_slug: str
    preview_project_key: str
    preview_state_path: Path


@dataclass(frozen=True)
class PreviewProject:
    """Preview project entry used by ``preview --list``."""

    project_key: str
    branch: str | None = None


def run_preview(
    config: Config,
    *,
    branch: str | None = None,
    refresh: bool = True,
) -> tuple[PreviewSpec, Plan, ApplyResult]:
    """Create/reuse preview project, apply config, and return results."""
    spec = compute_preview_spec(config, branch=branch)
    provider = _provider_from_config(config)
    _ensure_preview_project(provider, spec)

    preview_config = build_preview_config(config, spec)
    plan_obj = plan_fn(preview_config, refresh=refresh)
    result = apply_fn(plan_obj, preview_config)
    return spec, plan_obj, result


def destroy_preview(config: Config, *, branch: str | None = None) -> tuple[PreviewSpec, bool]:
    """Delete preview project + state artifacts. Returns (spec, project_deleted)."""
    spec = compute_preview_spec(config, branch=branch)
    provider = _provider_from_config(config)
    deleted = _delete_preview_project(provider, spec.preview_project_key)
    _cleanup_preview_state(spec.preview_state_path)
    return spec, deleted


def list_previews(config: Config) -> list[PreviewProject]:
    """List preview projects for the configured base project."""
    provider = _provider_from_config(config)
    base_key = _sanitize_project_segment(config.provider.project)
    prefix = f"{base_key}__"

    previews: list[PreviewProject] = []
    for key in sorted(provider.projects.list_projects()):
        if not key.startswith(prefix):
            continue

        branch: str | None = None
        try:
            meta = provider.client.get_project(key).get_metadata()
            tags = meta.get("tags", [])
            branch = _extract_tag(tags, _PREVIEW_BRANCH_PREFIX)
        except Exception:
            logger.debug("Could not read metadata for preview project %s", key, exc_info=True)

        previews.append(PreviewProject(project_key=key, branch=branch))

    return previews


def compute_preview_spec(config: Config, *, branch: str | None = None) -> PreviewSpec:
    """Compute branch, project key, and preview state path."""
    resolved_branch = _resolve_branch(config.config_dir, override=branch)
    branch_slug = _slug_branch(resolved_branch)
    base_key = _sanitize_project_segment(config.provider.project)

    return PreviewSpec(
        base_project_key=base_key,
        branch=resolved_branch,
        branch_slug=branch_slug,
        preview_project_key=_build_preview_project_key(base_key, branch_slug),
        preview_state_path=_build_preview_state_path(config.state_path, branch_slug),
    )


def build_preview_config(config: Config, spec: PreviewSpec) -> Config:
    """Build a deep-copied config routed to the preview project."""
    preview_config = config.model_copy(deep=True)
    preview_config.provider.project = spec.preview_project_key
    preview_config.state_path = spec.preview_state_path

    if any(lib.repository == "self" for lib in preview_config.libraries):
        origin = _git_output(config.config_dir, "config", "--get", "remote.origin.url")
        if not origin:
            msg = (
                "Library repository='self' requires a configured git remote origin URL "
                "(remote.origin.url)."
            )
            raise ConfigError(msg)
        preview_config.libraries = [
            lib.model_copy(update={"repository": origin, "checkout": spec.branch})
            if lib.repository == "self"
            else lib
            for lib in preview_config.libraries
        ]

    return preview_config


def _provider_from_config(config: Config) -> DSSProvider:
    if not config.provider.host:
        raise ConfigError("provider.host is required (set in YAML or DSS_HOST env var)")
    if not config.provider.api_key:
        raise ConfigError("provider.api_key is required (set DSS_API_KEY env var)")
    return DSSProvider(
        host=config.provider.host,
        auth=ApiKeyAuth(api_key=SecretStr(config.provider.api_key)),
    )


def _resolve_branch(config_dir: Path, *, override: str | None) -> str:
    if override:
        return override
    branch = _git_output(config_dir, "branch", "--show-current")
    if branch:
        return branch
    msg = (
        "Could not determine current git branch (detached HEAD or not a git repository). "
        "Use --branch to set it explicitly."
    )
    raise ConfigError(msg)


def _slug_branch(branch: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", branch.lower()).strip("_")
    return slug or "preview"


def _sanitize_project_segment(value: str) -> str:
    segment = re.sub(r"[^A-Z0-9_]+", "_", value.upper()).strip("_")
    return segment or "PREVIEW"


def _build_preview_project_key(base_key: str, branch_slug: str) -> str:
    branch_key = _sanitize_project_segment(branch_slug)
    candidate = f"{base_key}__{branch_key}"
    if len(candidate) <= _PROJECT_KEY_MAX_LEN:
        return candidate

    digest = hashlib.sha1(branch_slug.encode("utf-8")).hexdigest()[:6].upper()
    # Keep base visible and include a short hash suffix for deterministic truncation.
    available = _PROJECT_KEY_MAX_LEN - len("__") - len("_") - len(digest)
    base_part = base_key[: max(1, available - 4)]
    branch_part = branch_key[: max(1, available - len(base_part))]
    return f"{base_part}__{branch_part}_{digest}"


def _build_preview_state_path(base_state_path: Path, branch_slug: str) -> Path:
    suffixes = "".join(base_state_path.suffixes)
    basename = base_state_path.name[: -len(suffixes)] if suffixes else base_state_path.name
    preview_name = f"{basename}.preview.{branch_slug}{suffixes}"
    return base_state_path.with_name(preview_name)


def _ensure_preview_project(provider: DSSProvider, spec: PreviewSpec) -> None:
    if spec.preview_project_key not in set(provider.projects.list_projects()):
        auth = provider.client.get_auth_info()
        owner = auth.get("authIdentifier")
        if not owner:
            msg = "Could not determine DSS owner from auth info."
            raise ConfigError(msg)
        provider.projects.create(
            spec.preview_project_key,
            f"{spec.base_project_key} preview ({spec.branch})",
            owner=owner,
        )

    project = provider.client.get_project(spec.preview_project_key)
    _tag_preview_project(project, spec)


def _delete_preview_project(provider: DSSProvider, preview_project_key: str) -> bool:
    if preview_project_key not in set(provider.projects.list_projects()):
        return False
    provider.projects.delete(preview_project_key)
    return True


def _tag_preview_project(project: object, spec: PreviewSpec) -> None:
    # dataikuapi typing is permissive; this project handle supports get/set_metadata.
    meta = project.get_metadata()  # type: ignore[attr-defined]
    tags = set(meta.get("tags", []))
    tags.add(_PREVIEW_TAG)
    tags.add(f"{_PREVIEW_BASE_PREFIX}{spec.base_project_key}")
    tags.add(f"{_PREVIEW_BRANCH_PREFIX}{spec.branch}")
    meta["tags"] = sorted(tags)
    project.set_metadata(meta)  # type: ignore[attr-defined]


def _cleanup_preview_state(preview_state_path: Path) -> None:
    candidates = [
        preview_state_path,
        Path(str(preview_state_path) + ".backup"),
        Path(str(preview_state_path) + ".lock"),
    ]
    for path in candidates:
        with contextlib.suppress(FileNotFoundError):
            path.unlink()


def _extract_tag(tags: list[str], prefix: str) -> str | None:
    for tag in tags:
        if tag.startswith(prefix):
            return tag.removeprefix(prefix)
    return None


def _git_output(config_dir: Path, *args: str) -> str:
    cmd = ["git", "-C", str(config_dir), *args]
    try:
        completed = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        msg = f"Failed to run {' '.join(cmd)}"
        if stderr:
            msg += f": {stderr}"
        raise ConfigError(msg) from exc
    return completed.stdout.strip()
