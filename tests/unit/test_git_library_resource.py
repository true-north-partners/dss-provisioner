"""Tests for GitLibraryResource model."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dss_provisioner.resources.git_library import GitLibraryResource


class TestGitLibraryResource:
    def test_address(self) -> None:
        r = GitLibraryResource(name="shared_utils", repository="git@github.com:org/lib.git")
        assert r.address == "dss_git_library.shared_utils"

    def test_defaults(self) -> None:
        r = GitLibraryResource(name="lib", repository="git@github.com:org/lib.git")
        assert r.checkout == "main"
        assert r.path == ""
        assert r.add_to_python_path is True
        assert r.description == ""
        assert r.tags == []
        assert r.depends_on == []

    def test_custom_values(self) -> None:
        r = GitLibraryResource(
            name="mylib",
            repository="git@github.com:org/repo.git",
            checkout="v1.2.3",
            path="python",
            add_to_python_path=False,
        )
        assert r.name == "mylib"
        assert r.repository == "git@github.com:org/repo.git"
        assert r.checkout == "v1.2.3"
        assert r.path == "python"
        assert r.add_to_python_path is False

    def test_extra_forbid(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            GitLibraryResource(
                name="lib",
                repository="git@github.com:org/lib.git",
                unknown="x",  # type: ignore[call-arg]
            )

    def test_model_dump_shape(self) -> None:
        r = GitLibraryResource(name="lib", repository="git@github.com:org/lib.git", checkout="dev")
        dump = r.model_dump(exclude={"address"})
        assert dump["name"] == "lib"
        assert dump["repository"] == "git@github.com:org/lib.git"
        assert dump["checkout"] == "dev"
        assert dump["path"] == ""
        assert dump["add_to_python_path"] is True
        assert "address" not in dump

    def test_reference_names_empty(self) -> None:
        r = GitLibraryResource(name="lib", repository="git@github.com:org/lib.git")
        assert r.reference_names() == []

    def test_plan_priority(self) -> None:
        assert GitLibraryResource.plan_priority == 10
