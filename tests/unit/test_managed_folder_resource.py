"""Tests for managed folder resource models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dss_provisioner.resources.managed_folder import (
    FilesystemManagedFolderResource,
    ManagedFolderResource,
    UploadManagedFolderResource,
)


class TestManagedFolderResource:
    def test_address(self) -> None:
        mf = ManagedFolderResource(name="my_folder", type="Filesystem")
        assert mf.address == "dss_managed_folder.my_folder"

    def test_defaults(self) -> None:
        mf = ManagedFolderResource(name="my_folder", type="Filesystem")
        assert mf.connection is None
        assert mf.zone is None
        assert mf.description == ""
        assert mf.tags == []
        assert mf.depends_on == []

    def test_reference_names_without_zone(self) -> None:
        mf = ManagedFolderResource(name="my_folder", type="Filesystem")
        assert mf.reference_names() == []

    def test_reference_names_with_zone(self) -> None:
        mf = ManagedFolderResource(name="my_folder", type="Filesystem", zone="raw")
        assert mf.reference_names() == ["raw"]

    def test_extra_forbid(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            ManagedFolderResource(name="my_folder", type="Filesystem", unknown_field="x")  # type: ignore[call-arg]

    def test_to_dss_params_with_connection(self) -> None:
        mf = ManagedFolderResource(name="my_folder", type="Filesystem", connection="local")
        assert mf.to_dss_params() == {"connection": "local"}

    def test_to_dss_params_without_connection(self) -> None:
        mf = ManagedFolderResource(name="my_folder", type="Filesystem")
        assert mf.to_dss_params() == {}

    def test_name_validation(self) -> None:
        with pytest.raises(ValidationError):
            ManagedFolderResource(name="bad name!", type="Filesystem")


class TestFilesystemManagedFolderResource:
    def test_address(self) -> None:
        mf = FilesystemManagedFolderResource(
            name="trained_models", connection="filesystem_managed", path="/data/models"
        )
        assert mf.address == "dss_filesystem_managed_folder.trained_models"

    def test_defaults(self) -> None:
        mf = FilesystemManagedFolderResource(
            name="trained_models", connection="filesystem_managed", path="/data/models"
        )
        assert mf.type == "Filesystem"

    def test_required_fields(self) -> None:
        with pytest.raises(ValidationError):
            FilesystemManagedFolderResource(name="my_folder")  # type: ignore[call-arg]

    def test_type_locked(self) -> None:
        with pytest.raises(ValidationError):
            FilesystemManagedFolderResource(
                name="my_folder",
                connection="conn",
                path="/data",
                type="Oracle",  # type: ignore[arg-type]
            )

    def test_extra_forbid(self) -> None:
        with pytest.raises(ValidationError, match="extra"):
            FilesystemManagedFolderResource(
                name="my_folder",
                connection="conn",
                path="/data",
                unknown_field="x",  # type: ignore[call-arg]
            )

    def test_to_dss_params(self) -> None:
        mf = FilesystemManagedFolderResource(
            name="my_folder", connection="filesystem_managed", path="/data/models"
        )
        assert mf.to_dss_params() == {
            "connection": "filesystem_managed",
            "path": "/data/models",
        }

    def test_model_dump_shape(self) -> None:
        mf = FilesystemManagedFolderResource(
            name="my_folder", connection="filesystem_managed", path="/data/models"
        )
        dump = mf.model_dump(exclude_none=True, exclude={"address"})
        assert dump["type"] == "Filesystem"
        assert dump["connection"] == "filesystem_managed"
        assert dump["path"] == "/data/models"

    def test_path_min_length(self) -> None:
        with pytest.raises(ValidationError):
            FilesystemManagedFolderResource(name="my_folder", connection="conn", path="")


class TestUploadManagedFolderResource:
    def test_address(self) -> None:
        mf = UploadManagedFolderResource(name="reports")
        assert mf.address == "dss_upload_managed_folder.reports"

    def test_defaults(self) -> None:
        mf = UploadManagedFolderResource(name="reports")
        assert mf.type == "UploadedFiles"

    def test_type_locked(self) -> None:
        with pytest.raises(ValidationError):
            UploadManagedFolderResource(name="reports", type="Oracle")  # type: ignore[arg-type]

    def test_model_dump_shape(self) -> None:
        mf = UploadManagedFolderResource(name="reports")
        dump = mf.model_dump(exclude_none=True, exclude={"address"})
        assert dump["type"] == "UploadedFiles"
