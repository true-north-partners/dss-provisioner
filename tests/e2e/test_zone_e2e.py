"""E2e tests for zone resources against a live DSS instance (enterprise only)."""

from __future__ import annotations

from uuid import uuid4

import pytest

from dss_provisioner.config import apply, plan
from dss_provisioner.engine.types import Action
from dss_provisioner.resources.zone import ZoneResource
from tests.e2e.conftest import assert_changes

pytestmark = [pytest.mark.integration, pytest.mark.enterprise]


class TestZone:
    def test_lifecycle(self, make_config, cleanup_zones):
        name = f"e2e_zone_{uuid4().hex[:8]}"
        cleanup_zones.append(name)

        cfg = make_config(zones=[ZoneResource(name=name, color="#2ab1ac")])
        p = plan(cfg)
        assert_changes(p, {name: Action.CREATE})
        apply(p, cfg)

        # NOOP
        p2 = plan(cfg)
        assert_changes(p2, {name: Action.NOOP})

        # DESTROY
        p3 = plan(cfg, destroy=True)
        assert_changes(p3, {name: Action.DELETE})
        apply(p3, cfg)

    def test_dataset_zone_assignment(self, make_config, cleanup_zones, cleanup_datasets):
        suffix = uuid4().hex[:8]
        zone_name = f"e2e_zds_{suffix}"
        ds_name = f"e2e_zds_ds_{suffix}"

        cleanup_datasets.append(ds_name)
        cleanup_zones.append(zone_name)

        from dss_provisioner.resources.dataset import FilesystemDatasetResource

        cfg = make_config(
            zones=[ZoneResource(name=zone_name)],
            datasets=[
                FilesystemDatasetResource(
                    name=ds_name,
                    connection="filesystem_managed",
                    path=f"/tmp/{ds_name}",
                    zone=zone_name,
                )
            ],
        )
        p = plan(cfg)
        assert_changes(p, {zone_name: Action.CREATE, ds_name: Action.CREATE})
        apply(p, cfg)

        # NOOP — zone assignment should roundtrip cleanly
        p2 = plan(cfg)
        assert_changes(p2, {zone_name: Action.NOOP, ds_name: Action.NOOP})

        # DESTROY
        p3 = plan(cfg, destroy=True)
        apply(p3, cfg)
