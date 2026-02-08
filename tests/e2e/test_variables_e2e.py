"""E2e tests for project variables resource against a live DSS instance."""

from __future__ import annotations

import pytest

from dss_provisioner.config import apply, plan
from dss_provisioner.engine.types import Action
from dss_provisioner.resources.variables import VariablesResource
from tests.e2e.conftest import assert_changes

pytestmark = pytest.mark.integration


class TestVariables:
    def test_lifecycle(self, make_config):
        cfg = make_config(
            variables=VariablesResource(
                standard={"e2e_var": "hello"},
                local={"e2e_local": "world"},
            )
        )
        p = plan(cfg)
        assert_changes(p, {"variables": Action.CREATE})
        apply(p, cfg)

        # NOOP
        p2 = plan(cfg)
        assert_changes(p2, {"variables": Action.NOOP})

        # UPDATE
        cfg_updated = make_config(
            variables=VariablesResource(
                standard={"e2e_var": "updated"},
                local={"e2e_local": "updated_local"},
            )
        )
        p3 = plan(cfg_updated)
        assert_changes(p3, {"variables": Action.UPDATE})
        apply(p3, cfg_updated)

        # NOOP after update
        p4 = plan(cfg_updated)
        assert_changes(p4, {"variables": Action.NOOP})

        # DESTROY — restores to empty
        p5 = plan(cfg_updated, destroy=True)
        assert_changes(p5, {"variables": Action.DELETE})
        apply(p5, cfg_updated)
