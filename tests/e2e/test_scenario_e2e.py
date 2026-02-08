"""E2e tests for scenario resources against a live DSS instance."""

from __future__ import annotations

from uuid import uuid4

import pytest

from dss_provisioner.config import apply, plan
from dss_provisioner.engine.types import Action
from dss_provisioner.resources.scenario import PythonScenarioResource, StepBasedScenarioResource
from tests.e2e.conftest import assert_changes

pytestmark = pytest.mark.integration


class TestStepBasedScenario:
    def test_lifecycle(self, make_config, cleanup_scenarios):
        name = f"e2e_step_{uuid4().hex[:8]}"
        cleanup_scenarios.append(name)

        cfg = make_config(scenarios=[StepBasedScenarioResource(name=name)])
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


class TestPythonScenario:
    def test_lifecycle(self, make_config, cleanup_scenarios):
        name = f"e2e_pysc_{uuid4().hex[:8]}"
        cleanup_scenarios.append(name)

        cfg = make_config(
            scenarios=[
                PythonScenarioResource(
                    name=name,
                    code="# e2e scenario\nprint('hello')\n",
                )
            ]
        )
        p = plan(cfg)
        assert_changes(p, {name: Action.CREATE})
        apply(p, cfg)

        # NOOP
        p2 = plan(cfg)
        assert_changes(p2, {name: Action.NOOP})

        # UPDATE code
        cfg_updated = make_config(
            scenarios=[
                PythonScenarioResource(
                    name=name,
                    code="# updated e2e scenario\nprint('updated')\n",
                )
            ]
        )
        p3 = plan(cfg_updated)
        assert_changes(p3, {name: Action.UPDATE})
        apply(p3, cfg_updated)

        # NOOP after update
        p4 = plan(cfg_updated)
        assert_changes(p4, {name: Action.NOOP})

        # DESTROY
        p5 = plan(cfg_updated, destroy=True)
        assert_changes(p5, {name: Action.DELETE})
        apply(p5, cfg_updated)
