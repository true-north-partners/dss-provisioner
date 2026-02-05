from __future__ import annotations

from datetime import UTC, datetime, timedelta

from dss_provisioner.core.state import (
    ResourceInstance,
    State,
    compute_attributes_hash,
    compute_state_digest,
)


def test_state_digest_excludes_timestamps() -> None:
    t0 = datetime(2020, 1, 1, tzinfo=UTC)
    t1 = t0 + timedelta(days=1)

    inst = ResourceInstance(
        address="dummy.r1",
        resource_type="dummy",
        name="r1",
        attributes={"id": "r1", "value": 1},
        attributes_hash=compute_attributes_hash({"id": "r1", "value": 1}),
        dependencies=["dummy.dep"],
        created_at=t0,
        updated_at=t0,
    )
    state = State(project_key="PRJ", resources={"dummy.r1": inst})
    d0 = compute_state_digest(state)

    # Changing timestamps should not affect the digest.
    state.resources["dummy.r1"].created_at = t1
    state.resources["dummy.r1"].updated_at = t1
    d1 = compute_state_digest(state)

    assert d0 == d1


def test_state_digest_includes_serial_and_lineage() -> None:
    state = State(project_key="PRJ")
    d0 = compute_state_digest(state)

    state.serial += 1
    assert compute_state_digest(state) != d0

    # Reset serial; lineage change should still alter digest.
    state.serial = 0
    state.lineage = "different"
    assert compute_state_digest(state) != d0
