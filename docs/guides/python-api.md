# Python API

dss-provisioner can be used as a Python library for programmatic access. All public functions are available from the `dss_provisioner.config` module.

## Canonical project example

See [`examples/python_api`](https://github.com/true-north-partners/dss-provisioner/tree/main/examples/python_api)
for a complete layout with config, module code, recipe files, and an executable `app.py`
that performs `load -> plan -> apply`.

## Basic usage

```python
from dss_provisioner.config import load, plan, apply

# Load configuration
config = load("dss-provisioner.yaml")

# Plan changes
p = plan(config)
print(f"Changes: {p.summary()}")

# Apply changes
result = apply(p, config)
print(f"Applied: {result.summary()}")
```

## One-step plan and apply

```python
from dss_provisioner.config import load, plan_and_apply

config = load("dss-provisioner.yaml")
result = plan_and_apply(config)
```

## Drift detection

```python
from dss_provisioner.config import load, drift

config = load("dss-provisioner.yaml")
changes = drift(config)

for change in changes:
    print(f"{change.action.value}: {change.address}")
    if change.diff:
        for key, value in change.diff.items():
            print(f"  {key}: {value}")
```

## Refresh state

```python
from dss_provisioner.config import load, refresh, save_state

config = load("dss-provisioner.yaml")
changes, new_state = refresh(config)

if changes:
    print(f"Found {len(changes)} drifted resources")
    save_state(config, new_state)  # persist to disk
```

## Progress callbacks

Track apply progress with a callback:

```python
from dss_provisioner.config import load, plan, apply

config = load("dss-provisioner.yaml")
p = plan(config)

def on_progress(change, event):
    if event == "start":
        print(f"  {change.address}: applying...")
    else:
        print(f"  {change.address}: done")

result = apply(p, config, progress=on_progress)
```

## Error handling

```python
from dss_provisioner.config import load, plan, apply, ConfigError
from dss_provisioner.engine.errors import (
    ApplyError,
    StalePlanError,
    StateProjectMismatchError,
)

try:
    config = load("dss-provisioner.yaml")
except ConfigError as e:
    print(f"Invalid configuration: {e}")
    raise

p = plan(config)

try:
    result = apply(p, config)
except StalePlanError:
    print("State changed since plan was created — re-plan required")
except ApplyError as e:
    print(f"Apply failed at {e.address}")
    print(f"Successfully applied: {e.result.summary()}")
```

## API reference

See the full [API reference](../reference/api.md) for all classes, functions, and types.
