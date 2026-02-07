# CLI reference

dss-provisioner provides a Typer-based CLI with six commands.

## Global options

All commands accept:

| Option | Default | Description |
|---|---|---|
| `--config` | `dss-provisioner.yaml` | Path to configuration file |
| `--no-color` | `false` | Disable colored output |

## Commands

### `plan`

Show changes required by the current configuration.

```bash
dss-provisioner plan [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--out PATH` | — | Save plan to a JSON file for later `apply` |
| `--no-refresh` | `false` | Skip refreshing state from DSS before planning |

By default, `plan` refreshes state from the live DSS instance before computing the diff. Use `--no-refresh` to plan against the local state file only.

### `apply`

Apply the changes required by the current configuration.

```bash
dss-provisioner apply [PLAN_FILE] [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `PLAN_FILE` | — | Path to a saved plan (from `plan --out`) |
| `--auto-approve` | `false` | Skip confirmation prompt |
| `--no-refresh` | `false` | Skip refreshing state before planning |

If `PLAN_FILE` is provided, apply uses the saved plan (checking for staleness). Otherwise, it runs `plan` + `apply` in one step.

### `destroy`

Destroy all managed resources.

```bash
dss-provisioner destroy [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--auto-approve` | `false` | Skip confirmation prompt |

Plans deletion of all resources tracked in the state file, then applies in reverse dependency order.

### `refresh`

Refresh state from the live DSS instance.

```bash
dss-provisioner refresh [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--auto-approve` | `false` | Skip confirmation prompt |

Reads the current state of each tracked resource from DSS and updates the local state file. Useful after manual changes in the DSS UI.

### `drift`

Show drift between state and the live DSS instance.

```bash
dss-provisioner drift [OPTIONS]
```

Read-only command that compares the state file against live DSS. Does not modify state. Shows which resources have drifted and what changed.

### `validate`

Validate the configuration file.

```bash
dss-provisioner validate [OPTIONS]
```

Parses and validates the YAML configuration without connecting to DSS. Useful in CI pipelines or pre-commit hooks.
