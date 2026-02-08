# CLI reference

dss-provisioner provides a Typer-based CLI with seven commands.

## Global options

All commands accept:

| Option | Default | Description |
|---|---|---|
| `--config` | `dss-provisioner.yaml` | Path to configuration file |
| `--no-color` | `false` | Disable colored output |
| `-v` / `-vv` | — | Increase log verbosity (`-v` info, `-vv` debug) |

Verbosity flags are top-level options and must appear **before** the command name:

```bash
dss-provisioner -v plan          # INFO-level logs on stderr
dss-provisioner -vv apply        # DEBUG-level logs on stderr
```

The `DSS_LOG` environment variable overrides `-v` flags and accepts any Python logging level name (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`):

```bash
DSS_LOG=DEBUG dss-provisioner plan   # same as -vv
```

Logs are scoped to `dss_provisioner.*` loggers and written to stderr so they do not interfere with command output on stdout.

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

### `preview`

Create, list, or destroy branch-based preview projects.

```bash
dss-provisioner preview [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--branch TEXT` | current git branch | Override branch used for preview key and `repository: self` library checkout |
| `--destroy` | `false` | Delete the computed preview project and preview state files |
| `--list` | `false` | List active preview projects for the configured base project |
| `--force` | `false` | Allow reuse/delete of an existing project key even when it is not tagged as a provisioner-managed preview |
| `--no-refresh` | `false` | Skip refreshing preview state before planning |

`preview` derives a project key from `provider.project` + branch (for example `ANALYTICS__FEATURE_X`), rewrites `repository: self` libraries to the local `origin` URL at that branch, applies the config to the preview project, and uses an isolated state file (for example `.dss-state.preview.feature_x.json`). By default, reuse/delete operations only proceed for projects tagged as provisioner-managed previews; `--force` overrides that guard.

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
