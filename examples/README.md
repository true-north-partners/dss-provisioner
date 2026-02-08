# Examples

Canonical, copy/paste-ready examples for `dss-provisioner`.

## Example sets

- [`free/`](free/) - End-to-end project for DSS Free Edition (no zones, no enterprise-only sharing APIs).
- [`enterprise/`](enterprise/) - End-to-end project for DSS Enterprise (base stack + sharing follow-up config).
- [`modules/`](modules/) - Focused module patterns (`with` single invocation + `instances` multi-invocation).
- [`python_api/`](python_api/) - Programmatic `load -> plan -> apply` workflow with a practical repo layout.

## Coverage map

These examples collectively cover all currently supported resource categories:

- `variables`
- `code_envs`
- `zones` (enterprise)
- `libraries`
- `managed_folders` (`filesystem`, `upload`)
- `datasets` (`filesystem`, `upload`, `snowflake`, `oracle`)
- `exposed_objects` (`dataset`, `managed_folder`) (enterprise)
- `foreign_datasets` and `foreign_managed_folders` (enterprise)
- `recipes` (`python`, `sql_query`, `sync`)
- `scenarios` (`step_based`, `python`)
- `modules` (`with`, `instances`)

## Notes

- Resource names and project keys are placeholders; replace with your DSS naming conventions.
- Keep `DSS_API_KEY` in environment variables instead of YAML.
- Free Edition users should start with `free/` and skip Enterprise-only resources.
