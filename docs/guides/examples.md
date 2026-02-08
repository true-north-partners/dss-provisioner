# End-to-End Examples

Use these canonical examples as your starting point. They are maintained to match current resource support.

## Choose an edition

- **DSS Free Edition**: [`examples/free`](https://github.com/true-north-partners/dss-provisioner/tree/main/examples/free)
- **DSS Enterprise**: [`examples/enterprise`](https://github.com/true-north-partners/dss-provisioner/tree/main/examples/enterprise)

## Focused patterns

- **Modules (`with` + `instances`)**: [`examples/modules`](https://github.com/true-north-partners/dss-provisioner/tree/main/examples/modules)
- **Python API workflow (`load -> plan -> apply`)**: [`examples/python_api`](https://github.com/true-north-partners/dss-provisioner/tree/main/examples/python_api)

## Coverage

These example sets collectively cover all supported resource categories:

- Variables and project code environment defaults
- Zones (Enterprise)
- Git libraries
- Managed folders (`filesystem`, `upload`)
- Datasets (`filesystem`, `upload`, `snowflake`, `oracle`)
- Exposed objects (`dataset`, `managed_folder`) (Enterprise)
- Foreign objects (`foreign_datasets`, `foreign_managed_folders`) (Enterprise)
- Recipes (`python`, `sql_query`, `sync`)
- Scenarios (`step_based`, `python`)
- Modules (`with`, `instances`)

## Suggested workflow

1. Copy one example directory into your repo.
2. Replace project key, connection names, and source project references.
3. Set credentials via `DSS_HOST` and `DSS_API_KEY`.
4. Run `dss-provisioner plan` and review the output.
5. Run `dss-provisioner apply` when the plan looks correct.
