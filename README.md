# dss-provisioner

> Terraform-style resource-as-code for Dataiku DSS

[![Docs](https://img.shields.io/badge/docs-dss--provisioner.pages.dev-blue)](https://dss-provisioner.pages.dev)

Define Dataiku DSS resources as YAML (or Python), preview changes, and apply them with stateful plan/apply semantics.

## Why

- **Version control**: track DSS changes in git and review in PRs
- **Reproducibility**: provision consistent projects across dev/staging/prod
- **Change safety**: inspect a plan before applying
- **Automation**: integrate with CI/CD workflows
- **Composability**: generate resource stacks with Python modules

## Canonical examples

| Use case | Path | Notes |
|---|---|---|
| DSS Free end-to-end | [`examples/free/`](examples/free/) | No Enterprise-only APIs |
| DSS Enterprise end-to-end | [`examples/enterprise/`](examples/enterprise/) | Zones + cross-project sharing |
| Module invocation patterns | [`examples/modules/`](examples/modules/) | `with` and `instances` patterns |
| Python API workflow | [`examples/python_api/`](examples/python_api/) | Programmatic `load -> plan -> apply` |
| Coverage map for all examples | [`examples/README.md`](examples/README.md) | Resource-type coverage index |

## Quick start

```bash
export DSS_HOST=https://dss.company.com
export DSS_API_KEY=your-api-key

# Start with the Free or Enterprise example config

dss-provisioner plan --config ./examples/free/dss-provisioner.yaml
dss-provisioner apply --config ./examples/free/dss-provisioner.yaml
```

## Supported resource categories

- Variables
- Project code environment defaults
- Zones (Enterprise)
- Git libraries
- Managed folders (`filesystem`, `upload`)
- Datasets (`filesystem`, `upload`, `snowflake`, `oracle`)
- Exposed objects (`dataset`, `managed_folder`) (Enterprise)
- Foreign objects (`foreign_datasets`, `foreign_managed_folders`) (Enterprise)
- Recipes (`python`, `sql_query`, `sync`)
- Scenarios (`step_based`, `python`)
- Modules (`with`, `instances`)

## Installation

Not yet published to PyPI. Install from source:

```bash
pip install git+https://github.com/true-north-partners/dss-provisioner.git
```

For development:

```bash
git clone https://github.com/true-north-partners/dss-provisioner.git
cd dss-provisioner
uv sync
```

## Documentation

- Docs site: [dss-provisioner.pages.dev](https://dss-provisioner.pages.dev)
- Example guide: [End-to-end examples](https://dss-provisioner.pages.dev/guides/examples/)
- YAML reference: [docs/guides/yaml-config.md](docs/guides/yaml-config.md)
- Python API guide: [docs/guides/python-api.md](docs/guides/python-api.md)
- CLI reference: [docs/reference/cli.md](docs/reference/cli.md)

## Development

```bash
just format
just test
just check
```

## License

Apache 2.0

---

*This project is not affiliated with or endorsed by Dataiku. "Dataiku" and "DSS" are trademarks of Dataiku SAS.*
