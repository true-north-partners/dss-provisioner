# dss-provisioner

> Terraform-style resource-as-code for Dataiku DSS

[![Docs](https://img.shields.io/badge/docs-dss--provisioner.pages.dev-blue)](https://dss-provisioner.pages.dev)

**Status: In Development** — Core engine and CLI are functional. Not yet published to PyPI.

---

## What is dss-provisioner?

dss-provisioner lets you define Dataiku DSS resources as YAML configuration files, then plan and apply changes with a familiar CLI workflow.

Instead of clicking through the DSS UI or writing imperative API scripts, you declare what your project should look like:

```yaml
# dss-provisioner.yaml

provider:
  host: https://dss.company.com  # or DSS_HOST env var
  # api_key: set DSS_API_KEY env var (recommended over YAML)
  project: ANALYTICS

datasets:
  - name: customers_raw
    type: snowflake
    connection: snowflake_prod
    schema_name: RAW
    table: CUSTOMERS

  - name: customers_clean
    type: filesystem
    connection: filesystem_managed
    path: "${projectKey}/clean/customers"
    managed: true
    format_type: parquet

recipes:
  - name: clean_customers
    type: python
    inputs: customers_raw
    outputs: customers_clean
    code_file: ./recipes/clean_customers.py
```

Then preview and apply changes:

```bash
$ dss-provisioner plan

  # dss_snowflake_dataset.customers_raw will be created
  + resource "dss_snowflake_dataset" "customers_raw" {
      + name        = "customers_raw"
      + connection  = "snowflake_prod"
      + schema_name = "RAW"
      + table       = "CUSTOMERS"
    }

  # dss_filesystem_dataset.customers_clean will be created
  + resource "dss_filesystem_dataset" "customers_clean" { ... }

  # dss_python_recipe.clean_customers will be created
  + resource "dss_python_recipe" "clean_customers" { ... }

Plan: 3 to add, 0 to change, 0 to destroy.

$ dss-provisioner apply --auto-approve

  dss_snowflake_dataset.customers_raw: Creation complete
  dss_filesystem_dataset.customers_clean: Creation complete
  dss_python_recipe.clean_customers: Creation complete

Apply complete! Resources: 3 added, 0 changed, 0 destroyed.
```

## Why?

- **Version control** — Track pipeline changes in git, review in PRs
- **Reproducibility** — Spin up identical DSS resources across dev/staging/prod
- **Automation** — Deploy DSS changes in CI/CD without UI clicks
- **Visibility** — See exactly what will change before applying

## Installation

Not yet published to PyPI. For now, install from source:

```bash
pip install git+https://github.com/true-north-partners/dss-provisioner.git
```

For development:

```bash
git clone https://github.com/true-north-partners/dss-provisioner.git
cd dss-provisioner
uv sync
```

## Quick start

### 1. Set up credentials

```bash
export DSS_HOST=https://dss.company.com
export DSS_API_KEY=your-api-key
```

### 2. Create a config file

```yaml
# dss-provisioner.yaml
provider:
  project: MY_PROJECT

datasets:
  - name: raw_data
    type: filesystem
    connection: filesystem_managed
    path: "${projectKey}/raw"
```

### 3. Plan, apply, manage

```bash
dss-provisioner plan
dss-provisioner apply
dss-provisioner drift
dss-provisioner refresh
dss-provisioner validate
dss-provisioner destroy
```

## Documentation

Full documentation is available at **[dss-provisioner.pages.dev](https://dss-provisioner.pages.dev)**:

- [Configuration reference](https://dss-provisioner.pages.dev/guides/yaml-config/)
- [Python API](https://dss-provisioner.pages.dev/guides/python-api/)
- [CLI reference](https://dss-provisioner.pages.dev/reference/cli/)
- [Architecture](https://dss-provisioner.pages.dev/concepts/architecture/)

## Implemented

- [x] Plan/apply engine with state management and dependency graph
- [x] Handler registry with topological ordering
- [x] Dataset resources: Snowflake, Oracle, Filesystem, Upload
- [x] Recipe resources: Python, SQL Query, Sync (with `code_file` support)
- [x] YAML configuration with Pydantic validation
- [x] Python API (`dss_provisioner.config.plan()`, `apply()`, etc.)
- [x] CLI: plan, apply, destroy, refresh, drift, validate
- [x] DSS variable substitution (`${projectKey}`, custom variables)
- [x] Drift detection and state refresh
- [x] GitHub Actions CI
- [x] Documentation site (MkDocs)

## Roadmap

- [ ] Scenarios and triggers
- [ ] Managed folders
- [ ] Import existing DSS projects to YAML

## Development

```bash
just test       # Run tests with coverage
just check      # Lint + type check
just format     # Auto-format
```

## License

Apache 2.0

---

*This project is not affiliated with or endorsed by Dataiku. "Dataiku" and "DSS" are trademarks of Dataiku SAS.*
