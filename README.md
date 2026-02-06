# dss-provisioner

> Terraform-style resource-as-code for Dataiku DSS

**Status: In Development** — Core engine and CLI are functional. Not yet published to PyPI.

---

## What is dss-provisioner?

dss-provisioner lets you define Dataiku DSS resources as YAML configuration files, then plan and apply changes with a familiar CLI workflow.

Instead of clicking through the DSS UI or writing imperative API scripts, you declare what your project should look like:

```yaml
# dss-provisioner.yaml

provider:
  host: https://dss.company.com
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

```
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

```bash
pip install dss-provisioner  # not yet published to PyPI
```

For development:

```bash
git clone https://github.com/true-north-partners/dss-provisioner.git
cd dss-provisioner
uv sync --all-extras
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
# Preview changes
dss-provisioner plan

# Apply changes
dss-provisioner apply

# Apply without confirmation prompt
dss-provisioner apply --auto-approve

# Detect drift from manual DSS changes
dss-provisioner drift

# Refresh state from live DSS
dss-provisioner refresh

# Validate config without connecting
dss-provisioner validate

# Tear down all managed resources
dss-provisioner destroy

# Save plan for later apply
dss-provisioner plan --out plan.json
dss-provisioner apply plan.json
```

## Configuration reference

### Provider

```yaml
provider:
  host: https://dss.company.com  # or DSS_HOST env var
  api_key: secret                # or DSS_API_KEY env var (recommended)
  project: MY_PROJECT            # or DSS_PROJECT env var
```

### Datasets

| Type | YAML `type` | Required fields |
|------|-------------|-----------------|
| Snowflake | `snowflake` | `connection`, `schema_name`, `table` |
| Oracle | `oracle` | `connection`, `schema_name`, `table` |
| Filesystem | `filesystem` | `connection`, `path` |
| Upload | `upload` | — |

Common optional fields: `managed`, `format_type`, `format_params`, `columns`, `zone`, `description`, `tags`, `depends_on`.

### Recipes

| Type | YAML `type` | Required fields |
|------|-------------|-----------------|
| Python | `python` | `inputs`, `outputs`, `code` or `code_file` |
| SQL Query | `sql_query` | `inputs`, `outputs`, `code` or `code_file` |
| Sync | `sync` | `inputs`, `outputs` |

### State

State is stored in `.dss-state.json` (configurable via `state_path`). This file tracks what has been deployed and should be committed to version control for team coordination.

## Engine semantics

- One state file manages **one DSS project**. Plan/apply will error if the state belongs to a different project.
- `plan` performs a **refresh by default** — reads live DSS state and may persist updates.
- `apply` executes changes in dependency order with **no rollback**. If apply fails, state reflects what was completed.
- Saved plans are checked for staleness via lineage, serial, and state digest.
- DSS `${…}` variables (e.g. `${projectKey}`) are resolved transparently during plan comparison.

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

## Roadmap

- [ ] Scenarios and triggers
- [ ] Managed folders
- [ ] Import existing DSS projects to YAML
- [ ] Documentation site (MkDocs)

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
