# dss-provisioner

**Terraform-style resource-as-code for Dataiku DSS**

---

Define Dataiku DSS resources as YAML, then plan and apply changes with a familiar CLI workflow.

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
- **Composability** — Write Python modules that generate resources from parameters

## Next steps

- **[Quick start](quickstart.md)** — Get up and running in 5 minutes
- **[End-to-end examples](guides/examples.md)** — Choose canonical Free/Enterprise and Python API examples
- **[Architecture](concepts/architecture.md)** — Understand the plan/apply engine and state model
- **[YAML configuration](guides/yaml-config.md)** — Full config reference for all resource types
- **[Writing modules](guides/modules.md)** — Create reusable resource generators in Python
- **[Python API](guides/python-api.md)** — Use dss-provisioner as a library
- **[CLI reference](reference/cli.md)** — All commands and options
- **[API reference](reference/api.md)** — Auto-generated from source docstrings
