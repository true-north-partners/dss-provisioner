# dss-provisioner

> DSS-as-Code for Dataiku DSS

**Status: In Development** — Not yet ready for production use.

---

## What is dss-provisioner?

dss-provisioner lets you define Dataiku DSS assets as YAML configuration files, then plan and apply changes with a familiar CLI workflow.

Instead of clicking through the DSS UI or writing imperative API scripts, you declare what your flow should look like:

```yaml
# dss-provisioner.yaml

provider:
  host: https://dss.company.com
  project: ANALYTICS

datasets:
  - name: customers_raw
    connection: snowflake_prod
    table: RAW.CUSTOMERS

  - name: customers_clean
    connection: snowflake_prod
    table: CLEAN.CUSTOMERS
    managed: true

recipes:
  - name: clean_customers
    type: python
    input: customers_raw
    output: customers_clean
    code_file: ./recipes/clean_customers.py

scenarios:
  - name: daily_refresh
    triggers:
      - type: temporal
        frequency: daily
        hour: 6
    steps:
      - build: customers_clean
```

Then preview and apply changes:

```bash
$ dss-provisioner plan
+ dataset "customers_raw"
+ dataset "customers_clean"  
+ recipe "clean_customers"
+ scenario "daily_refresh"

Plan: 4 to add, 0 to change, 0 to destroy.

$ dss-provisioner apply
Applying changes...
✓ Created dataset "customers_raw"
✓ Created dataset "customers_clean"
✓ Created recipe "clean_customers"
✓ Created scenario "daily_refresh"

Apply complete! 4 added, 0 changed, 0 destroyed.
```

## Why?

- **Version control** — Track pipeline changes in git, review in PRs
- **Reproducibility** — Spin up identical DSS assets across dev/staging/prod
- **Automation** — Deploy DSS changes in CI/CD without UI clicks
- **Visibility** — See exactly what will change before applying

## Planned Features

- [ ] Core resources: datasets, recipes, scenarios, managed folders
- [ ] State management with drift detection
- [ ] Provider plugin system for extensibility
- [ ] DAG visualization
- [ ] Import existing DSS projects to YAML

## Extensions

dss-provisioner supports extensions for domain-specific transforms. Extensions add new resource types and compilation logic.

```yaml
# Example: a hypothetical validation extension
extensions:
  - dss_provisioner_validate

transforms:
  - type: schema_check
    input: raw_data
    rules:
      - column: email
        pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
      - column: age
        range: [0, 120]
```

## Installation

```bash
pip install dss-provisioner  # not yet published
```

## Documentation

Coming soon.

## Contributing

This project is in early development. If you're interested in contributing, please open an issue to discuss.

## License

Apache 2.0

---

*This project is not affiliated with or endorsed by Dataiku. "Dataiku" and "DSS" are trademarks of Dataiku SAS.*
