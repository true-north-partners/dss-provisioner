# Enterprise End-to-End Example

This example demonstrates Enterprise-capable patterns:

- Flow zones
- Cross-project sharing (`exposed_objects`)
- Cross-project consumption (`foreign_datasets`, `foreign_managed_folders`)
- SQL + filesystem + upload resources in one stack

## Files

- [`dss-provisioner.yaml`](dss-provisioner.yaml)
- [`dss-provisioner-sharing.yaml`](dss-provisioner-sharing.yaml)
- [`modules/enterprise.py`](modules/enterprise.py)
- [`recipes/enrich_customers.py`](recipes/enrich_customers.py)
- [`recipes/qa_checks.sql`](recipes/qa_checks.sql)
- [`scenarios/ops_watchdog.py`](scenarios/ops_watchdog.py)

## Run

```bash
export DSS_HOST=https://dss.company.com
export DSS_API_KEY=your-key

dss-provisioner plan --config ./examples/enterprise/dss-provisioner.yaml
dss-provisioner apply --config ./examples/enterprise/dss-provisioner.yaml

# Optional follow-up: configure cross-project exposure rules
dss-provisioner plan --config ./examples/enterprise/dss-provisioner-sharing.yaml
dss-provisioner apply --config ./examples/enterprise/dss-provisioner-sharing.yaml
```
