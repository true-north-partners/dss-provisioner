# Free Edition End-to-End Example

This example is safe for DSS Free Edition:

- No flow zones
- No exposed/foreign objects
- Uses filesystem/upload resources that are available in Free Edition

## Files

- [`dss-provisioner.yaml`](dss-provisioner.yaml)
- [`modules/pipelines.py`](modules/pipelines.py)
- [`recipes/clean_customers.py`](recipes/clean_customers.py)
- [`recipes/customers_quality.py`](recipes/customers_quality.py)
- [`scenarios/smoke_tests.py`](scenarios/smoke_tests.py)

## Run

```bash
export DSS_HOST=http://localhost:11200
export DSS_API_KEY=your-key

dss-provisioner plan --config ./examples/free/dss-provisioner.yaml
dss-provisioner apply --config ./examples/free/dss-provisioner.yaml
```
