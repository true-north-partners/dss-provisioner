# Python API Example

Practical project layout for using `dss-provisioner` as a Python library.

## Layout

```text
python_api/
├── app.py
├── dss-provisioner.yaml
├── modules/
│   └── pipelines.py
└── recipes/
    └── prepare_features.py
```

## Usage

```bash
export DSS_HOST=http://localhost:11200
export DSS_API_KEY=your-key

python examples/python_api/app.py --config examples/python_api/dss-provisioner.yaml
python examples/python_api/app.py --config examples/python_api/dss-provisioner.yaml --apply
```

The script prints a plan summary and optionally applies the plan with progress output.
