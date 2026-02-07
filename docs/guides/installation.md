# Installation

## Requirements

- Python 3.11 or later
- A running Dataiku DSS instance with API access

## Install from source

dss-provisioner is not yet published to PyPI. Install directly from GitHub:

```bash
pip install git+https://github.com/true-north-partners/dss-provisioner.git
```

### With uv

```bash
uv pip install git+https://github.com/true-north-partners/dss-provisioner.git
```

## Development setup

```bash
git clone https://github.com/true-north-partners/dss-provisioner.git
cd dss-provisioner
uv sync
```

This installs the project in editable mode with all development dependencies.

### Useful commands

```bash
just test       # Run tests with coverage
just check      # Lint + format check + type check
just format     # Auto-format code
just build      # Build wheel and sdist
just build_docs # Build documentation
just serve_docs # Serve documentation locally
```

## Verify installation

```bash
$ dss-provisioner --version
dss-provisioner 0.1.0

$ dss-provisioner --help
```
