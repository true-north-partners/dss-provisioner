# Quick start

Get dss-provisioner running and deploy your first resource in under 5 minutes.

## 1. Install

```bash
pip install git+https://github.com/true-north-partners/dss-provisioner.git
```

Or for development:

```bash
git clone https://github.com/true-north-partners/dss-provisioner.git
cd dss-provisioner
uv sync
```

See [Installation](guides/installation.md) for more options.

## 2. Set up credentials

```bash
export DSS_HOST=https://dss.company.com
export DSS_API_KEY=your-api-key
```

!!! tip
    Store credentials in environment variables rather than YAML to avoid committing secrets to version control.

## 3. Create a config file

Create `dss-provisioner.yaml` in your project root:

```yaml
provider:
  project: MY_PROJECT

datasets:
  - name: raw_data
    type: filesystem
    connection: filesystem_managed
    path: "${projectKey}/raw"
```

## 4. Plan, apply, manage

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

# Validate configuration
dss-provisioner validate

# Tear down all managed resources
dss-provisioner destroy

# Save plan for later apply
dss-provisioner plan --out plan.json
dss-provisioner apply plan.json
```

## What's next?

- [Architecture](concepts/architecture.md) — understand the plan/apply engine
- [YAML configuration](guides/yaml-config.md) — full config reference
- [Writing modules](guides/modules.md) — create reusable resource generators in Python
- [Python API](guides/python-api.md) — use dss-provisioner as a library
