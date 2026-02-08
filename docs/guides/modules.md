# Writing modules

Modules let you define reusable resource generators as Python functions. Instead of repeating similar YAML blocks, you write a function that returns `list[Resource]` and call it from your config.

## Your first module

Create a file next to your config:

```
my-project/
├── dss-provisioner.yaml
└── modules/
    └── pipelines.py
```

```python title="modules/pipelines.py"
from dss_provisioner.resources.base import Resource
from dss_provisioner.resources.dataset import FilesystemDatasetResource


def filesystem_pipeline(*, name: str, table: str, path_prefix: str = "/data") -> list[Resource]:
    """Create a raw dataset for a given table."""
    return [
        FilesystemDatasetResource(
            name=f"{name}_raw",
            connection="filesystem_managed",
            path=f"{path_prefix}/{table.lower()}",
            description=f"Raw {table} data",
        ),
    ]
```

Then reference it from your config:

```yaml title="dss-provisioner.yaml"
provider:
  project: MY_PROJECT

modules:
  - call: modules.pipelines:filesystem_pipeline
    instances:
      customers:
        table: CUSTOMERS
      orders:
        table: ORDERS
        path_prefix: /staging
```

This expands to 2 datasets: `customers_raw` and `orders_raw`.

## Module function signature

A module function is any Python callable that returns `list[Resource]`. It receives keyword arguments from the config.

With **`instances`**, each key becomes the `name=` kwarg, and values are passed as extra kwargs:

```yaml
modules:
  - call: modules.pipelines:filesystem_pipeline
    instances:
      customers:          # name="customers"
        table: CUSTOMERS  # table="CUSTOMERS"
      orders:             # name="orders"
        table: ORDERS
```

With **`with`**, all values are passed directly as kwargs (no automatic `name=`):

```yaml
modules:
  - call: modules.pipelines:filesystem_pipeline
    with:
      name: customers
      table: CUSTOMERS
```

Exactly one of `instances` or `with` must be provided.

## Returning multiple resources

A single function call can return any number of resources. This is where modules become powerful — one instance can generate an entire pipeline:

```python title="modules/pipelines.py"
from dss_provisioner.resources.base import Resource
from dss_provisioner.resources.dataset import (
    FilesystemDatasetResource,
    UploadDatasetResource,
)
from dss_provisioner.resources.recipe import PythonRecipeResource


def full_pipeline(
    *, name: str, table: str, code_file: str = ""
) -> list[Resource]:
    """Create a raw dataset, staging area, and cleaning recipe."""
    return [
        FilesystemDatasetResource(
            name=f"{name}_raw",
            connection="filesystem_managed",
            path=f"/data/{table.lower()}",
        ),
        UploadDatasetResource(
            name=f"{name}_clean",
        ),
        PythonRecipeResource(
            name=f"clean_{name}",
            inputs=f"{name}_raw",
            outputs=f"{name}_clean",
            code=code_file or f"# Clean {table}",
        ),
    ]
```

```yaml
modules:
  - call: modules.pipelines:full_pipeline
    instances:
      customers:
        table: CUSTOMERS
        code_file: "import dataiku\n# ..."
      orders:
        table: ORDERS
```

This generates 6 resources (3 per instance) from 8 lines of YAML.

## Callable resolution

The `call` string is resolved in three ways:

### 1. Local file (most common)

Use `module.path:function` syntax. The path is relative to your config file directory:

```yaml
# Loads ./modules/pipelines.py and calls filesystem_pipeline
- call: modules.pipelines:filesystem_pipeline
```

### 2. Installed package

If the module is installed in your Python environment (e.g., via `pip install`), the same `module.path:function` syntax resolves it via `importlib.import_module`:

```yaml
# Imports dss_modules_company.snowflake and calls snowflake_pipeline
- call: dss_modules_company.snowflake:snowflake_pipeline
```

Installed packages are tried first. If not found, the resolver falls back to a local file.

### 3. Entry point

Use a short name (no `:`) to look up a registered entry point:

```yaml
# Resolved via dss_provisioner.modules entry point group
- call: snowflake_pipeline
```

Package authors register entry points in their `pyproject.toml`:

```toml
[project.entry-points."dss_provisioner.modules"]
snowflake_pipeline = "dss_modules_company.snowflake:snowflake_pipeline"
```

## Error handling

Module errors are caught at config load time and reported as `ConfigError`:

- **Import errors** — module file not found, or broken internal imports
- **Missing function** — module exists but the function name doesn't
- **Not callable** — the attribute exists but isn't a function
- **Bad return type** — function must return `list[Resource]`
- **Function exception** — any exception raised by the function is wrapped with context

## Tips

- Keep module functions **pure** — they should only construct `Resource` objects, not call DSS APIs
- Use `**kwargs` to accept extra parameters gracefully if you plan to add fields later
- Module resources support all standard fields (`depends_on`, `tags`, `description`, etc.)
- Run `dss-provisioner validate` to check module expansion without connecting to DSS
