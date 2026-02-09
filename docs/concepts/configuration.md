# Configuration

dss-provisioner uses a single YAML file (`dss-provisioner.yaml` by default) validated by Pydantic at load time.

## Config structure

```yaml
provider:
  host: https://dss.company.com     # or DSS_HOST env var
  api_key: secret                   # or DSS_API_KEY env var (recommended)
  project: MY_PROJECT               # or DSS_PROJECT env var

state_path: .dss-state.json         # default

zones:
  - name: ...
    color: "#..."                   # optional hex color

datasets:
  - name: ...
    type: ...
    # type-specific fields

exposed_objects:
  - name: ...
    type: dataset|managed_folder
    target_projects: [...]

foreign_datasets:
  - name: ...
    source_project: ...
    source_name: ...

foreign_managed_folders:
  - name: ...
    source_project: ...
    source_name: ...

recipes:
  - name: ...
    type: ...
    # type-specific fields
```

## Provider settings

The `provider` block configures the DSS connection. All fields support environment variable fallbacks with the `DSS_` prefix:

| Field | Env var | Required | Default | Description |
|---|---|---|---|---|
| `host` | `DSS_HOST` | Yes | — | DSS instance URL |
| `api_key` | `DSS_API_KEY` | Yes | — | API key for authentication |
| `project` | `DSS_PROJECT` | Yes | — | Target DSS project key |
| `verify_ssl` | `DSS_VERIFY_SSL` | No | `true` | Verify SSL certificates. Set `false` for self-signed certs |

!!! tip
    Use environment variables for `host` and `api_key` to avoid committing secrets. Omit `api_key` from YAML entirely and set `DSS_API_KEY` in the environment instead.

A `.env` file next to the config file is loaded automatically. This is convenient for local development:

```dotenv
# .env
DSS_HOST=http://localhost:11200
DSS_API_KEY=your-api-key
```

Priority (highest wins): YAML value > shell environment variable > `.env` file > default.

## State path

The `state_path` field (default: `.dss-state.json`) controls where the state file is written. This file tracks deployed resources and should be committed to version control for team coordination.

## Type discriminators

Datasets and recipes use the `type` field as a discriminator for Pydantic's tagged union. Zones have no type discriminator — there is only one zone type.

```yaml
datasets:
  - name: my_dataset
    type: snowflake    # selects SnowflakeDatasetResource
    # ...

recipes:
  - name: my_recipe
    type: python       # selects PythonRecipeResource
    # ...
```

Available dataset types: `snowflake`, `oracle`, `filesystem`, `upload`.

Available exposed object types: `dataset`, `managed_folder`.

Available recipe types: `python`, `sql_query`, `sync`.

## Modules

The `modules` section lets you define reusable resource generators as Python functions. A module is a callable that accepts parameters and returns `list[Resource]`, expanded at config-load time before the engine sees them.

```yaml
modules:
  # Multiple instances of the same module — each key becomes name=
  - call: snowflake_pipeline
    instances:
      customers:
        table: CUSTOMERS
      orders:
        table: ORDERS

  # Single invocation with explicit parameters
  - call: modules.pipelines:customer_pipeline
    with:
      name: customers
      table: CUSTOMERS
```

Module callables are resolved in three ways:

1. **Entry point** — short name (no `:`) resolved via `dss_provisioner.modules` entry point group
2. **Installed package** — `module.path:function` resolved via `importlib.import_module`
3. **Local file** — when the import fails, falls back to loading from a file relative to the config directory

Module-generated resources are merged with top-level resources — the engine treats them identically. See [YAML configuration](../guides/yaml-config.md#modules) for the full field reference.

## Variable substitution

DSS variables like `${projectKey}` are supported in string fields. They are resolved transparently during plan comparison so they don't cause false drift. For example:

```yaml
datasets:
  - name: raw_data
    type: filesystem
    connection: filesystem_managed
    path: "${projectKey}/raw"    # resolved to "MY_PROJECT/raw" during planning
```

## Validation

Run `dss-provisioner validate` to check your config file without connecting to DSS:

```bash
$ dss-provisioner validate
Configuration is valid.

$ dss-provisioner validate --config custom-config.yaml
Configuration is valid.
```

Validation catches:

- Missing required fields (e.g., recipe `outputs`, SQL recipe `inputs`)
- Invalid type discriminators
- Pydantic type/constraint errors (name pattern `^[a-zA-Z0-9_]+$`, non-empty strings, hex color format)
- Invalid YAML syntax

At plan time, the engine additionally validates:

- `depends_on` addresses reference known resources
- `zone` references point to actual zone resources
- `sql_query` recipes have at least one SQL-capable input (local SQL dataset or foreign ref)
- `exposed_objects` reference local DSS objects that exist
- `foreign_*` source project differs from the target project

See [YAML configuration](../guides/yaml-config.md) for the full field reference.
