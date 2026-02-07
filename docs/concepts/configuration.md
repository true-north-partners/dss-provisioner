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

recipes:
  - name: ...
    type: ...
    # type-specific fields
```

## Provider settings

The `provider` block configures the DSS connection. All fields support environment variable fallbacks with the `DSS_` prefix:

| Field | Env var | Required | Description |
|---|---|---|---|
| `host` | `DSS_HOST` | Yes | DSS instance URL |
| `api_key` | `DSS_API_KEY` | Yes | API key for authentication |
| `project` | `DSS_PROJECT` | Yes | Target DSS project key |

!!! tip
    Use environment variables for `host` and `api_key` to avoid committing secrets. Omit `api_key` from YAML entirely and set `DSS_API_KEY` in the environment instead.

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

Available recipe types: `python`, `sql_query`, `sync`.

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
- `sql_query` recipes have at least one input that is a SQL-connection dataset

See [YAML configuration](../guides/yaml-config.md) for the full field reference.
