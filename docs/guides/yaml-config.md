# YAML configuration

Complete reference for the `dss-provisioner.yaml` configuration file.

## Minimal example

```yaml
provider:
  project: MY_PROJECT

datasets:
  - name: raw_data
    type: filesystem
    connection: filesystem_managed
    path: "${projectKey}/raw"
```

## Full example

```yaml
provider:
  host: https://dss.company.com
  # api_key: omit from YAML — set DSS_API_KEY env var instead
  project: ANALYTICS

state_path: .dss-state.json

variables:
  standard:
    env: prod
    data_root: /mnt/data
  local:
    debug: "false"

zones:
  - name: raw
    color: "#4a90d9"
  - name: curated
    color: "#7b61ff"

datasets:
  - name: customers_raw
    type: snowflake
    connection: snowflake_prod
    schema_name: RAW
    table: CUSTOMERS
    description: Raw customer data from Snowflake

  - name: customers_clean
    type: filesystem
    connection: filesystem_managed
    path: "${projectKey}/clean/customers"
    managed: true
    format_type: parquet
    columns:
      - name: id
        type: int
      - name: name
        type: string
      - name: email
        type: string
    tags:
      - production
      - pii

recipes:
  - name: clean_customers
    type: python
    inputs: customers_raw
    outputs: customers_clean
    code_file: ./recipes/clean_customers.py

  - name: sync_customers
    type: sync
    inputs: customers_clean
    outputs: customers_synced
    depends_on:
      - dss_python_recipe.clean_customers
```

## Provider

| Field | Env var | Required | Default | Description |
|---|---|---|---|---|
| `host` | `DSS_HOST` | Yes | — | DSS instance URL |
| `api_key` | `DSS_API_KEY` | Yes | — | API key |
| `project` | `DSS_PROJECT` | Yes | — | Target project key |

## Top-level fields

| Field | Type | Default | Description |
|---|---|---|---|
| `provider` | object | — | DSS connection settings (required) |
| `state_path` | string | `.dss-state.json` | Path to state file |
| `variables` | object | — | Project variables (singleton, applied first) |
| `zones` | list | `[]` | Flow zone definitions (provisioned before datasets/recipes) |
| `datasets` | list | `[]` | Dataset resource definitions |
| `recipes` | list | `[]` | Recipe resource definitions |

## Variables fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | `variables` | Resource name (singleton — rarely overridden) |
| `standard` | dict | `{}` | Standard project variables (shared across instances) |
| `local` | dict | `{}` | Local project variables (instance-specific) |
| `description` | string | `""` | Not used by DSS variables (ignored) |
| `tags` | list | `[]` | Not used by DSS variables (ignored) |
| `depends_on` | list | `[]` | Explicit resource dependencies (addresses) |

Variables use **partial semantics**: only declared keys are managed. Extra keys already in DSS are preserved.

Variables are always applied before other resource types due to their `plan_priority: 0` (other resources default to 100).

## Zone fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | — | **Required.** Zone identifier (referenced by dataset/recipe `zone` field) |
| `color` | string | `#2ab1ac` | Hex color in `#RRGGBB` format |
| `description` | string | `""` | Not used by DSS zones (ignored) |
| `tags` | list | `[]` | Not used by DSS zones (ignored) |
| `depends_on` | list | `[]` | Explicit resource dependencies (addresses) |

!!! note
    Flow zones require DSS Enterprise. On Free Edition the zone API is unavailable.

## Dataset fields

### Common fields (all types)

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | — | **Required.** Dataset name in DSS |
| `type` | string | — | **Required.** One of: `snowflake`, `oracle`, `filesystem`, `upload` |
| `connection` | string | — | DSS connection name |
| `managed` | bool | `false` | Whether DSS manages the data lifecycle |
| `format_type` | string | — | Storage format (`parquet`, `csv`, etc.) |
| `format_params` | dict | `{}` | Format-specific parameters |
| `columns` | list | `[]` | Schema column definitions |
| `zone` | string | — | Flow zone (Enterprise only) |
| `description` | string | `""` | Dataset description (metadata) |
| `tags` | list | `[]` | DSS tags |
| `depends_on` | list | `[]` | Explicit resource dependencies (addresses) |

### Snowflake-specific fields

| Field | Type | Default | Description |
|---|---|---|---|
| `connection` | string | — | **Required.** Snowflake connection name |
| `schema_name` | string | — | **Required.** Snowflake schema |
| `table` | string | — | **Required.** Table name |
| `catalog` | string | — | Snowflake database/catalog |
| `write_mode` | string | `OVERWRITE` | `OVERWRITE`, `APPEND`, or `TRUNCATE` |

### Oracle-specific fields

| Field | Type | Default | Description |
|---|---|---|---|
| `connection` | string | — | **Required.** Oracle connection name |
| `schema_name` | string | — | **Required.** Oracle schema |
| `table` | string | — | **Required.** Table name |

### Filesystem-specific fields

| Field | Type | Default | Description |
|---|---|---|---|
| `connection` | string | — | **Required.** Filesystem connection name |
| `path` | string | — | **Required.** File path (supports `${projectKey}`) |

### Upload-specific fields

Upload datasets have no additional required fields. They default to `managed: true`.

## Recipe fields

### Common fields (all types)

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | — | **Required.** Recipe name in DSS |
| `type` | string | — | **Required.** One of: `python`, `sql_query`, `sync` |
| `inputs` | string or list | `[]` | Input dataset name(s) |
| `outputs` | string or list | `[]` | Output dataset name(s) |
| `zone` | string | — | Flow zone (Enterprise only) |
| `description` | string | `""` | Recipe description |
| `tags` | list | `[]` | DSS tags |
| `depends_on` | list | `[]` | Explicit resource dependencies (addresses) |

!!! note
    `inputs` and `outputs` accept either a single string or a list of strings. A single string is automatically converted to a one-element list.

### Python-specific fields

| Field | Type | Default | Description |
|---|---|---|---|
| `code` | string | `""` | Inline Python code |
| `code_file` | string | — | Path to Python file (relative to config file) |
| `code_env` | string | — | DSS code environment name |
| `code_wrapper` | bool | `false` | Use DSS managed I/O wrapper |

### SQL query-specific fields

| Field | Type | Default | Description |
|---|---|---|---|
| `code` | string | `""` | Inline SQL code |
| `code_file` | string | — | Path to SQL file (relative to config file) |

### Sync-specific fields

Sync recipes have no additional fields beyond the common recipe fields.

## Column definition

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | — | **Required.** Column name |
| `type` | string | — | **Required.** One of: `string`, `int`, `bigint`, `float`, `double`, `boolean`, `date`, `array`, `object`, `map` |
| `description` | string | `""` | Column description |
| `meaning` | string | — | DSS column meaning |

## Dependencies

Resources can depend on each other in two ways:

### Explicit dependencies

Use `depends_on` with full resource addresses:

```yaml
recipes:
  - name: aggregate
    type: python
    depends_on:
      - dss_python_recipe.clean_data
```

### Implicit dependencies

Recipe `inputs` and `outputs` automatically create dependencies on the referenced datasets. You don't need to add `depends_on` for these.

```yaml
datasets:
  - name: raw_data
    type: filesystem
    connection: filesystem_managed
    path: "${projectKey}/raw"

recipes:
  - name: process
    type: python
    inputs: raw_data      # automatically depends on dss_filesystem_dataset.raw_data
    outputs: clean_data
```
