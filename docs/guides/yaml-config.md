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

code_envs:
  default_python: py311_pandas
  default_r: r_base

zones:
  - name: raw
    color: "#4a90d9"
  - name: curated
    color: "#7b61ff"

libraries:
  - name: shared_utils
    repository: git@github.com:org/dss-shared-lib.git
    checkout: main
    path: python

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

scenarios:
  - name: daily_build
    type: step_based
    active: true
    triggers:
      - type: temporal
        params:
          frequency: Daily
          hour: 2
          minute: 0
    steps:
      - type: build_flowitem
        name: Build all datasets
        params:
          builds:
            - type: DATASET
              itemId: customers_clean
              partitionsSpec: ""

  - name: e2e_test
    type: python
    active: false
    code_file: ./scenarios/e2e_test.py
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
| `code_envs` | object | — | Project default code environments (applied after variables, before libraries) |
| `zones` | list | `[]` | Flow zone definitions (provisioned before datasets/recipes) |
| `libraries` | list | `[]` | Git library references (applied after variables, before datasets/recipes) |
| `datasets` | list | `[]` | Dataset resource definitions |
| `recipes` | list | `[]` | Recipe resource definitions |
| `scenarios` | list | `[]` | Scenario resource definitions (applied after datasets/recipes) |

## Variables fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | `variables` | Resource name (singleton — rarely overridden). Must match `^[a-zA-Z0-9_]+$` |
| `standard` | dict | `{}` | Standard project variables (shared across instances) |
| `local` | dict | `{}` | Local project variables (instance-specific) |
| `description` | string | `""` | Not used by DSS variables (ignored) |
| `tags` | list | `[]` | Not used by DSS variables (ignored) |
| `depends_on` | list | `[]` | Explicit resource dependencies (addresses) |

Variables use **partial semantics**: only declared keys are managed. Extra keys already in DSS are preserved.

Variables are always applied before other resource types due to their `plan_priority: 0` (other resources default to 100).

## Code environment fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | `code_envs` | Resource name (singleton — rarely overridden). Must match `^[a-zA-Z0-9_]+$` |
| `default_python` | string | — | Default Python code environment name (must exist on DSS instance) |
| `default_r` | string | — | Default R code environment name (must exist on DSS instance) |
| `description` | string | `""` | Not used by DSS code envs (ignored) |
| `tags` | list | `[]` | Not used by DSS code envs (ignored) |
| `depends_on` | list | `[]` | Explicit resource dependencies (addresses) |

Code environments are **instance-scoped** in DSS. The provisioner does not create or manage them — it only selects existing environments as the project default. At plan time, the engine validates that referenced environments exist by calling `list_code_envs()`.

Code environment defaults have `plan_priority: 5`, applied after variables (0) but before libraries (10).

## Zone fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | — | **Required.** Zone identifier (must match `^[a-zA-Z0-9_]+$`) |
| `color` | string | `#2ab1ac` | Hex color in `#RRGGBB` format |
| `description` | string | `""` | Not used by DSS zones (ignored) |
| `tags` | list | `[]` | Not used by DSS zones (ignored) |
| `depends_on` | list | `[]` | Explicit resource dependencies (addresses) |

!!! note
    Flow zones require DSS Enterprise. On Free Edition the zone API is unavailable.

## Library fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | — | **Required.** Library key (single segment in the library hierarchy; letters, digits, and underscores only). Must match `^[a-zA-Z0-9_]+$` |
| `repository` | string | — | **Required.** Git repository URL (non-empty) |
| `checkout` | string | `main` | Branch, tag, or commit hash to check out |
| `path` | string | `""` | Subpath within the Git repository |
| `add_to_python_path` | bool | `true` | Add to `pythonPath` in `external-libraries.json` |
| `description` | string | `""` | Not used by DSS libraries (ignored) |
| `tags` | list | `[]` | Not used by DSS libraries (ignored). Elements must be non-empty strings |
| `depends_on` | list | `[]` | Explicit resource dependencies (addresses) |

!!! note
    `add_to_python_path` is a create-time-only field. To change it, delete and recreate the library.

## Dataset fields

### Common fields (all types)

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | — | **Required.** Dataset name in DSS. Must match `^[a-zA-Z0-9_]+$` |
| `type` | string | — | **Required.** One of: `snowflake`, `oracle`, `filesystem`, `upload` |
| `connection` | string | — | DSS connection name |
| `managed` | bool | `false` | Whether DSS manages the data lifecycle |
| `format_type` | string | — | Storage format (`parquet`, `csv`, etc.) |
| `format_params` | dict | `{}` | Format-specific parameters |
| `columns` | list | `[]` | Schema column definitions |
| `zone` | string | — | Flow zone (Enterprise only). Validated at plan time — must reference a known zone |
| `description` | string | `""` | Dataset description (metadata) |
| `tags` | list | `[]` | DSS tags. Elements must be non-empty strings |
| `depends_on` | list | `[]` | Explicit resource dependencies (addresses). Validated at plan time — each address must exist |

### Snowflake-specific fields

| Field | Type | Default | Description |
|---|---|---|---|
| `connection` | string | — | **Required.** Snowflake connection name |
| `schema_name` | string | — | **Required.** Snowflake schema (non-empty) |
| `table` | string | — | **Required.** Table name (non-empty) |
| `catalog` | string | — | Snowflake database/catalog |
| `write_mode` | string | `OVERWRITE` | `OVERWRITE`, `APPEND`, or `TRUNCATE` |

### Oracle-specific fields

| Field | Type | Default | Description |
|---|---|---|---|
| `connection` | string | — | **Required.** Oracle connection name |
| `schema_name` | string | — | **Required.** Oracle schema (non-empty) |
| `table` | string | — | **Required.** Table name (non-empty) |

### Filesystem-specific fields

| Field | Type | Default | Description |
|---|---|---|---|
| `connection` | string | — | **Required.** Filesystem connection name |
| `path` | string | — | **Required.** File path (non-empty, supports `${projectKey}`) |

### Upload-specific fields

Upload datasets have no additional required fields. They default to `managed: true`.

## Recipe fields

### Common fields (all types)

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | — | **Required.** Recipe name in DSS. Must match `^[a-zA-Z0-9_]+$` |
| `type` | string | — | **Required.** One of: `python`, `sql_query`, `sync` |
| `inputs` | string or list | `[]` | Input dataset name(s). Elements must be non-empty. **Required** for `sql_query` (min 1) |
| `outputs` | string or list | — | **Required.** Output dataset name(s) (min 1 element). Elements must be non-empty |
| `zone` | string | — | Flow zone (Enterprise only). Validated at plan time — must reference a known zone |
| `description` | string | `""` | Recipe description |
| `tags` | list | `[]` | DSS tags. Elements must be non-empty strings |
| `depends_on` | list | `[]` | Explicit resource dependencies (addresses). Validated at plan time — each address must exist |

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
| `inputs` | string or list | — | **Required.** Input dataset name(s) (min 1 element; validated at plan time — at least one input must reference a SQL-connection dataset) |
| `code` | string | `""` | Inline SQL code |
| `code_file` | string | — | Path to SQL file (relative to config file) |

### Sync-specific fields

Sync recipes have no additional fields beyond the common recipe fields.

## Scenario fields

### Common fields (all types)

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | — | **Required.** Scenario name in DSS. Must match `^[a-zA-Z0-9_]+$` |
| `type` | string | — | **Required.** One of: `step_based`, `python` |
| `active` | bool | `true` | Whether the scenario is enabled |
| `triggers` | list | `[]` | Trigger definitions (temporal, dataset change, etc.) |
| `description` | string | `""` | Scenario description |
| `tags` | list | `[]` | DSS tags |
| `depends_on` | list | `[]` | Explicit resource dependencies (addresses) |

!!! note
    Triggers and steps are stored as raw dicts matching the DSS API format. The provisioner echoes your declared values on read to avoid false drift from auto-generated fields.

### Step-based-specific fields

| Field | Type | Default | Description |
|---|---|---|---|
| `steps` | list | `[]` | Step definitions (build, run scenario, etc.) |

### Python-specific fields

| Field | Type | Default | Description |
|---|---|---|---|
| `code` | string | `""` | Inline Python code |
| `code_file` | string | — | Path to Python file (relative to config file) |

## Column definition

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | — | **Required.** Column name (non-empty) |
| `type` | string | — | **Required.** One of: `string`, `int`, `bigint`, `float`, `double`, `boolean`, `date`, `array`, `object`, `map` |
| `description` | string | `""` | Column description |
| `meaning` | string | — | DSS column meaning |

## Validation

All resource names must match `^[a-zA-Z0-9_]+$` (letters, digits, and underscores only). This is enforced at config load time.

Additional parse-time constraints:

- **Tags**: each tag must be a non-empty string
- **Recipe outputs**: at least one output is required
- **SQL recipe inputs**: at least one input is required
- **Zone color**: must be a valid hex color in `#RRGGBB` format
- **Snowflake/Oracle `schema_name` and `table`**: must be non-empty
- **Filesystem `path`**: must be non-empty
- **Git library `repository`**: must be non-empty

At plan time, the engine additionally validates:

- **`depends_on`** addresses must reference a known resource (in config or state)
- **`zone`** references must point to a resource of type `dss_zone`
- **SQL recipe inputs** must include at least one SQL-connection dataset
- **`code_envs`** `default_python` and `default_r` must reference existing code environments on the DSS instance
- **Python recipe `code_env`** must reference an existing Python code environment on the DSS instance

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
