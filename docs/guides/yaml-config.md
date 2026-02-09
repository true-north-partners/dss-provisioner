# YAML configuration

Complete reference for the `dss-provisioner.yaml` configuration file.

For full copy/paste starter projects, see [End-to-end examples](examples.md).

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

managed_folders:
  - name: trained_models
    type: filesystem
    connection: filesystem_managed
    path: "${projectKey}/models"

  - name: reports
    type: upload

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

exposed_objects:
  - name: customers_clean
    type: dataset
    target_projects:
      - ANALYTICS_APP
      - REPORTING

  - name: trained_models
    type: managed_folder
    target_projects:
      - ML_SERVING

foreign_datasets:
  - name: shared_orders
    source_project: DATA_LAKE
    source_name: curated_orders

foreign_managed_folders:
  - name: shared_reference_data
    source_project: GOVERNANCE
    source_name: master_reference

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

modules:
  - call: modules.pipelines:snowflake_pipeline
    instances:
      customers:
        table: CUSTOMERS
        schema_name: RAW
      orders:
        table: ORDERS
        schema_name: STAGING
```

## Provider

| Field | Env var | Required | Default | Description |
|---|---|---|---|---|
| `host` | `DSS_HOST` | Yes | — | DSS instance URL |
| `api_key` | `DSS_API_KEY` | Yes | — | API key |
| `project` | `DSS_PROJECT` | Yes | — | Target project key |

A `.env` file in the working directory is loaded automatically (priority: YAML > env var > `.env` > default).

## Top-level fields

| Field | Type | Default | Description |
|---|---|---|---|
| `provider` | object | — | DSS connection settings (required) |
| `state_path` | string | `.dss-state.json` | Path to state file |
| `variables` | object | — | Project variables (singleton, applied first) |
| `code_envs` | object | — | Project default code environments (applied after variables, before libraries) |
| `zones` | list | `[]` | Flow zone definitions (provisioned before datasets/recipes) |
| `libraries` | list | `[]` | Git library references (applied after variables, before datasets/recipes) |
| `managed_folders` | list | `[]` | Managed folder resource definitions |
| `datasets` | list | `[]` | Dataset resource definitions |
| `exposed_objects` | list | `[]` | Cross-project exposure rules for local datasets/folders |
| `foreign_datasets` | list | `[]` | Foreign dataset aliases from other DSS projects |
| `foreign_managed_folders` | list | `[]` | Foreign managed folder aliases from other DSS projects |
| `recipes` | list | `[]` | Recipe resource definitions |
| `scenarios` | list | `[]` | Scenario resource definitions (applied after datasets/recipes) |
| `modules` | list | `[]` | Python module invocations that expand into resources at config-load time |

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
| `name` | string | `code_envs` | Fixed singleton name (cannot be changed) |
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

!!! tip
    For `dss-provisioner preview`, set `repository: self` to reuse the current repo's `origin` URL and automatically pin `checkout` to the current branch in the preview project.

## Managed folder fields

### Common fields (all types)

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | — | **Required.** Managed folder name in DSS. Must match `^[a-zA-Z0-9_]+$` |
| `type` | string | — | **Required.** One of: `filesystem`, `upload` |
| `connection` | string | — | DSS connection name |
| `zone` | string | — | Flow zone (Enterprise only). Validated at plan time — must reference a known zone |
| `description` | string | `""` | Managed folder description |
| `tags` | list | `[]` | DSS tags. Elements must be non-empty strings |
| `depends_on` | list | `[]` | Explicit resource dependencies (addresses) |

### Filesystem-specific fields

| Field | Type | Default | Description |
|---|---|---|---|
| `connection` | string | — | **Required.** Filesystem connection name |
| `path` | string | — | **Required.** File path (non-empty, supports `${projectKey}`) |

### Upload-specific fields

Upload managed folders have no additional required fields.

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

## Exposed object fields

Use `exposed_objects` to share local datasets/folders with other projects.

### Common fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | — | **Required.** Local object name to expose |
| `type` | string | — | **Required.** One of: `dataset`, `managed_folder` |
| `target_projects` | list | — | **Required.** Target project keys (min 1; duplicates removed) |
| `description` | string | `""` | Not used by DSS exposed objects (kept in state) |
| `tags` | list | `[]` | Not used by DSS exposed objects (kept in state) |
| `depends_on` | list | `[]` | Explicit resource dependencies (addresses) |

## Foreign object fields

Use foreign resources to declare cross-project aliases consumed by this project.

### Foreign datasets (`foreign_datasets`)

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | — | **Required.** Local alias used in recipes |
| `source_project` | string | — | **Required.** Source project key |
| `source_name` | string | — | **Required.** Source dataset name |
| `description` | string | `""` | Not used by DSS foreign refs (kept in state) |
| `tags` | list | `[]` | Not used by DSS foreign refs (kept in state) |
| `depends_on` | list | `[]` | Explicit resource dependencies (addresses) |

### Foreign managed folders (`foreign_managed_folders`)

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | — | **Required.** Local alias used in recipes |
| `source_project` | string | — | **Required.** Source project key |
| `source_name` | string | — | **Required.** Source managed folder name |
| `description` | string | `""` | Not used by DSS foreign refs (kept in state) |
| `tags` | list | `[]` | Not used by DSS foreign refs (kept in state) |
| `depends_on` | list | `[]` | Explicit resource dependencies (addresses) |

## Recipe fields

### Common fields (all types)

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | — | **Required.** Recipe name in DSS. Must match `^[a-zA-Z0-9_]+$` |
| `type` | string | — | **Required.** One of: `python`, `sql_query`, `sync` |
| `inputs` | string or list | `[]` | Input refs. Can be local names, foreign aliases, or `PROJECT.object` refs. **Required** for `sql_query` (min 1) |
| `outputs` | string or list | — | **Required.** Output refs (min 1 element). Elements must be non-empty |
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
| `inputs` | string or list | — | **Required.** Input refs (min 1 element; validated at plan time — at least one input must be SQL-capable or a foreign ref) |
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

## Modules

Modules let you define reusable resource generators as Python functions. Each module entry specifies a callable and how to invoke it.

### Module entry fields

| Field | Type | Default | Description |
|---|---|---|---|
| `call` | string | — | **Required.** Callable reference — short name (entry point) or `module.path:function` |
| `instances` | dict | — | Named instances. Each key becomes `name=`, values are extra kwargs |
| `with` | dict | — | Single invocation kwargs (passed directly to the callable) |

Exactly one of `instances` or `with` must be provided.

### Callable resolution

The `call` string is resolved in order:

1. **Entry point** — if no `:` is present, looks up the name in the `dss_provisioner.modules` entry point group
2. **Installed package** — `module.path:function` tries `importlib.import_module` first
3. **Local file** — falls back to loading `module/path.py` relative to the config file directory

### Entry point registration

Package authors register module callables as entry points:

```toml
# pyproject.toml
[project.entry-points."dss_provisioner.modules"]
snowflake_pipeline = "my_package.snowflake:snowflake_pipeline"
```

### Examples

```yaml
# Multiple instances — each key becomes name= kwarg
modules:
  - call: snowflake_pipeline
    instances:
      customers:
        table: CUSTOMERS
      orders:
        table: ORDERS

  # Single invocation — kwargs passed directly
  - call: modules.pipelines:customer_pipeline
    with:
      name: customers
      table: CUSTOMERS
```

The callable must return `list[Resource]`. Module-generated resources are merged with top-level resources before planning.

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
- **SQL recipe inputs** must include at least one SQL-capable input (local SQL dataset or foreign ref)
- **`exposed_objects`** names must exist as local objects in DSS
- **`foreign_*`** `source_project` must differ from the target project
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

Recipe `inputs` and `outputs` automatically create dependencies on referenced local/foreign resources by name. You don't need to add `depends_on` for these in the common case.

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
