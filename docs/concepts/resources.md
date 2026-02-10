# Resources

Resources are Pydantic models that describe the desired state of a DSS object. They are pure data — handlers know how to CRUD them.

## Resource model

Every resource has:

| Field | Description |
|---|---|
| `name` | Unique name within the DSS project. Must match `^[a-zA-Z0-9_]+$` (letters, digits, underscores) |
| `description` | Optional description (stored as DSS metadata) |
| `tags` | Optional list of tags (each element must be a non-empty string) |
| `depends_on` | Explicit dependencies on other resources. Validated at plan time — each address must exist |
| `address` | Computed as `{resource_type}.{name}` (e.g., `dss_filesystem_dataset.raw_data`) |

The `address` is the primary key used in state tracking and plan output.

## Variables resource

The `variables` resource manages DSS project variables — a singleton key-value store per project. Variables are split into two scopes:

- **`standard`**: shared across all instances
- **`local`**: instance-specific overrides

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | `variables` | Resource name (singleton — rarely overridden) |
| `standard` | `dict[str, Any]` | `{}` | Standard project variables |
| `local` | `dict[str, Any]` | `{}` | Local project variables |

```yaml
variables:
  standard:
    env: prod
    data_root: /mnt/data
  local:
    debug: "false"
```

Variables use **merge/partial semantics**: only declared keys are managed. Extra keys in DSS are left alone.

Variables have `plan_priority: 0`, so they are always applied before other resources (zones, datasets, recipes all have priority 100). This ensures variables referenced via `${…}` in other resources are set before those resources are created.

## Code environment defaults

The `code_envs` resource selects existing instance-level code environments as the project defaults. Code environments are **not created or managed** by the provisioner — only the project-level default pointers are set.

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | `code_envs` | Fixed singleton name |
| `default_python` | `str \| None` | `None` | Default Python code environment name |
| `default_r` | `str \| None` | `None` | Default R code environment name |

```yaml
code_envs:
  default_python: py39_ml
  default_r: r_base
```

Both fields are optional. Omitting a field means "don't manage this default." Only fields that are set are validated and applied — the provisioner calls `client.list_code_envs()` at plan time to verify referenced environments exist on the DSS instance.

Code environment defaults have `plan_priority: 5`, so they are applied after variables (0) but before libraries (10) and other resources.

## Zone resources

Zones partition a project's flow into logical sections (e.g. raw, curated, reporting). They are provisioned **before** datasets and recipes so that resources can reference them via the `zone` field.

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | — | Zone identifier (must match `^[a-zA-Z0-9_]+$`) |
| `color` | `str` | `#2ab1ac` | Hex color in `#RRGGBB` format |

```yaml
zones:
  - name: raw
    color: "#4a90d9"
  - name: curated
    color: "#7b61ff"
```

!!! note
    Flow zones require DSS Enterprise. On Free Edition the zone API returns 404: `read` and `delete` degrade gracefully (return None / no-op), while `create` and `update` raise a clear `RuntimeError` since the zone cannot actually be provisioned.

## Git library resources

Git libraries import external Git repositories into a project's library, making shared code available to recipes. Each library entry maps to a Git reference in DSS's `external-libraries.json`.

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | — | Library key / single directory name in the project library (must match `^[a-zA-Z0-9_]+$`; nested paths not supported) |
| `repository` | `str` | — | Git repository URL (non-empty) |
| `checkout` | `str` | `main` | Branch, tag, or commit hash |
| `path` | `str` | `""` | Subpath within the Git repository |
| `add_to_python_path` | `bool` | `true` | Add to `pythonPath` in `external-libraries.json` |

```yaml
libraries:
  - name: shared_utils
    repository: git@github.com:org/dss-shared-lib.git
    checkout: main
    path: python
```

Libraries have `plan_priority: 10`, so they are applied after variables (0) but before datasets and recipes (100). This ensures library code is available before recipes that import from it are created.

!!! note
    `add_to_python_path` is a create-time-only field. Changing it after creation requires deleting and recreating the library. Credentials (SSH keys) are configured at the DSS instance level — no `login`/`password` fields are needed in the YAML config.

## Managed folder resources

Managed folders store arbitrary files (models, reports, artifacts) and are commonly used as recipe I/O. Unlike datasets, DSS accesses managed folders by an internal ID — the provisioner resolves names automatically.

All managed folders share common fields from `ManagedFolderResource`:

| Field | Type | Default | Description |
|---|---|---|---|
| `connection` | `str` | — | DSS connection name |
| `zone` | `str` | — | Flow zone (Enterprise only) |

Metadata (description/tags) is stored inside the managed folder settings, unlike datasets which use a separate metadata API.

### Supported types

| Type | YAML `type` | Extra required fields |
|---|---|---|
| Filesystem | `filesystem` | `connection`, `path` |
| Upload | `upload` | — |

#### Filesystem managed folders

```yaml
managed_folders:
  - name: trained_models
    type: filesystem
    connection: filesystem_managed
    path: "${projectKey}/models"
```

The `path` field supports DSS variable substitution — `${projectKey}` is resolved transparently during plan comparison.

#### Upload managed folders

```yaml
managed_folders:
  - name: reports
    type: upload
```

## Exposed object resources

Exposed objects are project-level sharing rules: they declare which local datasets or managed folders this project makes available to other projects.

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `dataset \| managed_folder` | — | **Required.** Exposed object type |
| `target_projects` | `list[str]` | — | **Required.** Target project keys (deduplicated) |

```yaml
exposed_objects:
  - name: curated_customers
    type: dataset
    target_projects:
      - ANALYTICS
      - REPORTING

  - name: model_artifacts
    type: managed_folder
    target_projects:
      - ML_SERVING
```

Exposed object resources have `plan_priority: 150`, so they run after local datasets/folders are in place.

## Foreign object resources

Foreign resources declare cross-project inputs this project consumes. They do not create datasets/folders in the source project; they validate that the source object exists and is exposed to this project.

### Foreign datasets

| Field | Type | Default | Description |
|---|---|---|---|
| `source_project` | `str` | — | **Required.** Source project key |
| `source_name` | `str` | — | **Required.** Source dataset name |

```yaml
foreign_datasets:
  - name: shared_customers
    source_project: DATA_LAKE
    source_name: curated_customers
```

### Foreign managed folders

| Field | Type | Default | Description |
|---|---|---|---|
| `source_project` | `str` | — | **Required.** Source project key |
| `source_name` | `str` | — | **Required.** Source managed folder name |

```yaml
foreign_managed_folders:
  - name: shared_models
    source_project: MODELING
    source_name: model_artifacts
```

Foreign resources use the same namespace as local objects (`dataset` / `managed_folder`), so names must be unique across local + foreign declarations.

Recipes can reference foreign aliases (`shared_customers`) in `inputs`/`outputs`; the engine resolves them to DSS full refs (`DATA_LAKE.curated_customers`) during planning/apply.

## Dataset resources

All datasets share common fields from `DatasetResource`:

| Field | Type | Default | Description |
|---|---|---|---|
| `connection` | `str` | — | DSS connection name |
| `managed` | `bool` | `false` | Whether DSS manages the data lifecycle |
| `format_type` | `str` | — | Storage format (e.g., `parquet`, `csv`) |
| `format_params` | `dict` | `{}` | Format-specific parameters |
| `columns` | `list[Column]` | `[]` | Schema columns |
| `zone` | `str` | — | Flow zone (Enterprise only) |

### Supported types

| Type | YAML `type` | Extra required fields |
|---|---|---|
| Snowflake | `snowflake` | `connection`, `schema_name`, `table` |
| Oracle | `oracle` | `connection`, `schema_name`, `table` |
| Filesystem | `filesystem` | `connection`, `path` |
| Upload | `upload` | — |

#### Snowflake datasets

```yaml
datasets:
  - name: my_table
    type: snowflake
    connection: snowflake_prod
    mode: table             # table (default) or query
    schema_name: RAW
    table: CUSTOMERS
    catalog: MY_DB          # optional
    write_mode: OVERWRITE   # OVERWRITE (default), APPEND, or TRUNCATE
```

#### Filesystem datasets

```yaml
datasets:
  - name: raw_data
    type: filesystem
    connection: filesystem_managed
    path: "${projectKey}/raw"
    format_type: parquet
    managed: true
```

The `path` field supports DSS variable substitution — `${projectKey}` is resolved transparently during plan comparison.

#### Upload datasets

```yaml
datasets:
  - name: lookup_table
    type: upload
    format_type: csv
    format_params:
      separator: ","
      charset: utf-8
```

Upload datasets are always managed (`managed: true` by default).

## Recipe resources

All recipes share common fields from `RecipeResource`:

| Field | Type | Default | Description |
|---|---|---|---|
| `inputs` | `str \| list[str]` | `[]` | Input dataset name(s). **Required** for SQL recipes (min 1) |
| `outputs` | `str \| list[str]` | — | **Required.** Output dataset name(s) (min 1 element) |
| `zone` | `str` | — | Flow zone (Enterprise only). Validated at plan time — must reference a known zone |

Recipe `inputs` and `outputs` create **implicit dependencies** — the engine automatically orders recipes after their input datasets.

### Supported types

| Type | YAML `type` | Extra fields |
|---|---|---|
| Python | `python` | `code` or `code_file`, `code_env`, `code_wrapper` |
| SQL Query | `sql_query` | `code` or `code_file` |
| Sync | `sync` | — |

#### Python recipes

```yaml
recipes:
  - name: clean_customers
    type: python
    inputs: customers_raw
    outputs: customers_clean
    code_file: ./recipes/clean_customers.py  # loaded at plan time
    code_env: py311_pandas                   # optional code environment
```

You can provide code inline or via `code_file`. If both are set, `code_file` takes precedence. The `code_wrapper` flag controls whether the code runs in DSS's managed I/O wrapper.

#### SQL query recipes

```yaml
recipes:
  - name: aggregate_orders
    type: sql_query
    inputs: orders_raw
    outputs: orders_summary
    code_file: ./recipes/aggregate_orders.sql
```

SQL recipes must have at least one SQL-capable input. Inputs declared as foreign refs
(`PROJECT.dataset`) or via `foreign_datasets` are accepted and validated by DSS at runtime.

#### Sync recipes

```yaml
recipes:
  - name: sync_customers
    type: sync
    inputs: customers_raw
    outputs: customers_synced
```

## Scenario resources

Scenarios define automated workflows in DSS — triggers (when to run) and actions (what to do). They are provisioned **after** datasets and recipes (`plan_priority: 200`) since scenario steps often reference them.

All scenarios share common fields from `ScenarioResource`:

| Field | Type | Default | Description |
|---|---|---|---|
| `active` | `bool` | `true` | Whether the scenario is enabled |
| `triggers` | `list[dict]` | `[]` | Trigger definitions (temporal, dataset change, etc.) |

### Supported types

| Type | YAML `type` | Extra fields |
|---|---|---|
| Step-based | `step_based` | `steps` |
| Python | `python` | `code` or `code_file` |

#### Step-based scenarios

```yaml
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
              itemId: my_dataset
              partitionsSpec: ""
```

#### Python scenarios

```yaml
scenarios:
  - name: e2e_test
    type: python
    active: false
    code_file: ./scenarios/e2e_test.py
```

You can provide code inline or via `code_file`. If neither is set, the provisioner looks for `scenarios/{name}.py` by convention.

!!! note
    Triggers and steps use a **desired-echo** strategy: the provisioner stores your declared values and echoes them on read, rather than reading back from DSS. This avoids false drift from auto-generated fields that DSS adds internally.

## Columns

Define schema columns on datasets:

```yaml
datasets:
  - name: customers
    type: filesystem
    connection: filesystem_managed
    path: "${projectKey}/customers"
    columns:
      - name: id
        type: int
        description: Customer ID
      - name: email
        type: string
      - name: score
        type: double
        meaning: customer_score  # optional DSS meaning
```

Supported column types: `string`, `int`, `bigint`, `float`, `double`, `boolean`, `date`, `array`, `object`, `map`.
