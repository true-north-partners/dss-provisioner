# Resources

Resources are Pydantic models that describe the desired state of a DSS object. They are pure data — handlers know how to CRUD them.

## Resource model

Every resource has:

| Field | Description |
|---|---|
| `name` | Unique name within the DSS project |
| `description` | Optional description (stored as DSS metadata) |
| `tags` | Optional list of tags |
| `depends_on` | Explicit dependencies on other resources |
| `address` | Computed as `{resource_type}.{name}` (e.g., `dss_filesystem_dataset.raw_data`) |

The `address` is the primary key used in state tracking and plan output.

## Zone resources

Zones partition a project's flow into logical sections (e.g. raw, curated, reporting). They are provisioned **before** datasets and recipes so that resources can reference them via the `zone` field.

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | — | Zone identifier (used by `zone` field on datasets/recipes) |
| `color` | `str` | `#2ab1ac` | Hex color displayed in the flow graph |

```yaml
zones:
  - name: raw
    color: "#4a90d9"
  - name: curated
    color: "#7b61ff"
```

!!! note
    Flow zones require DSS Enterprise. On Free Edition the zone API returns 404: `read` and `delete` degrade gracefully (return None / no-op), while `create` and `update` raise a clear `RuntimeError` since the zone cannot actually be provisioned.

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
| `inputs` | `str \| list[str]` | `[]` | Input dataset name(s) |
| `outputs` | `str \| list[str]` | `[]` | Output dataset name(s) |
| `zone` | `str` | — | Flow zone (Enterprise only) |

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

#### Sync recipes

```yaml
recipes:
  - name: sync_customers
    type: sync
    inputs: customers_raw
    outputs: customers_synced
```

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
