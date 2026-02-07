# Architecture

dss-provisioner follows Terraform's plan/apply model. You declare desired state in YAML, the engine computes a diff against actual state, and apply executes the changes.

## Core loop

```
YAML config ──► plan() ──► Plan (diff) ──► apply() ──► DSS API calls
                  ▲                                         │
                  │                                         ▼
              State file ◄──────────────────────────── Updated state
```

1. **Load** — Parse `dss-provisioner.yaml` into a validated `Config` object
2. **Refresh** — Read live DSS state for each tracked resource, update the state file
3. **Plan** — Compare desired resources against state, produce a `Plan` of `ResourceChange` items
4. **Apply** — Execute the plan in dependency order, updating state after each resource

## Key components

### Engine (`DSSEngine`)

The engine is the central orchestrator. It holds references to the provider, state, and handler registry. Its two main methods are:

- **`plan()`** — Compares desired resources against current state. Optionally refreshes state first (default: yes). Returns a `Plan` containing a list of `ResourceChange` items, each with an action: `create`, `update`, `delete`, or `no-op`.
- **`apply()`** — Executes a plan. Processes changes in topological order (respecting `depends_on` and inferred dependencies). Updates the state file after each successful operation. If a resource fails, the error is raised with a partial `ApplyResult` so you can see what succeeded.

### State file

The state file (`.dss-state.json` by default) tracks:

- **Lineage** — A UUID identifying this state's history, used for stale-plan detection
- **Serial** — Incremented on every write, used for concurrency detection
- **Resources** — A map of resource addresses to their last-known attributes
- **Digest** — SHA256 hash of resource attributes, used to detect plan staleness

State is written atomically (temp file + rename) and a `.backup` copy is kept.

### Handlers

Handlers implement CRUD operations for each resource type. The engine delegates to the appropriate handler based on resource type. Handler categories:

- **VariablesHandler** — Reads and writes DSS project variables (singleton per project)
- **ZoneHandler** — Creates, updates, reads, and deletes DSS flow zones
- **GitLibraryHandler** — Creates, updates, reads, and deletes DSS Git library references
- **DatasetHandler** — Creates, updates, reads, and deletes DSS datasets
- **RecipeHandler** — Creates, updates, reads, and deletes DSS recipes
- **ScenarioHandler** — Creates, updates, reads, and deletes DSS scenarios (step-based and Python)

### Dependency graph

Resources can declare dependencies explicitly via `depends_on` or implicitly through recipe `inputs`/`outputs`. The engine builds a directed acyclic graph and processes resources in topological order during apply.

If dependencies contain a cycle, the engine raises `DependencyCycleError`.

## Engine semantics

- One state file manages **one DSS project**. Plan/apply will error if the state belongs to a different project (`StateProjectMismatchError`).
- `plan` performs a **refresh by default** — reads live DSS state and may persist updates. Disable with `--no-refresh` or `refresh=False`.
- `apply` executes changes in dependency order with **no rollback**. If apply fails, state reflects what was completed. The `ApplyError` carries a partial `ApplyResult`.
- Saved plans (via `--out`) are checked for staleness via lineage, serial, and state digest before apply.
- DSS `${…}` variables (e.g. `${projectKey}`) are resolved transparently during plan comparison so they don't cause false drift.

## Partial failure

Apply does not support rollback. If a resource fails mid-apply:

1. All previously applied resources remain in place and are tracked in state
2. The failing resource is **not** recorded in state
3. An `ApplyError` is raised with the partial `ApplyResult` attached
4. Re-running `plan` + `apply` will retry only the failed and remaining resources
