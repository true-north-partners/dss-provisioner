# API reference

Auto-generated documentation from source code docstrings.

## Public API

The main entry point for programmatic use. All functions are importable from `dss_provisioner.config`.

::: dss_provisioner.config
    options:
      members:
        - load
        - plan
        - apply
        - plan_and_apply
        - refresh
        - save_state
        - drift

## Configuration

### Config

::: dss_provisioner.config.schema.Config

### ProviderConfig

::: dss_provisioner.config.schema.ProviderConfig

## Engine types

### Plan

::: dss_provisioner.engine.types.Plan

### ResourceChange

::: dss_provisioner.engine.types.ResourceChange

### Action

::: dss_provisioner.engine.types.Action

### ApplyResult

::: dss_provisioner.engine.types.ApplyResult

### PlanMetadata

::: dss_provisioner.engine.types.PlanMetadata

## Engine

### DSSEngine

::: dss_provisioner.engine.engine.DSSEngine

## State

### State

::: dss_provisioner.core.state.State

### ResourceInstance

::: dss_provisioner.core.state.ResourceInstance

## Provider

### DSSProvider

::: dss_provisioner.core.provider.DSSProvider

### ApiKeyAuth

::: dss_provisioner.core.provider.ApiKeyAuth

## Errors

::: dss_provisioner.engine.errors
    options:
      members:
        - EngineError
        - UnknownResourceTypeError
        - DuplicateAddressError
        - DependencyCycleError
        - StateProjectMismatchError
        - StalePlanError
        - StateLockError
        - ValidationError
        - ApplyError
        - ApplyCanceled
