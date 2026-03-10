# Phase 1 Sub-Plan: Package-Aware Loader

## Purpose

Phase 1 should solve only one problem:

the runtime must understand the `v2.6.0` package/flow layout well enough to discover contracts correctly, without changing runtime semantics yet.

This phase is **not**:
- flow execution migration
- routing derivation migration
- handler-first engine migration
- bridge removal

It is strictly a loader/data-model phase.

## Current Gap

Today the runtime loader in [`internal/runtime/contracts/workflow_contracts.go`](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts.go) assumes:

- one workflow file
- one hook registry
- one nodes file
- one event catalog
- one agent registry
- one tools file
- one policy file

`v2.6.0` instead introduces:

- project-level package manifest:
  - [`docs/specs/empireai-v2_6_0/contracts-v250/empire/package.yaml`](/Users/youmew/dev/empireai/docs/specs/empireai-v2_6_0/contracts-v250/empire/package.yaml)
- project-level files:
  - `agents.yaml`
  - `events.yaml`
  - `nodes.yaml`
  - `policy.yaml`
  - `tools.yaml`
- per-flow directories:
  - `flows/discovery`
  - `flows/scoring`
  - `flows/validation`
  - `flows/operating`
- bridge files:
  - `runtime/nodes.yaml`
  - `runtime/events.yaml`
  - `runtime/agents.yaml`

So the main Phase 1 problem is:

the current loader cannot represent package-level contracts plus multiple flow directories plus runtime bridge metadata in one coherent API.

## Phase 1 Deliverables

### 1. Add explicit package/flow contract types

Add typed models for:

- project package manifest
- flow entries from `package.yaml`
- runtime contract paths
- target contract paths
- flow schema paths

Minimum new concepts:

- `ProjectPackageDocument`
- `FlowPackageRef`
- `FlowContractPaths`
- `RuntimeBridgePaths`

Goal:
- the loader can describe the new layout explicitly rather than stuffing everything into flat-path fields

### 2. Extend `ContractPaths` into a package-aware layout model

Current `ContractPaths` is flat and file-oriented.

Refactor it so it can express:

- project root
- project-level contract files
- flow directories
- runtime bridge files
- target flow files

Suggested direction:

- keep existing flat fields temporarily for backward compatibility
- add structured fields for:
  - `ProjectPackageFile`
  - `ProjectAgentsFile`
  - `ProjectEventsFile`
  - `ProjectNodesFile`
  - `ProjectPolicyFile`
  - `ProjectToolsFile`
  - `RuntimeBridge`
  - `Flows`

Goal:
- current runtime can still ask for “the merged nodes file” later
- but the loader internally understands the package layout first

### 3. Add package discovery logic

Implement discovery for:

- `contracts/empire/package.yaml` when present
- fallback to current root/flat layout when not present

Discovery should resolve:

- project-level files
- `flows/*/` directories listed by package manifest
- runtime bridge files listed by `runtime_contracts`
- target patterns declared in `target_contracts`

Important:
- do not implement wildcard merging in Phase 1
- just discover and record what exists

Goal:
- runtime can answer “what is the package layout?” deterministically

### 4. Load flow manifests and schemas as data, not semantics

Phase 1 should parse and store:

- `flows/*/schema.yaml`
- optional flow `agents.yaml`
- optional flow `events.yaml`
- optional flow `nodes.yaml`
- optional flow `policy.yaml`
- optional flow `tools.yaml`

But it should **not** yet attempt to interpret them into runtime behavior.

Goal:
- the contract loader becomes aware of flows and their files
- pipeline/runtime code can remain unchanged for now

### 5. Keep the runtime bundle API backward-compatible

The rest of the runtime still expects a `WorkflowContractBundle`.

For Phase 1:

- keep `WorkflowContractBundle` usable by current code
- add package-aware fields to it instead of replacing it immediately

Suggested additions:

- `Package` metadata
- `FlowSchemas`
- `FlowFiles`
- `RuntimeBridgePresent`

Goal:
- no downstream runtime churn yet

### 6. Add focused loader tests for all discovery scenarios

Test matrix:

1. current flat/root layout only
2. `empire/` package layout with runtime bridge
3. `empire/` package layout with per-flow directories discovered
4. mixed package with missing optional files
5. malformed package manifest

Goal:
- Phase 1 confidence comes from loader tests, not pipeline tests

## Execution Order

1. add package manifest types
2. extend `ContractPaths`
3. implement package discovery
4. load flow file metadata into bundle
5. preserve backward compatibility
6. add loader tests
7. run runtime contract tests

## Explicit Non-Goals

Do **not** do these in Phase 1:

- consume `runtime/events.yaml` as the active runtime source
- merge project + flow files into runtime views
- derive routing from `agents.yaml + nodes.yaml`
- derive transitions from handler fields
- replace `workflow-schema.yaml` semantics

Those belong to later phases.

## Risks

### Risk 1: loader type churn spills into runtime

Mitigation:
- keep `WorkflowContractBundle` backward-compatible
- add fields instead of replacing them

### Risk 2: package discovery accidentally changes active file selection

Mitigation:
- Phase 1 discovery only
- do not switch active runtime inputs yet

### Risk 3: spec tree contains optional files not present in all flows

Mitigation:
- only `schema.yaml`, `nodes.yaml`, `events.yaml`, `agents.yaml` should be treated as required where the spec says so
- `tools.yaml` and `policy.yaml` remain optional

## Exit Criteria

Phase 1 is done when all of the following are true:

- loader can parse `empire/package.yaml`
- loader discovers all 4 flows and their file sets
- loader records runtime bridge paths and target flow paths
- `WorkflowContractBundle` contains package/flow metadata without breaking current runtime callers
- current runtime contract tests are green
- no runtime behavior has changed yet

## Acceptance Commands

```bash
go test ./internal/runtime/contracts -count=1
go test ./internal/runtime -run TestContractCompliance -count=1
```
