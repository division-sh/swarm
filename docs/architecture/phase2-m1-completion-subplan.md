# Phase 2 M1 Completion Sub-Plan

## Goal

Finish `M1` of the runtime modularization refactor so `FlowTree` and the semantic model are genuinely centered in `flowmodel`, while `contracts` becomes a loader/assembler adapter instead of the semantic owner.

## Current State

What is already done:

- `flowmodel` owns the generic semantic primitives:
  - policy types
  - tree and URI registry types
  - traversal, indexing, and resolution helpers
- `contracts` no longer exposes raw project/scoped/source maps outside the package.
- downstream code increasingly uses accessor methods instead of raw loader storage.

What still blocks `M1` completion:

1. `contracts` still owns the concrete semantic model via `FlowContractView`, `ProjectContractView`, and `FlowTree` aliases.
2. `contracts` still owns semantic assembly:
   - tree build
   - package-root projection
   - URI population
   - semantic derivation wrappers
3. there is no narrow semantic-source interface yet; downstream packages still depend on `*WorkflowContractBundle`.
4. routing still depends directly on contracts bundle/view types.
5. package-root projection is still contracts-owned and not yet classified as canonical model behavior versus adapter logic.
6. too many semantic query methods still live directly on `WorkflowContractBundle`.

## Execution Order

### Step 1. Move tree assembly mechanics into `flowmodel`

Extract the generic tree-materialization and tree-assembly helpers out of `internal/runtime/contracts/workflow_contracts.go`.

Scope:

- move materialization/build-node mechanics into `internal/runtime/flowmodel`
- keep contracts-specific path/file/package logic in `contracts`
- keep behavior identical

Acceptance:

- `contracts` no longer owns local tree-materialization helpers
- `go test ./internal/runtime/flowmodel ./internal/runtime/contracts -count=1`

### Step 2. Move concrete semantic view types into `flowmodel`

Create concrete semantic types in `flowmodel` for the recursive model, replacing contracts-side aliases as the semantic owner.

Scope:

- introduce concrete `flowmodel.FlowView` and `flowmodel.ProjectView`
- stop treating `contracts` aliases as the architectural home
- keep contracts-specific raw document/path types where needed

Acceptance:

- semantic tree/view ownership is visibly in `flowmodel`
- downstream read-only consumers compile unchanged or with minimal adapter changes

### Step 3. Move semantic assembly helpers out of `contracts`

Extract the remaining semantic assembly from `workflow_contracts.go`.

Scope:

- tree construction wrapper
- package-root projection if it is canonical
- URI population
- semantic lookup helpers that are not YAML-specific

Acceptance:

- `contracts` assembles raw documents and invokes `flowmodel` for semantic tree creation

### Step 4. Introduce a semantic-source interface

Define a narrow semantic interface used by downstream packages.

Candidate surface:

- `ProjectViews()`
- `FlowViews()`
- `FlowViewByID()`
- `FlowPath()`
- `ResolvedPolicyForFlow()`
- `ResolvedEventCatalog()`
- canonical node/agent/event/tool accessors

Acceptance:

- `WorkflowContractBundle` implements the interface
- at least one downstream package depends on the interface instead of the concrete bundle

### Step 5. Migrate routing first

Refactor `internal/runtime/bus/routing_derivation.go` to consume the semantic interface, not `WorkflowContractBundle`.

Acceptance:

- routing no longer imports concrete contracts view/bundle types for read-only semantic access

### Step 6. Migrate remaining read-only consumers

Priority targets:

- `internal/runtime/contracts/prompts.go`
- `internal/runtime/pipeline/workflow_nodes.go`
- `internal/runtime/pipeline/workflow_contract_validation.go`
- `internal/runtime/manager/flow_activation.go`
- `internal/dashboard/contracts_summary.go`
- `internal/empire/factory/contracts_policy.go`

Acceptance:

- these consumers depend on semantic accessors or the semantic interface, not the loader surface

### Step 7. Reassess package-root projection

Decide whether `packageFlowTreeView(...)` is canonical model behavior or adapter residue.

If canonical:

- move it into `flowmodel`

If transitional:

- replace it with explicit root-scope modeling and delete it

### Step 8. Lock M1

Verification:

- `go test ./internal/runtime/flowmodel ./internal/runtime/contracts ./internal/runtime/pipeline ./internal/runtime/manager ./internal/runtime/bus -count=1`
- `go build ./...`
- grep confirms routing and read-only semantic consumers no longer depend on raw contracts bundle internals

## Definition of Done

`M1` is complete when:

- `flowmodel` owns the concrete semantic tree model, not just generic helpers
- `contracts` is primarily loader/assembler code
- routing no longer depends directly on contracts bundle/view types
- downstream read-only semantic consumers depend on a narrow semantic surface
- package-root projection has been explicitly classified and handled
