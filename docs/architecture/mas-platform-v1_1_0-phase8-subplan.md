# MAS Platform v1.1.0 Phase 8 Subplan

Date: 2026-03-11
Author: Codex implementer

## Phase 8 Goal

Finish the repo-wide genericity burn-down so the codebase can host a second product without editing generic packages.

By the end of Phase 8:

- generic code no longer embeds Empire taxonomy or Empire-only scope primitives
- remaining Empire references exist only in declarative product assets or explicitly product-owned packages
- hardcoded node registries, bus classifiers, package paths, and compatibility constants are either generic or product-owned
- the end-state is verified by a repo-wide genericity audit, not just runtime green tests

## Why This Phase Exists

The audit found that the codebase still carries thousands of Empire references across generic packages. Even when runtime behavior is converging, the repo is not yet product-agnostic if:

- the event envelope is product-shaped
- bus classification is product-shaped
- node registries are hardwired to Empire IDs
- product package paths are compiled into generic code
- manager/workspace/store layers still speak `vertical`, `factory`, `opco`, or `holding` as generic concepts

This phase is where those structural leaks are burned down deliberately instead of being left as “cleanup later.”

## Starting Point

Known structural blockers entering Phase 8:

- `internal/events/types.go` still makes `VerticalID` a first-class event-envelope field
- `internal/runtime/bus/routing.go` still classifies events with Empire namespace logic
- `internal/runtime/pipeline/workflow_nodes_runtime.go` still hardwires Empire node IDs to executor types
- `internal/runtime/contracts/workflow_contracts.go` and `internal/runtime/contracts/prompts.go` still contain Empire-root path assumptions
- `internal/runtime/manager/opco.go` still exists as compatibility product logic inside a generic package
- `internal/runtime/workspace/manager.go`, `internal/runtime/mcp/diagnostics.go`, and parts of `internal/store/` still speak in `vertical`-shaped terms
- `internal/factory/` and `internal/commgraph/empire/` still need final disposition

## Slice 8.1: Event Envelope And Scope Model

### Objective

Remove Empire scope vocabulary from the generic event envelope.

### Target files

- `internal/events/types.go`
- `internal/runtime/bus/*`
- `internal/runtime/tools/*`
- `internal/runtime/agents/*`
- `internal/runtime/workspace/*`
- `internal/runtime/mcp/*`
- `internal/store/*`

### Work

- replace `VerticalID` with a generic scope/entity/instance model
- migrate runtime callers off Empire-named scope helpers
- align storage and routing helpers to the new generic scope model

### Acceptance

- generic event structs no longer expose Empire vocabulary
- downstream packages consume generic scope data rather than `VerticalID`

## Slice 8.2: Bus And Routing Genericity

### Objective

Remove Empire event-namespace classification from generic bus/routing code.

### Target files

- `internal/runtime/bus/routing.go`
- `internal/runtime/bus/eventbus_routing.go`
- `internal/runtime/bus/eventbus_publish.go`
- `internal/runtime/bus/*test.go`

### Work

- remove hardcoded factory/opco namespace tables
- classify routing and delivery using generic contract/module metadata
- keep namespaced flow-instance routing generic instead of Empire-specific

### Acceptance

- no Empire namespace table is required in generic bus code
- routing tests assert generic behavior, not Empire prefixes

## Slice 8.3: Node Registry And Runtime Executor Registration

### Objective

Replace the hardwired Empire node registry with an extensible registration boundary.

### Target files

- `internal/runtime/pipeline/workflow_nodes_runtime.go`
- `internal/runtime/pipeline/module.go`
- `internal/runtime/pipeline/empire/module.go`
- related runtime tests

### Work

- replace hardcoded node-ID-to-executor maps with registration APIs
- move Empire node binding into product composition
- keep genuinely generic nodes inside generic runtime only

### Acceptance

- generic runtime does not require Empire node IDs to boot
- a second product can register its own node set without editing generic runtime files

## Slice 8.4: Product Package Disposition Finalization

### Objective

Finish the disposition of product-domain packages that still sit in platform-shaped locations.

### Target files

- `internal/factory/`
- `internal/commgraph/empire/`
- `internal/runtime/manager/opco.go`
- `internal/runtime/manager/bootstrap.go`

### Work

- move product-owned code behind explicit product composition boundaries
- delete dead compatibility code that no longer participates in runtime correctness
- quarantine any remaining compatibility-only product logic

### Acceptance

- no product-domain package remains on the generic runtime critical path
- compatibility product code is explicit and bounded

## Slice 8.5: Generic Package Taxonomy Sweep

### Objective

Remove residual Empire literals, constant names, and table/name assumptions from generic code.

### Target files

- `internal/runtime/`
- `internal/store/`
- `internal/commgraph/`
- `internal/events/`
- `internal/runtime/contracts/`

### Work

- rename generic types/helpers/constants that still carry Empire vocabulary
- remove hardcoded path assumptions like `contracts/empire` or `mas-platform/empire/contracts` from generic code
- move remaining product-only literals into product-owned configuration or assets

### Acceptance

- generic production files no longer gain or retain Empire literals except where explicitly approved as compatibility-only boundaries

## Slice 8.6: Final Genericity Audit Gate

### Objective

Prove the codebase is genuinely platformized.

### Target files

- repo-wide audit docs
- architecture guard tests
- compliance docs and final checklists

### Work

- run a repo-wide genericity audit excluding only explicitly out-of-scope product or dashboard areas
- classify every remaining Empire reference as:
  - product-owned acceptable
  - compatibility-only quarantine
  - blocker
- fail the phase if generic packages still require Empire code for correctness

### Acceptance

- a second-product thought experiment does not require editing generic runtime/platform packages
- remaining Empire references are intentional, documented, and outside generic runtime/platform correctness paths

## Phase 8 Order

1. Slice 8.1
2. Slice 8.2
3. Slice 8.3
4. Slice 8.4
5. Slice 8.5
6. Slice 8.6

Reason:

- the event-envelope and routing model set the vocabulary for the rest of the repo
- executor registration must become extensible before product packages can be cleanly moved
- final taxonomy cleanup is easier after structure and ownership are already generic

## Exit Gate

Phase 8 is complete only if:

- generic packages are no longer Empire-shaped in naming, routing, registration, or scope semantics
- product packages are explicit and isolated
- repo-wide genericity is proven by an adversarial audit, not inferred from partial runtime success
