# `v2.2.0` Adoption Plan

Status: active working brief
Spec source: `docs/specs/empireai-v2_2_0/contracts-v220`
Prepared: 2026-03-08

## Summary

`v2.2.0` is a structural runtime change, not a point compliance update.

The main architectural shift is:

- remove the monolithic `pipeline-coordinator` system-node role
- replace it with explicit system nodes:
  - `scan-orchestrator`
  - `discovery-aggregator`
  - `validation-orchestrator`
  - `lifecycle-orchestrator`
  - `scoring-node` remains
- drive runtime observation and dispatch from `event-catalog.yaml` via:
  - `runtime_handling`
  - `owning_node`
- keep workflow transitions in `workflow-schema.yaml`, but stop treating static interceptor subscription breadth as the routing authority

This spec is the contract-side replacement for the older interceptor/runtime overlay.

## Immediate Contract Deltas From `v2.1.0`

### System-node model

`system-nodes.yaml` is rewritten around 5 focused system nodes.

Key changes:

- `pipeline-coordinator` is removed as the main runtime interceptor role
- `scan-orchestrator` now owns:
  - `scan.requested`
  - `*.scan_complete`
  - `timer.scan_timeout`
  - `timer.campaign_deadline`
- `discovery-aggregator` now owns:
  - `category.assessed`
  - `trend.identified`
  - `source.scraped`
  - `dedup.resolved`
  - `synthesis.resolved`
- `validation-orchestrator` now owns the factory 4-gate workflow
- `lifecycle-orchestrator` now owns approval/handoff/operating/timer lifecycle behavior
- handler logic is partially encoded in YAML under `event_handlers`
- node-local state schemas are now declared in contracts

### Event-catalog routing

`event-catalog.yaml` now explicitly drives runtime routing semantics:

- `runtime_handling` is populated on many more events
- `owning_node` is introduced broadly
- many events become:
  - `consuming`
  - `dual_delivery`
  - `passthrough`

This means routing should now be derived from the catalog first, not from ad hoc node policy tables.

### Workflow schema

`workflow-schema.yaml` changes include:

- version bumps to `2.2.0`
- transition node ownership remapped from `pipeline-coordinator` to the new orchestrator nodes
- timer owners remapped from `pipeline-coordinator` / `runtime` / `empire-coordinator` to the new orchestrators
- `entity_schema` added
- `data_accumulation` added to multiple transitions
- `winding_down` remains non-terminal

### DDL / platform state

The `workflow_instances` platform table is now explicitly part of the audit story in `2.2.0`, which reinforces the direction already started in code.

## Current Runtime Position Before Cutover

The codebase is much better positioned than it was for `v2.1.0`, but several areas still assume the older runtime shape:

- generic `pipeline/` still dispatches through the current coordinator/scoring-node runtime shell
- the system-node executor model does not yet match the 5-node `v2.2.0` decomposition
- runtime event observation still has compatibility behavior that assumes a coordinator-centric model
- the event-catalog-driven routing layer exists, but it does not yet fully replace the current node/interceptor dispatch shape

## Adoption Strategy

Do this in order.

### Phase 1: Spec intake without destructive copy

Objective: treat `docs/specs/empireai-v2_2_0/contracts-v220` as the authoritative incoming spec while the repo still contains in-flight spec file moves.

Tasks:

1. Diff the incoming `v2.2.0` contracts against root `contracts/*`.
2. Identify first-order breakpoints in:
   - `system-nodes.yaml`
   - `event-catalog.yaml`
   - `workflow-schema.yaml`
   - `verification-gates.yaml`
   - `ddl-canonical.sql`
3. Do not overwrite root `contracts/*` until the file move under `docs/specs/` is stable and deliberate.

Exit criteria:

- `v2.2.0` breakpoints are mapped
- no accidental overwrite of user/spec-writer in-flight spec files

### Phase 2: Replace coordinator-centric routing assumptions

Objective: make the runtime read `owning_node` + `runtime_handling` as the real routing authority.

Tasks:

1. Extend contract loading to expose:
   - `owning_node`
   - richer `runtime_handling`
   - node handler metadata where needed
2. Replace coordinator-centric routing assumptions in:
   - `internal/runtime/pipeline/workflow_nodes.go`
   - event-policy derivation
   - runtime handler coverage checks
3. Introduce a contract-driven node dispatch layer that can route runtime-observed events to:
   - `scoring-node`
   - `scan-orchestrator`
   - `discovery-aggregator`
   - `validation-orchestrator`
   - `lifecycle-orchestrator`

Exit criteria:

- runtime dispatch is catalog-first
- `pipeline-coordinator` is no longer a required routing fiction

### Phase 3: Decompose current coordinator runtime into explicit node executors

Objective: move from one coordinator shell to the 5-node model required by `v2.2.0`.

Tasks:

1. Introduce new runtime executors:
   - `scan_orchestrator.go`
   - `discovery_aggregator.go`
   - `validation_orchestrator.go`
   - `lifecycle_orchestrator.go`
2. Move the relevant handler logic out of the current coordinator pathways.
3. Keep shared services/state access reusable; do not duplicate persistence logic per node.
4. Keep `scoring-node` aligned with the new handler model.

Exit criteria:

- runtime has explicit executors for the contract nodes
- node ownership matches `system-nodes.yaml`

### Phase 4: Align timer ownership and event handling to `v2.2.0`

Objective: move timer fire handling to the new owning nodes.

Tasks:

1. Reassign timer runtime consumers to:
   - `scan-orchestrator` for `timer.scan_timeout`, `timer.campaign_deadline`
   - `lifecycle-orchestrator` for `timer.marginal_review`, `timer.marginal_kill`, `timer.portfolio_digest`
2. Remove leftover assumptions that these flow through the old coordinator role.
3. Keep the scheduler/store model, but make handler ownership contract-aligned.

Exit criteria:

- timer ownership/consumption matches `workflow-schema.yaml` and `event-catalog.yaml`

### Phase 5: Implement handler-driven node behavior from `system-nodes.yaml`

Objective: align code behavior with the new `event_handlers` contract surface.

Tasks:

1. Map each runtime node handler to the corresponding YAML handler description.
2. Prefer table-driven dispatch from contract metadata where reasonable.
3. Preserve code-based business logic where needed, but keep the handler/action mapping contract-first.

Key areas:

- scoring completion and composite classification
- scan timeout / campaign deadline
- discovery accumulation and dedup/synthesis outcomes
- validation gate progression
- lifecycle approval / OpCo handoff / operating transitions

Exit criteria:

- code paths can be traced directly back to `system-nodes.yaml` handlers

### Phase 6: Consume `entity_schema` and `data_accumulation`

Objective: use the new workflow-schema metadata instead of ad hoc field mapping.

Tasks:

1. Extend contract parsing for:
   - `entity_schema`
   - `data_accumulation`
2. Use `data_accumulation` as the source for workflow/entity projection rules where practical.
3. Compare declared entity fields with actual DB/runtime projections.

Exit criteria:

- the new schema metadata is loaded and validated
- projection logic starts converging on contract-declared field ownership

### Phase 7: Re-run compliance under `v2.2.0`

Objective: make the existing contract/runtime compliance suite understand the new system-node model.

Tasks:

1. Update compliance tests to validate:
   - zero intercepted/runtime-handled events without `owning_node`
   - node subscription coverage against the new nodes
   - handler coverage for timer and lifecycle events
   - `workflow_instances` usage expectations
2. Remove checks that assume `pipeline-coordinator` owns broad runtime observation.

Exit criteria:

- contract compliance passes against root `2.2.0` contracts

### Phase 8: Copy `2.2.0` contracts into root and finish cutover

Objective: switch the codebase from `2.1.0` root contracts to authoritative `2.2.0`.

Tasks:

1. Copy `docs/specs/empireai-v2_2_0/contracts-v220/*` into root `contracts/*`.
2. Regenerate/update any contract-derived runtime artifacts if needed.
3. Fix the resulting breakpoints until:
   - runtime starts
   - contract compliance passes
   - pipeline tests pass
   - full suite is green

Exit criteria:

- root `contracts/*` are `2.2.0`
- runtime is green against them

## First Concrete Breakpoints To Expect

These are the first files likely to need changes:

- [`internal/runtime/contracts/workflow_contracts.go`](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts.go)
- [`internal/runtime/pipeline/workflow_nodes.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_nodes.go)
- [`internal/runtime/pipeline/coordinator.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/coordinator.go)
- [`internal/runtime/pipeline/workflow_contract_validation.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_contract_validation.go)
- [`internal/runtime/contract_compliance_test.go`](/Users/youmew/dev/empireai/internal/runtime/contract_compliance_test.go)
- [`internal/runtime/runtime.go`](/Users/youmew/dev/empireai/internal/runtime/runtime.go)

Likely new files:

- [`internal/runtime/pipeline/scan_orchestrator.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/scan_orchestrator.go)
- [`internal/runtime/pipeline/discovery_aggregator.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/discovery_aggregator.go)
- [`internal/runtime/pipeline/validation_orchestrator.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/validation_orchestrator.go)
- [`internal/runtime/pipeline/lifecycle_orchestrator.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/lifecycle_orchestrator.go)

## Acceptance Gates

During adoption:

```bash
go test ./internal/runtime -run TestContractCompliance -count=1
go test ./internal/runtime/pipeline -count=1
go test ./internal/runtime -count=1
go test ./... -count=1
```

## Notes

- The current working tree already has an in-flight spec file move from `docs/toreview/...` into `docs/specs/empireai-v2_2_0/...`. Do not flatten or revert that move casually.
- `2.2.0` is the spec-side replacement for the old runtime interceptor model. The implementation goal is to finish that replacement, not to preserve the old coordinator abstraction under a new name.
