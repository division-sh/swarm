# Phase 10 Subplan: Normative Handler Execution Order

## Goal

Start implementing the `v2.6.0` normative handler execution order as the real runtime model without destabilizing the current hybrid execution boundary.

The key rule for this phase:

- promoted handler-first events may move deeper into handler-driven execution
- intentionally flat-first events stay on the flat transition path unless and until their semantic mismatches are resolved

## Current Safe Boundary

### Promoted handler-first candidate set

- `vertical.shortlisted`
- `research.completed`
- `cto.spec_approved`
- `vertical.ready_for_review`
- `research.vertical_rejected`
- `spec.validation_failed`
- `cto.spec_revision_needed`
- `opco.steady_state_reached`
- `opco.growth_triggered`
- `opco.growth_stabilized`
- `build_complete`
- `launch_ready`
- `opco.teardown_requested`

### Intentionally flat-first set

- `vertical.approved`
- `vertical.needs_more_data`

## Steps

### 1. Introduce a handler-execution plan type

Build a normalized internal representation for one handler execution:

- guard
- accumulate
- compute
- on_complete
- advances_to
- sets_gate
- data_accumulation
- emits
- rules
- action hook

Done when:
- the transition engine can construct a handler execution plan without executing it

### 2. Build a read-only execution-plan shadow path

For promoted events only, derive the execution plan and compare it to the current flat transition execution shape.

Done when:
- shadow comparisons can classify plan-shape parity without changing runtime behavior

### 3. Promote the first execution-order-safe subset

Move a narrow subset of promoted events from:
- handler-first candidate selection only

to:
- handler-first execution-plan ordering for the pre-stage semantic steps

Likely initial subset:
- `research.completed`
- `cto.spec_approved`
- `build_complete`
- `launch_ready`

Done when:
- the first safe subset executes through the new plan ordering and still passes the full suite

### 4. Preserve flat fallback for the unsafe set

Keep these events flat-first:
- `vertical.approved`
- `vertical.needs_more_data`

Done when:
- the engine explicitly routes these through the old flat path by design

### 5. Expand plan parity coverage

Add tests for:
- plan construction
- plan shadow parity
- promoted execution-order subset
- preserved fallback subset

Done when:
- the promoted/fallback boundary is fully locked by tests

### 6. Close Phase 10 only when execution order is real

Phase 10 is complete when:
- a nontrivial promoted subset actually executes through handler-order semantics
- fallback remains explicit for unresolved cases
- full suite is green

## Acceptance Gate

```bash
go test ./internal/runtime/pipeline -run 'TestFactoryPipelineCoordinator_Resolve(WorkflowTransitionByEvent|DerivedWorkflowTransitionByEvent)' -count=1
go test ./... -count=1
```
