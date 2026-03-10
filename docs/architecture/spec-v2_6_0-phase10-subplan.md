# Phase 10 Subplan: Normative Handler Execution Order

## Goal

Start implementing the `v2.6.0` normative handler execution order as the real runtime model without destabilizing the current hybrid execution boundary.

The key rule for this phase:

- promoted handler-first events may move deeper into handler-driven execution
- intentionally flat-first events stay on the flat transition path unless and until their semantic mismatches are resolved

## Current Safe Boundary

### Candidate-promoted set

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

### Execution-order-safe set

- `opco.steady_state_reached`
- `opco.growth_triggered`
- `opco.growth_stabilized`
- `opco.teardown_requested`
- `build_complete`
- `launch_ready`
- `spec.validation_failed`
- `cto.spec_revision_needed`

These currently execute through handler-order pre-stage semantics while still returning the same flat transition outcome shape.

### Candidate-safe but execution-unsafe set

- `vertical.shortlisted`
- `research.completed`
- `cto.spec_approved`
- `vertical.ready_for_review`
- `research.vertical_rejected`

Current mismatch reasons:

- `vertical.ready_for_review` -> `guard_mismatch`
- `research.vertical_rejected` -> `action_mismatch`
- `research.completed` -> gate/data mismatch
- `cto.spec_approved` -> gate/emit mismatch

### Intentionally flat-first set

- `vertical.approved`
- `vertical.needs_more_data`

## Current Checkpoint

Already complete:

- handler-first candidate promotion for the candidate-promoted set above
- shadow parity classification for promoted vs flat transitions
- mismatch inventory for the intentionally flat-first set
- normalized revision-loop alias handling for candidate parity:
  - `spec.validation_failed`
  - `cto.spec_revision_needed`
- execution-safe normalization for:
  - `build_complete`
  - `launch_ready`
  - `spec.validation_failed`
  - `cto.spec_revision_needed`
- shadow-only execution-plan model in the transition engine
- focused execution-plan parity coverage for:
  - `research.completed`
  - `cto.spec_approved`
  - `vertical.ready_for_review`
  - `research.vertical_rejected`

## Steps

### 1. Finish the shadow execution-plan lane

Keep the execution-plan model read-only, but extend it enough to support a safe first promotion tranche.

This includes:

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
- the transition engine can construct and compare handler execution plans for the promoted subset without executing them

### 2. Classify the promoted subset by execution-order safety

Split the promoted subset into:

- execution-order-safe now
- candidate-safe but execution-order-risky

Current proven safe tranche:

- `opco.steady_state_reached`
- `opco.growth_triggered`
- `opco.growth_stabilized`
- `opco.teardown_requested`

Done when:
- the first execution-order-safe tranche is explicitly documented and covered by tests

### 3. Introduce pre-stage handler-order execution for the first safe tranche

Move a narrow subset of promoted events from:
- handler-first candidate selection only

to:
- handler-first execution-plan ordering for the pre-stage semantic steps

Scope for the first cut:

- allow handler-order execution only for the explicitly safe tranche
- still return or mirror the same flat transition outcome shape
- keep stage update and post-stage hooks stable

Done when:
- the first safe subset executes through the new plan ordering and still passes the full suite

### 4. Preserve flat fallback for the unresolved set

Keep these events flat-first:
- `vertical.approved`
- `vertical.needs_more_data`

Also keep any promoted-but-not-yet-execution-safe events on candidate-only promotion until their execution parity is proven.

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

### 6. Close Phase 10 only when handler-order execution is real

Phase 10 is complete when:
- a nontrivial promoted subset actually executes through handler-order semantics
- fallback remains explicit for unresolved cases
- the promoted execution-order-safe tranche is documented
- full suite is green

## Acceptance Gate

```bash
go test ./internal/runtime/pipeline -run 'Test(FactoryPipelineCoordinator_Resolve(WorkflowTransitionByEvent|DerivedWorkflowTransitionByEvent|DerivedHandlerExecutionPlanByEvent)|HandlerExecutionPlan(Parity|Safety)_|FactoryPipelineCoordinator_ApplyWorkflowEventTransition_UsesHandlerExecutionPlanFor(OperatingAdvanceSubset|TeardownRequested|BuildComplete|LaunchReady|SpecValidationFailed|CTORevisionNeeded))' -count=1
go test ./... -count=1
```
