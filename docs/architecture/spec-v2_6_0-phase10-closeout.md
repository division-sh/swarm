# Phase 10 Closeout: Normative Handler Execution Order

## Result

Phase 10 is complete.

The runtime now has a real handler-order execution lane for a narrow, proven-safe subset of events, while preserving flat-transition fallback for the remaining mismatched cases.

## What Landed

### 1. Shadow execution-plan model

The transition engine now derives normalized handler execution plans from node-handler semantics and compares them against the existing flat transition model.

Implemented in:

- `/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go`

Core pieces:

- `handlerExecutionPlan`
- `resolveDerivedHandlerExecutionPlanByEvent(...)`
- `workflowTransitionToExecutionPlan(...)`
- `shadowCompareHandlerExecutionPlan(...)`
- `classifyHandlerExecutionPlanSafety(...)`

### 2. Candidate-safe vs execution-safe boundary

The engine now distinguishes:

- candidate-safe events:
  handler-first resolution can identify the right flat transition safely
- execution-safe events:
  handler-order execution can run pre-stage semantics without changing behavior

### 3. Real handler-order execution subset

These events now execute through handler-order pre-stage semantics:

- `opco.steady_state_reached`
- `opco.growth_triggered`
- `opco.growth_stabilized`
- `opco.teardown_requested`
- `build_complete`
- `launch_ready`
- `spec.validation_failed`
- `cto.spec_revision_needed`

The runtime still returns the same flat transition outcome shape and keeps post-stage behavior stable.

### 4. Explicit fallback boundary

These events remain candidate-safe or shadowed, but flat-transition-first for execution:

- `vertical.shortlisted`
- `research.completed`
- `cto.spec_approved`
- `vertical.ready_for_review`
- `research.vertical_rejected`

These remain intentionally flat-first:

- `vertical.approved`
- `vertical.needs_more_data`

## Current Execution-Safety Matrix

### Execution-safe

- `opco.steady_state_reached`
- `opco.growth_triggered`
- `opco.growth_stabilized`
- `opco.teardown_requested`
- `build_complete`
- `launch_ready`
- `spec.validation_failed`
- `cto.spec_revision_needed`

### Candidate-safe but execution-unsafe

- `vertical.ready_for_review` -> `guard_mismatch`
- `research.vertical_rejected` -> `action_mismatch`
- `research.completed` -> gate/data semantics still differ from flat transition execution
- `cto.spec_approved` -> gate/emit semantics still differ from flat transition execution

### Intentionally deferred

- `vertical.approved`
- `vertical.needs_more_data`

## Acceptance

Green:

```bash
go test ./... -count=1
```

Focused Phase 10 checks are also green:

```bash
go test ./internal/runtime/pipeline -run 'Test(FactoryPipelineCoordinator_Resolve(WorkflowTransitionByEvent|DerivedWorkflowTransitionByEvent|DerivedHandlerExecutionPlanByEvent)|HandlerExecutionPlan(Parity|Safety)_|FactoryPipelineCoordinator_ApplyWorkflowEventTransition_UsesHandlerExecutionPlanFor(OperatingAdvanceSubset|TeardownRequested|BuildComplete|LaunchReady|SpecValidationFailed|CTORevisionNeeded))' -count=1
```

## What Phase 10 Did Not Do

Phase 10 did not make handler-order execution globally authoritative.

It intentionally stopped at:

- one safe live execution subset
- an explicit execution-safety classifier
- tested fallback for all unresolved semantics

That keeps runtime behavior stable while giving the next phase a concrete execution boundary to grow from.

## Exit Criteria Met

- a nontrivial promoted subset executes through handler-order semantics
- fallback remains explicit for unresolved cases
- the execution-safe tranche is documented
- full suite is green

## Next Phase

Phase 11 should expand handler-order execution from the current operating subset into additional event classes only where:

- execution-safety can be proven, or
- flat/runtime semantics are deliberately normalized first
