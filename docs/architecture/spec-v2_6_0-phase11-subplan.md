# Phase 11 Subplan: Expand Normative Handler Execution

## Goal

Expand real handler-order execution beyond the current operating subset while preserving the explicit flat fallback boundary for unresolved semantics.

## Starting Point

Already live in handler-order execution:

- `opco.steady_state_reached`
- `opco.growth_triggered`
- `opco.growth_stabilized`
- `opco.teardown_requested`
- `build_complete`
- `launch_ready`
- `spec.validation_failed`
- `cto.spec_revision_needed`
- `research.vertical_rejected`
- `cto.spec_vetoed`

Still candidate-safe or shadowed only:

- `vertical.shortlisted`
- `research.completed`
- `cto.spec_approved`
- `vertical.ready_for_review`

Still intentionally flat-first:

- `vertical.approved`
- `vertical.needs_more_data`

## Steps

### 1. Confirm the remaining redesign-only set

Revisit the non-safe set and group by mismatch cause:

- guard mismatch
- action mismatch
- gate-setting timing mismatch
- data accumulation mismatch
- emit mismatch that reflects real side-effect drift rather than a safe alias

Done when:
- the remaining non-safe set is clearly redesign-only, not alias-cleanup

### 2. Preserve the live handler-order subset

Keep the proven-safe handler-order subset live and covered:

- `opco.steady_state_reached`
- `opco.growth_triggered`
- `opco.growth_stabilized`
- `opco.teardown_requested`
- `build_complete`
- `launch_ready`
- `spec.validation_failed`
- `cto.spec_revision_needed`
- `research.vertical_rejected`
- `cto.spec_vetoed`

Done when:
- the safe subset is explicit and fully covered by tests

### 3. Keep the unresolved set explicitly flat-first

The remaining unresolved set is:

- `vertical.shortlisted`
- `research.completed`
- `cto.spec_approved`
- `vertical.ready_for_review`
- `vertical.approved`
- `vertical.needs_more_data`

Known mismatch causes:

- `research.completed` -> gate/data mismatch
- `cto.spec_approved` -> gate/emit mismatch
- `vertical.ready_for_review` -> guard mismatch with implicit packaging/data semantics
- `vertical.approved` -> intentionally flat-first lifecycle handoff
- `vertical.needs_more_data` -> intentionally flat-first human/research reset

Done when:
- the unresolved boundary is explicit and justified

### 4. Expand live handler-order execution only behind proof

For each newly safe event:

- enable handler-order execution behind the explicit allowlist
- keep flat outcome shape stable
- preserve flat fallback for the rest

Done when:
- the allowlist grows only through tested execution-safe additions, or no more safe additions remain

### 5. Keep fallback boundary explicit

Maintain:

- clear promoted execution subset
- clear candidate-only subset
- clear intentionally flat-first subset

Done when:
- the runtime never silently flips an event from fallback to handler-order execution

## Acceptance Gate

```bash
go test ./internal/runtime/pipeline -run 'Test(FactoryPipelineCoordinator_Resolve(WorkflowTransitionByEvent|DerivedWorkflowTransitionByEvent|DerivedHandlerExecutionPlanByEvent)|HandlerExecutionPlan(Parity|Safety)_|FactoryPipelineCoordinator_ApplyWorkflowEventTransition_UsesHandlerExecutionPlanFor(OperatingAdvanceSubset|TeardownRequested|BuildComplete|LaunchReady|SpecValidationFailed|CTORevisionNeeded|ResearchRejected|CTOVetoed))' -count=1
go test ./... -count=1
```

## Definition Of Done

Phase 11 is complete when:

- the live handler-order execution subset is materially larger than the operating-only tranche, and
- the remaining events are explicitly documented as requiring a later spec/runtime semantic redesign
- fallback boundaries remain explicit and tested
- full suite is green
