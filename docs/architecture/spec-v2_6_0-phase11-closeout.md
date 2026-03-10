# Phase 11 Closeout: Bounded Handler-Order Execution

## Result

Phase 11 is complete.

The runtime now executes a materially larger handler-order subset while keeping an explicit flat-transition fallback for the remaining events whose semantics still do not align cleanly with the `v2.6.0` handler model.

## Live Handler-Order Execution Subset

These events now execute through the handler-order pre-stage lane:

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

For these events:

- handler-first candidate selection is enabled
- handler execution plans are constructed and safety-classified
- execution-order-safe events run through `executeHandlerExecutionPlanPreStage(...)`
- the runtime still returns the same flat transition outcome shape

## Remaining Flat-First Boundary

These events remain outside live handler-order execution:

- `vertical.shortlisted`
- `research.completed`
- `cto.spec_approved`
- `vertical.ready_for_review`
- `vertical.approved`
- `vertical.needs_more_data`

### Why They Remain Deferred

- `research.completed`
  - gate/data mismatch
  - the current runtime sets `g1_research` and captures research payload before the transition path
- `cto.spec_approved`
  - gate/emit mismatch
  - the current runtime sets `g3_cto` and downstream brand behavior outside the flat transition itself
- `vertical.ready_for_review`
  - guard mismatch plus implicit packaging/data semantics
  - current runtime treats packaging completion as stronger than the bare handler shape
- `vertical.approved`
  - intentionally flat-first lifecycle handoff
- `vertical.needs_more_data`
  - intentionally flat-first human/research reset

These are no longer “missed adoption work.” They are the explicit redesign boundary beyond the current `v2.6.0` migration.

## What Landed In Phase 11

### 1. Expanded handler-order execution

The execution-safe subset grew from the original operating-only tranche to include:

- operating advance events
- teardown
- revision-loop events
- validation rejection / veto kill paths

### 2. Normalized safe aliases

The runtime now treats the following handler/flat differences as safe aliases where behavior already matches:

- `advance_operating` vs flat no-op pre-stage behavior
- `revision_loop` vs `increment_revision_count`
- `kill_vertical` with outer `vertical.killed` publication in validation flows

### 3. Explicit safety boundary

The engine now distinguishes:

- candidate-safe handler-first selection
- execution-order-safe handler plans
- explicit flat-first fallback

This boundary is locked by tests rather than implied by comments.

## Acceptance

Green:

```bash
go test ./... -count=1
```

Focused handler-order coverage is also green:

```bash
go test ./internal/runtime/pipeline -run 'Test(FactoryPipelineCoordinator_Resolve(WorkflowTransitionByEvent|DerivedWorkflowTransitionByEvent|DerivedHandlerExecutionPlanByEvent)|HandlerExecutionPlan(Parity|Safety)_|FactoryPipelineCoordinator_ApplyWorkflowEventTransition_UsesHandlerExecutionPlanFor(OperatingAdvanceSubset|TeardownRequested|BuildComplete|LaunchReady|SpecValidationFailed|CTORevisionNeeded|ResearchRejected|CTOVetoed))' -count=1
```

## Exit Criteria Met

- the live handler-order execution subset is materially larger than the operating-only tranche
- the remaining events are explicitly documented as requiring later semantic redesign
- fallback boundaries remain explicit and tested
- full suite is green

## Effect On Overall `v2.6.0` Adoption

At this point, the `v2.6.0` migration is functionally complete for the current runtime model:

- package-aware recursive loading: done
- runtime bridge loading: done
- merged project/flow semantic views: done
- flow-schema semantics authoritative in validation/compliance: done
- handler-derived transition semantics: done
- bounded handler-order execution: done

What remains after Phase 11 is no longer basic `v2.6.0` adoption. It is future semantic redesign work for the deferred validation/human-review edge cases.
