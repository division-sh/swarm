# Phase 8 Closeout

## Summary
Phase 8 is effectively complete.

The active `2.2.x` runtime now speaks in terms of the current node model:
- `scan-orchestrator`
- `discovery-aggregator`
- `validation-orchestrator`
- `lifecycle-orchestrator`
- `scoring-node`

The retired `pipeline-coordinator` identity is no longer the active runtime face for:
- event-bus subscription
- default runtime emits
- transition diagnostics
- validation mailbox authorship
- active scan/validation wrapper `NodeID()` values

## What Changed In Phase 8

### Active runtime identity
- Coordinator runtime subscription moved from `pipeline-coordinator` to `workflow-runtime`.
- Default runtime emit `SourceAgent` moved from `pipeline-coordinator` to `workflow-runtime`.

### Active owner labels
- Pipeline transition diagnostics now record the current runtime owner instead of the retired coordinator.
- Validation mailbox items now use `validation-orchestrator` as `from_agent`.

### Wrapper/runtime model cleanup
- `ScanCoordinator` and `ValidationGate` no longer advertise the retired coordinator as their active node identity.
- Legacy coordinator behavior is carried by explicit fallback adapters only.

### Policy/executor split
- Current runtime policy overrides are separated from legacy coordinator-only overrides.
- Executor assembly now treats the legacy coordinator path as explicit fallback, not the default model.

### Guard tightening
- Architecture guards now only allow `pipeline-coordinator` in a small explicit set of legacy compatibility files.

## Remaining Non-Test References
The remaining non-test references to `pipeline-coordinator` / `legacyPipelineCoordinatorID` are:

1. [`legacy_ids.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/legacy_ids.go)
- Defines the legacy token and compatibility helper.
- Status: acceptable legacy compatibility.

2. [`workflow_contract_validation.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_contract_validation.go)
- Allows the legacy coordinator only if the loaded bundle still contains it.
- Status: acceptable legacy compatibility.

3. [`workflow_nodes.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_nodes.go)
- Keeps a legacy-only runtime policy override branch.
- Status: acceptable legacy compatibility.

4. [`workflow_nodes_runtime.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_nodes_runtime.go)
- Keeps the explicit legacy fallback adapters and legacy-model detection.
- Status: acceptable legacy compatibility.

Everything else under `internal/runtime/pipeline` is now either:
- current-node runtime code
- tests
- compatibility assertions that verify the retired coordinator no longer appears in the active node model

## Audit Result
Closeout grep:

```bash
rg "legacyPipelineCoordinatorID|pipeline-coordinator" internal/runtime/pipeline/*.go
```

Interpretation:
- Active runtime paths: no surprising coordinator-era ownership remains.
- Legacy compatibility helpers: yes, intentionally.
- Tests: many, intentionally.

## Phase 8 Exit Assessment
Phase 8 completion: about `95/100`.

Why not `100`:
- the explicit legacy fallback helpers still exist
- they are intentionally preserved for compatibility rather than deleted outright

Why this is enough to move on:
- active runtime behavior is no longer coordinator-shaped
- the remaining coordinator references are quarantined and guarded
- further deletion of legacy fallback is later-phase cleanup, not a blocker for Phase 8 goals

## Ready For Phase 9
Phase 9 can start from here.

The next focus should be:
- continue deleting now-quarantined legacy fallback if safe
- tighten the broader non-test genericity audit beyond `pipeline/`
- prepare the codebase for the later fully declarative/generic execution phases
