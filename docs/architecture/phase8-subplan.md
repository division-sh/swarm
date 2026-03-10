# Phase 8 Sub-Plan

## Goal
Finish removing coordinator-era runtime language and assumptions from active `2.2.x` runtime behavior, while preserving explicit legacy compatibility only where still required.

## Current State
- Active runtime defaults already use `workflow-runtime` instead of `pipeline-coordinator`.
- Transition diagnostics now record current node owners instead of the retired coordinator.
- Validation mailbox authorship now uses `validation-orchestrator`.
- The remaining work is concentrated in a few active compatibility seams, not scattered across the runtime.

## Remaining Work

### 1. Retire legacy wrapper node identities from active runtime paths
- Replace `ScanCoordinator.NodeID()` and `ValidationGate.NodeID()` as active runtime identities.
- Keep legacy coordinator naming only in explicitly legacy fallback code if still needed.
- Exit bar:
  - active executor selection and policy lookup for scan/validation no longer depend on `legacyPipelineCoordinatorID`

### 2. Remove legacy-coordinator keyed runtime policy from current node flow
- Move runtime policy overrides in `workflow_nodes.go` from `legacyPipelineCoordinatorID` to the real current owners:
  - `scan-orchestrator`
  - `validation-orchestrator`
  - `lifecycle-orchestrator`
- Preserve only true legacy-mode overrides behind an explicit legacy branch.
- Exit bar:
  - current `2.2.x` runtime nodes own their own policy overrides directly

### 3. Remove legacy coordinator from executor-selection logic
- Simplify `workflowNodeExecutors()` and related runtime assembly so current node selection does not branch on the presence of `legacyPipelineCoordinatorID`.
- Keep a narrow compatibility path only if the bundle actually loads a legacy runtime model.
- Exit bar:
  - active `2.2.x` runtime assembly is expressed only in terms of the 5 current nodes

### 4. Quarantine legacy compatibility paths
- Identify the remaining runtime files where `legacyPipelineCoordinatorID` is still referenced.
- Classify each reference as one of:
  - current-runtime required
  - legacy-mode fallback
  - dead/replaceable
- Move legacy-only references behind explicit helpers or comments so they stop looking like active architecture.
- Exit bar:
  - every remaining `legacyPipelineCoordinatorID` use is clearly compatibility-only

### 5. Tighten architecture guards for the end state
- Update `pipeline_architecture_test.go` so the legacy coordinator token is allowed only in explicitly approved compatibility files.
- Add assertions that active runtime policy/executor paths use current node IDs instead.
- Exit bar:
  - reintroduction of coordinator-era active behavior fails fast in tests

### 6. Run closeout audit
- Re-run:
  - `rg "pipeline-coordinator|legacyPipelineCoordinatorID" internal/runtime/pipeline`
- Separate results into:
  - active-runtime paths
  - tests
  - explicit legacy compatibility helpers
- Exit bar:
  - no surprising active-runtime coordinator references remain

## Suggested Execution Order
1. Move runtime policy overrides to current owners
2. Remove legacy node identity from scan/validation active paths
3. Simplify executor selection / runtime assembly
4. Quarantine remaining compatibility references
5. Tighten guards
6. Audit and close

## Definition Of Done
- Active `2.2.x` runtime behavior no longer identifies itself through `pipeline-coordinator`
- The 5-node model is the only active runtime architecture in `internal/runtime/pipeline`
- Remaining coordinator references are limited to:
  - tests
  - explicit legacy compatibility helpers
  - well-labeled migration-only code
