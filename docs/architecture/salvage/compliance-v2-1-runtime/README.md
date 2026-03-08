# Compliance Worktree Salvage List

Source worktree: `/Users/youmew/dev/empireai-compliance`
Source branch: `compliance/v2-1-runtime`
Source HEAD at audit time: `db0316e`
Audit date: `2026-03-08`

## Purpose

This folder preserves the parts of the stale compliance worktree that still look useful after `main` advanced past it.

The compliance worktree is not an ahead branch. It is an old checkout with a large dirty tree. Most of its diff against `main` is stale divergence. The artifacts saved here are the small subset that still look worth mining later.

## What Was Preserved

### Candidate source files

These only existed in the compliance worktree and are preserved here as raw source files for later reference:

- [`internal/runtime/pipeline/workflow_state_projection.go`](/Users/youmew/dev/empireai/docs/architecture/salvage/compliance-v2-1-runtime/internal/runtime/pipeline/workflow_state_projection.go)
- [`internal/runtime/pipeline/workflow_runtime_coverage.go`](/Users/youmew/dev/empireai/docs/architecture/salvage/compliance-v2-1-runtime/internal/runtime/pipeline/workflow_runtime_coverage.go)
- [`internal/runtime/pipeline/scan_policy.go`](/Users/youmew/dev/empireai/docs/architecture/salvage/compliance-v2-1-runtime/internal/runtime/pipeline/scan_policy.go)
- [`internal/runtime/pipeline/empire/scan_policy.go`](/Users/youmew/dev/empireai/docs/architecture/salvage/compliance-v2-1-runtime/internal/runtime/pipeline/empire/scan_policy.go)
- [`contracts/test-vectors/e2e-happy-path/opco-ceo.yaml`](/Users/youmew/dev/empireai/docs/architecture/salvage/compliance-v2-1-runtime/contracts/test-vectors/e2e-happy-path/opco-ceo.yaml)
- [`contracts/test-vectors/e2e-happy-path/cto-agent.yaml`](/Users/youmew/dev/empireai/docs/architecture/salvage/compliance-v2-1-runtime/contracts/test-vectors/e2e-happy-path/cto-agent.yaml)

### Focused patch files

These preserve the most relevant modified-file deltas from the compliance worktree:

- [`patches/coordinator_state.patch`](/Users/youmew/dev/empireai/docs/architecture/salvage/compliance-v2-1-runtime/patches/coordinator_state.patch)
- [`patches/coordinator_scan_compat.patch`](/Users/youmew/dev/empireai/docs/architecture/salvage/compliance-v2-1-runtime/patches/coordinator_scan_compat.patch)
- [`patches/workflow_node_validation.patch`](/Users/youmew/dev/empireai/docs/architecture/salvage/compliance-v2-1-runtime/patches/workflow_node_validation.patch)
- [`patches/workflow_transition_engine_test.patch`](/Users/youmew/dev/empireai/docs/architecture/salvage/compliance-v2-1-runtime/patches/workflow_transition_engine_test.patch)

## Salvage Assessment

### High-value salvage

- `workflow_state_projection.go`
  This is the most valuable preserved artifact. It pushes the `workflow_instances` cutover much further by:
  - projecting validation state, scoring state, scan accumulators, and pending dedup into workflow-backed buckets
  - restoring those projections back into live runtime state
  - introducing scan-timeout scheduling tied to workflow-backed scan projections
  Use this as source material for the final canonical-state cutover.

- `workflow_transition_engine_test.patch`
  This contains useful operating-flow and guard-behavior tests:
  - `approved -> full_speccing`
  - `full_speccing -> building`
  - `building -> pre_launch`
  - `pre_launch -> launched`
  - `launched -> operating`
  - growth transitions
  - teardown guard behavior
  - persisted evidence behavior
  Reapply these against the newer runtime shape when the next spec lands.

- `opco-ceo.yaml` and `cto-agent.yaml`
  These are good seed fixtures for operating-mode end-to-end scenarios and should be revisited when the next spec formalizes the system-node replacement for interceptor behavior.

### Medium-value salvage

- `workflow_runtime_coverage.go`
  Useful for intent, not for direct copy. It introduces stronger runtime coverage validation for executable hooks and participant visibility, but it is coupled to an older module shape and older allowlist strategy.

- `coordinator_scan_compat.patch`
  Contains practical implementations for:
  - `timer.scan_timeout`
  - `timer.marginal_kill`
  Those are worth mining when timer platformization resumes.

- `workflow_node_validation.patch`
  Useful as a checklist of operating evidence and event coverage:
  - `qa.validation_passed`
  - `review.deploy_feedback`
  - `build_complete`
  - `launch_ready`
  - `opco.steady_state_reached`
  - `opco.growth_triggered`
  - `opco.growth_stabilized`
  It should be reapplied only after checking the latest spec and current `main`.

- `scan_policy.go` and `empire/scan_policy.go`
  These are an earlier scan-policy extraction pass. The direction is still useful, but `main` has already moved some of this responsibility elsewhere. Treat them as behavioral reference only.

### Low-value or superseded salvage

- `coordinator_state.patch`
  The intent is good, but `main` has already implemented part of this restore precedence and mutation work. This patch is more useful for spotting missed edge cases than for direct replay.

## What To Avoid Reapplying Blindly

Do not salvage the following directly from the compliance worktree:

- contract edits under `contracts/*`
- config edits under `configs/agents/*`
- old module/runtime-interface rewrites that reintroduce Empire-specific loaders or remove newer hook APIs

Those changes were tied to an older point in the migration and are now likely to conflict with the authoritative contracts and the newer platformization work already on `main`.

## Suggested Reuse Order

1. Revisit `workflow_state_projection.go` during the final `workflow_instances` source-of-truth cutover.
2. Reapply the operating-flow tests from `workflow_transition_engine_test.patch`.
3. Reuse the two operating e2e fixtures once the next spec clarifies system-node ownership.
4. Mine `coordinator_scan_compat.patch` when finishing timer platformization.
5. Rework `workflow_runtime_coverage.go` into the current compliance/architecture gate model instead of copying it directly.
