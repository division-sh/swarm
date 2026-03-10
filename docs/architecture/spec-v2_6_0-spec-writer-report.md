# Spec Writer Report: `v2.6.0` Runtime Adoption

## Executive Summary

The runtime has successfully adopted the `v2.6.0` contract model far enough to be considered functionally compliant for the current engine architecture.

What is now true:

- package-aware recursive contract loading works
- `runtime_contracts` bridge loading works
- project + flow metadata is merged with provenance
- flow-schema semantics are authoritative in validation/compliance and selected runtime paths
- handler-derived transition semantics exist and are consumed by runtime/compliance
- bounded handler-order execution is live for a proven-safe subset

What is **not** true yet:

- the runtime is not fully handler-first for every event
- a small set of validation/human-review events still remain flat-transition-first by design

This is no longer a basic `v2.6.0` adoption gap. The remaining flat-first events are semantic redesign territory.

## Current Runtime Status

### Completed migration phases

- Phase 1: package-aware recursive loader
- Phase 2: bridge-first active loading
- Phase 3: merged project/flow semantic views with provenance
- Phase 4: compliance rebased to package-aware bundle semantics
- Phase 5: semantic adapter layer over flat workflow/hook docs
- Phase 6: flow-schema semantics authoritative
- Phase 7: derived handler-transition semantics in runtime/compliance
- Phase 8: handler-first candidate resolution with fallback
- Phase 9: mismatch classification and narrowed promoted set
- Phase 10: live handler-order execution for a proven-safe subset
- Phase 11: bounded closeout, documenting the remaining redesign-only set

### Verification status

Green:

```bash
go test ./... -count=1
```

## What The Runtime Now Consumes From `v2.6.0`

### Loader / packaging

The runtime now understands:

- project `package.yaml`
- recursive package trees
- flow-local:
  - `schema.yaml`
  - `nodes.yaml`
  - `events.yaml`
  - `agents.yaml`
  - `tools.yaml`
  - `policy.yaml`
- root `runtime_contracts` bridge files when present

### Semantic bundle

The bundle now exposes:

- merged nodes/events/agents/tools/policy with provenance
- flow states / initial / terminal states
- flow namespaces
- flow pins
- flow required agents
- runtime event owners
- node handler semantics
- derived handler-transition semantics

### Runtime consumers already using semantic views

The runtime/compliance stack now uses semantic bundle APIs in:

- workflow-node policy assembly
- workflow contract validation
- contract compliance
- runtime node discovery
- handler-first transition candidate resolution
- handler-first execution-plan shadowing

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

### Why these are safe

They were promoted only after the runtime proved:

- candidate resolution parity
- execution-plan parity or safe aliasing
- stable stage outcomes
- stable post-stage behavior under the existing flat transition outcome shape

### Safe alias rules now relied on by runtime

- `advance_operating` can safely behave like a flat no-op pre-stage action
- `revision_loop` can safely normalize to `increment_revision_count`
- `kill_vertical` can safely remain side-effect free in the pre-stage handler lane because the surrounding validation runtime already publishes `vertical.killed`

## Remaining Flat-First Boundary

These events remain outside live handler-order execution:

- `vertical.shortlisted`
- `research.completed`
- `cto.spec_approved`
- `vertical.ready_for_review`
- `vertical.approved`
- `vertical.needs_more_data`

### Why they remain deferred

#### `research.completed`

Blocked by:

- gate-setting timing mismatch
- data accumulation mismatch

Current runtime behavior:

- sets `g1_research`
- stores research payload/context
- then applies the transition

Handler shape alone does not yet encode that behavior identically enough to replace the current flat path safely.

#### `cto.spec_approved`

Blocked by:

- gate-setting timing mismatch
- downstream emit mismatch

Current runtime behavior:

- sets `g3_cto`
- coordinates downstream brand behavior
- then applies the transition

Again, handler-order execution is not yet a drop-in replacement.

#### `vertical.ready_for_review`

Blocked by:

- guard mismatch
- packaging/data-finalization semantics

Current runtime treats package completion and validation packaging as stronger than the bare handler shape.

#### `vertical.approved`

Intentionally deferred.

This is a lifecycle handoff event with more coupled downstream behavior than the current bounded handler-order lane is intended to absorb.

#### `vertical.needs_more_data`

Intentionally deferred.

This is a human/research reset path with state reset behavior that is still clearer in the current flat/runtime path.

## What Spec G Should Know

### 1. The `runtime_contracts` bridge is doing real work

The runtime is not yet consuming the target flow-packaged semantics directly for execution. The bridge layer is still the safe active contract surface.

That means:

- `runtime/nodes.yaml`
- `runtime/events.yaml`
- `runtime/agents.yaml`

remain important, not transitional noise.

### 2. Handler-first semantics are viable, but only in bounded slices

The engine can now:

- derive transition-like semantics from handlers
- classify parity vs mismatch
- safely promote execution for a subset

But broad “replace flat transitions with handlers” behavior is still unsafe for some validation/human-review events.

### 3. The remaining gap is semantic, not structural

The unresolved events are not blocked by loader shape or missing fields anymore.

They are blocked by genuine semantic mismatches such as:

- when gates become true
- when/where payloads are accumulated
- when side effects are emitted
- when human-review packaging is considered complete

So the next spec conversation should focus on those semantics, not on more packaging reshuffles.

## Recommendations For Future Spec Work

### If the goal is to finish handler-first execution for the deferred set

The spec should make the following clearer or more explicit:

1. **Gate-setting timing**
   - whether `research.completed` and `cto.spec_approved` are supposed to set gates as part of the handler’s own semantic step, or merely imply a transition whose guard is already satisfied

2. **Data accumulation timing**
   - whether event payload writes like:
     - `business_brief`
     - `cto_feasibility`
     - `brand`
     - `validation_kit`
   should occur before, during, or after stage advancement

3. **Side-effect equivalence**
   - whether emits such as:
     - `brand.requested`
     - `spec.revision_requested`
     - `vertical.killed`
   are normative parts of handler execution, or allowed to remain runtime-adjacent side effects outside the pure handler plan

4. **Packaging/finalization semantics**
   - `vertical.ready_for_review` is the main remaining example
   - the spec should say whether its handler semantics alone are sufficient, or whether packaging completion is intentionally richer than a simple `advances_to + finalize_validation`

### If the goal is only `v2.6.0` adoption for the current runtime

No urgent spec change is required.

The current contracts are sufficient for the adopted runtime boundary.

## Suggested Next Spec Checkpoint Topics

If Spec G wants to keep pushing past this migration, the highest-value checkpoint topics are:

1. validation gate-setting semantics
2. accumulation timing semantics
3. handler-side emit semantics vs runtime-side post-processing
4. whether `vertical.ready_for_review` should become more explicitly contract-defined

## Bottom Line

`v2.6.0` adoption is functionally complete for the current runtime architecture.

What remains is not “missing migration work.” It is a bounded set of semantic redesign questions for the deferred validation/human-review transitions.
