# Phase 7 Execution Gap

## Purpose

Record exactly what is still transition-first after Phase 7 semantic bridging, so Phase 8 can start execution migration without rediscovering the remaining gap.

## What Phase 7 Now Covers

- Recursive package-aware contract loading
- Merged project/flow contract views with provenance
- Semantic bundle accessors for:
  - workflow stages
  - flow states / terminals / pins / required agents
  - node handlers
  - runtime event owners
- Derived handler-transition semantics for:
  - `advances_to`
  - `guard`
  - `sets_gate`
  - `data_accumulation`
  - `emits`
  - `action`
  - `completion_rule`
  - `condition`
  - `on_complete`
  - `rules`
- Read-only consumers now using derived semantics:
  - workflow-node policy assembly
  - workflow contract validation
  - contract compliance
  - runtime node discovery fallback

## What Is Still Transition-First

These runtime paths still fundamentally execute against flat workflow transitions and hook registries:

1. `internal/runtime/pipeline/workflow_transition_engine.go`
- Candidate transition lookup
- Guard/action sequencing
- Stage mutation
- Implicit platform actions
- Transition history recording

2. `internal/runtime/pipeline/workflow.go`
- Runtime workflow assembly still centers the flat transition list as the active execution graph

3. `internal/runtime/pipeline/guard_action_registry.go`
- Hook execution still follows flat transition-owned guard/action references

4. Timer lifecycle execution
- Timers are still attached to the flat workflow transition/timer model during runtime execution

5. Some ownership/coverage checks
- Validation/compliance now use derived semantics in several places, but flat transition coverage remains the authoritative fallback

## What Phase 8 Must Do

1. Introduce an execution-facing internal transition view derived from handlers
- Preserve flat transitions as fallback at first

2. Move candidate transition resolution off direct flat transition iteration

3. Preserve current execution order while proving handler-derived equivalence

4. Keep timer and hook execution stable until derived transition execution is trusted

## Success Condition For Leaving This Gap Behind

We can leave this gap behind when:

- the transition engine resolves candidate transitions from handler-derived semantics first
- flat transitions are fallback only
- hook execution and timer ownership still remain stable
- full suite stays green
