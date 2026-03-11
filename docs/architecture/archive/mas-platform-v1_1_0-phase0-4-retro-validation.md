# MAS Platform v1.1.0 Phase 0-4 Retro-Validation

Date: 2026-03-10
Author: Codex implementer

## Verdict

Do not restart from Phase 0.

Phases 0-4 delivered real convergence and should remain materially complete. But under the stricter “absolute pure platformization / no exceptions” standard, they left several real gaps behind.

The correct action is:

- keep Phase 0-4 marked materially complete
- reopen a short set of concrete gaps that belong to those phases
- carry the rest forward into the newly-expanded Phases 5-8

## Reopen Now

These are real under-completions of Phases 0-4, not just later genericity cleanup.

### 1. Phase 0/1 gap: wildcard handler ownership is runtime-aware, but handler-transition execution is still exact-match only

Files:

- [workflow_contracts.go](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts.go#L344)
- [workflow_contracts.go](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts.go#L394)
- [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go#L352)
- [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go#L389)

Why this is a phase regression:

- `NodeEventHandler()` and `RuntimeEventOwners()` match wildcard handler patterns.
- `DerivedHandlerTransition()` still only does exact lookup by `eventType`.
- The transition engine uses `DerivedHandlerTransition()` when building derived transitions and execution plans.

Result:

- wildcard semantic ownership exists
- wildcard handler execution still does not fully exist

This should have been closed by the end of Phase 0/1.

### 2. Phase 0 gap: package merging is still “duplicate key or equal,” not explicit path-keyed merge conformance

Files:

- [workflow_contracts.go](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts.go#L1671)
- [workflow_contracts.go](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts.go#L1714)

Why this is a phase regression:

- merged package views still reject duplicate node IDs unless they are byte-for-byte equal
- that is weaker than explicit path-keyed merge semantics from the MAS platform model

Result:

- loader can parse the MAS tree
- loader merge behavior is not yet proven to match the MAS merger model

This belongs back on the Phase 0/5 boundary, but it is fundamentally a loader-contract-surface gap.

### 3. Phase 2 gap: generic runtime still executes handwritten Empire guard/action semantics by ID

Files:

- [workflow_hooks.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_hooks.go#L23)
- [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go#L1453)
- [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go#L1838)

Why this is a phase regression:

- `workflow_hooks.go` still declares executable guard/action IDs such as `gate_g1_research`, `all_gates_met`, `emit_opco_spinup_requested`, and `spinup_opco_org`
- `workflow_transition_engine.go` still implements these IDs through handwritten switch logic

Result:

- declarative semantics are live for many paths
- but generic runtime still contains active product-shaped guard/action execution logic

That means Phase 2 is materially complete for active path convergence, but not fully complete for genericity.

### 4. Phase 2 gap: Empire validation lifecycle is still partially implemented directly in generic Go

Files:

- [workflow_node_validation.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_node_validation.go#L18)
- [coordinator_validation.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/coordinator_validation.go#L11)

Why this is a phase regression:

- `ValidationGate.Handle()` still dispatches a long list of Empire event names directly
- validation packaging, gate mutation, revision loops, and follow-on emits still live in handwritten Go logic

Result:

- handler-first transition semantics exist
- but the validation lifecycle is still not fully declarative

This is not just naming debt. It is active runtime behavior that Phase 2 was meant to drain.

### 5. Phase 3 gap: dynamic flow instances do not yet have first-class path identity in persistence

Files:

- [workflow_instance_store.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_instance_store.go#L81)
- [workflow_instance_store.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_instance_store.go#L268)

Why this is a phase regression:

- instance persistence still reads and writes `workflow_instances.instance_id` as `uuid`
- path identity is stored indirectly through metadata such as `flow_path`
- `current_stage` also remains the persisted name rather than MAS-aligned state naming

Result:

- dynamic instances work functionally
- but path identity is not first-class in persistence

That means Phase 3 is materially complete for behavior, but not structurally complete for the MAS instance model.

### 6. Phase 4 gap: persisted lifecycle timer identity is still not exact enough

Files:

- [workflow_timer_lifecycle.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_timer_lifecycle.go#L231)
- [scheduler.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/scheduler.go#L201)
- [schedule_store.go](/Users/youmew/dev/empireai/internal/store/schedule_store.go#L25)
- [schedule_store.go](/Users/youmew/dev/empireai/internal/store/schedule_store.go#L85)

Why this is a phase regression:

- runtime `Schedule` includes `TaskID`, and lifecycle timers use it for `timer.ID`
- scheduler in memory keys on `agent_id + event_type + vertical_id + task_id`
- persisted schedule replacement/cancellation still uses only `agent_id + event_type + vertical_id`

Result:

- two timers with the same owner/event/entity scope would collapse in persistence even if they are distinct timer contracts

That is a real Phase 4 correctness gap, even if current contracts do not trigger it often.

## Carry Forward To Later Phases

These are real gaps, but they belong to the expanded Phases 5-8 rather than reopening earlier phases completely.

### 1. Generic boot is still Empire-default

Files:

- [runtime.go](/Users/youmew/dev/empireai/internal/runtime/runtime.go#L92)

This is Phase 8 boot wiring cleanup, not a reason to restart Phase 0.

### 2. Event envelope still bakes in `VerticalID`

Files:

- [types.go](/Users/youmew/dev/empireai/internal/events/types.go#L10)

This is a larger scope/entity model change and belongs to Phase 6/7.

### 3. Generic node registry still hardcodes Empire topology

Files:

- [workflow_nodes_runtime.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_nodes_runtime.go#L49)

This is real genericity debt, but it is better handled in Phase 6/8 than by reopening Phase 1.

### 4. Inbound, workspace, and store layers are still `vertical`-shaped

Files:

- [inbound.go](/Users/youmew/dev/empireai/internal/runtime/inbound.go#L67)
- [manager.go](/Users/youmew/dev/empireai/internal/runtime/workspace/manager.go#L30)

This belongs to Phase 7/8.

## Recommended Action

Do not restart earlier phases.

Instead:

1. reopen six targeted items:
   - wildcard handler execution
   - contract merger conformance
   - handwritten guard/action execution
   - handwritten validation lifecycle
   - first-class path identity in workflow persistence
   - exact persisted timer identity
2. leave the broader genericity and datastore authority work in Phases 5-8
3. write Phase 5 with explicit backflow references to these reopened items so they are not forgotten

## Practical Conclusion

Phases 0-4 were good enough to converge runtime behavior onto MAS.

They were not enough to satisfy the stricter final bar by themselves.

The missing work is now explicit, and most of it is narrower than a full restart.
