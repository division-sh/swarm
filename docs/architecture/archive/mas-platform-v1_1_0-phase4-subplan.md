# MAS Platform v1.1.0 Phase 4 Subplan

Date: 2026-03-10
Repo: `/Users/youmew/dev/empireai`
Parent plan: `docs/architecture/mas-platform-v1_1_0-implementation-plan.md`
Prior phase: `docs/architecture/mas-platform-v1_1_0-phase3-subplan.md`

## Goal

Phase 4 exists to replace handwritten timer conventions with durable MAS timer lifecycle semantics.

The practical outcome of this phase is:

- runtime honors contract timer `start_on` / `cancel_on`
- lifecycle timers are persisted in `workflow_instances.timer_state`
- timers are scheduled and cancelled from contract semantics, not ad hoc stage logic
- recurring boot-time timers continue to work

## Current Readout

### Timer contracts are ahead of runtime lifecycle

The runtime already parses MAS timer fields in [workflow_contracts.go](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts.go):

- `delay`
- `start_on`
- `cancel_on`
- `recurring`

The authoritative MAS contracts already declare lifecycle timers, for example:

- validation research timer in [validation/nodes.yaml](/Users/youmew/dev/empireai/docs/specs/mas-platform/empire/contracts/flows/validation/nodes.yaml)
- operating timeout timer in [operating/nodes.yaml](/Users/youmew/dev/empireai/docs/specs/mas-platform/empire/contracts/flows/operating/nodes.yaml)

But runtime behavior still only uses the older recurring bootstrap path in [runtime.go](/Users/youmew/dev/empireai/internal/runtime/runtime.go).

### Boot only provisions recurring timers

[ensureRecurringWorkflowSchedules](/Users/youmew/dev/empireai/internal/runtime/runtime.go) currently provisions only timers with `recurring: true`.

That is still correct for boot-time recurring schedules such as `timer.portfolio_digest`, and [runtime_bootstrap_test.go](/Users/youmew/dev/empireai/internal/runtime/runtime_bootstrap_test.go) already proves that stage-scoped timers are not provisioned at startup.

What is missing is durable lifecycle behavior after boot:

- start timer on matching state or event
- cancel timer on matching state or event
- rehydrate active timers on restart

### Scheduler and instance persistence are usable but underconnected

[scheduler.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/scheduler.go) already supports:

- one-shot schedules
- cron / `@every`
- explicit cancellation

[workflow_instance_store.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_instance_store.go) already persists `timer_state`, but the active runtime does not yet use that field as the source of truth for MAS lifecycle timers.

So Phase 4 is not a scheduler invention phase. It is a timer-lifecycle integration phase.

## Main Design Constraints

### Constraint 1: Recurring boot timers and lifecycle timers must stay separate

Do not force recurring boot-time schedules into the lifecycle-timer path.

We need both:

- recurring boot schedules, e.g. portfolio digest
- per-instance lifecycle timers driven by workflow state

### Constraint 2: Timer state must be restart-safe

If runtime starts a lifecycle timer, the source of truth must be persisted in `workflow_instances.timer_state`, not only in in-memory scheduler registrations.

### Constraint 3: State transitions must own timer lifecycle

Timer creation/cancellation should happen from generic workflow state/event progression, not from Empire-specific node code.

If a timer depends on entering `researching` or reaching `ready_for_review`, the trigger belongs in generic runtime transition/state machinery.

### Constraint 4: Timer events must flow through normal routing

When a timer fires, the emitted event must re-enter normal event handling with the same ownership, interception, and persistence semantics as any other event.

## Phase 4 Work Slices

### Slice 4.1: Timer Lifecycle Inventory And Typed State

Objective:

- define the persisted lifecycle-timer model and map contract timers to runtime state

Primary files:

- [workflow_instance_store.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_instance_store.go)
- [workflow_contracts.go](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts.go)
- timer-related tests

Work:

- audit current MAS timer declarations
- decide the runtime identity for a lifecycle timer record:
  - `timer_id`
  - start trigger
  - fire time
  - cancelled state
- tighten `WorkflowTimerState` if more fields are required
- add focused store tests for lifecycle timer persistence

Done when:

- runtime has a stable persisted representation for MAS lifecycle timers

### Slice 4.2: Generic Start/Cancel Hooks From Workflow Progression

Objective:

- start and cancel lifecycle timers from generic workflow semantics

Primary files:

- [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go)
- coordinator/state projection paths
- [runtime.go](/Users/youmew/dev/empireai/internal/runtime/runtime.go)

Work:

- detect state-entry and event-trigger matches for `start_on`
- detect state-entry and event-trigger matches for `cancel_on`
- schedule one-shot timers through the generic scheduler
- persist timer state into workflow instances
- cancel active timers when cancellation conditions are met

Done when:

- lifecycle timers start/cancel from contract semantics without handwritten Empire branching

### Slice 4.3: Fire Path And Restart Rehydration

Objective:

- make fired timers and restarted runtimes behave durably

Primary files:

- [scheduler.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/scheduler.go)
- [runtime.go](/Users/youmew/dev/empireai/internal/runtime/runtime.go)
- runtime bootstrap/recovery tests

Work:

- ensure fired lifecycle timers publish normal runtime events
- mark timer state fired/cancelled/idempotent as needed
- rehydrate active non-recurring lifecycle timers from persisted `timer_state` on startup/recovery
- preserve existing recurring schedule bootstrap behavior

Done when:

- restart does not lose active lifecycle timers
- recurring timers still behave as before

## Recommended Execution Order

1. Slice 4.1
2. Slice 4.2
3. Slice 4.3

Reason:

- timer persistence shape should be fixed first
- state-driven start/cancel logic depends on that shape
- restart behavior should be built after start/cancel is real

## First Slice Recommendation

Start with `Slice 4.1`.

That means:

- inventory the active MAS timer declarations
- verify whether current `WorkflowTimerState` is sufficient
- add persistence tests before wiring scheduling behavior

## What Counts As Success

Phase 4 is successful when all of the following are true:

- contract lifecycle timers start and cancel from MAS semantics
- active timers survive restart via persisted state
- recurring boot timers still pass existing tests
- timer behavior no longer depends on Empire-specific runtime conventions

## Live Status

Current state after the first implementation pass:

- `Slice 4.1` is complete enough for forward progress.
- `Slice 4.2` is materially underway.

What is now true in code:

- MAS node-level timers are promoted into the semantic bundle alongside legacy root timers.
- semantic timers now carry stable inferred `owner`, `flow_id`, and `node_id`
- shorthand node timers like `scan_timeout` resolve to their event catalog event (`timer.scan_timeout`)
- workflow stage projection now starts and cancels state-driven lifecycle timers generically from `start_on` / `cancel_on`
- no-stage declarative node handlers now start and cancel `event:*` lifecycle timers generically too
- lifecycle timer state is persisted in `workflow_instances.timer_state`
- scheduled lifecycle timers now use exact `(agent_id, event_type, vertical_id)` identity in runtime scheduling and the Postgres schedule store
- startup now restores lifecycle timers from both persisted `schedules` and persisted `workflow_instances.timer_state`

Verified on this pass:

- `go test ./internal/runtime/contracts -run 'TestLoadWorkflowContractBundle_LoadsPackageAndFlowSchemas|TestLoadWorkflowContractBundle_LoadsCurrentRootFields' -count=1`
- `go test ./internal/runtime/pipeline -run 'TestFactoryPipelineCoordinator_ReconcilesLifecycleTimersFromStageProjection|TestValidateWorkflowContracts_CurrentBundle' -count=1`
- `go test ./internal/runtime -run 'TestRuntimeStart_RestoresPersistedLifecycleSchedule|TestEnsureRecurringWorkflowSchedules_DoesNotProvisionSchedulesWhenContractsDeclareNoRecurringWorkflowTimers|TestEnsureRecurringWorkflowSchedules_DoesNotProvisionStageTimersAtStartup' -count=1`
- `go test ./internal/runtime/pipeline -run 'TestExecuteNodeHandlerPlan_EventTriggeredLifecycleTimerWithoutStageChange|TestFactoryPipelineCoordinator_ReconcilesLifecycleTimersFromStageProjection' -count=1`
- `go test ./internal/runtime -run 'TestEnsureLifecycleWorkflowSchedules_RestoresFromWorkflowTimerState|TestRuntimeStart_RestoresPersistedLifecycleSchedule' -count=1`
- `go test ./internal/store -count=1`
- `go test ./internal/runtime/pipeline -count=1`
- `go test ./internal/runtime -count=1`

The full `internal/runtime` package still fails only on the known out-of-scope dashboard architecture guard.

Remaining Phase 4 work:

- decide whether persisted `schedules` should remain a first-class restart source or become a derived cache of `timer_state`
- decide whether lifecycle timers should migrate from the root vertical workflow instance to flow-instance-local state for template flows
