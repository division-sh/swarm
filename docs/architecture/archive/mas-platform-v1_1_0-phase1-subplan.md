# MAS Platform v1.1.0 Phase 1 Subplan

Date: 2026-03-10
Repo: `/Users/youmew/dev/empireai`
Parent plan: `docs/architecture/mas-platform-v1_1_0-implementation-plan.md`
Failure inventory: `docs/architecture/mas-platform-v1_1_0-mas-default-failure-inventory.md`

## Goal

Phase 1 exists to make MAS-default runtime execution real rather than nominal.

The practical outcome of this phase is:

- the runtime executes the authoritative MAS package tree by default
- the current deferred validation/operating events run handler-first under MAS semantics
- bridge-era test and runtime assumptions stop blocking progress
- remaining red tests mostly represent real runtime/spec gaps, not obsolete expectations

## Current Readout

### MAS-default is active

The Empire runtime module now loads:

- `docs/specs/mas-platform/empire/contracts`
- `docs/specs/mas-platform/platform/contracts/platform-spec.yaml`

That means Phase 1 is no longer about preparing for MAS. It is about migrating the runtime and tests to the MAS baseline.

### The current failures are structured

The first full MAS-default runs show three clear buckets:

1. `keep`

These should stay blocking:

- MAS-invalid validation assumptions in `workflow_contract_validation.go`
- runtime/bootstrap failures caused by those validation errors
- likely real runtime bugs such as duplicate emits and consumed-vs-processed mismatches

2. `rewrite`

These still test useful intent, but the assertions are stale:

- flat transition identity and alias parity assertions
- five-node topology assertions
- legacy workflow instance projection assumptions
- canned LLM scenarios tied to the old validation event sequence

3. `delete`

These preserve obsolete architecture and should not be carried forward:

- tests whose sole purpose is to preserve bridge-era flat-transition identity
- tests whose sole purpose is to preserve the five-node runtime model

### The main risk

The main risk in Phase 1 is not code churn. It is false confidence from stale tests.

If we keep treating bridge-era tests as fixed targets, we will either:

- reintroduce legacy compatibility behavior that MAS is explicitly replacing, or
- stall the migration behind obsolete assertions

### Progress Update

The first two slices are now materially underway:

- Slice 1.1:
  - MAS validation/boot alignment landed for current-bundle runs
  - explicit MAS override loading no longer falls back into the legacy root schema/hook registry
- Slice 1.2:
  - MAS node topology tests were rebaselined
  - `portfolio-node` executor coverage was added for MAS-owned portfolio/runtime events
  - scan workflow projection now resolves `expected_scanners` from MAS rule fan-out targets
  - workflow-instance projection tests were migrated to MAS workflow identity helpers
  - state-machine / contract-verification tests no longer preserve legacy flat workflow identity

Current readout:

- targeted MAS topology/projection/runtime coverage is green
- the full `pipeline` suite is green
- targeted `internal/runtime` MAS-default coverage is green, including:
  - Scenario 5 revision loop
  - Scenario 6 mailbox more-data loop
  - Scenario 8 budget threshold scenario
  - full directive-to-OpCo happy path
  - recurring timer bootstrap alignment
- the only remaining full `internal/runtime` failure is the unrelated dashboard architecture-guard debt already called out separately

## Phase 1 Work Slices

### Slice 1.1: Validation Alignment

Objective:
- make validation/bootstrap failures mean something under MAS-default loading

Primary files:
- [workflow_contract_validation.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_contract_validation.go)
- [workflow_contract_validation_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_contract_validation_test.go)
- [runtime_bootstrap_test.go](/Users/youmew/dev/empireai/internal/runtime/runtime_bootstrap_test.go)

Work:
- review each current validation error and classify it as:
  - real contract inconsistency
  - loader/merge bug
  - stale validation rule
- fix merged-agent and merged-event visibility if the loader is dropping data
- relax or rewrite validation checks that encode bridge-era assumptions
- keep checks that enforce real MAS invariants

Done when:
- `TestValidateWorkflowContracts_CurrentBundle` is either green or failing only on confirmed MAS-spec issues
- runtime bootstrap failures are no longer dominated by obviously stale validation assumptions

### Slice 1.2: Runtime Topology Rebaseline

Objective:
- stop asserting the old runtime node model

Primary files:
- [workflow_nodes_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_nodes_test.go)
- [runtime_interfaces_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/runtime_interfaces_test.go)
- [workflow_conformance_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_conformance_test.go)

Work:
- replace node-count assertions with ownership/executor coverage assertions that reflect MAS node declarations
- delete tests whose only value is preserving the five-node bridge-era layout
- keep assertions about generic runtime guarantees:
  - every executable node has an executor
  - policy ownership is internally coherent
  - node exposure matches contract intent

Done when:
- topology tests describe MAS runtime structure instead of old Empire runtime structure

### Slice 1.3: Transition Engine Test Migration

Objective:
- rewrite transition-engine tests around handler-first semantics

Primary files:
- [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go)
- [workflow_transition_engine_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine_test.go)

Work:
- remove expectations that MAS handler-owned events must still resolve to flat transition IDs
- replace alias-parity assertions with MAS-semantic assertions:
  - stage advancement
  - gate mutation
  - mapped writes
  - emitted events
  - guard enforcement
- keep only the parity checks that remain architecturally relevant during the migration
- delete tests that are now asserting behavior MAS explicitly replaces

Done when:
- transition-engine tests are validating MAS outcomes rather than bridge-era compatibility names

### Slice 1.4: Handler-First Completion

Objective:
- finish the first real handler-first migration tranche

Primary files:
- [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go)
- [workflow_nodes.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_nodes.go)

Target events:
- `vertical.shortlisted`
- `research.completed`
- `cto.spec_approved`
- `vertical.ready_for_review`
- `vertical.approved`
- `vertical.needs_more_data`

Work:
- replace event-specific promotion logic with contract-driven handler resolution where the handler plan is sufficiently supported
- keep shrinking the hard-coded allowlist until it is no longer the control mechanism
- support the MAS semantics already present in current handlers:
  - `guard`
  - `advances_to`
  - `sets_gate`
  - `clear_gates`
  - mapped `data_accumulation`
  - `emits`
- document any handler fields still out of scope for later phases rather than silently degrading them

Done when:
- these six events execute handler-first by design, not by compatibility shim
- remaining flat fallback is explicit migration debt, not the default semantic path

### Slice 1.5: Projection And Bookkeeping Cleanup

Objective:
- fix the likely real runtime mismatches surfaced after MAS-default activation

Primary files:
- [coordinator_workflow_instance_projection_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/coordinator_workflow_instance_projection_test.go)
- [coordinator_transitions_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/coordinator_transitions_test.go)
- relevant runtime/projection files under `internal/runtime/pipeline/`

Work:
- investigate duplicate `opco.spinup_requested`
- investigate consumed-vs-processed transition bookkeeping
- rebaseline workflow-instance projection tests where the structure is expected to change under MAS
- keep genuine accounting bugs as blocking

Done when:
- bookkeeping/projection tests fail only on real semantic defects, not stale structure assumptions

### Slice 1.6: Canned Scenario Rebaseline

Objective:
- realign high-level end-to-end scenarios to MAS validation semantics

Primary files:
- [canned_llm_additional_scenarios_e2e_test.go](/Users/youmew/dev/empireai/internal/runtime/canned_llm_additional_scenarios_e2e_test.go)
- [canned_llm_full_pipeline_e2e_test.go](/Users/youmew/dev/empireai/internal/runtime/canned_llm_full_pipeline_e2e_test.go)

Work:
- update expected event sequences once validation semantics are stable under Slices 1.1-1.4
- remove reliance on old validation event order where MAS now emits different intermediate events
- preserve scenario intent:
  - revision cycle behavior
  - human rejection/approval flow
  - multi-mode campaign execution
  - full directive-to-OpCo progression

Done when:
- end-to-end scenarios prove MAS behavior rather than bridge-era choreography

Status:
- complete

## Execution Order

Recommended order:

1. Slice 1.1
2. Slice 1.2
3. Slice 1.3
4. Slice 1.4
5. Slice 1.5
6. Slice 1.6

Reason:

- validation noise needs to be reduced first
- node-model and transition-engine tests are currently the largest stale-assertion surface
- only after those are corrected do the remaining failures become trustworthy runtime signals

## What Counts As Success

Phase 1 is successful when all of the following are true:

- MAS-default remains the runtime default
- validation is enforcing MAS-meaningful rules rather than bridge-era assumptions
- the six deferred events are handler-first under MAS semantics
- the test suite has been materially rebaselined away from flat-transition and five-node legacy assertions
- remaining failures, if any, are concrete runtime/spec problems rather than ambiguity about source of truth

Current status:

- achieved for the runtime/pipeline migration surface
- the only remaining full `internal/runtime` failure is the unrelated dashboard architecture-guard check in the dashboard package

## What Is Explicitly Not Required For Phase 1

Phase 1 does not need to finish:

- CEL
- generic dynamic flow instance creation
- wildcard flow-instance expansion
- timer lifecycle semantics
- full boot verification port

Those remain later phases. Phase 1 only needs to stop the MAS-default runtime from being trapped behind obsolete bridge-era expectations.
