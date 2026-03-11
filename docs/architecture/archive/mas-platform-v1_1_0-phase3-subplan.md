# MAS Platform v1.1.0 Phase 3 Subplan

Date: 2026-03-10
Repo: `/Users/youmew/dev/empireai`
Parent plan: `docs/architecture/mas-platform-v1_1_0-implementation-plan.md`
Prior phase: `docs/architecture/mas-platform-v1_1_0-phase2-subplan.md`

## Goal

Phase 3 exists to replace Empire-specific instance/bootstrap pathways with the MAS dynamic flow instance model.

The practical outcome of this phase is:

- generic runtime can execute `create_flow_instance`
- template-mode flows can be instantiated at runtime from MAS contracts
- runtime supports MAS instance addressing and wildcard subscription expansion
- OpCo spinup no longer depends on the legacy manager/template bootstrap path

## Current Readout

### The contract surface is ahead of runtime lifecycle

The contract loader already preserves the fields Phase 3 needs:

- flow `mode`
- `instance_variables`
- `auto_emit_on_create`
- node action metadata:
  - `template`
  - `instance_id_from`
  - `config_from`

Those fields are parsed in [workflow_contracts.go](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts.go). Runtime now carries them through handler execution in [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go), and `create_flow_instance` can persist a template instance plus emit the namespaced `auto_emit_on_create` event. Full instance activation is still pending.

### The live runtime now uses the generic activation path

[opco.go](/Users/youmew/dev/empireai/internal/runtime/manager/opco.go) still exists as a compatibility/test surface, but the live runtime no longer depends on it for spinup.

`NewRuntime` now wires the pipeline coordinator to the generic flow-instance activator, and the manager spinup control loop is disabled in the runtime-owned manager.

That means the active `opco.spinup_requested` path is now:

- contract handler executes `create_flow_instance`
- workflow instance is created generically
- flow-local operating agents are activated from the flow package
- namespaced `auto_emit_on_create` is published as `operating/{instance_id}/opco.ceo_ready`

### Workflow instance storage now supports path identity without a DDL fork

[workflow_instance_store.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_instance_store.go) still writes into the existing `workflow_instances.instance_id UUID` column, but runtime identity is no longer flat.

The runtime now derives storage identity like this:

- root/static instances keep direct UUID identity
- template instances use deterministic UUIDs derived from `flow_path`
- runtime loads template instances by path reference such as `operating/{vertical_id}`

That lets root and template instances coexist for the same logical entity without reintroducing product-specific shortcuts.

### Bus validation still assumes dot-only event names

[routing.go](/Users/youmew/dev/empireai/internal/runtime/bus/routing.go) used to validate event names as dot-separated tokens only.

That specific blocker is now removed. Slash-path events are accepted and classified by their local event name, which is enough for path-style auto-emits and wildcard subscribers to start observing them.

### The runtime already has useful pieces

The runtime does already have:

- persistent workflow instance state
- node and agent subscription machinery
- route pattern matching using `path.Match`
- MAS template-mode flow metadata
- an active contract producer for `opco.spinup_requested`

So Phase 3 is not starting from zero. It is a convergence phase around:

- addressing
- runtime registry expansion
- generic instance bootstrap

## Main Design Constraints

### Constraint 1: Addressing and creation must be separated

Do not implement `create_flow_instance` on top of the current flat address model and then retrofit paths later.

That would create a second migration problem immediately.

Phase 3 should first define:

- instance identity
- path construction
- subscription expansion rules

Then wire creation to those rules.

### Constraint 2: Flow instance persistence must stop assuming UUID primary keys

The current workflow store treats `instance_id` as a UUID and uses it as the whole identity.

The MAS model needs at least:

- template/static flow identity
- instance identity within template scope
- a stable runtime path key

If that is not corrected first, generic instance creation will be forced into Empire-specific shortcuts again.

### Constraint 3: Retire legacy OpCo bootstrap by replacement, not by parallelism

Do not keep two live OpCo creation systems longer than necessary.

The sequence should be:

1. generic dynamic instance infrastructure exists
2. `opco.spinup_requested` uses it
3. old `SpawnOpCo` bootstrap path becomes fallback-only or is removed

### Constraint 4: Wildcard execution must be real, not decorative

The spec explicitly expects existing wildcard subscribers to observe new dynamic instances.

That means Phase 3 must do more than accept slash names in validation. It must make runtime delivery expand correctly when an instance appears.

## Phase 3 Work Slices

### Slice 3.1: Addressing And Identity Baseline

Objective:

- make runtime storage and event validation capable of representing MAS flow instance paths

Primary files:

- [routing.go](/Users/youmew/dev/empireai/internal/runtime/bus/routing.go)
- [eventbus_publish.go](/Users/youmew/dev/empireai/internal/runtime/bus/eventbus_publish.go)
- [workflow_instance_store.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_instance_store.go)
- instance-store and bus tests

Work:

- define the runtime identity model for:
  - static flow instances
  - template instances
- stop assuming `workflow_instances.instance_id` is always UUID-shaped in runtime code
- add/derive a stable path key for workflow instances
- allow slash-based MAS event paths in bus validation
- add tests for:
  - valid slash event names
  - wildcard path matching
  - loading/upserting workflow instances with non-UUID path-style identity

Done when:

- runtime can store and validate MAS-style instance identifiers and path-style events without behavior changes elsewhere

Status:

- started
- bus validation now accepts MAS slash-path event names, including hyphenated instance segments
- bus publish/delivery tests now cover slash-path events directly
- factory/opco event classification now uses the local event name for slash-path events
- generic handler execution now preserves `template`, `instance_id_from`, and `config_from`
- generic runtime now executes `create_flow_instance` for direct handler plans, persists the template instance, records `flow_path`, and emits the namespaced `auto_emit_on_create` event
- runtime startup now wires a generic flow-instance activator into the pipeline coordinator
- the manager can now activate a flow instance from flow-local contract agents and install namespaced routing rules without the legacy org template loader
- `NewRuntime` disables the legacy manager spinup control loop so the live runtime no longer has two active `opco.spinup_requested` activation paths
- focused regression coverage now exists for `opco.spinup_requested -> create_flow_instance -> operating/{instance_id}/opco.ceo_ready`
- deferred events now re-enter interception once before direct delivery, which is what allows the `vertical.approved -> opco.spinup_requested` chain to complete under MAS-default
- canned runtime scenarios are rebaselined around namespaced operating bootstrap events
- top-level runtime recovery/semantic/tooling tests that used to depend on direct `SpawnOpCo` bootstrap have been migrated to MAS flow activation where that was the behavior under test
- full [pipeline](/Users/youmew/dev/empireai/internal/runtime/pipeline), full [bus](/Users/youmew/dev/empireai/internal/runtime/bus), and full [runtime](/Users/youmew/dev/empireai/internal/runtime) are green again except for the known out-of-scope dashboard architecture guard

### Slice 3.2: Generic `create_flow_instance`

Objective:

- implement the platform action itself in generic runtime

Primary files:

- [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go)
- coordinator/runtime support files under `internal/runtime/pipeline/`
- contract-loading helpers

Work:

- implement platform execution for action `create_flow_instance`
- resolve:
  - `template`
  - `instance_id_from`
  - `config_from`
- validate:
  - template exists
  - template flow is `mode: template`
  - instance is unique within template scope
- create the new workflow instance record at template initial state

Done when:

- `opco.spinup_requested` can create an operating flow instance through generic runtime without using manager bootstrap

Status:

- materially complete
- generic runtime can now create the operating workflow instance from the portfolio-node handler without going through `agent-manager-controller`
- generic runtime can now activate the operating instance through flow-local agent contracts and namespaced routes
- full canned end-to-end validation now reaches the namespaced operating bootstrap event through the live runtime path
- remaining gap in this slice is cleanup and deprecation, not core platform behavior:
  - legacy `SpawnOpCo` still exists as a direct manager API and compatibility/test path
  - `DefaultOpCoRoster` / `DefaultOpCoRoutes` still exist as compatibility surfaces and some legacy-focused tests/compliance checks still reference them
  - the DB column remains UUID-shaped even though runtime identity is now path-based

Note:

- I ran a MAS-authoritative audit pass on [contract_compliance_test.go](/Users/youmew/dev/empireai/internal/runtime/contract_compliance_test.go) during this phase. It immediately exposed stale compliance assumptions around:
  - root-vs-flow contract loading
  - hard-coded spec version expectations
  - legacy implementation-path coverage rules
  - bridge-era workflow graph semantics
- That audit is useful, but it is Phase 5 work. The compliance suite was intentionally left on its prior green baseline for now so Phase 3 does not expand the red surface for non-Phase-3 reasons.

### Slice 3.3: Runtime Registration And Wildcards

Objective:

- register dynamic instance nodes/agents/subscriptions and make wildcard delivery real

Primary files:

- runtime registry/bootstrap code
- bus routing code
- system-node runner / agent startup code

Work:

- construct instance-local paths:
  - `{template_id}/{instance_id}/{local_name}`
- register new nodes and agents for the created instance
- resolve local subscriptions within the instance
- expand existing wildcard subscriptions to include the new instance
- add tests for:
  - wildcard observer sees new instance traffic
  - local subscriptions stay instance-scoped
  - no cross-instance leakage

Done when:

- dynamic instance traffic is routed by MAS addressing rules rather than product routing shims

Status:

- materially complete
- activated flow agents now register namespaced local subscriptions such as `operating/{instance_id}/...`
- wildcard observers can now see dynamic instance traffic without per-instance product shims
- regression coverage now proves:
  - wildcard observer sees `operating/{instance_id}/opco.ceo_ready`
  - local subscriptions stay instance-scoped
  - no cross-instance leakage occurs between two operating instances

### Slice 3.4: `auto_emit_on_create` And Operating Spinup Rebaseline

Objective:

- complete the operating-flow boot path declaratively

Primary files:

- dynamic instance creation runtime code
- operating flow tests
- canned/runtime spinup tests

Work:

- honor `auto_emit_on_create`
- emit the configured event only after instance registration/startup is complete
- rebaseline `opco.ceo_ready` behavior around the generic instance path
- prove operating boot works from:
  - `vertical.approved`
  - `opco.spinup_requested`
  - `create_flow_instance`
  - `auto_emit_on_create`

Done when:

- the operating flow boots from MAS contracts without a handwritten manager bootstrap event path

### Slice 3.5: Legacy Bootstrap Retirement

Objective:

- remove the old Empire-specific OpCo lifecycle path as a required runtime mechanism

Primary files:

- [opco.go](/Users/youmew/dev/empireai/internal/runtime/manager/opco.go)
- runtime/manager tests
- architecture/compliance tests

Work:

- classify remaining uses of:
  - `SpawnOpCo`
  - template-store roster expansion
  - manual routing-table bootstrap
- delete or narrow them to explicit fallback/test-only usage
- update tests to assert generic instance creation semantics instead

Done when:

- the runtime no longer requires the old Empire OpCo bootstrap path for operating flow creation

Status:

- materially complete
- the legacy manager control-loop path for `opco.spinup_requested -> SpawnOpCo` is now opt-in compatibility behavior, not the default manager/runtime behavior
- active MAS runtime construction no longer depends on the legacy spinup control loop
- remaining legacy surfaces are explicit compatibility/manual-test entry points:
  - [opco.go](/Users/youmew/dev/empireai/internal/runtime/manager/opco.go)
  - [bootstrap.go](/Users/youmew/dev/empireai/internal/runtime/manager/bootstrap.go)
  - dashboard/manual seed callers outside the active runtime path

## Recommended Execution Order

1. Slice 3.1
2. Slice 3.2
3. Slice 3.3
4. Slice 3.4
5. Slice 3.5

Reason:

- identity and addressing are the foundation
- instance creation depends on them
- wildcard/runtime registration depends on creation
- `auto_emit_on_create` depends on the full bootstrap path existing
- bootstrap retirement should be last so there is always one working path

## First Slice Recommendation

Start with `Slice 3.1`.

That means:

- relax bus validation for MAS slash paths
- rework workflow instance persistence away from UUID-only assumptions
- add tests that prove the runtime can represent template instances before it tries to start them

This is the smallest Phase 3 tranche that materially removes the current architectural blocker.

## What Counts As Success

Phase 3 is successful when all of the following are true:

- generic runtime can create template flow instances from contracts
- dynamic instances are addressed by MAS path rules
- wildcard subscribers observe new instances correctly
- operating flow spinup no longer depends on the legacy Empire manager bootstrap path
- no Empire-specific instance-creation shim is required on the active runtime path

Current readout:

- those success criteria are met for the active runtime path
- the only remaining red in broad runtime verification is the known out-of-scope dashboard architecture guard, not a Phase 3 runtime failure

## What Is Explicitly Not Required For Phase 3

Phase 3 does not need to finish:

- timer lifecycle semantics
- boot verification/compliance port
- dashboard cleanup

Those remain later work.
