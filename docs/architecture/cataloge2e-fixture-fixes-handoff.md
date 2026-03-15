# Catalog E2E Fixture Fixes Handoff

## Purpose

This handoff is for the test writer to fix the remaining `fixture-issue` exclusions in `cataloge2e`.

Scope:
- Fixture-only fixes
- No runtime changes
- No spec changes

Current state:
- `go test ./... -count=1` is green
- `cataloge2e` has broad Tier 1-8 coverage
- The remaining non-Tier-8 exclusions are mostly fixture dialect drift against the live loader/runtime

This document covers all current `fixture-issue` entries from:
- `internal/runtime/cataloge2e/tier1_primitives_e2e_test.go`
- `internal/runtime/cataloge2e/tier4_cross_entity_e2e_test.go`
- `internal/runtime/cataloge2e/tier5_lifecycle_e2e_test.go`
- `internal/runtime/cataloge2e/tier6_event_loop_e2e_test.go`
- `internal/runtime/cataloge2e/tier7_composition_e2e_test.go`
- `internal/runtime/cataloge2e/tier8_boot_e2e_test.go`

## Quick Count

There are 29 fixture issues to clean up:
- Tier 1: 2
- Tier 4: 1
- Tier 5: 10
- Tier 6: 7
- Tier 7: 6
- Tier 8: 3

## Global Migration Rules

Apply these rules consistently before doing one-off tweaks.

### 1. `produces: []` is not accepted by the real loader

Do not leave empty `produces` lists on nodes or agents.

Use one of:
- remove `produces` entirely if the fixture actor emits nothing
- replace with a non-empty list of actual events if the fixture really produces events

Affected fixtures:
- `tests/tier1-primitives/test-rules-data-accumulation/nodes.yaml`
- `tests/tier5-flow-lifecycle/test-auto-emit-on-create/nodes.yaml`
- `tests/tier5-flow-lifecycle/test-terminal-state-preserves/nodes.yaml`
- `tests/tier5-flow-lifecycle/test-terminal-state-rejects/nodes.yaml`
- `tests/tier5-flow-lifecycle/test-timer-cancel/nodes.yaml`
- `tests/tier7-composition/test-agent-emits-to-node/nodes.yaml`
- `tests/tier7-composition/test-dual-delivery/agents.yaml`
- `tests/tier7-composition/test-multi-gate-pipeline/nodes.yaml`
- `tests/tier8-boot-verification/test-boot-event-no-producer/nodes.yaml`

### 2. `sets_gates` is obsolete; use live `sets_gate`

The real loader rejects `sets_gates`.

Replace:
```yaml
sets_gates:
  gate_name: true
```

With either:
```yaml
sets_gate: gate_name
```

Or:
```yaml
sets_gate:
  name: gate_name
  value: true
```

Important:
- the shorthand map form under `sets_gate` is also not supported
- this is why `test-sets-gate` is still excluded even after `sets_gates` was renamed

Affected fixtures:
- `tests/tier1-primitives/test-sets-gate/nodes.yaml`
- `tests/tier6-event-loop/test-atomicity-commit/nodes.yaml`
- `tests/tier6-event-loop/test-atomicity-guard-rollback/nodes.yaml`
- `tests/tier6-event-loop/test-atomicity-rollback/nodes.yaml`
- `tests/tier7-composition/test-full-lifecycle/nodes.yaml`
- `tests/tier7-composition/test-multi-gate-pipeline/nodes.yaml`

### 3. Legacy `handler.timer` syntax must be rewritten to node-level `timers:`

The real loader rejects handler-local `timer:` blocks like:
```yaml
event_handlers:
  some.event:
    timer:
      delay_ms: 1000
      emit: timer.some_timeout
```

Use the live node-level dialect instead:
```yaml
timers:
  - id: some_timeout
    event: timer.some_timeout
    delay: 1s
    start_on: some.event
```

Then keep the timeout event as a normal handler:
```yaml
event_handlers:
  timer.some_timeout:
    advances_to: timed_out
```

Reference example:
- `internal/runtime/testdata/generic-mas-bundle/flows/delivery/nodes.yaml`
- `tests/tier5-flow-lifecycle/test-timer-start-on/nodes.yaml`

Affected fixtures:
- `tests/tier5-flow-lifecycle/test-timer-fire/nodes.yaml`
- `tests/tier5-flow-lifecycle/test-timer-recurring/nodes.yaml`
- `tests/tier5-flow-lifecycle/test-timer-cancel/nodes.yaml`

### 4. Legacy `action_params` must be rewritten to structured `action`

The real loader rejects:
```yaml
action: create_flow_instance
action_params:
  template: worker-flow
  instance_id: "payload.instance_id"
```

Use the live dialect:
```yaml
action:
  type: create_flow_instance
  flow_template: worker-flow
  instance_id: "{{payload.instance_id}}"
```

Reference example:
- `tests/tier11-flow-composition/test-dynamic-flow-instance/nodes.yaml`

Affected fixtures:
- `tests/tier5-flow-lifecycle/test-create-flow-instance/nodes.yaml`
- `tests/tier5-flow-lifecycle/test-create-flow-instance-config/nodes.yaml`
- `tests/tier5-flow-lifecycle/test-create-flow-instance-duplicate/nodes.yaml`

### 5. Use `entity.current_state`, not `entity.state`

The live expression context exposes `entity.current_state`.

Affected fixture:
- `tests/tier6-event-loop/test-entity-serialization/nodes.yaml`

### 6. `simulate_failure` is not part of the live loader dialect

Affected fixture:
- `tests/tier6-event-loop/test-atomicity-rollback/nodes.yaml`

If this case must remain a real-runtime E2E fixture, re-express it through supported behavior instead of synthetic handler failure injection.

### 7. Cross-flow fixtures must use real nested flow package shape

Flat top-level `flows: [flow-a, flow-b]` without actual nested flow packages does not build as a real workflow module.

Use nested flow directories:
- `flows/flow-a/...`
- `flows/flow-b/...`

Reference examples:
- `tests/tier11-flow-composition/test-data-pin-write-conflict/flows/flow-a/package.yaml`
- `tests/tier11-flow-composition/test-data-pin-write-conflict/flows/flow-b/package.yaml`

Affected fixtures:
- `tests/tier7-composition/test-cross-flow-subscription/*`
- `tests/tier7-composition/test-wildcard-cross-flow/*`

## Per-Fixture Fix List

### Tier 1

#### `tests/tier1-primitives/test-rules-data-accumulation`

Files to edit:
- `tests/tier1-primitives/test-rules-data-accumulation/nodes.yaml`

Fix:
- remove `produces: []` or replace it with a real non-empty produced event list

Why:
- the real validator rejects the node before runtime execution

#### `tests/tier1-primitives/test-sets-gate`

Files to edit:
- `tests/tier1-primitives/test-sets-gate/nodes.yaml`

Current invalid shape:
```yaml
sets_gate:
  g1_check: true
```

Replace with one of:
```yaml
sets_gate: g1_check
```

Or:
```yaml
sets_gate:
  name: g1_check
  value: true
```

Why:
- `sets_gate` is supported
- only the shorthand map form is unsupported

### Tier 4

#### `tests/tier4-cross-entity/test-create-entity`

Files to edit:
- `tests/tier4-cross-entity/test-create-entity/nodes.yaml`
- likely add a nested child flow under `tests/tier4-cross-entity/test-create-entity/flows/child-flow/...`

Fix:
- the fixture references `create_flow_instance` template `child-flow`
- the bundle does not actually contain a `child-flow` contract
- either:
  - add a real nested `flows/child-flow/` package, or
  - change the action to reference an existing template that is present in the fixture bundle

Also rewrite the action to the live structured `action` dialect if it still uses legacy `action_params`.

### Tier 5

#### `tests/tier5-flow-lifecycle/test-auto-emit-on-create`

Files to edit:
- `tests/tier5-flow-lifecycle/test-auto-emit-on-create/nodes.yaml`

Fix:
- remove `produces: []` or replace it with a real non-empty produced-event list

#### `tests/tier5-flow-lifecycle/test-create-flow-instance`

Files to edit:
- `tests/tier5-flow-lifecycle/test-create-flow-instance/nodes.yaml`

Fix:
- replace legacy `action: create_flow_instance` + `action_params` with structured `action`

#### `tests/tier5-flow-lifecycle/test-create-flow-instance-config`

Files to edit:
- `tests/tier5-flow-lifecycle/test-create-flow-instance-config/nodes.yaml`

Fix:
- same structured `action` rewrite as above

#### `tests/tier5-flow-lifecycle/test-create-flow-instance-duplicate`

Files to edit:
- `tests/tier5-flow-lifecycle/test-create-flow-instance-duplicate/nodes.yaml`

Fix:
- same structured `action` rewrite as above

#### `tests/tier5-flow-lifecycle/test-terminal-state-preserves`

Files to edit:
- `tests/tier5-flow-lifecycle/test-terminal-state-preserves/nodes.yaml`

Fix:
- remove or replace the invalid `produces: []` on `update-node`

#### `tests/tier5-flow-lifecycle/test-terminal-state-rejects`

Files to edit:
- `tests/tier5-flow-lifecycle/test-terminal-state-rejects/nodes.yaml`

Fix:
- remove or replace the invalid `produces: []` on `reopen-node`

#### `tests/tier5-flow-lifecycle/test-timer-cancel`

Files to edit:
- `tests/tier5-flow-lifecycle/test-timer-cancel/nodes.yaml`

Fixes:
- rewrite legacy handler-local `timer:` into node-level `timers:`
- remove or replace invalid `produces: []` on `cancel-node`

#### `tests/tier5-flow-lifecycle/test-timer-fire`

Files to edit:
- `tests/tier5-flow-lifecycle/test-timer-fire/nodes.yaml`

Fix:
- rewrite legacy handler-local `timer:` into node-level `timers:`

#### `tests/tier5-flow-lifecycle/test-timer-recurring`

Files to edit:
- `tests/tier5-flow-lifecycle/test-timer-recurring/nodes.yaml`

Fix:
- rewrite legacy handler-local `timer:` into node-level `timers:`
- keep recurring behavior on the timer definition, not inside handler-local `timer:`

#### `tests/tier5-flow-lifecycle/test-timer-start-on`

Files to edit:
- `tests/tier5-flow-lifecycle/test-timer-start-on/nodes.yaml`
- `tests/tier5-flow-lifecycle/test-timer-start-on/events.yaml`
- possibly `tests/tier5-flow-lifecycle/test-timer-start-on/schema.yaml`

Fix:
- keep the node-level `timers:` shape
- fix the timer contract so the real loader considers the timer fire event fully declared
- this fixture is already close to the live dialect; the failure is not the old `handler.timer` syntax

Practical target:
- mirror the live node-level timer fields used in `internal/runtime/testdata/generic-mas-bundle/flows/delivery/nodes.yaml`
- make sure the timer event and produced event are fully declared and consistent across:
  - `nodes.yaml`
  - `events.yaml`
  - `schema.yaml` pins

### Tier 6

#### `tests/tier6-event-loop/test-atomicity-commit`

Files to edit:
- `tests/tier6-event-loop/test-atomicity-commit/nodes.yaml`

Fix:
- replace `sets_gates` with supported `sets_gate`

#### `tests/tier6-event-loop/test-atomicity-guard-rollback`

Files to edit:
- `tests/tier6-event-loop/test-atomicity-guard-rollback/nodes.yaml`

Fix:
- replace `sets_gates` with supported `sets_gate`

#### `tests/tier6-event-loop/test-atomicity-rollback`

Files to edit:
- `tests/tier6-event-loop/test-atomicity-rollback/nodes.yaml`

Fixes:
- replace `sets_gates` with supported `sets_gate`
- remove `simulate_failure`
- if rollback behavior is still required, redesign the fixture to use a supported failure path instead of synthetic loader-only failure injection

#### `tests/tier6-event-loop/test-chain-depth-limit`

Files to edit:
- `tests/tier6-event-loop/test-chain-depth-limit/nodes.yaml`
- possibly `expected.yaml`

Current problem:
- the fixture self-emits `chain.continue` from the `chain.continue` handler
- real boot validation rejects it before runtime chain-depth behavior is exercised

Fix:
- rewrite the case so it still creates a chain-depth scenario without violating self-emit boot rules
- likely requires at least two events or two handlers/nodes instead of direct self-emit

#### `tests/tier6-event-loop/test-dead-letter`

Files to edit:
- `tests/tier6-event-loop/test-dead-letter/expected.yaml`
- possibly fixture structure if you want a different failure mechanism

Current mismatch:
- live runtime does not model this case as `dead_letter`
- it records `spec.contradiction_detected` diagnostics for unroutable contract events

Fix options:
- easiest: update the fixture to assert the live diagnostic behavior instead of `dead_letter`
- or redesign the fixture to exercise a real dead-letter path that exists in the runtime

#### `tests/tier6-event-loop/test-entity-serialization`

Files to edit:
- `tests/tier6-event-loop/test-entity-serialization/nodes.yaml`

Fix:
- replace `entity.state` with `entity.current_state`

#### `tests/tier6-event-loop/test-event-validation`

Files to edit:
- `tests/tier6-event-loop/test-event-validation/expected.yaml`
- optionally move or redesign the fixture

Current mismatch:
- default runtime payload validation is warning-only
- the fixture expects reject + dead-letter under default mode

Fix options:
- easiest: update the expectation to match warning-only default runtime behavior
- or move this case out of default real-runtime E2E and into a strict-validation-specific suite

### Tier 7

#### `tests/tier7-composition/test-agent-emits-to-node`

Files to edit:
- `tests/tier7-composition/test-agent-emits-to-node/agents.yaml`
- possibly `nodes.yaml`

Fix:
- add required agent fields:
  - `model_tier`
  - `conversation_mode`
  - `subscriptions`
  - `emit_events`

Reference example:
- `docs/specs/mas-platform/empire/contracts/flows/operating/agents.yaml`

#### `tests/tier7-composition/test-cross-flow-subscription`

Files to edit:
- whole fixture structure

Fix:
- convert from flat multi-flow shape to nested flow package shape:
  - `flows/flow-a/...`
  - `flows/flow-b/...`

Current problem:
- real module construction fails with `workflow.name missing`

#### `tests/tier7-composition/test-dual-delivery`

Files to edit:
- `tests/tier7-composition/test-dual-delivery/agents.yaml`

Fix:
- add required agent fields:
  - `model_tier`
  - `conversation_mode`
  - `subscriptions`
  - `emit_events`
- remove `produces: []` if present and invalid

#### `tests/tier7-composition/test-full-lifecycle`

Files to edit:
- `tests/tier7-composition/test-full-lifecycle/nodes.yaml`

Fix:
- replace `sets_gates` with supported `sets_gate`

#### `tests/tier7-composition/test-multi-gate-pipeline`

Files to edit:
- `tests/tier7-composition/test-multi-gate-pipeline/nodes.yaml`

Fixes:
- replace all `sets_gates` with supported `sets_gate`
- remove or replace any invalid `produces: []`

#### `tests/tier7-composition/test-wildcard-cross-flow`

Files to edit:
- whole fixture structure

Fix:
- convert from flat multi-flow shape to nested flow package shape

### Tier 8

#### `tests/tier8-boot-verification/test-boot-event-no-producer`

Files to edit:
- `tests/tier8-boot-verification/test-boot-event-no-producer/nodes.yaml`

Fix:
- remove the unrelated fixture error so the intended warning can surface
- specifically, fix the node-level contract issue that currently causes an earlier failure than `EVENT-NO-PRODUCER`

Goal:
- after cleanup, the fixture should fail only for the intended `ghost.event` producer gap

#### `tests/tier8-boot-verification/test-boot-missing-pin`

Files to edit:
- `tests/tier8-boot-verification/test-boot-missing-pin/flows/child/*`
- especially `flows/child/package.yaml`, `flows/child/schema.yaml`, `flows/child/nodes.yaml`

Fix:
- complete the child flow so it boots cleanly enough for the intended missing-producer warning to surface
- right now the child flow fails earlier than the target warning

#### `tests/tier8-boot-verification/test-boot-permission-tool-mismatch`

Files to edit:
- `tests/tier8-boot-verification/test-boot-permission-tool-mismatch/agents.yaml`
- `tests/tier8-boot-verification/test-boot-permission-tool-mismatch/tools.yaml`
- possibly `expected.yaml`

Fix:
- make sure the fixture references a real tool that successfully loads before permission validation runs
- right now it fails earlier as a missing-tool problem instead of reaching `PERMISSION-MISMATCH`

Practical target:
- use a live permission-gated tool and make the fixture invalid only on missing permission, not on missing tool definition

## Recommended Order

Fix in this order:

1. Bulk dialect rewrites
- `produces: []`
- `sets_gates`
- `sets_gate` shorthand maps
- `handler.timer`
- `entity.state`

2. Structured action rewrites
- all `create_flow_instance` fixtures

3. Cross-flow package rewrites
- Tier 7 cross-flow fixtures

4. Expectation-only rewrites
- `test-dead-letter`
- `test-event-validation`

5. Tier 8 cleanup
- isolate intended warning/error category by removing earlier unrelated fixture failures

## Verification

After each batch:

```bash
go test ./internal/runtime/cataloge2e -count=1
go test ./... -count=1
```

When a fixture is fixed:
- move it from the tier’s `fixture-issue` map into the supported list in the corresponding `cataloge2e` test file
- do not reclassify it blindly; rerun the targeted tier test and confirm it passes under the real runtime

## Expected Outcome

After this fixture pass:
- Tiers 1-7 should be mostly limited by real runtime gaps or intended exclusions, not fixture dialect drift
- Tier 8 should isolate true validation coverage gaps cleanly
- `cataloge2e` classifications should stop carrying avoidable `fixture-issue` noise
