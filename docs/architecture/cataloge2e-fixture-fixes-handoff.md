# Catalog E2E Fixture Fixes Handoff

## Purpose

This is the current fixture-only backlog from the live `cataloge2e` exclusion maps.

Scope:
- fixture edits only
- no runtime changes
- no spec changes

Current baseline:
- `go test ./... -count=1` is green
- remaining exclusions below are all classified as `fixture-issue`
- once a fixture is fixed and passes, promote it from the excluded map into the supported list in the matching `tier*_e2e_test.go`

## Quick Count

There are 23 remaining fixture issues:
- Tier 5: 3
- Tier 6: 4
- Tier 7: 5
- Tier 8: 2
- Tier 9: 5
- Tier 11: 4

## Tier 5

### `test-terminal-state-preserves`

Files:
- `tests/tier5-flow-lifecycle/test-terminal-state-preserves/expected.yaml`
- `tests/tier5-flow-lifecycle/test-terminal-state-preserves/nodes.yaml`

Current issue:
- the fixture still expects no emitted events
- the first `task.completed` step legitimately emits `task.finished` before the terminal follow-up is rejected

Fix direction:
- update `expected.emitted_events` to include `task.finished`
- do not change runtime behavior here

### `test-terminal-state-rejects`

Files:
- `tests/tier5-flow-lifecycle/test-terminal-state-rejects/expected.yaml`
- `tests/tier5-flow-lifecycle/test-terminal-state-rejects/nodes.yaml`

Current issue:
- same stale emitted-event expectation as above
- the first `task.completed` step legitimately emits `task.finished` before the reopen request is rejected

Fix direction:
- update `expected.emitted_events` to include `task.finished`

### `test-timer-cancel`

Files:
- `tests/tier5-flow-lifecycle/test-timer-cancel/expected.yaml`
- `tests/tier5-flow-lifecycle/test-timer-cancel/nodes.yaml`

Current issue:
- the fixture still expects `emitted_events: []`
- the live fixture now emits `timer.cancelled` through `cancel-node`

Fix direction:
- update `expected.emitted_events` to include `timer.cancelled`

## Tier 6

### `test-atomicity-guard-rollback`

Files:
- `tests/tier6-event-loop/test-atomicity-guard-rollback/schema.yaml`
- `tests/tier6-event-loop/test-atomicity-guard-rollback/nodes.yaml`

Current issue:
- the fixture writes `counter` via `data_accumulation`
- `counter` is still missing from the declared workflow `entity_schema`
- real validation rejects the bundle before runtime behavior is exercised

Fix direction:
- declare `counter` in `schema.yaml`

### `test-chain-depth-limit`

Files:
- `tests/tier6-event-loop/test-chain-depth-limit/events.yaml`
- `tests/tier6-event-loop/test-chain-depth-limit/nodes.yaml`
- `tests/tier6-event-loop/test-chain-depth-limit/schema.yaml`
- `tests/tier6-event-loop/test-chain-depth-limit/expected.yaml`

Current issue:
- the fixture still boot-fails with `EVENT-CYCLE`
- it never reaches chain-depth runtime behavior

Fix direction:
- redesign the fixture so it exercises chain depth without a static cycle that boot validation rejects
- if the fixture is intended as runtime-only, keep that explicit in `expected.yaml`

### `test-dead-letter`

Files:
- `tests/tier6-event-loop/test-dead-letter/expected.yaml`
- `tests/tier6-event-loop/test-dead-letter/events.yaml`
- `tests/tier6-event-loop/test-dead-letter/nodes.yaml`
- `tests/tier6-event-loop/test-dead-letter/schema.yaml`

Current issue:
- the fixture now expects `pipeline.dead_letter`
- the live runtime still reports this unroutable event as a discard-path outcome instead

Fix direction:
- either redesign the fixture to hit a real dead-letter path
- or update the expectation to the runtime’s actual discard-path semantics

### `test-guards-pre-handler-state`

Files:
- `tests/tier6-event-loop/test-guards-pre-handler-state/schema.yaml`
- `tests/tier6-event-loop/test-guards-pre-handler-state/nodes.yaml`

Current issue:
- same as `test-atomicity-guard-rollback`
- `counter` is written through `data_accumulation` but still missing from `entity_schema`

Fix direction:
- declare `counter` in `schema.yaml`

## Tier 7

### `test-agent-emits-to-node`

Files:
- `tests/tier7-composition/test-agent-emits-to-node/expected.yaml`
- `tests/tier7-composition/test-agent-emits-to-node/agents.yaml`
- `tests/tier7-composition/test-agent-emits-to-node/events.yaml`

Current issue:
- the fixture still expects only `task.finalized`
- the real runtime also persists the agent-emitted `task.completed` event in the chain

Fix direction:
- update `expected.emitted_events` to include both `task.completed` and `task.finalized`

### `test-cross-flow-subscription`

Files:
- `tests/tier7-composition/test-cross-flow-subscription/events.yaml`
- `tests/tier7-composition/test-cross-flow-subscription/flows/flow-a/events.yaml`
- `tests/tier7-composition/test-cross-flow-subscription/flows/flow-b/events.yaml`

Current issue:
- prefixed cross-flow events like `flow-b/order.completed` are still not declared in the real event catalog

Fix direction:
- declare the prefixed cross-flow event names in the fixture event catalogs

### `test-dual-delivery`

Files:
- `tests/tier7-composition/test-dual-delivery/events.yaml`
- `tests/tier7-composition/test-dual-delivery/agents.yaml`
- `tests/tier7-composition/test-dual-delivery/expected.yaml`

Current issue:
- real boot now reaches emit-schema enforcement
- the fixture is still missing an explicit schema entry for the agent-emitted audit event

Fix direction:
- add the missing event schema for the agent-emitted audit event in `events.yaml`

### `test-multi-gate-pipeline`

Files:
- `tests/tier7-composition/test-multi-gate-pipeline/nodes.yaml`
- `tests/tier7-composition/test-multi-gate-pipeline/events.yaml`

Current issue:
- gate-setter nodes still omit required `produces`
- real boot validation rejects the package

Fix direction:
- add valid `produces` entries for every gate-setting node that emits

### `test-wildcard-cross-flow`

Files:
- `tests/tier7-composition/test-wildcard-cross-flow/events.yaml`
- `tests/tier7-composition/test-wildcard-cross-flow/flows/flow-alpha/events.yaml`
- `tests/tier7-composition/test-wildcard-cross-flow/flows/flow-beta/events.yaml`

Current issue:
- prefixed wildcard triggers like `*/job.*` are still not declared in the real event catalog

Fix direction:
- declare the prefixed wildcard event surface explicitly in the fixture event catalogs

## Tier 8

### `test-boot-missing-pin`

Files:
- `tests/tier8-boot-verification/test-boot-missing-pin/events.yaml`
- `tests/tier8-boot-verification/test-boot-missing-pin/flows/child/events.yaml`

Current issue:
- the fixture still fails earlier because `child/task.result` is not declared in the real event catalog

Fix direction:
- declare `child/task.result` in the parent-visible event catalog surface

### `test-boot-permission-tool-mismatch`

Files:
- `tests/tier8-boot-verification/test-boot-permission-tool-mismatch/expected.yaml`
- `tests/tier8-boot-verification/test-boot-permission-tool-mismatch/agents.yaml`
- `tests/tier8-boot-verification/test-boot-permission-tool-mismatch/tools.yaml`
- `tests/tier8-boot-verification/test-boot-permission-tool-mismatch/events.yaml`
- `tests/tier8-boot-verification/test-boot-permission-tool-mismatch/nodes.yaml`

Current issue:
- the fixture still does not isolate `PERMISSION-MISMATCH`
- current boot only surfaces generic producer/consumer/prompt warnings

Fix direction:
- simplify the fixture until the only remaining warning/error is the permission mismatch itself
- remove unrelated topology or prompt noise first

## Tier 9

### `test-compose-accumulate-compute-branch`

Files:
- `tests/tier9-composition-patterns/test-compose-accumulate-compute-branch/nodes.yaml`
- `tests/tier9-composition-patterns/test-compose-accumulate-compute-branch/expected.yaml`

Current issue:
- the fixture still uses unsupported accumulate keys `completion_mode` and `expected_count`
- the real loader falls back to default completion and completes after the first score with `composite=80`

Fix direction:
- rewrite the accumulate block to the live dialect
- remove unsupported keys

### `test-compose-clear-gates-reenter`

Files:
- `tests/tier9-composition-patterns/test-compose-clear-gates-reenter/nodes.yaml`
- `tests/tier9-composition-patterns/test-compose-clear-gates-reenter/expected.yaml`

Current issue:
- the fixture re-enters from terminal state `approved` without declaring an explicit terminal exit
- the hardened runtime now correctly keeps the entity in `approved`

Fix direction:
- either add an explicit terminal-exit path if re-entry is intended
- or update the expected state to remain `approved`

### `test-compose-create-instance-config`

Files:
- `tests/tier9-composition-patterns/test-compose-create-instance-config/nodes.yaml`
- `tests/tier9-composition-patterns/test-compose-create-instance-config/flows/worker/package.yaml`
- `tests/tier9-composition-patterns/test-compose-create-instance-config/expected.yaml`

Current issue:
- the fixture still uses legacy create-flow-instance action keys `type/flow_template/instance_id`
- the real loader never executes `create_flow_instance`

Fix direction:
- rewrite the action to the live structured dialect used by currently passing create-instance fixtures

### `test-compose-gate-data-advance-emit`

Files:
- `tests/tier9-composition-patterns/test-compose-gate-data-advance-emit/schema.yaml`
- `tests/tier9-composition-patterns/test-compose-gate-data-advance-emit/nodes.yaml`

Current issue:
- `stage_one_result` and `stage_two_result` are written via `data_accumulation`
- both are still missing from the declared `entity_schema`

Fix direction:
- add both fields to `schema.yaml`

### `test-compose-multi-emit-cross-flow`

Files:
- `tests/tier9-composition-patterns/test-compose-multi-emit-cross-flow/events.yaml`
- `tests/tier9-composition-patterns/test-compose-multi-emit-cross-flow/flows/tracker/events.yaml`
- `tests/tier9-composition-patterns/test-compose-multi-emit-cross-flow/expected.yaml`

Current issue:
- the fixture expects `tracker/task.record`
- that prefixed cross-flow event is still not declared in `events.yaml`
- only `task.logged` is emitted on the real runtime path

Fix direction:
- declare `tracker/task.record` in the event catalog
- then keep the expectation aligned with the actual emitted set

## Tier 11

### `test-dynamic-flow-instance`

Files:
- `tests/tier11-flow-composition/test-dynamic-flow-instance/nodes.yaml`
- `tests/tier11-flow-composition/test-dynamic-flow-instance/flows/worker/package.yaml`
- `tests/tier11-flow-composition/test-dynamic-flow-instance/expected.yaml`

Current issue:
- the fixture still uses legacy create-flow-instance action keys `type/flow_template/instance_id`
- the real loader never executes `create_flow_instance`
- no worker instance is created

Fix direction:
- rewrite the action to the live structured dialect used by passing child-flow instance fixtures

### `test-data-pin-wiring`

Files:
- `tests/tier11-flow-composition/test-data-pin-wiring/schema.yaml`
- `tests/tier11-flow-composition/test-data-pin-wiring/flows/processor/schema.yaml`
- `tests/tier11-flow-composition/test-data-pin-wiring/nodes.yaml`
- `tests/tier11-flow-composition/test-data-pin-wiring/flows/processor/nodes.yaml`

Current issue:
- parent and child handlers now fail real validation because `task_config` and `result` are written via `data_accumulation`
- both fields are still missing from the declared `entity_schema`

Fix direction:
- add the missing fields to the relevant schemas

### `test-data-pin-write-conflict`

Files:
- `tests/tier11-flow-composition/test-data-pin-write-conflict/nodes.yaml`
- `tests/tier11-flow-composition/test-data-pin-write-conflict/flows/flow-a/nodes.yaml`
- `tests/tier11-flow-composition/test-data-pin-write-conflict/flows/flow-b/nodes.yaml`
- `tests/tier11-flow-composition/test-data-pin-write-conflict/expected.yaml`

Current issue:
- the fixture still uses unsupported nested `outputs.data.writes` pins
- the bundle exposes no flow write pins
- it never reaches the intended `DATA-PIN-CONFLICT` validation

Fix direction:
- rewrite the data pin declarations to the live supported pin/output dialect

### `test-tool-override`

Files:
- `tests/tier11-flow-composition/test-tool-override/tools.yaml`
- `tests/tier11-flow-composition/test-tool-override/flows/child/tools.yaml`
- `tests/tier11-flow-composition/test-tool-override/flows/child/agents.yaml`
- `tests/tier11-flow-composition/test-tool-override/expected.yaml`

Current issue:
- the child fixture still references missing tool `lookup_data` from the merged bundle
- boot fails before tool override behavior can be asserted

Fix direction:
- make sure `lookup_data` is declared in the merged tool surface the child actually sees
- then keep the fixture focused on override behavior

## Verification

Run after each batch:

```bash
go test ./internal/runtime/cataloge2e -count=1
go test ./internal/runtime/masflowtest -count=1
go test ./... -count=1
```

## Promotion Rule

Once a fixture is fixed and passes:
- remove it from the relevant `tier*ExcludedFixtures` map in `internal/runtime/cataloge2e`
- add it to the relevant supported fixture list in that same test file
- do not leave the classification stale after fixing the YAML
